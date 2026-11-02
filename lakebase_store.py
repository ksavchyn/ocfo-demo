"""Lakebase (Postgres) operational serving layer for the CFO app.

Two jobs, both on the AI Cost & Governance tab:

1. READ  - the "Enforce Internal Spend Controls" table is served from a Lakebase
           synced table (cfo_serving.gold_ai_cost_by_cost_center), kept in sync
           from the Unity Catalog gold table. Low-latency point reads for the app
           instead of a SQL warehouse scan.
2. WRITE - every governance action a user takes (Nudge Dept Head, Flag for Margin
           Review, Generate Usage Summary, Set fallbacks) is recorded as an OLTP row
           in cfo_ops.ai_actions, so finance has a durable audit trail of who acted
           on which cost center / engagement and when.

Connection: OAuth token from the Databricks SDK (refreshed before the 1-hour
expiry), psycopg 3, sslmode=require. Env vars, in priority order:
  - Deployed app with a Lakebase resource: PGHOST, PGDATABASE, PGUSER, LAKEBASE_ENDPOINT (auto-injected)
  - Local / explicit: CFO_LAKEBASE_HOST, CFO_LAKEBASE_DATABASE, CFO_LAKEBASE_ENDPOINT (user = current user)
If neither is set, is_configured() is False and the app falls back to the SQL warehouse.
"""
import os
import threading
import time
import uuid

_lock = threading.Lock()
_token = {"value": None, "issued": 0.0}
_TOKEN_TTL_S = 40 * 60  # refresh well before the 1-hour expiry

SERVING_SCHEMA = os.environ.get("CFO_LAKEBASE_SERVING_SCHEMA", "cfo_serving")
OPS_SCHEMA = os.environ.get("CFO_LAKEBASE_OPS_SCHEMA", "cfo_ops")


def _cfg():
    host = os.environ.get("PGHOST") or os.environ.get("CFO_LAKEBASE_HOST")
    endpoint = os.environ.get("LAKEBASE_ENDPOINT") or os.environ.get("CFO_LAKEBASE_ENDPOINT")
    dbname = os.environ.get("PGDATABASE") or os.environ.get("CFO_LAKEBASE_DATABASE") or "databricks_postgres"
    user = os.environ.get("PGUSER") or os.environ.get("CFO_LAKEBASE_USER")
    return host, endpoint, dbname, user


def is_configured() -> bool:
    host, endpoint, _, _ = _cfg()
    return bool(host and endpoint)


def _workspace_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def _get_token(w, endpoint):
    with _lock:
        if _token["value"] and time.time() - _token["issued"] < _TOKEN_TTL_S:
            return _token["value"]
        _token["value"] = w.postgres.generate_database_credential(endpoint=endpoint).token
        _token["issued"] = time.time()
        return _token["value"]


def connect():
    import psycopg
    host, endpoint, dbname, user = _cfg()
    w = _workspace_client()
    if not user:
        user = w.current_user.me().user_name
    return psycopg.connect(host=host, port=int(os.environ.get("PGPORT", "5432")), dbname=dbname,
                           user=user, password=_get_token(w, endpoint), sslmode="require",
                           connect_timeout=10)


def ensure_ops_schema():
    """Create the OLTP actions table if missing (idempotent)."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.ai_actions (
                action_id    UUID PRIMARY KEY,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                action_type  TEXT NOT NULL,          -- nudge_dept_head | flag_margin_review | usage_summary | set_fallbacks
                target_type  TEXT NOT NULL,          -- cost_center | engagement
                target_id    TEXT NOT NULL,          -- cost center code or project name
                client_name  TEXT,
                amount_usd   NUMERIC(14,2),          -- attributed AI spend at the time of the action
                status       TEXT NOT NULL DEFAULT 'open',
                actor        TEXT,
                note         TEXT
            )""")
        cur.execute(f"CREATE INDEX IF NOT EXISTS ai_actions_target_idx ON {OPS_SCHEMA}.ai_actions (target_type, target_id)")
        conn.commit()


def read_cost_centers(limit=25):
    """Spend Controls rows from the Lakebase synced table (same shape as the warehouse query)."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT cost_center, cost_center_department, policy_name,
                   ROUND(ttm_ai_cost_usd::numeric, 0), ROUND(annual_budget_usd::numeric, 0),
                   budget_utilization_pct, budget_status
            FROM {SERVING_SCHEMA}.gold_ai_cost_by_cost_center
            WHERE annual_budget_usd > 0
            ORDER BY budget_utilization_pct DESC NULLS LAST
            LIMIT %s""", (limit,))
        return cur.fetchall()


def log_action(action_type, target_type, target_id, client_name=None, amount_usd=None, actor=None, note=None):
    action_id = uuid.uuid4()
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {OPS_SCHEMA}.ai_actions
                (action_id, action_type, target_type, target_id, client_name, amount_usd, actor, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (action_id, action_type, target_type, target_id, client_name, amount_usd, actor, note))
        conn.commit()
    return str(action_id)


def recent_actions(limit=50):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT action_id::text, created_at, action_type, target_type, target_id,
                   client_name, amount_usd, status, actor, note
            FROM {OPS_SCHEMA}.ai_actions ORDER BY created_at DESC LIMIT %s""", (limit,))
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
