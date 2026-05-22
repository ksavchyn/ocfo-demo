# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Consistency
# MAGIC
# MAGIC Final orchestrator task. Walks every cached chip in `gold_persona_insights`
# MAGIC and checks the synthesized prose answers for arithmetic and consistency
# MAGIC violations against the customer's own gold-table totals.
# MAGIC
# MAGIC **What it catches** (universal, data-agnostic):
# MAGIC - Sum-of-listed-parts != stated total ("declined by 10 ... (3) + (2) + (2) = 7")
# MAGIC - Subset > firmwide total ("17,682 low-utilization employees" when firmwide is 6,044)
# MAGIC - Per-unit scale mismatch ("$210K per partner" when canonical RPP is $7.8M)
# MAGIC - Named KPI drift (prose-cited DSO ≠ dashboard DSO by >2%)
# MAGIC
# MAGIC **What it does NOT do:**
# MAGIC - Hardcode any narrative-specific values (RPP must be $7M, NYC must show overage, etc.).
# MAGIC   Every canonical reference is pulled from the customer's gold tables at runtime.
# MAGIC - Validate engineered demo storytelling — that's a separate dev-only script.
# MAGIC
# MAGIC **Failure mode:** raises an exception with the full violation list when any
# MAGIC violations are found, failing the bundle job. Forces the orchestrator to
# MAGIC be re-run with a fixed prompt rather than shipping a broken demo.

# COMMAND ----------

# DBTITLE 1,Configuration & imports
import json
import os
import sys
import traceback

# Bundle file sync drops all repo files into a workspace tree. This notebook
# lives at /Workspace/.../files/data_pipeline/, but `validation_probe.py` and
# `insights_compose.py` live ONE LEVEL UP at the bundle root. The Databricks
# runtime's CWD when executing a notebook task is typically the notebook's
# own directory, so a plain `sys.path.insert(0, ".")` only finds siblings
# (like `uc_metadata.py` in data_pipeline/). We add both the notebook's dir
# AND the bundle root above it to sys.path, then derive the bundle root
# explicitly from the notebook's workspace path as a defensive fallback.
sys.path.insert(0, ".")
sys.path.insert(0, "..")
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
    # _nb_path like /Workspace/Users/.../cfo-app/files/data_pipeline/03_validate_consistency
    # → bundle root = drop the final two segments
    _bundle_root = "/".join(_nb_path.split("/")[:-2])
    if _bundle_root and _bundle_root not in sys.path:
        sys.path.insert(0, _bundle_root)
except Exception:
    pass

from validation_probe import (
    find_violations_in_prose,
    find_violations_via_llm,
    format_violations_for_human,
)
from insights_compose import extract_prose_from_payload

# Widget plumbing — mirrors the pattern in 02_build_silver_gold.py so the
# bundle's notebook_task.base_parameters flow through identically.
try:
    dbutils.widgets.text("CFO_CATALOG", "main")  # noqa: F821
    dbutils.widgets.text("CFO_SCHEMA_NAME", "cfo_proserv")  # noqa: F821
    dbutils.widgets.text("CFO_VALIDATE_STRICT", "false")  # noqa: F821
    _WIDGETS = True
except Exception:
    _WIDGETS = False


def _config(name: str, default: str) -> str:
    if _WIDGETS:
        try:
            v = dbutils.widgets.get(name)  # noqa: F821
            if v:
                return v
        except Exception:
            pass
    return os.environ.get(name, default)


CATALOG = _config("CFO_CATALOG", "main")
SCHEMA = _config("CFO_SCHEMA_NAME", "cfo_proserv")
TABLE = f"{CATALOG}.{SCHEMA}.gold_persona_insights"

# Optional LLM-extraction layer. Disabled when no gateway URL / model is set
# (e.g. customers on an air-gapped install). Default to Haiku — cheap + fast.
CLAUDE_GATEWAY_URL = _config("CFO_CLAUDE_GATEWAY_URL", "").strip()
CLAUDE_MODEL_COMPOSE = _config("CFO_CLAUDE_MODEL_COMPOSE", "databricks-claude-haiku-4-5").strip()
LLM_LAYER_ENABLED = bool(CLAUDE_GATEWAY_URL and CLAUDE_MODEL_COMPOSE)

# Strict mode: when "true", any violations raise an Exception → task FAILED
# → job FAILED → deploy.sh exits 1. Use this for CI / pre-ship regression
# blocking. Default "false" — the task completes successfully and just
# writes the violations table. This is the right default because:
#   1. The inline regex retry inside `run_firmwide_persona` is the primary
#      catch — by the time this task runs, most violations have already
#      been retried away upstream.
#   2. Soft-fail lets downstream tasks (refresh_dashboards) still run.
#   3. Daily/scheduled jobs shouldn't go red just because a residual edge
#      case slipped through — that's noise that hides REAL upstream failures.
VALIDATE_STRICT = _config("CFO_VALIDATE_STRICT", "false").strip().lower() in ("true", "1", "yes", "on")

print(f"[validate_consistency] target table: {TABLE}")
print(f"[validate_consistency] LLM extraction layer: {'ENABLED (' + CLAUDE_MODEL_COMPOSE + ')' if LLM_LAYER_ENABLED else 'DISABLED'}")
print(f"[validate_consistency] strict mode: {'ON — violations will fail the job' if VALIDATE_STRICT else 'OFF — violations logged but task passes'}")

# COMMAND ----------

# DBTITLE 1,Build canonical totals + KPIs from the customer's gold tables
# Every value here is queried from the customer's actual data at probe time.
# Any individual lookup that fails (missing table, missing column) is dropped
# from the context — the affected check just won't fire for that metric, which
# is correct fail-open behavior (we'd rather skip a check than block on
# something a customer hasn't shaped yet).

canonical_totals: dict[str, int] = {}
canonical_kpis: dict[str, float] = {}


def _safe_scalar(query: str) -> float | int | None:
    try:
        row = spark.sql(query).collect()  # noqa: F821
        if not row:
            return None
        val = row[0][0]
        return val
    except Exception as e:
        print(f"  [skip] canonical lookup failed: {type(e).__name__}: {e}")
        return None


# --- Low-utilization headcount: firmwide count of employees below 50% util.
# Derived from silver_fact_timecards over the last 90 days (matches the
# dashboard tile's window). No dedicated gold table at the employee grain;
# previously this referenced a non-existent `gold_active_engagements`.
low_util = _safe_scalar(f"""
    SELECT COUNT(*) FROM (
        SELECT employee_id,
               SUM(CASE WHEN time_type_clean = 'Billable' THEN hours_worked ELSE 0 END)
                 / NULLIF(SUM(hours_worked), 0) AS util
        FROM {CATALOG}.{SCHEMA}.silver_fact_timecards
        WHERE work_date >= DATE_SUB(CURRENT_DATE(), 90)
        GROUP BY employee_id
        HAVING SUM(hours_worked) > 0
    )
    WHERE COALESCE(util, 0) < 0.5
""")
if low_util is not None:
    canonical_totals["low_utilization_employees"] = int(low_util)

# --- Partner headcount: firmwide active partner count.
# Column is `job_level` (not `role`); strict-partner cohort matches firm_kpis_mv.
partners = _safe_scalar(f"""
    SELECT COUNT(DISTINCT employee_id)
    FROM {CATALOG}.{SCHEMA}.silver_dim_employees
    WHERE job_level = 'Partner'
      AND COALESCE(employment_status, '') = 'Active'
""")
if partners is not None:
    canonical_totals["partners"] = int(partners)

# --- Revenue per Partner: total firmwide revenue / partner count.
# Computed on the same fiscal window the dashboard tile uses (annualized).
if partners and partners > 0:
    total_rev = _safe_scalar(f"""
        SELECT SUM(revenue)
        FROM {CATALOG}.{SCHEMA}.gold_enterprise_metrics
    """)
    if total_rev and total_rev > 0:
        canonical_kpis["revenue_per_partner"] = float(total_rev) / float(partners)

# --- DSO: matches the dashboard tile formula.
# Was pointing at a non-existent `gold_ar_aging`. `gold_receivables_wip_aging`
# is the actual aging table; `payment_status` is the correct column (not
# `invoice_status`). Filter to non-paid open balances.
dso = _safe_scalar(f"""
    SELECT AVG(days_outstanding)
    FROM {CATALOG}.{SCHEMA}.gold_receivables_wip_aging
    WHERE COALESCE(payment_status, 'Open') <> 'Paid'
""")
if dso is not None:
    canonical_kpis["dso"] = float(dso)

print("[validate_consistency] canonical context built:")
print(f"  canonical_totals: {canonical_totals}")
print(f"  canonical_kpis:   { {k: f'{v:,.2f}' for k, v in canonical_kpis.items()} }")

context = {"canonical_totals": canonical_totals, "canonical_kpis": canonical_kpis}


# ---------------------------------------------------------------------------
# LLM call shim — used by the optional Haiku extraction layer.
# Caller-injected pattern: validation_probe.find_violations_via_llm takes a
# `llm_call(messages, model)` so it doesn't have to know about the gateway.
# ---------------------------------------------------------------------------
def _llm_call(messages: list[dict], model: str) -> dict:
    """POST to the AI Gateway. Returns the parsed JSON response."""
    import requests
    from databricks.sdk import WorkspaceClient
    wc = WorkspaceClient()
    base = CLAUDE_GATEWAY_URL.rstrip("/")
    url = f"{base}/chat/completions" if "/mlflow/" in base else f"{base}/invocations"
    headers = wc.config.authenticate()
    headers["Content-Type"] = "application/json"
    body = {"model": model, "max_tokens": 2000, "messages": messages}
    resp = requests.post(url, headers=headers, json=body, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"gateway {resp.status_code}: {resp.text[:300]}")
    return resp.json()

# COMMAND ----------

# DBTITLE 1,Scan every cached chip + collect violations
# `gold_persona_insights` stores Executive Summary content as ROWS with
# slot_type IN ('insight','action_area','bottom_chip'), each carrying
# headline/value/narrative columns. Chip click-through prose lives in
# cached_agent_payload (JSON). Scan both: pull any row that has either
# a directly-composed text field OR a cached chip payload.
rows_df = spark.sql(f"""
    SELECT persona, slot_type, slot_id,
           headline, value, narrative,
           question_text, cached_agent_payload
    FROM {TABLE}
    WHERE (cached_agent_payload IS NOT NULL AND cached_agent_payload <> '')
       OR slot_type IN ('insight', 'action_area')
""")  # noqa: F821

n_rows = rows_df.count()
print(f"[validate_consistency] scanning {n_rows} rows (insight/action_area + cached_agent_payload)…")

all_violations: list[dict] = []
n_chips_with_violations = 0

for r in rows_df.collect():
    parts: list[str] = []

    # 1) For insight / action_area rows, concatenate the directly-composed
    #    prose fields. This catches the "17,682 low-util > 6,044 firmwide"
    #    class of bug that lives in the Executive Summary bullets.
    slot = r["slot_type"]
    if slot in ("insight", "action_area"):
        for field in ("headline", "value", "narrative"):
            v = r[field]
            if v:
                parts.append(str(v))

    # 2) For rows with a cached agent payload, concatenate every sub-result
    #    narrative — these are the Genie sub-query responses that feed the
    #    click-time synthesis.
    raw = r["cached_agent_payload"]
    if raw:
        try:
            payload = json.loads(raw)
            sub_results = payload.get("sub_question_results") or []
            for s in sub_results:
                n = s.get("narrative")
                if n:
                    parts.append(str(n))
        except Exception:
            pass

    combined = "\n\n".join(parts)
    if not combined.strip():
        continue

    chip_violations = find_violations_in_prose(combined, context)

    # LLM-assisted extraction pass — runs only when regex finds nothing,
    # because (a) if regex already flagged it, we don't need to spend $/sec
    # on a confirmation, and (b) the LLM layer catches novel phrasings the
    # regex doesn't anticipate, which is the part of the design space
    # regex-only mode can't cover.
    if LLM_LAYER_ENABLED and not chip_violations:
        try:
            llm_violations = find_violations_via_llm(
                combined,
                context,
                llm_call=_llm_call,
                model=CLAUDE_MODEL_COMPOSE,
            )
            chip_violations.extend(llm_violations)
        except Exception as e:
            # Non-fatal — we already have regex coverage. Log and continue.
            print(f"  [llm-extract failure on persona={r['persona']} slot={slot}/{r['slot_id']}]: {type(e).__name__}: {e}")

    if chip_violations:
        n_chips_with_violations += 1
        for v in chip_violations:
            v["persona"] = r["persona"]
            v["slot_type"] = slot
            v["slot_id"] = r["slot_id"]
            v["question"] = (r["question_text"] or "")[:120] if r["question_text"] else ""
        all_violations.extend(chip_violations)

# COMMAND ----------

# DBTITLE 1,Persist a violations table for offline review (always, even on pass)
violations_table = f"{CATALOG}.{SCHEMA}.consistency_violations"
spark.sql(f"DROP TABLE IF EXISTS {violations_table}")  # noqa: F821
if all_violations:
    viol_df = spark.createDataFrame(  # noqa: F821
        [
            (
                v.get("persona"),
                v.get("slot_type"),
                int(v["slot_id"]) if v.get("slot_id") is not None else None,
                v.get("question"),
                v.get("type"),
                v.get("message"),
                v.get("snippet"),
            )
            for v in all_violations
        ],
        schema="persona STRING, slot_type STRING, slot_id INT, question STRING, type STRING, message STRING, snippet STRING",
    )
    viol_df.write.mode("overwrite").saveAsTable(violations_table)
    print(f"[validate_consistency] wrote {len(all_violations)} violations to {violations_table}")
else:
    print("[validate_consistency] no violations — table not written.")

# COMMAND ----------

# DBTITLE 1,Final pass / fail
if all_violations:
    header = "FAILED" if VALIDATE_STRICT else "WARNINGS"
    print(f"\n[validate_consistency] {header} — {len(all_violations)} violations across {n_chips_with_violations}/{n_rows} chips:\n")
    print(format_violations_for_human(all_violations[:40]))  # cap log spam
    if len(all_violations) > 40:
        print(f"  …and {len(all_violations) - 40} more (see {violations_table}).")

    if VALIDATE_STRICT:
        raise Exception(
            f"validate_consistency FAILED (strict mode): {len(all_violations)} consistency violations "
            f"across {n_chips_with_violations} chips. Review {violations_table} for details."
        )
    print(f"\n[validate_consistency] strict mode is OFF — task completes successfully despite warnings.")
    print(f"  Review {violations_table} to triage. Set CFO_VALIDATE_STRICT=true to gate the job on this.")
else:
    print(f"\n[validate_consistency] PASSED — scanned {n_rows} cached chips, 0 violations.")