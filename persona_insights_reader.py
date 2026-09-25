"""Persona insights reader — table-driven replacement for the legacy insights_queries.py functions.

Reads pre-computed insight content from `{CATALOG}.{SCHEMA}.gold_persona_insights`
(populated by the daily Genie-orchestrator notebook) instead of computing from raw data.

Drop-in replacement for these `insights_queries` functions:
  - get_insights_for_persona(persona, filters)  → dict with 'insights' and 'actions' lists of HTML strings
  - get_priorities_for_persona(persona, filters) → list of {title, description, icon} dicts
  - PRESET_QUESTION_MAPPINGS-equivalent reads via get_bottom_chips_for_page() and get_cached_payload()

Compatibility note: filters parameter is accepted but ignored — orchestrator runs unfiltered.
Filter-aware Insights are a v2 feature (would require a per-filter-cube run of the orchestrator).
"""

from __future__ import annotations

import os
import json
import logging
from typing import Any, Optional

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# Catalog/schema come from env so prod and dev resolve correctly.
# The full schema string can be either the form "main.cfo_proserv_dev" (matches CFO_SCHEMA)
# or split across CFO_CATALOG + CFO_SCHEMA_NAME.
def _split_schema() -> tuple[str, str]:
    full = os.environ.get("CFO_SCHEMA", "main.cfo_proserv")
    if "." in full:
        cat, sch = full.split(".", 1)
        return cat, sch
    return os.environ.get("CFO_CATALOG", "main"), full

CATALOG, SCHEMA = _split_schema()
TABLE = f"{CATALOG}.{SCHEMA}.gold_persona_insights"
logger.info(f"[persona_insights_reader] CONFIG CATALOG={CATALOG} SCHEMA={SCHEMA} TABLE={TABLE} CFO_SCHEMA_env={os.environ.get('CFO_SCHEMA', '<unset>')}")

# Persona keys are ROLES (finance / admin / hr) — customer-environment friendly.
# Sarah / Priya / Michael are demo display names only; the role is what's stored.
# Frontend may pass either form — accept both for resilience.
_PERSONA_MAP = {
    "finance": "finance", "sarah": "finance",
    "admin": "admin", "priya": "admin",
    "hr": "hr", "michael": "hr",
}

# Page name normalization for page-scoped chips
_PAGE_MAP = {
    "finance": "finance",
    "finance-deepdive": "finance",
    "admin": "admin",
    "admin-deepdive": "admin",
}

SHARED_PERSONA_KEY = "_shared"

_workspace_client = None


def _get_workspace_client():
    """Lazy-init WorkspaceClient. Inside Databricks Apps it picks up the
    runtime OAuth token automatically — no interactive auth needed."""
    global _workspace_client
    if _workspace_client is None:
        _workspace_client = WorkspaceClient()
        logger.info("[persona_insights_reader] WorkspaceClient initialized")
    return _workspace_client


def _quote_param(v) -> str:
    """Inline-quote a parameter value for SQL. Callers pass internal keys (persona,
    slot_type, slot_id, question_text from cached rows), never raw user input.

    IMPORTANT — escape with backslash, NOT doubled apostrophe. Databricks SQL
    treats `'foo''bar'` as adjacent-string-literal concatenation `'foo' || 'bar'`
    (= `'foobar'` with the apostrophe stripped), NOT as the ANSI-SQL escape for
    a literal apostrophe. So `_quote_param("Chicago's")` previously produced
    `'Chicago''s'` which Databricks parsed as `'Chicagos'`. Every cache lookup
    where the question_text contained an apostrophe (e.g., "Chicago's actual
    costs", "Munich's gross margin") silently missed cache and fell through to
    live computation. Use `\\'` to backslash-escape — that gives the apostrophe
    back as a literal character.
    """
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _execute_select(query_template: str, parameters=None, columns=None):
    """Run a SELECT, return list of row dicts. `columns` MUST be provided in the order
    the SELECT projects them — we don't rely on manifest.schema since it's not
    always populated.
    """
    parameters = parameters or []
    columns = columns or []
    query = query_template
    for p in parameters:
        query = query.replace("?", _quote_param(p), 1)

    try:
        client = _get_workspace_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "")
        if not warehouse_id:
            raise RuntimeError("SQL_WAREHOUSE_ID env var not set — bundle's apps.config.env should populate it at deploy time.")
        # Anchor wall-clock CURRENT_DATE() to the frozen dataset's as-of date.
        if "CURRENT_DATE()" in query:
            import demo_anchor
            query = demo_anchor.anchor(query, demo_anchor.as_of_via_statement(client, warehouse_id, f"{CATALOG}.{SCHEMA}"))
        compact_query = " ".join(query.split())
        logger.info(f"[persona_insights_reader] EXEC warehouse={warehouse_id} table={TABLE} query={compact_query[:300]}")
        res = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="30s",
        )
        status = (res.status.state if res.status else None)
        n_rows = len(res.result.data_array) if (res.result and res.result.data_array) else 0
        logger.info(f"[persona_insights_reader] RESULT status={status} n_rows={n_rows}")
        if n_rows == 0:
            return []
        return [dict(zip(columns, row)) for row in res.result.data_array]
    except Exception as e:
        logger.error(f"[persona_insights_reader] SQL execute FAILED: {type(e).__name__}: {e}")
        logger.error(f"  query: {query[:300]}")
        return []


# ---------------------------------------------------------------------------
# Public API — drop-in replacements for legacy insights_queries.py functions
# ---------------------------------------------------------------------------

def _resolve_filter_slice(filters: Any) -> tuple[str, str]:
    """Resolve user's filter selection to a single (filter_axis, filter_value) slice
    that exists in gold_persona_insights. Single-axis only — if multiple filters are
    set, picks the FIRST non-default one (caller is expected to UI-enforce single-axis).
    Falls back to ('firmwide', 'all') if nothing meaningful is set.

    `filters` dict expected keys: region, location, practice_area, industry, customer.
    """
    if not filters or not isinstance(filters, dict):
        logger.info(f"[FILTER-RESOLVE] filters is empty or not dict ({type(filters).__name__}) → firmwide/all")
        return ("firmwide", "all")
    # Priority order matters: if multiple are set, we pick the first non-default
    for axis in ("region", "location", "practice_area", "industry"):
        v = filters.get(axis)
        if v and v not in ("All", "", None, "all"):
            logger.info(f"[FILTER-RESOLVE] resolved to axis={axis!r} value={v!r} (from filters={filters!r})")
            return (axis, v)
    logger.info(f"[FILTER-RESOLVE] no non-default filter set (filters={filters!r}) → firmwide/all")
    return ("firmwide", "all")


def get_insights_for_persona(persona: str, filters: Any = None) -> dict:
    """Return {'insights': [<html>...], 'insights_with_clickthrough': [{html, question}, ...], 'actions': []}.

    Filter handling:
      - Resolves `filters` dict to a single (filter_axis, filter_value) slice via
        `_resolve_filter_slice`.
      - Looks up `gold_persona_insights` rows tagged with that slice.
      - Falls back to ('firmwide', 'all') if the requested slice has no rows
        (e.g., orchestrator hasn't generated that combo yet).
    """
    p = _PERSONA_MAP.get((persona or "").lower())
    if not p:
        logger.warning(f"Unknown persona: {persona}")
        return {"insights": [], "insights_with_clickthrough": [], "actions": []}

    filter_axis, filter_value = _resolve_filter_slice(filters)

    def _fetch(slot_type: str, axis: str, val: str) -> list[dict]:
        return _execute_select(
            f"""
            SELECT slot_id, headline, value, comparison, trend, trend_direction, status_color, narrative
            FROM {TABLE}
            WHERE persona = ? AND filter_axis = ? AND filter_value = ? AND slot_type = ?
            ORDER BY slot_id
            """,
            [p, axis, val, slot_type],
            columns=["slot_id", "headline", "value", "comparison", "trend", "trend_direction", "status_color", "narrative"],
        )

    logger.info(f"[GET-INSIGHTS-DB] querying for persona={p!r} axis={filter_axis!r} value={filter_value!r}")
    insight_rows = _fetch("insight", filter_axis, filter_value)
    logger.info(f"[GET-INSIGHTS-DB] returned {len(insight_rows)} rows for (axis={filter_axis!r}, value={filter_value!r})")
    # Fallback to firmwide if scoped slice has no rows yet (orchestrator may not have generated it)
    if not insight_rows and filter_axis != "firmwide":
        logger.warning(f"[GET-INSIGHTS-DB] No rows for ({p}, {filter_axis}={filter_value}) — falling back to firmwide")
        filter_axis, filter_value = "firmwide", "all"
        insight_rows = _fetch("insight", filter_axis, filter_value)
        logger.info(f"[GET-INSIGHTS-DB] firmwide fallback returned {len(insight_rows)} rows")

    # Insight tiles are read-only displays — no deep-dive question/payload needed.
    insights_html = []
    insights_with_clickthrough = []
    for r in insight_rows:
        html = _render_insight_html(r)
        insights_html.append(html)
        insights_with_clickthrough.append({
            "html": html,
            "question_text": "",
            "status_color": r.get("status_color") or "yellow",
        })

    return {
        "insights": insights_html,
        "insights_with_clickthrough": insights_with_clickthrough,
        "actions": [],
    }


def get_priorities_for_persona(persona: str, filters: Any = None) -> list[dict]:
    """Return list of {title, description, status_color, target_entity_type, target_entity_value, icon} dicts.

    Filter-aware: pulls the action_area rows for the (persona, filter_axis, filter_value)
    slice. Falls back to firmwide if no rows for the requested slice.

    Returns an EMPTY list if even the firmwide slice has no rows — the template
    interprets empty as "loading" and shows a single 'Generating Action Areas...' tile,
    matching the Key Insights loading state.
    """
    p = _PERSONA_MAP.get((persona or "").lower())
    if not p:
        return []

    filter_axis, filter_value = _resolve_filter_slice(filters)

    def _fetch(axis: str, val: str) -> list[dict]:
        return _execute_select(
            f"""
            SELECT slot_id, headline, narrative, status_color, target_entity_type, target_entity_value, question_text
            FROM {TABLE}
            WHERE persona = ? AND filter_axis = ? AND filter_value = ? AND slot_type = 'action_area'
            ORDER BY slot_id
            """,
            [p, axis, val],
            columns=["slot_id", "headline", "narrative", "status_color", "target_entity_type", "target_entity_value", "question_text"],
        )

    rows = _fetch(filter_axis, filter_value)
    if not rows and filter_axis != "firmwide":
        rows = _fetch("firmwide", "all")

    if not rows:
        return []

    priorities = []
    for r in rows:
        priorities.append({
            "title": r["headline"] or "",
            "description": r["narrative"] or "",
            "status_color": r["status_color"] or "yellow",
            "target_entity_type": r["target_entity_type"],
            "target_entity_value": r["target_entity_value"],
            "question_text": r["question_text"] or "",  # action_deepdive click-through question
            "icon": "PRIORITY",
        })
    return priorities[:3]


def get_bottom_chips_for_page(persona: str, page: Optional[str] = None) -> list[dict]:
    """Return the bottom-of-page demo questions for the given persona+page combination.

    page=None or page='executive' → persona-scoped chips (slot_type='bottom_chip')
    page='finance' or 'admin'    → page-scoped chips (slot_type='bottom_chip_<page>', persona='_shared')

    Returns list of {slot_id, question_text, narrative}.
    """
    page_norm = _PAGE_MAP.get((page or "").lower()) if page else None
    if page_norm:
        rows = _execute_select(
            f"""
            SELECT slot_id, question_text, narrative
            FROM {TABLE}
            WHERE persona = ? AND slot_type = ?
            ORDER BY slot_id
            """,
            [SHARED_PERSONA_KEY, f"bottom_chip_{page_norm}"],
            columns=["slot_id", "question_text", "narrative"],
        )
    else:
        p = _PERSONA_MAP.get((persona or "").lower())
        if not p:
            return []
        rows = _execute_select(
            f"""
            SELECT slot_id, question_text, narrative
            FROM {TABLE}
            WHERE persona = ? AND slot_type = 'bottom_chip'
            ORDER BY slot_id
            """,
            [p],
            columns=["slot_id", "question_text", "narrative"],
        )
    return rows


def get_cached_payload(persona: str, slot_type: str, slot_id: int) -> Optional[dict]:
    """Look up a cached agent payload by persona+slot — used by chip-click handlers.

    Returns parsed JSON dict (thoughts, sql_steps, final_narrative, suggested_questions)
    or None if no cached row exists.
    """
    rows = _execute_select(
        f"""
        SELECT cached_agent_payload
        FROM {TABLE}
        WHERE persona = ? AND slot_type = ? AND slot_id = ?
        LIMIT 1
        """,
        [persona, slot_type, slot_id],
        columns=["cached_agent_payload"],
    )
    if not rows or not rows[0].get("cached_agent_payload"):
        return None
    try:
        return json.loads(rows[0]["cached_agent_payload"])
    except json.JSONDecodeError:
        return None


def get_cached_payload_by_question(question_text: str) -> Optional[dict]:
    """Look up a cached payload by exact question text — useful when the app only knows
    the chip text the user clicked, not the slot_id."""
    rows = _execute_select(
        f"""
        SELECT cached_agent_payload
        FROM {TABLE}
        WHERE question_text = ?
          AND cached_agent_payload IS NOT NULL
        ORDER BY last_refreshed DESC
        LIMIT 1
        """,
        [question_text],
        columns=["cached_agent_payload"],
    )
    if not rows or not rows[0].get("cached_agent_payload"):
        return None
    try:
        return json.loads(rows[0]["cached_agent_payload"])
    except json.JSONDecodeError:
        return None


def write_cached_payload_for_question(question_text: str, payload: dict) -> bool:
    """Persist a Genie + sub-query payload back to gold_persona_insights for any
    row whose `question_text` matches exactly. Used by the live-chat path as a
    write-through cache: the FIRST click of a chip pays the full Opus+Genie+Haiku
    compute cost (~15-30s) and writes the resulting sub_question_results blob
    here; subsequent clicks of the same chip read it back from
    get_cached_payload_by_question() in ~2-3s.

    Updates ALL rows matching the question_text (e.g., a chip exists in both
    bottom_chip and bottom_chip_admin slot_types). Failures are non-fatal —
    we still return the live-composed response; the cache just stays cold.
    """
    if not question_text or not payload:
        return False
    try:
        payload_json = json.dumps(payload, separators=(",", ":"))
    except (TypeError, ValueError) as e:
        logger.warning(f"[CACHE-WRITE] payload not JSON-serializable: {e}")
        return False

    # Escape single quotes for SQL string literal safety. _quote_param wraps in
    # quotes and doubles internal ones; we use it for both the question_text
    # match and the payload JSON body.
    stmt = (
        f"UPDATE {TABLE} "
        f"SET cached_agent_payload = {_quote_param(payload_json)}, "
        f"    last_refreshed = CURRENT_TIMESTAMP() "
        f"WHERE question_text = {_quote_param(question_text)}"
    )

    try:
        client = _get_workspace_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "")
        if not warehouse_id:
            logger.warning("[CACHE-WRITE] no SQL_WAREHOUSE_ID set, skipping")
            return False
        res = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=stmt,
            wait_timeout="30s",
        )
        status = (res.status.state if res.status else None)
        logger.info(f"[CACHE-WRITE] UPDATE status={status} question_preview={question_text[:80]!r}")
        return True
    except Exception as e:
        logger.warning(f"[CACHE-WRITE] failed (non-fatal): {type(e).__name__}: {e}")
        return False


# ---------------------------------------------------------------------------
# HTML rendering — matches insights_queries.py format_finance_insights output style
# ---------------------------------------------------------------------------

def _render_insight_html(row: dict) -> str:
    """Render an insight row as an HTML string matching the existing template contract.

    Format: <span class="status-{color}"><strong>{headline}:</strong> {value}
            <span class='{trend_class}'>({comparison})</span>. {narrative}
            <span class='{trend_class}'>{trend}</span></span>

    Color discipline (matches the production app's restraint):
      - headline + value: bold, default text color (NOT colored)
      - comparison: COLORED by status_color (the overall STATE — over-budget is bad
        even if the trend is moving in the right direction; this reflects "is the
        absolute level healthy?")
      - narrative: plain default color (most of the body text stays neutral)
      - trend: COLORED by trend_direction (the MOTION — is this metric moving in
        the favorable direction relative to its prior period? Independent of state)

    Why two different signals: comparison and trend can disagree. Example:
    Expenses +9% over budget AND -4.5% MoM → comparison should look bad (yellow
    border on the card; comparison text yellow), trend should look good (motion
    is favorable, green). Coloring both with the same logic (the old behavior)
    made an over-budget number look green just because MoM was improving — wrong
    semantic signal.

    The wrapping <span class="status-{color}"> applies a colored left-border via CSS so
    the tile still has a status accent, but the inline text reads cleanly.
    """
    headline = row.get("headline") or ""
    value = row.get("value") or ""
    comparison = row.get("comparison") or ""
    narrative = row.get("narrative") or ""
    trend = row.get("trend") or ""
    status = (row.get("status_color") or "yellow").lower()
    if status not in ("red", "yellow", "green"):
        status = "yellow"

    # trend_direction = SEMANTIC motion (improving/deteriorating/flat), NOT the
    # arrow direction. DSO ↑ = deteriorating; DPO ↑ = improving; Expense ↓ =
    # improving. Claude emits this per insight in the JSON.
    direction = (row.get("trend_direction") or "flat").lower()
    if direction not in ("improving", "deteriorating", "flat"):
        direction = "flat"
    trend_class = {
        "improving": "positive-change",
        "deteriorating": "negative-change",
        "flat": "neutral",
    }[direction]

    # Comparison reflects the absolute-state semantic via status_color.
    #   green → positive-change (green text)
    #   yellow → warning-change (amber text — reinforces the watch state)
    #   red → negative-change (red text)
    # 2026-05-21 — yellow used to render gray (None class) but a CFO reading
    # "(0.81% above budgeted forecast)" in default gray reads as "no signal,"
    # which is wrong: marginal over-budget is the WATCH state. Now amber.
    comparison_class = {
        "green": "positive-change",
        "yellow": "warning-change",
        "red": "negative-change",
    }[status]

    # Headline + value stay in default text color (just bold). Comparison +
    # trend each get their own semantic-aware color.
    parts = [f"<strong>{headline}:</strong>"]
    if value:
        parts.append(f"<strong>{value}</strong>")
    if comparison:
        if comparison_class:
            parts.append(f"<span class='{comparison_class}'>({comparison})</span>")
        else:
            # No special color — render with default body text color, single set of parens.
            parts.append(f"({comparison})")
    parts.append(".")
    if narrative:
        parts.append(narrative)
    if trend:
        parts.append(f"<span class='{trend_class}'>{trend}</span>")
    body = " ".join(parts)
    return f"<span class=\"status-{status}\" data-status=\"{status}\">{body}</span>"


# _default_priorities was removed — empty action_areas now means "still loading"
# rather than masking it with generic placeholders. Template handles the empty
# case with a single "Generating Action Areas..." tile that matches the Key
# Insights loading state.


# Backward-compat alias
def get_all_insights(persona: str, filters: Any = None) -> dict:
    return get_insights_for_persona(persona, filters)


# ---------------------------------------------------------------------------
# Write-through filter cache for /api/insights-live
# ---------------------------------------------------------------------------
# Convention: multi-axis filter combos are stored with filter_axis='compound'
# and filter_value=<sorted "k=v|k=v" string>. This sidesteps the single-axis
# schema baked into _resolve_filter_slice (which the firmwide / get-insights
# path uses) and keeps the cardinality concern resolved naturally — only
# combos people actually visit get materialized.
#
# Orchestrator invalidates this cache by deleting all compound rows on every
# rerun (see write_firmwide_rows in generate_insights.py).

def _execute_dml(statement: str) -> bool:
    """Run a single DML statement (INSERT / DELETE / UPDATE). Returns True on
    success, False on error. Mirrors _execute_select's logging."""
    try:
        client = _get_workspace_client()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "")
        if not warehouse_id:
            raise RuntimeError("SQL_WAREHOUSE_ID env var not set")
        compact = " ".join(statement.split())
        logger.info(f"[persona_insights_reader] DML warehouse={warehouse_id} stmt={compact[:300]}")
        res = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=statement,
            wait_timeout="30s",
        )
        status = (res.status.state if res.status else None)
        logger.info(f"[persona_insights_reader] DML RESULT status={status}")
        return True
    except Exception as e:
        logger.error(f"[persona_insights_reader] DML FAILED: {type(e).__name__}: {e}")
        logger.error(f"  statement: {statement[:300]}")
        return False


def load_compound_cache(persona: str, compound_filter_value: str) -> Optional[dict]:
    """Return cached {insights, action_areas} for a persona × compound-filter
    combo, or None if nothing cached.

    `insights` shape mirrors what `compose_for_persona_with_filters` returns
    BEFORE HTML rendering: list of dicts with headline/value/comparison/trend/
    trend_direction/status_color/narrative. Same for `action_areas`.
    """
    p = _PERSONA_MAP.get((persona or "").lower())
    if not p or not compound_filter_value:
        return None

    insight_rows = _execute_select(
        f"""
        SELECT slot_id, headline, value, comparison, trend, trend_direction, status_color, narrative
        FROM {TABLE}
        WHERE persona = ? AND filter_axis = 'compound' AND filter_value = ? AND slot_type = 'insight'
        ORDER BY slot_id
        """,
        [p, compound_filter_value],
        columns=["slot_id", "headline", "value", "comparison", "trend", "trend_direction", "status_color", "narrative"],
    )
    if not insight_rows:
        return None

    action_rows = _execute_select(
        f"""
        SELECT slot_id, headline, narrative, status_color, target_entity_type, target_entity_value, question_text
        FROM {TABLE}
        WHERE persona = ? AND filter_axis = 'compound' AND filter_value = ? AND slot_type = 'action_area'
        ORDER BY slot_id
        """,
        [p, compound_filter_value],
        columns=["slot_id", "headline", "narrative", "status_color", "target_entity_type", "target_entity_value", "question_text"],
    )

    logger.info(f"[compound-cache] HIT persona={p} fv={compound_filter_value!r} insights={len(insight_rows)} actions={len(action_rows)}")
    return {
        "insights": [{k: r.get(k) for k in ("headline", "value", "comparison", "trend", "trend_direction", "status_color", "narrative")} for r in insight_rows],
        "action_areas": [{k: r.get(k) for k in ("headline", "narrative", "status_color", "target_entity_type", "target_entity_value", "question_text")} for r in action_rows],
    }


def write_compound_cache(persona: str, compound_filter_value: str, insights: list, action_areas: list) -> bool:
    """Persist insights + action_areas to gold_persona_insights with
    filter_axis='compound' and filter_value=<compound key>. DELETEs any
    existing rows for the same key first so re-writes are idempotent.

    Returns True if both writes succeeded, False otherwise. Failures are
    non-fatal for the caller — the user still gets the live-composed result;
    they just won't benefit from the cache next time.
    """
    p = _PERSONA_MAP.get((persona or "").lower())
    if not p or not compound_filter_value:
        return False

    # Clear any prior rows for this exact key (idempotent overwrite).
    delete_ok = _execute_dml(
        f"DELETE FROM {TABLE} WHERE persona = {_quote_param(p)} "
        f"AND filter_axis = 'compound' AND filter_value = {_quote_param(compound_filter_value)} "
        f"AND slot_type IN ('insight','action_area')"
    )
    if not delete_ok:
        return False

    def _v(d: dict, k: str) -> str:
        v = d.get(k)
        if v is None:
            return "NULL"
        return _quote_param(str(v))

    rows_sql = []
    for i, ins in enumerate(insights or [], start=1):
        rows_sql.append(
            "("
            f"{_quote_param(p)}, 'insight', {i}, NULL, NULL, "
            f"{_v(ins, 'headline')}, {_v(ins, 'value')}, {_v(ins, 'comparison')}, "
            f"{_v(ins, 'trend')}, {_v(ins, 'trend_direction')}, {_v(ins, 'status_color')}, "
            f"{_v(ins, 'narrative')}, NULL, NULL, NULL, NULL, NULL, "
            f"CURRENT_TIMESTAMP(), NULL, 'compound', {_quote_param(compound_filter_value)}"
            ")"
        )
    for i, a in enumerate(action_areas or [], start=1):
        rows_sql.append(
            "("
            f"{_quote_param(p)}, 'action_area', {i}, NULL, NULL, "
            f"{_v(a, 'headline')}, NULL, NULL, NULL, NULL, {_v(a, 'status_color')}, "
            f"{_v(a, 'narrative')}, {_v(a, 'question_text')}, NULL, NULL, "
            f"{_v(a, 'target_entity_type')}, {_v(a, 'target_entity_value')}, "
            f"CURRENT_TIMESTAMP(), NULL, 'compound', {_quote_param(compound_filter_value)}"
            ")"
        )

    if not rows_sql:
        return True  # nothing to write, but not an error

    insert_sql = (
        f"INSERT INTO {TABLE} ("
        "persona, slot_type, slot_id, parent_slot_id, parent_slot_type, "
        "headline, value, comparison, trend, trend_direction, status_color, "
        "narrative, question_text, routed_subqueries, cached_agent_payload, "
        "target_entity_type, target_entity_value, last_refreshed, fiscal_period_anchor, "
        "filter_axis, filter_value"
        f") VALUES {', '.join(rows_sql)}"
    )
    ok = _execute_dml(insert_sql)
    if ok:
        logger.info(f"[compound-cache] WROTE persona={p} fv={compound_filter_value!r} insights={len(insights or [])} actions={len(action_areas or [])}")
    return ok
