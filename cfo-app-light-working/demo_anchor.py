"""Single source of truth for the demo's "as of" date.

The synthetic dataset is a FROZEN snapshot — the generator pins it to a fixed
ANCHOR_DATE (see data_pipeline/01_generate_bronze_data.py) so the engineered
narrative is byte-reproducible. Every time-relative query must therefore anchor
to the DATA's own latest date, NOT wall-clock CURRENT_DATE(). If queries use
CURRENT_DATE(), then once real calendar time passes the frozen anchor they ask
for months the data only partially contains (e.g. a half-populated "current"
month), and revenue/MoM/expense tiles silently collapse to implausible numbers.

Design:
  * `as_of_via_statement` / `as_of_via_spark` compute the as-of date ONCE per
    process from the data (MAX(work_date)), memoized — so there is no hardcoded
    date to keep in sync with the generator. Env CFO_AS_OF_DATE overrides;
    final fallback matches the shipped ANCHOR_DATE.
  * `anchor` rewrites CURRENT_DATE() -> a date LITERAL (not a subquery), so
    individual queries cost exactly what they did before. NOTE: this targets
    CURRENT_DATE() only — CURRENT_TIMESTAMP() (cache-write audit times) is left
    alone on purpose.
"""

from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)

# Matches data_pipeline/01_generate_bronze_data.py ANCHOR_DATE. Only used if the
# data-derived lookup AND the CFO_AS_OF_DATE env var are both unavailable.
_FALLBACK_AS_OF = "2026-05-15"

_cache: dict = {}


def _normalize(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s[:10] if len(s) >= 10 else (s or None)


def _finalize(val, key) -> str:
    val = val or os.environ.get("CFO_AS_OF_DATE") or _FALLBACK_AS_OF
    _cache[key] = val
    logger.info(f"[demo_anchor] as_of resolved to {val} ({key})")
    return val


def as_of_via_statement(client, warehouse_id: str, schema_fqn: str) -> str:
    """as-of date via Statement Execution API (Databricks Apps path)."""
    key = ("stmt", schema_fqn)
    if key in _cache:
        return _cache[key]
    val = None
    try:
        r = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SELECT CAST(MAX(work_date) AS STRING) FROM {schema_fqn}.silver_fact_timecards",
            wait_timeout="30s",
        )
        if r.result and r.result.data_array and r.result.data_array[0]:
            val = _normalize(r.result.data_array[0][0])
    except Exception as e:
        logger.warning(f"[demo_anchor] as-of statement lookup failed; using fallback. {e}")
    return _finalize(val, key)


def as_of_via_spark(spark, schema_fqn: str) -> str:
    """as-of date via Spark (notebook / pipeline path)."""
    key = ("spark", schema_fqn)
    if key in _cache:
        return _cache[key]
    val = None
    try:
        rows = spark.sql(
            f"SELECT CAST(MAX(work_date) AS STRING) AS v FROM {schema_fqn}.silver_fact_timecards"
        ).collect()
        if rows:
            val = _normalize(rows[0][0])
    except Exception as e:
        logger.warning(f"[demo_anchor] as-of spark lookup failed; using fallback. {e}")
    return _finalize(val, key)


def anchor(sql: str, as_of: str) -> str:
    """Rewrite CURRENT_DATE() -> DATE('<as_of>'). No-op if not present."""
    if not sql or "CURRENT_DATE()" not in sql:
        return sql
    return sql.replace("CURRENT_DATE()", f"DATE('{as_of}')")
