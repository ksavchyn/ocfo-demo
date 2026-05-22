"""Shared SQL + Haiku composition module.

Extracted from `genie_insights/generate_insights.py` so both the offline
firmwide orchestrator (notebook) AND the live filter-compute Flask endpoint
can share the same pull/prompt/compose pipeline.

Two entry points:
  - `pull_admin_data / pull_finance_data / pull_hr_data` — run canonical SQL
    against the warehouse, optionally restricted to a filter scope via the new
    `filters` argument (e.g. {"region": "EMEA"}).
  - `compose_for_persona_with_filters` — pull → build prompt → call Haiku →
    parse JSON. Returns `{"insights": [...], "action_areas": [...],
    "bottom_chips": [...]}`.

All SQL runs via the Databricks Statement Execution API on a SQL warehouse
(no Spark session needed), so this module is portable between the notebook
orchestrator AND the Databricks Apps Flask runtime.

The notebook orchestrator continues to write firmwide rows into
`gold_persona_insights`; the Flask endpoint just returns the composed JSON
without writing anything (live filter compute is intentionally NOT cached).
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Optional

import requests
from databricks.sdk import WorkspaceClient

# Schema-qualified table prefix. Apps set CFO_SCHEMA=main.cfo_proserv; the
# notebook orchestrator sets CFO_CATALOG + CFO_SCHEMA_NAME separately.
def _resolve_schema_fqn() -> str:
    full = os.environ.get("CFO_SCHEMA", "").strip()
    if full and "." in full:
        return full
    cat = os.environ.get("CFO_CATALOG", "main").strip() or "main"
    sch = os.environ.get("CFO_SCHEMA_NAME", "cfo_proserv").strip() or "cfo_proserv"
    return f"{cat}.{sch}"


SCHEMA_FQN = _resolve_schema_fqn()


# ----------------------------------------------------------------------------
# SQL execution helper — accepts either a WorkspaceClient or a spark session.
# Notebook code paths pass spark (no warehouse round-trip needed); apps and
# CLI callers pass a WorkspaceClient and rely on CFO_WAREHOUSE_ID /
# SQL_WAREHOUSE_ID env.
# ----------------------------------------------------------------------------

def _is_spark(obj: Any) -> bool:
    """Heuristic: is `obj` a Spark session? (Avoids importing pyspark for typing.)"""
    return hasattr(obj, "sql") and not hasattr(obj, "statement_execution")


def _warehouse_id() -> str:
    # Both names are used in the codebase. Prefer the apps-side name.
    wid = os.environ.get("SQL_WAREHOUSE_ID", "").strip() or os.environ.get("CFO_WAREHOUSE_ID", "").strip()
    return wid


def _run_sql(executor: Any, query: str) -> list[list]:
    """Execute SQL via spark (notebook) or Statement Execution API (apps).

    Returns list-of-lists. Decimal columns come back as strings via Statement
    Execution API; callers apply `_f()` to coerce to float.
    """
    if _is_spark(executor):
        df = executor.sql(query)
        return [list(r) for r in df.collect()]
    # WorkspaceClient path
    wid = _warehouse_id()
    if not wid:
        raise RuntimeError(
            "No SQL warehouse configured. Set SQL_WAREHOUSE_ID or CFO_WAREHOUSE_ID."
        )
    r = executor.statement_execution.execute_statement(
        warehouse_id=wid, statement=query, wait_timeout="50s",
    )
    return r.result.data_array if (r.result and r.result.data_array) else []


def _f(x, default=0.0) -> float:
    """Safe float — handles None and Decimal-as-string from API path."""
    if x is None:
        return default
    return float(x)


# ----------------------------------------------------------------------------
# Filter clause builder — whitelist axes to prevent SQL injection
# ----------------------------------------------------------------------------

ALLOWED_FILTER_AXES = {"region", "location", "practice_area", "industry", "customer"}


def _build_filter_clause(filters: Optional[dict]) -> str:
    """Build SQL WHERE-clause additions for active filters.

    - Returns "" when filters is None / empty / all "All".
    - Returns " AND axis = 'value' AND axis2 = 'value2'..." for active filters.
    - Whitelists axis names against ALLOWED_FILTER_AXES (prevents injection
      via crafted keys).
    - Escapes single quotes in values for SQL string-literal safety.
    """
    if not filters or not isinstance(filters, dict):
        return ""
    clauses: list[str] = []
    for axis, value in filters.items():
        if axis not in ALLOWED_FILTER_AXES:
            continue
        if value is None:
            continue
        s = str(value).strip()
        if not s or s.lower() == "all":
            continue
        safe = s.replace("'", "''")
        clauses.append(f"AND {axis} = '{safe}'")
    return " " + " ".join(clauses) if clauses else ""


# gold_ar_snapshot_aging carries region / location / customer_name; it does NOT
# carry practice_area or industry. Snapshot-based prior-DSO queries can only be
# filtered on these three axes.
_SNAPSHOT_SAFE_AXES = {"region", "location", "customer"}


def _build_snapshot_filter_clause(filters: Optional[dict]) -> str:
    """Like _build_filter_clause but only emits axes present in
    gold_ar_snapshot_aging, and translates `customer` → `customer_name`."""
    if not filters or not isinstance(filters, dict):
        return ""
    clauses: list[str] = []
    for axis, value in filters.items():
        if axis not in _SNAPSHOT_SAFE_AXES:
            continue
        if value is None:
            continue
        s = str(value).strip()
        if not s or s.lower() == "all":
            continue
        safe = s.replace("'", "''")
        col = "customer_name" if axis == "customer" else axis
        clauses.append(f"AND {col} = '{safe}'")
    return " " + " ".join(clauses) if clauses else ""


def _has_snapshot_incompatible_filter(filters: Optional[dict]) -> bool:
    """True if filters include practice_area or industry — axes not present in
    gold_ar_snapshot_aging. Caller should fall back to current-only DSO."""
    if not filters or not isinstance(filters, dict):
        return False
    for axis in ("practice_area", "industry"):
        v = filters.get(axis)
        if v and str(v).strip().lower() not in ("", "all"):
            return True
    return False


# ----------------------------------------------------------------------------
# Per-persona SQL pulls — accept `executor` (WorkspaceClient or spark) +
# optional `filters` dict.
# ----------------------------------------------------------------------------

def pull_admin_data(executor: Any, filters: Optional[dict] = None) -> dict:
    """Admin (Senior Partner) firmwide KPIs + supporting drill-downs."""
    fc = _build_filter_clause(filters)

    # KPI 1: Accrued Revenue (annualized run-rate vs budget, MoM, pipeline)
    # 2026-05-20 FIX: actual and budget are MONTHLY in gold_regional_pnl (one
    # row per fiscal_period). Prior version returned monthly values labeled
    # "annualized run-rate" — wrong unit. Multiply by 12 here so the rendered
    # value matches the label and reconciles with the Admin tile's ARR ($20B
    # firmwide) instead of showing $1.67B for one month.
    # MoM % stays as ratio (annualizing both endpoints leaves the ratio intact).
    # Pipeline NOT annualized — pipeline_revenue is a snapshot, not a rate.
    rev = _run_sql(executor, f"""
        WITH cur AS (
          SELECT SUM(total_revenue) AS rev, SUM(budgeted_revenue) AS budget
          FROM {SCHEMA_FQN}.gold_regional_pnl
          WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
        ),
        prior AS (
          SELECT SUM(total_revenue) AS rev
          FROM {SCHEMA_FQN}.gold_regional_pnl
          WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -2){fc}
        ),
        pipe AS (
          SELECT SUM(pipeline_revenue) AS pipe
          FROM {SCHEMA_FQN}.gold_enterprise_metrics
          WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
        )
        SELECT (cur.rev * 12)/1e9, (cur.budget * 12)/1e9,
               (cur.rev - cur.budget)/cur.budget*100,
               (cur.rev - prior.rev)/prior.rev*100,
               pipe.pipe/1e9
        FROM cur CROSS JOIN prior CROSS JOIN pipe
    """)[0]

    # KPI 2: Project Gross Margin (90d windows)
    margin = _run_sql(executor, f"""
        WITH c AS (
          SELECT SUM(actual_margin)/NULLIF(SUM(actual_revenue),0) AS m
          FROM {SCHEMA_FQN}.gold_project_profitability
          WHERE project_end_date >= DATE_SUB(CURRENT_DATE(), 90){fc}
        ),
        p AS (
          SELECT SUM(actual_margin)/NULLIF(SUM(actual_revenue),0) AS m
          FROM {SCHEMA_FQN}.gold_project_profitability
          WHERE project_end_date BETWEEN DATE_SUB(CURRENT_DATE(), 180) AND DATE_SUB(CURRENT_DATE(), 90){fc}
        )
        SELECT c.m*100, p.m*100, (c.m - p.m)*100 FROM c CROSS JOIN p
    """)[0]

    # KPI 3: Revenue per Partner (annualized, YoY).
    # silver_dim_employees carries region/location/practice_area/industry/customer
    # (per 02_build_silver_gold.py); gold_enterprise_metrics carries the same axes.
    rpp = _run_sql(executor, f"""
        WITH ms AS (SELECT MAX(snapshot_date) AS d FROM {SCHEMA_FQN}.silver_dim_employees),
        ys AS (SELECT MAX(snapshot_date) AS d FROM {SCHEMA_FQN}.silver_dim_employees, ms
               WHERE snapshot_date <= ADD_MONTHS(ms.d, -12)),
        cp AS (SELECT COUNT(DISTINCT employee_id) AS n FROM {SCHEMA_FQN}.silver_dim_employees, ms
               WHERE job_level='Partner' AND snapshot_date=ms.d AND employment_status='Active'{fc}),
        yp AS (SELECT COUNT(DISTINCT employee_id) AS n FROM {SCHEMA_FQN}.silver_dim_employees, ys
               WHERE job_level='Partner' AND snapshot_date=ys.d AND employment_status='Active'{fc}),
        cr AS (SELECT SUM(revenue)*12 AS r FROM {SCHEMA_FQN}.gold_enterprise_metrics
               WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}),
        yr AS (SELECT SUM(revenue)*12 AS r FROM {SCHEMA_FQN}.gold_enterprise_metrics
               WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -13){fc})
        SELECT cr.r/cp.n/1e6, yr.r/yp.n/1e6,
               (cr.r/cp.n - yr.r/yp.n)/(yr.r/yp.n)*100,
               cp.n, yp.n
        FROM cr CROSS JOIN cp CROSS JOIN yr CROSS JOIN yp
    """)[0]

    # KPI 4: Expenses vs budget
    exp = _run_sql(executor, f"""
        WITH cur AS (
          SELECT SUM(billable_expenses+corporate_expenses+marketing_expenses+tech_expenses+other_expenses) AS exp,
                 SUM(budgeted_expenses) AS budget
          FROM {SCHEMA_FQN}.gold_regional_pnl
          WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
        ),
        prior AS (
          SELECT SUM(billable_expenses+corporate_expenses+marketing_expenses+tech_expenses+other_expenses) AS exp
          FROM {SCHEMA_FQN}.gold_regional_pnl
          WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -2){fc}
        )
        SELECT cur.exp/1e6, cur.budget/1e6,
               (cur.exp - cur.budget)/cur.budget*100,
               (cur.exp - prior.exp)/prior.exp*100
        FROM cur CROSS JOIN prior
    """)[0]

    # Supporting drill-downs (also respect the filter scope)
    top_rev = _run_sql(executor, f"""
        SELECT location,
               ROUND(SUM(total_revenue - budgeted_revenue)/1e6, 2),
               ROUND((SUM(total_revenue)/NULLIF(SUM(budgeted_revenue),0)-1)*100, 2)
        FROM {SCHEMA_FQN}.gold_regional_pnl
        WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
        GROUP BY location ORDER BY 2 DESC LIMIT 5
    """)
    bottom_rev = _run_sql(executor, f"""
        SELECT location,
               ROUND(SUM(total_revenue - budgeted_revenue)/1e6, 2),
               ROUND((SUM(total_revenue)/NULLIF(SUM(budgeted_revenue),0)-1)*100, 2)
        FROM {SCHEMA_FQN}.gold_regional_pnl
        WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
        GROUP BY location ORDER BY 2 ASC LIMIT 5
    """)
    top_exp = _run_sql(executor, f"""
        SELECT location,
               ROUND(SUM(billable_expenses+corporate_expenses+marketing_expenses+tech_expenses+other_expenses
                         - budgeted_expenses)/1e6, 2),
               ROUND((SUM(billable_expenses+corporate_expenses+marketing_expenses+tech_expenses+other_expenses)
                      /NULLIF(SUM(budgeted_expenses),0)-1)*100, 2)
        FROM {SCHEMA_FQN}.gold_regional_pnl
        WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
        GROUP BY location ORDER BY 3 DESC LIMIT 5
    """)
    practice_margins = _run_sql(executor, f"""
        SELECT practice_area,
               ROUND(SUM(actual_margin)/NULLIF(SUM(actual_revenue),0)*100, 2)
        FROM {SCHEMA_FQN}.gold_project_profitability
        WHERE project_end_date >= DATE_SUB(CURRENT_DATE(), 90){fc}
        GROUP BY practice_area ORDER BY 2 DESC
    """)

    return {
        "kpi_revenue":          dict(zip(("rev_b","budget_b","pct_over","mom_pct","pipeline_b"), [_f(x) for x in rev])),
        "kpi_margin":           dict(zip(("cur_pct","prior_pct","qoq_pp"),                       [_f(x) for x in margin])),
        "kpi_rev_per_partner":  dict(zip(("cur_m","yoy_m","yoy_pct","cur_partners","yoy_partners"), [_f(x) for x in rpp])),
        "kpi_expenses":         dict(zip(("exp_m","budget_m","pct_over","mom_pct"),              [_f(x) for x in exp])),
        "top_offices_revenue_above_budget":  top_rev,
        "top_offices_revenue_below_budget":  bottom_rev,
        "top_offices_expense_overage":       top_exp,
        "practice_gross_margins":            practice_margins,
    }


def pull_finance_data(executor: Any, filters: Optional[dict] = None) -> dict:
    """Finance (Finance Director) firmwide KPIs + supporting drill-downs."""
    fc = _build_filter_clause(filters)

    # DSO — current from silver (correct, snapshot semantics), prior from
    # gold_ar_snapshot_aging. Replaces the shift-back-30-days hack which
    # produced 30-50 day false "improvements" once per-customer payment
    # archetypes populated real aged AR (chronic clients pulled the
    # subset-of-aged-invoices average way up vs the firmwide average).
    #
    # snapshot table doesn't carry practice_area / industry. When the chip is
    # scoped to one of those, we fall back to "prior = current" (delta = 0)
    # rather than fabricate a baseline.
    dso_current_row = _run_sql(executor, f"""
        SELECT
          ROUND(AVG(CASE WHEN payment_status NOT IN ('Paid','Closed') THEN days_outstanding END), 1) AS dso
        FROM {SCHEMA_FQN}.silver_fact_accounts_receivable
        WHERE 1=1{fc}
    """)[0]
    dso_current = dso_current_row[0]
    if _has_snapshot_incompatible_filter(filters):
        prior_dso = dso_current
    else:
        snap_fc = _build_snapshot_filter_clause(filters)
        prior_row = _run_sql(executor, f"""
            WITH per_snap AS (
              SELECT snapshot_date,
                ROUND(SUM(open_ar_balance * weighted_dso_days)
                      / NULLIF(SUM(open_ar_balance), 0), 1) AS dso
              FROM {SCHEMA_FQN}.gold_ar_snapshot_aging
              WHERE snapshot_date < LAST_DAY(CURRENT_DATE()){snap_fc}
              GROUP BY snapshot_date
            ),
            ranked AS (
              SELECT *, ROW_NUMBER() OVER (ORDER BY snapshot_date DESC) AS rn
              FROM per_snap
            )
            SELECT MAX(CASE WHEN rn = 2 THEN dso END) AS prior_dso
            FROM ranked WHERE rn <= 2
        """)
        prior_dso = (prior_row[0][0] if prior_row and prior_row[0] else None) or dso_current
    dso = (dso_current, prior_dso)

    # silver_fact_accounts_payable also carries all 5 axes.
    dpo = _run_sql(executor, f"""
        SELECT
          ROUND(AVG(CASE WHEN payment_status NOT IN ('Paid','Closed') THEN days_outstanding END), 1) AS dpo,
          ROUND(AVG(CASE WHEN payment_status NOT IN ('Paid','Closed') AND days_outstanding > 30
                         THEN days_outstanding - 30 END), 1) AS prior_dpo
        FROM {SCHEMA_FQN}.silver_fact_accounts_payable
        WHERE 1=1{fc}
    """)[0]

    top_clients = _run_sql(executor, f"""
        SELECT customer_name,
               ROUND(SUM(amount)/1e6, 2) AS total_m,
               COUNT(*) AS n_invoices,
               ROUND(AVG(days_outstanding), 1) AS avg_days
        FROM {SCHEMA_FQN}.silver_fact_accounts_receivable
        WHERE payment_status NOT IN ('Paid','Closed'){fc}
        GROUP BY customer_name
        ORDER BY total_m DESC LIMIT 5
    """)

    top_vendors = _run_sql(executor, f"""
        SELECT vendor_name,
               ROUND(SUM(amount_due)/1e6, 2) AS total_m,
               COUNT(*) AS n_invoices,
               ROUND(AVG(days_outstanding), 1) AS avg_days
        FROM {SCHEMA_FQN}.gold_payables_aging
        WHERE UPPER(COALESCE(payment_status,'')) NOT IN ('PAID','CLOSED'){fc}
        GROUP BY vendor_name
        ORDER BY total_m DESC LIMIT 5
    """)

    dso_by_office = _run_sql(executor, f"""
        SELECT location,
               ROUND(AVG(CASE WHEN payment_status NOT IN ('Paid','Closed') THEN days_outstanding END), 1) AS dso,
               ROUND(SUM(CASE WHEN payment_status NOT IN ('Paid','Closed') THEN amount END)/1e6, 2) AS open_ar_m
        FROM {SCHEMA_FQN}.silver_fact_accounts_receivable
        WHERE location IS NOT NULL{fc}
        GROUP BY location ORDER BY dso DESC LIMIT 5
    """)

    return {
        "kpi_dso":             dict(zip(("dso","prior_dso"),                     [_f(x) for x in dso])),
        "kpi_dpo":             dict(zip(("dpo","prior_dpo"),                     [_f(x) for x in dpo])),
        "top_unpaid_clients":  top_clients,
        "top_unpaid_vendors":  top_vendors,
        "dso_movers_by_office": dso_by_office,
    }


def pull_hr_data(executor: Any, filters: Optional[dict] = None) -> dict:
    """HR (Talent leader) firmwide KPIs + supporting drill-downs."""
    fc = _build_filter_clause(filters)

    util = _run_sql(executor, f"""
        WITH cur AS (
          SELECT SUM(billable_hours)/NULLIF(SUM(total_hours),0)*100 AS pct
          FROM {SCHEMA_FQN}.gold_enterprise_metrics
          WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
        ),
        prior AS (
          SELECT SUM(billable_hours)/NULLIF(SUM(total_hours),0)*100 AS pct
          FROM {SCHEMA_FQN}.gold_enterprise_metrics
          WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -2){fc}
        )
        SELECT cur.pct, prior.pct, (cur.pct - prior.pct) FROM cur CROSS JOIN prior
    """)[0]

    partners = _run_sql(executor, f"""
        WITH ms AS (SELECT MAX(snapshot_date) AS d FROM {SCHEMA_FQN}.silver_dim_employees),
        ps AS (SELECT MAX(snapshot_date) AS d FROM {SCHEMA_FQN}.silver_dim_employees, ms
               WHERE snapshot_date <= DATE_SUB(ms.d, 30)),
        cur AS (SELECT COUNT(DISTINCT employee_id) AS n FROM {SCHEMA_FQN}.silver_dim_employees, ms
                WHERE job_level='Partner' AND snapshot_date=ms.d AND employment_status='Active'{fc}),
        prior AS (SELECT COUNT(DISTINCT employee_id) AS n FROM {SCHEMA_FQN}.silver_dim_employees, ps
                  WHERE job_level='Partner' AND snapshot_date=ps.d AND employment_status='Active'{fc})
        SELECT cur.n, prior.n, (cur.n - prior.n) FROM cur CROSS JOIN prior
    """)[0]

    # Bench cost = sum of (hours × cost_rate) over Non-Billable time entries.
    # We compute on-the-fly here rather than summing silver_fact_timecards.cost_amount
    # because the silver-layer cost_amount field is zeroed out for non-billable rows
    # by design (so gold_project_profitability.actual_margin reflects billable-only
    # economics). Without the on-the-fly multiplication, bench cost would be $0 for
    # any month outside the last 90 days (only the reclassified-billable→bench rows
    # contribute via the time_type_clean CASE). That produced a "$0 → $1.65M → $3.84M"
    # cliff in the time series. Same computation pattern in bench_by_practice +
    # bench_by_level below — keep them aligned.
    bench = _run_sql(executor, f"""
        WITH cur AS (
          SELECT SUM(CASE WHEN time_type_clean='Non-Billable' THEN hours_worked * cost_rate ELSE 0 END)/1e6 AS m
          FROM {SCHEMA_FQN}.silver_fact_timecards
          WHERE DATE_TRUNC('MONTH', work_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
        ),
        prior AS (
          SELECT SUM(CASE WHEN time_type_clean='Non-Billable' THEN hours_worked * cost_rate ELSE 0 END)/1e6 AS m
          FROM {SCHEMA_FQN}.silver_fact_timecards
          WHERE DATE_TRUNC('MONTH', work_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -2){fc}
        )
        SELECT cur.m, prior.m,
               CASE WHEN prior.m = 0 THEN NULL ELSE (cur.m - prior.m)/prior.m*100 END
        FROM cur CROSS JOIN prior
    """)[0]

    low_util = _run_sql(executor, f"""
        WITH per_emp AS (
          SELECT employee_id,
                 SUM(CASE WHEN time_type_clean='Billable' THEN hours_worked ELSE 0 END) AS bh,
                 SUM(hours_worked) AS th
          FROM {SCHEMA_FQN}.silver_fact_timecards
          WHERE DATE_TRUNC('MONTH', work_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
          GROUP BY employee_id
        )
        SELECT COUNT(DISTINCT employee_id)
        FROM per_emp
        WHERE th > 0 AND bh/th < 0.50
    """)[0]

    low_util_prior = _run_sql(executor, f"""
        WITH per_emp AS (
          SELECT employee_id,
                 SUM(CASE WHEN time_type_clean='Billable' THEN hours_worked ELSE 0 END) AS bh,
                 SUM(hours_worked) AS th
          FROM {SCHEMA_FQN}.silver_fact_timecards
          WHERE DATE_TRUNC('MONTH', work_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -2){fc}
          GROUP BY employee_id
        )
        SELECT COUNT(DISTINCT employee_id)
        FROM per_emp
        WHERE th > 0 AND bh/th < 0.50
    """)[0]

    bench_by_practice = _run_sql(executor, f"""
        SELECT practice_area,
               ROUND(SUM(CASE WHEN time_type_clean='Non-Billable' THEN hours_worked * cost_rate ELSE 0 END)/1e6, 2) AS bench_m
        FROM {SCHEMA_FQN}.silver_fact_timecards
        WHERE DATE_TRUNC('MONTH', work_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
          AND practice_area IS NOT NULL{fc}
        GROUP BY practice_area ORDER BY bench_m DESC LIMIT 6
    """)

    # NOTE: this join references e.job_level so we need to apply the timecard
    # filter via the t alias. silver_dim_employees also carries the filter axes
    # but to keep the bench-by-level semantics consistent with the scoped pull,
    # we filter on the timecard side only.
    bench_by_level = _run_sql(executor, f"""
        SELECT e.job_level,
               ROUND(SUM(CASE WHEN t.time_type_clean='Non-Billable' THEN t.hours_worked * t.cost_rate ELSE 0 END)/1e6, 2) AS bench_m,
               COUNT(DISTINCT t.employee_id) AS emps
        FROM {SCHEMA_FQN}.silver_fact_timecards t
        JOIN {SCHEMA_FQN}.silver_dim_employees e
          ON e.employee_id = t.employee_id AND e.is_latest_snapshot = TRUE
        WHERE DATE_TRUNC('MONTH', t.work_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
          {_build_filter_clause_qualified(filters, 't')}
        GROUP BY e.job_level ORDER BY bench_m DESC
    """)

    # Dedup employees by computing utilization at the employee grain (no
    # practice_area in GROUP BY), THEN join silver_dim_employees to assign
    # each employee their canonical practice. Grouping by (employee_id,
    # practice_area) in the timecard CTE — the prior implementation — double-
    # counted any employee whose timecards spanned multiple practices in the
    # month, so per-practice counts could sum to > firmwide. The fix matches
    # the firmwide low_util query's employee grain and produces practice
    # numbers that always sum to <= firmwide.
    low_util_by_practice = _run_sql(executor, f"""
        WITH per_emp AS (
          SELECT employee_id,
                 SUM(CASE WHEN time_type_clean='Billable' THEN hours_worked ELSE 0 END) AS bh,
                 SUM(hours_worked) AS th
          FROM {SCHEMA_FQN}.silver_fact_timecards
          WHERE DATE_TRUNC('MONTH', work_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
          GROUP BY employee_id
        ),
        low_util_emps AS (
          SELECT employee_id FROM per_emp WHERE th > 0 AND bh/th < 0.50
        )
        SELECT e.practice_area, COUNT(DISTINCT e.employee_id) AS low_util_n
        FROM low_util_emps lu
        JOIN {SCHEMA_FQN}.silver_dim_employees e
          ON e.employee_id = lu.employee_id AND e.is_latest_snapshot = TRUE
        WHERE e.practice_area IS NOT NULL
          {_build_filter_clause_qualified(filters, 'e')}
        GROUP BY e.practice_area ORDER BY low_util_n DESC LIMIT 6
    """)

    partner_mom_practice = _run_sql(executor, f"""
        WITH ms AS (SELECT MAX(snapshot_date) AS d FROM {SCHEMA_FQN}.silver_dim_employees),
        ps AS (SELECT MAX(snapshot_date) AS d FROM {SCHEMA_FQN}.silver_dim_employees, ms
               WHERE snapshot_date <= DATE_SUB(ms.d, 30)),
        cur AS (
          SELECT practice_area, COUNT(DISTINCT employee_id) AS n
          FROM {SCHEMA_FQN}.silver_dim_employees, ms
          WHERE job_level='Partner' AND snapshot_date=ms.d AND employment_status='Active'
            AND practice_area IS NOT NULL{fc}
          GROUP BY practice_area
        ),
        prior AS (
          SELECT practice_area, COUNT(DISTINCT employee_id) AS n
          FROM {SCHEMA_FQN}.silver_dim_employees, ps
          WHERE job_level='Partner' AND snapshot_date=ps.d AND employment_status='Active'
            AND practice_area IS NOT NULL{fc}
          GROUP BY practice_area
        )
        SELECT cur.practice_area, cur.n, prior.n, (cur.n - prior.n) AS delta
        FROM cur LEFT JOIN prior USING (practice_area)
        ORDER BY delta ASC LIMIT 6
    """)

    # Low-utilization headcount by JOB LEVEL (Associate, Engagement Manager,
    # Director, etc.) — exposed so the LLM has a distinct number set for
    # level-cohort action areas. Previously the prompt only carried per-PRACTICE
    # numbers, and Haiku would stitch those into level-cohort sentences
    # (e.g. "Associate and Engagement Manager cohorts account for 1,315 and
    # 1,073 low-util employees" where 1,315 and 1,073 are actually S&C and
    # Technology PRACTICE numbers). Same dedup pattern as low_util_by_practice:
    # employee grain in CTE → join to dim_employees → group by job_level.
    low_util_by_level = _run_sql(executor, f"""
        WITH per_emp AS (
          SELECT employee_id,
                 SUM(CASE WHEN time_type_clean='Billable' THEN hours_worked ELSE 0 END) AS bh,
                 SUM(hours_worked) AS th
          FROM {SCHEMA_FQN}.silver_fact_timecards
          WHERE DATE_TRUNC('MONTH', work_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1){fc}
          GROUP BY employee_id
        ),
        low_util_emps AS (
          SELECT employee_id FROM per_emp WHERE th > 0 AND bh/th < 0.50
        )
        SELECT e.job_level, COUNT(DISTINCT e.employee_id) AS low_util_n
        FROM low_util_emps lu
        JOIN {SCHEMA_FQN}.silver_dim_employees e
          ON e.employee_id = lu.employee_id AND e.is_latest_snapshot = TRUE
        WHERE e.job_level IS NOT NULL
          {_build_filter_clause_qualified(filters, 'e')}
        GROUP BY e.job_level ORDER BY low_util_n DESC LIMIT 8
    """)

    return {
        "kpi_utilization":         dict(zip(("cur_pct","prior_pct","mom_pp"),                  [_f(x) for x in util])),
        "kpi_partner_headcount":   dict(zip(("cur","prior","mom_delta"),                       [_f(x) for x in partners])),
        "kpi_bench_cost":          dict(zip(("cur_m","prior_m","mom_pct"),                     [_f(x) for x in bench])),
        "kpi_low_util":            {"cur": _f(low_util[0]), "prior": _f(low_util_prior[0]),
                                    "delta": _f(low_util[0]) - _f(low_util_prior[0])},
        "bench_by_practice":       bench_by_practice,
        "bench_by_level":          bench_by_level,
        "low_util_by_practice":    low_util_by_practice,
        "low_util_by_level":       low_util_by_level,
        "partner_mom_by_practice": partner_mom_practice,
    }


def _build_filter_clause_qualified(filters: Optional[dict], alias: str) -> str:
    """Like `_build_filter_clause` but prefixes each axis with an alias (e.g. `t.region`).

    Used in queries that join multiple tables and need to disambiguate the
    filter columns to one side of the join.
    """
    if not filters or not isinstance(filters, dict):
        return ""
    clauses: list[str] = []
    for axis, value in filters.items():
        if axis not in ALLOWED_FILTER_AXES:
            continue
        if value is None:
            continue
        s = str(value).strip()
        if not s or s.lower() == "all":
            continue
        safe = s.replace("'", "''")
        clauses.append(f"AND {alias}.{axis} = '{safe}'")
    return " " + " ".join(clauses) if clauses else ""


# ----------------------------------------------------------------------------
# Prompt builders (per persona)
# ----------------------------------------------------------------------------

COMMON_INSTRUCTIONS = """
TONE & STYLE — CRITICAL:
This is an executive financial dashboard read by CFOs, Senior Partners, and Finance
Directors at a global Tier 1 consultancy. Write in a measured, dry, factual register
— the way a CFO or board-pack analyst writes. NOT punchy startup voice, NOT
journalistic, NOT colloquial.

HEADLINE FORMAT: every insight's `headline` field MUST be a short, formal
noun-phrase tile label that names the KPI directly — matching the corresponding
Finance/Admin dashboard tile labels. Examples of GOOD headlines:
  "Accrued Billable Revenue vs Annual Budget"
  "Project Gross Margins"
  "Revenue per Partner"
  "Expenses vs Forecast"
  "Days Sales Outstanding"
  "Days Payable Outstanding"
  "Top Unpaid Client"
  "Top Unpaid Vendor"
  "Utilization Rate"
  "Partner Headcount"
  "Bench Cost"
  "Low Utilization Headcount"
Examples of BAD (do not use):
  "Run-rate revenue blowing past plan"
  "Project margin slipping across the book"
  "Partner economics flat-to-down"
  "Expenses running hot vs plan"
  "Cash conversion deteriorating sharply"
  Any headline containing colloquialisms like "blowing past", "running hot",
     "slipping", "stretched thin", "flat-to-down", "hot", "cool", "off the rails"

BANNED VERBS in narratives + action areas: surged, exploded, blew past, blowing,
plunged, plummeted, crashed, cratered, slipping, stretching, running hot,
running cool, off the rails, pulling ahead, falling behind (use "behind plan"
instead), gathering steam, losing steam. Use measured verbs: declined, increased,
expanded, contracted, exceeded budget by X%, fell below plan by Y%, eased to,
compressed to.

NARRATIVE LENGTH: 1-2 short, dense sentences. No flourish. State what the data
shows, then point to the named entities driving it. No "this is concerning" or
"worth watching" filler — just numbers + entities.

ACTION_AREA HEADLINES: short imperative noun-phrases like the old production
version: "Investigate <Office> Expense Overage", "Address <Practice> Margin
Compression", "Escalate <Client> Collection", "Reprice <Practice> Engagements".
NOT colloquial like "Fix Atlanta or reallocate the book" or "Cap London cost
growth" — instead use "Investigate Atlanta Revenue Shortfall", "Investigate
London Expense Overage".

JSON FIELD STRUCTURE:

INSIGHT structure: {headline, value, comparison, trend, trend_direction, status_color, narrative, target_entity_type, target_entity_value}
- value: 2-decimal formatted number with the MOST READABLE unit for the
  magnitude. Pick the unit by INSPECTING THE ABSOLUTE NUMBER FIRST, then choosing:
    - >= $1B            → "$X.XXB"     (e.g. "$1.54B", "$18.48B")
    - >= $1M  but < $1B → "$XXX.XXM"   (e.g. "$472.18M", "$110.00M")
    - >= $1K  but < $1M → "$XXX.XXK"   (e.g. "$180.00K", "$42.50K", "$850.00K")
    - <  $1K            → "$XXX"       (raw dollars, e.g. "$487")
    - %, days, count → native unit with 2 decimals where applicable
       (e.g. "46.07%", "50.9 days", "2,473 partners")
  Multi-axis filters narrow scope and SHRINK magnitudes — when Bangkok+Audit revenue
  is $180,000 you MUST format as "$180.00K", NEVER as "$0.18M". When the filtered
  margin compresses an expense to $40,000 you MUST format as "$40.00K", NEVER "$0.04M".
  Banned formats (always wrong, no exceptions):
    - "$0.18M"  — wrong, format as "$180.00K"
    - "$0.04M"  — wrong, format as "$40.00K"
    - "$0.11B"  — wrong, format as "$110.00M"
    - "$0.0034B" — wrong, format as "$3.40M"
  The same magnitude rule applies INSIDE narratives and comparisons. If you find
  yourself writing "$0.XM" or "$0.XB" in ANY field, you picked the wrong unit —
  step down to the next smaller unit (B → M → K → raw dollars). NO parens in
  the value field.

EMPTY / SPARSE DATA SLICES (CRITICAL — multi-axis filters often produce these):
  When a user applies multiple filters (e.g. Location=Bangkok AND Practice=Audit),
  the resulting slice may have ZERO underlying rows for some KPIs — especially
  90-day windows like Project Gross Margins. The SQL aggregates return NULL or 0
  in that case. DO NOT render "0.00%" or "$0.00" as if it were a real measurement.
  Instead, set:
    - value: "Insufficient sample"
    - comparison: "fewer than required engagements in scope"
    - trend: "" (empty string)
    - trend_direction: "neutral"
    - status_color: "yellow"
    - narrative: ONE sentence — name the KPI, name the filter scope, state that
      the slice is too narrow to support the metric (e.g. "Bangkok Audit project
      gross margin cannot be computed for the current 90-day window because no
      projects completed in this scope. Widen filter to enable measurement.")
  Detection heuristic: if a KPI's numeric value comes through as None / null / 0
  AND the data dict indicates zero supporting rows (e.g. SUM-based fields are
  null/zero, or count-based fields are 0) AND the comparison would produce a
  trivial 0%/0.00 result, treat it as sparse and use the "Insufficient sample"
  pattern. This protects narratives from claiming "Bangkok Audit projects
  compressed to 0.00%" when the truth is "we have no Bangkok Audit projects in
  the window".

NUMERIC CONSISTENCY (applies to insight narratives, action_area narratives,
and any other prose you compose):
  - **Prose ranges must match data ranges.** If your narrative states a range
    like "fell between 1.83% and 2.97% across all practice areas", every
    endpoint quoted MUST be present in the supporting data you were given.
    Don't extrapolate a range from your own narrative summary — quote the
    actual min/max from the data.
  - **Units always labeled.** "Bill rate" / "billing rate" must be stated
    per-hour explicitly ("$2,400/hour"). Revenue per partner must be labeled
    per-year or per-month. Never quote a bare dollar amount whose unit is
    ambiguous.
  - **Window / time-basis must be labeled** for any metric where the same KPI
    name can appear on multiple tabs / dashboards on different windows. The
    supporting data dict labels the window explicitly (e.g. "annualized run-rate",
    "trailing-12-month", "latest fiscal month", "90-day window", "YoY") — that
    label MUST appear either in the comparison field or inside the narrative,
    NOT only in the headline. Skipping the window basis causes a tile that
    reads "$8.45M" to look contradictory with another tile that reads "$7.2M"
    on the same metric. Specifically:
      * Revenue per Partner — always label as "annualized" / "monthly" /
        "TTM" / "YoY" depending on the data dict.
      * Expenses — always label the period basis ("latest fiscal month",
        "trailing-3-months", "year-to-date", "annualized").
      * Margin — always label the window ("90-day window", "month", "YTD").
      * DSO / DPO — always label whether it's open-AR-only, paid-only, or
        snapshot-month, mirroring the supporting data dict.
  - **Magnitude sanity check via RELATIVE ratios** (not absolute dollar
    thresholds — customers operate at different scales). Check:
      * A single office / practice / region's monthly figure should be a
        sensible fraction of the firmwide figure (typically 5-15% per office).
        If one cell's number exceeds firmwide, it's a SQL bug.
      * Per-partner values should reconcile arithmetically: total revenue /
        partner count = revenue per partner. If sub-query results imply two
        different RPP numbers, the queries used different denominators or
        numerators — flag it transparently.
      * Utilization, margin, and other percent-of-base metrics should fall
        in plausible ranges (utilization 40-90%, margin -20% to +70%). A
        21% partner utilization or a 95% gross margin is suspicious at any
        firm scale.
    If a SQL-returned figure violates relative consistency with other
    figures, DO NOT quote it confidently. Either omit or use "Insufficient
    sample" / "data needs validation" framing.
- comparison: a SINGLE descriptive phrase comparing the value to a benchmark.
  THE RENDERER WRAPS COMPARISON IN PARENTHESES AT DISPLAY TIME — so the JSON
  string MUST NOT include its own parens.
  Pick ONE comparison anchor per field: EITHER the baseline value OR a single
  percentage/delta. The OTHER number lives in the `trend` field.

  ANCHOR SELECTION (CRITICAL — applies to every insight comparison):
  Choose the anchor in this priority order, using whatever is present in the
  supporting data for that KPI:
    1. Budget / annual plan / target / forecast (the planned-vs-actual gap)
    2. Prior baseline of the same metric — prior month, prior quarter, prior
       year, or trailing-12-month baseline (whichever the data dict provides)
    3. Run-rate vs trailing average, if both budget and prior baseline are absent

  DO NOT use these as the comparison anchor (they describe distributional noise,
  not standing vs a benchmark):
    - "X pp above lowest-margin practice / office / region / cohort"
    - "X% above smallest client / quietest month / weakest cohort"
    - "X above the median of the breakdown"
  These framings tell the reader nothing about whether the metric is healthy.
  If the only data you have is a breakdown ranking, prefer to describe the KPI
  using the prior-period or budget anchor (from the headline KPI dict, not the
  breakdown), and leave dispersion observations for the `narrative` field.
- trend: directional motion with arrow + magnitude (e.g. "↑ 2.10% MoM growth",
  "↓ 1.13pp QoQ", "↑ 6.9 days deterioration"). One direction indicator per trend.
  TEXT MUST match the SEMANTIC direction, not the arithmetic direction.
  IMPORTANT: trend is for CHANGES, not for levels. If the metric is a level
  (average invoice age, total open balance, current headcount, top-client
  exposure), the trend field must describe the MoM / QoQ / YoY MOVEMENT in
  that level, never re-state the level itself with an arrow. Banned patterns:
    BAD: "↑ <level value> aging" — quoting the average age itself with an
         arrow implies the metric moved by that amount, which is false
    BAD: "↑ <balance> outstanding" — that's the current balance, not a delta
    GOOD: "↑ <delta> days vs prior month" — the change in average age MoM
    GOOD: "" (empty string — if the data dict provides no change measurement,
              leave trend blank rather than fabricating one)
- trend_direction: PURELY the SEMANTIC motion direction — "improving" / "deteriorating" / "flat".
  Mapping:
  | Metric category                         | up means          | down means         |
  | Revenue, gross profit, pipeline         | improving         | deteriorating      |
  | Margin %, gross margin %, profitability | improving         | deteriorating      |
  | Revenue per Partner                     | improving         | deteriorating      |
  | Partner headcount (when growth is goal) | improving         | deteriorating      |
  | Utilization %, billable hours           | improving         | deteriorating      |
  | Expenses $ (firmwide or category)       | deteriorating     | improving          |
  | DSO, AR aging, collection days          | deteriorating     | improving          |
  | DPO (when extension is goal)            | improving         | deteriorating      |
  | Bench cost, idle hours, low-util count  | deteriorating     | improving          |
  | Variance over budget %                  | deteriorating     | improving          |
- status_color: "red" / "yellow" / "green" — choose by ABSOLUTE STATE, not motion.

  status_color reflects where the CURRENT VALUE stands relative to its benchmark
  (budget, plan, target, prior baseline). It is INDEPENDENT of trend_direction.
  A metric can be in a bad state AND improving (still red — the standing is bad
  even if motion is good); or in a good state and deteriorating (green/yellow
  depending on how marginal the standing is).

  Semantic rule (applies to ANY data, including remapped customer data — never
  hardcode dollar / percentage thresholds against specific entities here):
  - Look at the variance between the current value and its comparison anchor
    (e.g. "0.82% above budget", "0.43% below plan", "+1.6 days improvement").
  - Map the variance direction to a semantic outcome using the SAME polarity
    table used for trend_direction above:
      * Expenses, bench cost, DSO, AR aging, variance-over-budget %, low-util,
        idle hours → "above / over / higher" is BAD
      * Revenue, margin %, RPP, utilization %, billable hours, partner
        headcount (when growth is goal), pipeline → "below / under / lower" is BAD
      * DPO → "higher" is intentional in most policies, treat extension as
        yellow unless the narrative reads as accidental slippage
  - Outcome → color:
      * BAD direction with non-marginal variance → "red"
      * BAD direction with marginal variance (within roughly 1pp / 1% of the
        anchor, or near zero on a count-delta) → "yellow"
      * GOOD direction → "green"
      * Genuinely neutral / Insufficient sample → "yellow" (per sparse-data rule)

  Worked examples (use the comparison field for each tile, NOT hardcoded values):
  - "Expenses 0.82% above budget" → red. Over budget is bad regardless of MoM motion.
  - "Accrued Revenue 0.43% below annual plan" → yellow or red. Below plan is bad
    even if the MoM trend is upward. NEVER green when below plan.
  - "DSO 1.6 days lower than prior baseline" → green. Lower DSO is good.
  - "Project margin 2.86pp below prior period" → red. Margin compression is bad.
  - "Utilization 0.35pp above prior month" → green. Higher utilization is good.

- narrative: ≤ 2 short sentences, ≤ 2 named entities with their specific numbers. NO firmwide MoM/YoY/QoQ percentages in the narrative.

**BREAKDOWN ARITHMETIC FIDELITY (HARD STOP).** If the headline value is a
COUNT or TOTAL (employees, partners, projects, $ amount, hours, etc.) and the
narrative names breakdown contributors with their specific sub-counts, the
sum of those sub-counts MUST NOT EXCEED the headline total. Example BAD:
headline says "Low Utilization Headcount: 6,044 employees" and narrative says
"Strategy & Consulting and Technology practices together account for 17,682
low-utilization employees" — 17,682 > 6,044 is mathematically impossible for
a subset of the same firm-defined population. This breaks reader trust
instantly. Either (a) make sub-counts sum to ≤ headline, or (b) skip the
breakdown numbers and just name the entities qualitatively ("Strategy &
Consulting and Technology together account for the largest share"). If the
supporting data dict shows breakdown counts that exceed the headline,
suspect a different counting basis (employee-months vs distinct employees,
FTE vs headcount) — DO NOT mix them in the same sentence; either pick one
basis and label it explicitly, or omit the conflicting figure.
- target_entity_type: "office" | "practice" | "customer" | "region" | null
- target_entity_value: specific entity name, or null

ACTION_AREA structure: {headline, status_color, narrative, target_entity_type, target_entity_value}
- 3 distinct concrete actions. Each names a specific entity from the supporting data and quantifies $-impact + timeline.
- Headline format: imperative noun-phrase.
- status_color: red or yellow ONLY — never green. Action_area headlines begin
  with an imperative verb (Investigate, Escalate, Address, Resolve, Reprice,
  Cap, Audit, Validate, Renegotiate, Realign, Mitigate). Those verbs ASSERT
  that intervention is needed; a green chip directly contradicts that. If the
  underlying situation is genuinely "all good", there should be no action_area
  for it at all — pick a different action area instead.
    * red    → high-severity / material $-impact / immediate timeline
    * yellow → lower-severity / monitoring posture / longer timeline
  This rule is independent of the insight tile coloring above and overrides
  any inherited semantics from the source metric's trend_direction.

BOTTOM_CHIP structure: {question_text}
- 3 root-cause "why" questions that drill into NOT-already-covered areas.
- ONE sentence, ≤ 25 words, ends in `?`.
- **NO NAMED ENTITIES IN CHIP TEXT.** Never name specific offices (London,
  Bangkok, Munich, etc.), practices (Tax, Audit, Strategy & Consulting),
  clients (Tencent, BP, Microsoft), or industries in the chip. Use the
  category words only: "offices", "practices", "clients", "regions",
  "industries", "partners", "projects". The chip is pre-cached and frozen
  for the entire demo window — naming a specific entity locks Genie into
  ONE story and dies if the data shifts. Let Genie surface the named
  outliers at click time.
  GOOD: "What is driving the year-over-year movement in revenue per partner?"
  BAD:  "Why did Bangkok's project margin drop while Mumbai's grew?"

- **DIRECTIONAL FIDELITY (HARD STOP).** Avoid asserting a specific direction
  ("decline", "growth", "drop", "expand", "shrink", "contraction", "increase",
  "decrease", "compression", "deterioration", "improvement") in chip text
  UNLESS that direction is consistent at BOTH the firmwide level AND the
  breakdown level Genie will drill into when the chip is clicked.
  Why this matters: chips often look at a firmwide KPI to set their topic
  (e.g. firmwide RPP fell 2% YoY), but Genie's drill-down query slices the
  same metric by practice / office / region. Mix-shift / Simpson's paradox
  effects mean firmwide direction and breakdown direction can DISAGREE
  (firmwide RPP down while every practice's RPP up, due to heavier weighting
  of lower-RPP practices). A chip that asserts "decline" then triggers a
  drill-down where every row shows growth produces an AI response that says
  "the premise is not supported by the underlying data" — embarrassing in
  a CFO demo.
  Safest pattern: phrase chips around the QUESTION, not the DIRECTION.
    GOOD: "What is driving the year-over-year movement in revenue per partner?"
    GOOD: "How is partner headcount split between hires, promotions, and exits?"
    GOOD: "Where is the largest variance in expense vs budget by office?"
    BAD:  "Why is revenue per partner declining despite headcount growth?"
    BAD:  "What's driving margin compression across practices?"
    BAD:  "Why are expense overages concentrated in specific offices?"
        (last one is borderline — only OK if firmwide overage AND
        per-office breakdown both clearly show concentration)
  When in doubt, use a neutral verb ("driving", "explaining", "behind") or
  rephrase as a magnitude question ("Which X has the largest Y?") rather
  than a directional one.

CRITICAL: Use ONLY the values provided above. Do NOT invent numbers. Do NOT use practice/office-level numbers as firmwide claims.

Return ONLY the raw JSON object. No preamble, no markdown code fences.

JSON schema:
{
  "insights":    [ {...}, {...}, {...}, {...} ],
  "action_areas":[ {...}, {...}, {...} ],
  "bottom_chips":[ {...}, {...}, {...} ]
}
"""


def _filter_context_clause(filters: Optional[dict]) -> str:
    """Render a human-readable filter-scope blurb for the prompt header. Empty
    when no filters active. Tells Haiku to write copy that's scoped, not firmwide."""
    if not filters:
        return ""
    parts: list[str] = []
    for axis in ("region", "location", "practice_area", "industry", "customer"):
        v = filters.get(axis)
        if v and str(v).strip() and str(v).lower() != "all":
            parts.append(f"{axis}={v}")
    if not parts:
        return ""
    return (
        f"\n\nFILTER SCOPE: this composition is FILTERED to {', '.join(parts)}. "
        f"All KPI values and supporting data above are restricted to this slice. "
        f"Write narratives that ACKNOWLEDGE the filter scope (e.g. 'within EMEA' "
        f"or 'for the Tax practice') and DO NOT claim firmwide patterns.\n"
    )


def build_admin_prompt(d: dict, filters: Optional[dict] = None) -> str:
    rev, m, rpp, exp = d["kpi_revenue"], d["kpi_margin"], d["kpi_rev_per_partner"], d["kpi_expenses"]
    scope = _filter_context_clause(filters)
    return f"""You are the Insights Orchestrator composing the Senior Partner's Executive Summary for firmwide scope.

VOICE: Senior Partner — partner economics, comp pool, practice strategy. Measured, factual, board-pack register (NOT punchy/colloquial). Insights should read like the executive summary in a partner pre-read deck.{scope}

CANONICAL HEADLINE KPIs — use these EXACT values; do NOT recompute:

1. Accrued Revenue (annualized run-rate): ${rev['rev_b']:.2f}B
   - Annual budget plan: ${rev['budget_b']:.2f}B
   - Variance vs budget: {rev['pct_over']:+.2f}%
   - MoM movement: {rev['mom_pct']:+.2f}%
   - Pipeline backing: ${rev['pipeline_b']:.2f}B

2. Project Gross Margin (90-day window):
   - Current 90d: {m['cur_pct']:.2f}%
   - Prior 90d: {m['prior_pct']:.2f}%
   - QoQ delta: {m['qoq_pp']:+.2f}pp

3. Revenue per Partner (annualized run-rate):
   - Current: ${rpp['cur_m']:.2f}M / partner (across {rpp['cur_partners']:.0f} active Partners)
   - YoY baseline: ${rpp['yoy_m']:.2f}M / partner (across {rpp['yoy_partners']:.0f} active Partners 12 months ago)
   - YoY change: {rpp['yoy_pct']:+.2f}%

4. Expenses (latest fiscal month):
   - Current: ${exp['exp_m']:.2f}M
   - Budgeted: ${exp['budget_m']:.2f}M
   - Variance vs budget: {exp['pct_over']:+.2f}%
   - MoM movement: {exp['mom_pct']:+.2f}%

SUPPORTING DATA (for narratives + action areas — use ONLY these):

Top 5 offices ABOVE budget on revenue (highest positive variance):
{chr(10).join(f"  - {r[0]}: +${_f(r[1]):.2f}M ({_f(r[2]):+.2f}% vs plan)" for r in d['top_offices_revenue_above_budget'])}

Top 5 offices BELOW budget on revenue (largest shortfalls):
{chr(10).join(f"  - {r[0]}: ${_f(r[1]):.2f}M ({_f(r[2]):+.2f}% vs plan)" for r in d['top_offices_revenue_below_budget'])}

Top 5 offices with biggest EXPENSE overage % vs budget:
{chr(10).join(f"  - {r[0]}: +${_f(r[1]):.2f}M ({_f(r[2]):+.2f}% over)" for r in d['top_offices_expense_overage'])}

Practice gross margins (90d), ranked:
{chr(10).join(f"  - {r[0]}: {_f(r[1]):.2f}%" for r in d['practice_gross_margins'])}

YOUR TASK: Compose 4 INSIGHTS + 3 ACTION_AREAS + 3 BOTTOM_CHIPS as a single JSON object.
The 4 insights MUST cover: Accrued Revenue, Project Gross Margin, Revenue per Partner, Expenses — in that order.
{COMMON_INSTRUCTIONS}"""


def build_finance_prompt(d: dict, filters: Optional[dict] = None) -> str:
    dso, dpo = d["kpi_dso"], d["kpi_dpo"]
    top_client = d["top_unpaid_clients"][0] if d["top_unpaid_clients"] else None
    top_vendor = d["top_unpaid_vendors"][0] if d["top_unpaid_vendors"] else None
    top_client_str = f"{top_client[0]}: ${_f(top_client[1]):.2f}M across {int(_f(top_client[2]))} invoices, avg {_f(top_client[3]):.1f} days outstanding" if top_client else "(none)"
    top_vendor_str = f"{top_vendor[0]}: ${_f(top_vendor[1]):.2f}M across {int(_f(top_vendor[2]))} invoices, avg {_f(top_vendor[3]):.1f} days outstanding" if top_vendor else "(none)"
    scope = _filter_context_clause(filters)

    return f"""You are the Insights Orchestrator composing the Finance Director's Executive Summary for firmwide scope.

VOICE: Finance Director — cash conversion, working capital, AR/AP discipline. Measured, factual, audit-committee register (NOT colloquial/punchy). Insights should read like a CFO's monthly close memo.{scope}

CANONICAL HEADLINE KPIs — use these EXACT values; do NOT recompute:

1. DSO (Days Sales Outstanding, open AR only):
   - Current: {dso['dso']:.1f} days
   - Prior baseline (>30d aged open): {dso['prior_dso']:.1f} days
   - Deterioration: {(dso['dso'] - dso['prior_dso']):+.1f} days

2. DPO (Days Payable Outstanding, open AP only):
   - Current: {dpo['dpo']:.1f} days
   - Prior baseline (>30d aged open): {dpo['prior_dpo']:.1f} days
   - Movement: {(dpo['dpo'] - dpo['prior_dpo']):+.1f} days

3. Top Unpaid Client:
   - {top_client_str}

4. Top Unpaid Vendor:
   - {top_vendor_str}

SUPPORTING DATA (for narratives + action areas — use ONLY these):

Top 5 unpaid clients (open AR, ranked by $):
{chr(10).join(f"  - {r[0]}: ${_f(r[1]):.2f}M / {int(_f(r[2]))} invoices / avg {_f(r[3]):.1f} days" for r in d['top_unpaid_clients'])}

Top 5 unpaid vendors (open AP, ranked by $):
{chr(10).join(f"  - {r[0]}: ${_f(r[1]):.2f}M / {int(_f(r[2]))} invoices / avg {_f(r[3]):.1f} days" for r in d['top_unpaid_vendors'])}

DSO movers by office (highest DSO):
{chr(10).join(f"  - {r[0]}: {_f(r[1]):.1f} days DSO / ${_f(r[2]):.2f}M open AR" for r in d['dso_movers_by_office'])}

YOUR TASK: Compose 4 INSIGHTS + 3 ACTION_AREAS + 3 BOTTOM_CHIPS as a single JSON object.
The 4 insights MUST cover: DSO, DPO, Top Unpaid Client, Top Unpaid Vendor — in that order.
{COMMON_INSTRUCTIONS}"""


def build_hr_prompt(d: dict, filters: Optional[dict] = None) -> str:
    u, p, b, lu = d["kpi_utilization"], d["kpi_partner_headcount"], d["kpi_bench_cost"], d["kpi_low_util"]
    scope = _filter_context_clause(filters)
    return f"""You are the Insights Orchestrator composing the HR / Talent leader's Executive Summary for firmwide scope.

VOICE: Chief Human Resources Officer — utilization discipline, bench redeployment, partner growth, cohort health. Measured, factual, leadership-team-memo register (NOT colloquial/punchy). Insights should read like an HR business-review brief.{scope}

CANONICAL HEADLINE KPIs — use these EXACT values; do NOT recompute:

1. Utilization Rate (last complete fiscal month):
   - Current: {u['cur_pct']:.2f}%
   - Prior month: {u['prior_pct']:.2f}%
   - MoM delta: {u['mom_pp']:+.2f}pp

2. Partner Headcount (latest snapshot):
   - Current: {p['cur']:.0f} active Partners
   - ~30 days prior: {p['prior']:.0f} active Partners
   - MoM delta: {p['mom_delta']:+.0f} partners

3. Bench Cost (Non-Billable cost in last complete fiscal month):
   - Current: ${b['cur_m']:.2f}M
   - Prior month: ${b['prior_m']:.2f}M
   - MoM delta: {b['mom_pct']:+.2f}%

4. Low Utilization Headcount (employees with billable/total < 50% in last complete fiscal month):
   - Current: {lu['cur']:.0f} employees
   - Prior month: {lu['prior']:.0f} employees
   - MoM delta: {lu['delta']:+.0f}

SUPPORTING DATA (for narratives + action areas — use ONLY these):

Bench cost by practice (last complete fiscal month):
{chr(10).join(f"  - {r[0]}: ${_f(r[1]):.2f}M" for r in d['bench_by_practice'])}

Bench cost by job level / cohort (last complete fiscal month):
{chr(10).join(f"  - {r[0]}: ${_f(r[1]):.2f}M ({int(_f(r[2]))} employees)" for r in d['bench_by_level'])}

Low-utilization (<50%) headcount BY PRACTICE (each employee counted in ONE practice):
{chr(10).join(f"  - {r[0]}: {int(_f(r[1]))} employees" for r in d['low_util_by_practice'])}

Low-utilization (<50%) headcount BY JOB LEVEL / COHORT (each employee counted in ONE level):
{chr(10).join(f"  - {r[0]}: {int(_f(r[1]))} employees" for r in d['low_util_by_level'])}

Partner MoM change by practice (negative = partner exits):
{chr(10).join(f"  - {r[0]}: {int(_f(r[1]))} now / {int(_f(r[2])) if r[2] is not None else 0} prior / delta {int(_f(r[3])) if r[3] is not None else 0:+d}" for r in d['partner_mom_by_practice'])}

PRACTICE vs LEVEL — HARD STOP, do NOT cross-pollinate numbers:
- "Bench cost by practice" and "Low-utilization by practice" use the PRACTICE
  decomposition (Strategy & Consulting, Technology, etc.).
- "Bench cost by job level" and "Low-utilization by job level" use the LEVEL
  decomposition (Associate, Engagement Manager, Director, etc.).
- When an action_area or narrative talks about LEVEL cohorts (Associate,
  Engagement Manager, etc.), the headcount/cost numbers MUST come from the
  "by level" sections above — NEVER from the "by practice" sections.
- Symmetrically, when talking about PRACTICE concentration, numbers MUST come
  from the "by practice" sections.
- Example of WRONG (do NOT do this): "Associate and Engagement Manager
  cohorts account for 1,315 and 1,073 low-utilization employees" when 1,315
  and 1,073 are PRACTICE numbers (Strategy & Consulting and Technology). The
  reader trusts that Associate cohort has 1,315 — but it doesn't, that's
  S&C's per-practice count.

YOUR TASK: Compose 4 INSIGHTS + 3 ACTION_AREAS + 3 BOTTOM_CHIPS as a single JSON object.
The 4 insights MUST cover: Utilization Rate, Partner Headcount, Bench Cost, Low Utilization — in that order.
{COMMON_INSTRUCTIONS}"""


# ----------------------------------------------------------------------------
# Compose-tier LLM invocation + JSON parsing
# ----------------------------------------------------------------------------
#
# Resilient model fallback chain. Each Databricks FMAPI endpoint has its own
# per-minute output-token quota pool. Bursts during a chip-regen cycle (3
# personas × 4 insights + 3 actions + 3 bottom-chips, all firing in quick
# succession) can overflow Haiku 4.5's pool. To survive that without
# preserving stale chips, we retry-with-backoff on the primary, then cascade
# to two Sonnet endpoints that share Anthropic's JSON formatting behavior
# but have independent rate-limit lanes.
COMPOSE_MODEL_FALLBACK_CHAIN: list[str] = [
    # Primary: cheapest, fastest, current default.
    "databricks-claude-haiku-4-5",
    # Fallback 1: older Sonnet, lightly used in most workspaces → likely fresh
    # quota; slower (~2-3×) but identical JSON-format behavior.
    "databricks-claude-sonnet-4-5",
    # Fallback 2: current Sonnet, separate endpoint pool from Sonnet-4-5.
    "databricks-claude-sonnet-4-6",
]

# Per-model retry budget on the SAME endpoint before cascading to the next.
# Set to 1 (no retry) — Databricks FMAPI rate limits are per-minute, so a
# 20s in-place wait rarely clears them. Faster to cascade immediately to
# Sonnet 4.5 (which has its own quota pool) and avoid the 20-40s sleeps
# that accumulated across chip + follow-up calls. If a customer ever needs
# in-place retry tolerance (e.g., very bursty traffic on a single model),
# bump this back to 2 or higher.
COMPOSE_RETRY_ATTEMPTS_PER_MODEL = 1
COMPOSE_BACKOFF_BASE_SECONDS = 20

# HTTP status codes that trigger model-fallback (not just retry-same-model).
# 429 = rate-limit. 5xx = upstream model-server transient issue.
_RATE_LIMIT_STATUS_CODES = {429, 500, 502, 503, 504}


def _resolved_compose_models() -> list[str]:
    """Build the fallback chain.

    Honors `CFO_CLAUDE_MODEL_COMPOSE` env var: if set, it goes at the top of
    the chain (so customer-configured overrides still work), and the
    remaining default chain entries serve as fallbacks. Duplicates removed.
    """
    primary = (
        os.environ.get("CFO_CLAUDE_MODEL_COMPOSE", "").strip()
        or os.environ.get("CFO_CLAUDE_MODEL", "").strip()
        or COMPOSE_MODEL_FALLBACK_CHAIN[0]
    )
    chain: list[str] = [primary]
    for m in COMPOSE_MODEL_FALLBACK_CHAIN:
        if m and m not in chain:
            chain.append(m)
    return chain


def _haiku_endpoint_url(workspace_client: WorkspaceClient, model: str | None = None) -> str:
    """Resolve the serving-endpoint URL for compose calls. Optional `model`
    override; defaults to the first entry of the resolved chain."""
    chosen_model = model or _resolved_compose_models()[0]
    host = workspace_client.config.host.rstrip("/")
    return f"{host}/serving-endpoints/{chosen_model}/invocations"


def call_haiku(workspace_client: WorkspaceClient, prompt: str) -> str:
    """Single compose call with rate-limit retry + model fallback.

    Tries each model in `_resolved_compose_models()` in order. For each, on
    a 429 or 5xx response, sleeps with exponential backoff and retries up to
    `COMPOSE_RETRY_ATTEMPTS_PER_MODEL` times. If all retries on a model fail
    with rate-limit/server errors, cascades to the next model. Non-retryable
    errors (4xx other than 429) raise immediately — those are programming
    bugs, not capacity issues.

    Returns the message content string. Function name kept as `call_haiku`
    for backward compatibility with the many call sites; the rest of the
    code base doesn't need to know about the fallback chain.
    """
    import random as _rand  # local alias to avoid module-level dependency surprise
    import time as _time

    headers = workspace_client.config.authenticate()
    headers["Content-Type"] = "application/json"
    body = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 4096}
    host = workspace_client.config.host.rstrip("/")

    last_exc: Exception | None = None
    for tier_idx, model in enumerate(_resolved_compose_models()):
        url = f"{host}/serving-endpoints/{model}/invocations"
        for attempt in range(COMPOSE_RETRY_ATTEMPTS_PER_MODEL):
            r = requests.post(url, headers=headers, json=body, timeout=120)
            if r.status_code == 200:
                data = r.json()
                text = (data.get("choices") or [{}])[0].get("message", {}).get("content", "") or ""
                if tier_idx > 0:
                    # Surface the fallback path so logs make the cascade visible.
                    print(f"  [compose] succeeded via fallback model '{model}' (tier {tier_idx + 1}, attempt {attempt + 1})")
                return text

            # Decide: retry-same-model, fallback-next-model, or raise.
            err_preview = (r.text or "")[:300]
            if r.status_code in _RATE_LIMIT_STATUS_CODES:
                last_exc = RuntimeError(
                    f"Compose endpoint error ({r.status_code}) at {url}: {err_preview}"
                )
                if attempt < COMPOSE_RETRY_ATTEMPTS_PER_MODEL - 1:
                    # Retry same model after exponential-backoff + jitter.
                    sleep_s = COMPOSE_BACKOFF_BASE_SECONDS * (attempt + 1) + _rand.uniform(0, 5)
                    print(f"  [compose] {r.status_code} on '{model}' attempt {attempt + 1}; sleeping {sleep_s:.0f}s then retrying same model...")
                    _time.sleep(sleep_s)
                    continue
                # Exhausted retries on this model → break inner loop, cascade to next.
                print(f"  [compose] '{model}' exhausted {COMPOSE_RETRY_ATTEMPTS_PER_MODEL} retries with {r.status_code}; cascading to next fallback model")
                break
            else:
                # Non-retryable status (e.g. 400 bad prompt, 401 auth). Raise.
                raise RuntimeError(
                    f"Compose endpoint error ({r.status_code}) at {url}: {err_preview}"
                )

    # All models in the chain exhausted.
    raise last_exc or RuntimeError("Compose endpoint failed across all fallback models with no exception captured")


def parse_json_with_fence_strip(text: str) -> dict:
    """Parse Haiku JSON output, defensively stripping ```json fences if present."""
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"No JSON object found in Haiku output. Preview: {stripped[:300]}")
    parsed = json.loads(stripped[start : end + 1])
    # Normalize magnitude formatting before returning. Belt-and-suspenders against
    # the LLM emitting "$0.73B" when the value is < $1B — the prompt rule says to
    # step down units, but compliance under filtered (smaller-magnitude) scopes is
    # inconsistent. Post-processor enforces deterministically.
    return _normalize_magnitudes(parsed)


# Compiled once. Matches dollar values where the integer part is exactly 0,
# i.e. the LLM picked too large a unit.
_RE_BAD_B = re.compile(r"\$0\.(\d+)B\b")
_RE_BAD_M = re.compile(r"\$0\.(\d+)M\b")
_RE_BAD_K = re.compile(r"\$0\.(\d+)K\b")


def _rewrite_magnitude_str(s: str) -> str:
    """Convert "$0.73B" → "$730.00M", "$0.04M" → "$40.00K", "$0.5K" → "$500"."""
    def b_to_m(m):
        # 0.XX billions × 1000 = XXX millions
        val = float("0." + m.group(1)) * 1000
        return f"${val:.2f}M"

    def m_to_k(m):
        val = float("0." + m.group(1)) * 1000
        return f"${val:.2f}K"

    def k_to_dollars(m):
        val = float("0." + m.group(1)) * 1000
        return f"${val:.0f}"

    s = _RE_BAD_B.sub(b_to_m, s)
    s = _RE_BAD_M.sub(m_to_k, s)
    s = _RE_BAD_K.sub(k_to_dollars, s)
    return s


def _normalize_magnitudes(obj):
    """Recursively walk a parsed JSON object and rewrite any "$0.XX[BMK]"
    strings to the next-smaller unit. Mutates dicts/lists in place."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = _normalize_magnitudes(v)
        return obj
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            obj[i] = _normalize_magnitudes(v)
        return obj
    if isinstance(obj, str):
        return _rewrite_magnitude_str(obj)
    return obj


# ----------------------------------------------------------------------------
# Compose-and-return — used by both the live-filter Flask endpoint AND the
# firmwide orchestrator.
# ----------------------------------------------------------------------------

PERSONA_PIPELINES = {
    "admin":   (pull_admin_data,   build_admin_prompt),
    "finance": (pull_finance_data, build_finance_prompt),
    "hr":      (pull_hr_data,      build_hr_prompt),
}


def stamp_canonical_kpi_values(persona: str, data: dict, payload: dict) -> dict:
    """Override the `value` field on each headline KPI insight with the exact
    SQL-derived value from the orchestrator's `data` dict. This is a
    deterministic guard against Haiku/Opus small-number drift (e.g. writing
    "2,483 partners" in the value field when SQL returned 2,466). The value
    field is what renders as the big tile number on the Executive Summary —
    those numbers MUST match the dashboard tile, no exceptions.

    Narrative prose is intentionally left untouched; it can still have
    cosmetic drift, but the headline tile is locked to SQL truth. The dashboard
    tile reads the same underlying data via its own SQL, so this guarantees
    KPI-tile↔dashboard-tile agreement.

    Each persona's 4 headline insights are stamped in canonical order:
      - admin:   1=Revenue vs Budget, 2=Project Gross Margins, 3=RPP, 4=Expenses
      - finance: 1=DSO, 2=DPO, 3=Top Unpaid Client, 4=Top Unpaid Vendor
      - hr:      1=Utilization, 2=Partner Headcount, 3=Bench Cost, 4=Low Util HC
    Insight order is also enforced by the per-persona prompts, so the slot
    indices below are stable across runs.
    """
    insights = payload.get("insights") or []
    if len(insights) < 4 or not isinstance(insights[0], dict):
        return payload

    def _set(idx: int, formatted: str) -> None:
        if idx < len(insights) and isinstance(insights[idx], dict):
            insights[idx]["value"] = formatted

    try:
        if persona == "admin":
            rev = data.get("kpi_revenue") or {}
            m = data.get("kpi_margin") or {}
            rpp = data.get("kpi_rev_per_partner") or {}
            exp = data.get("kpi_expenses") or {}
            if rev.get("rev_b") is not None:
                _set(0, f"${rev['rev_b']:.2f}B")
            if m.get("cur_pct") is not None:
                _set(1, f"{m['cur_pct']:.2f}%")
            if rpp.get("cur_m") is not None:
                _set(2, f"${rpp['cur_m']:.2f}M")
            if exp.get("exp_m") is not None:
                _set(3, f"${exp['exp_m']:.2f}M")
        elif persona == "finance":
            dso = data.get("kpi_dso") or {}
            dpo = data.get("kpi_dpo") or {}
            top_c = (data.get("top_unpaid_clients") or [None])[0]
            top_v = (data.get("top_unpaid_vendors") or [None])[0]
            if dso.get("dso") is not None:
                _set(0, f"{dso['dso']:.1f} days")
            if dpo.get("dpo") is not None:
                _set(1, f"{dpo['dpo']:.1f} days")
            if top_c and len(top_c) > 1 and top_c[1] is not None:
                _set(2, f"${float(top_c[1]):.2f}M")
            if top_v and len(top_v) > 1 and top_v[1] is not None:
                _set(3, f"${float(top_v[1]):.2f}M")
        elif persona == "hr":
            u = data.get("kpi_utilization") or {}
            p = data.get("kpi_partner_headcount") or {}
            b = data.get("kpi_bench_cost") or {}
            lu = data.get("kpi_low_util") or {}
            if u.get("cur_pct") is not None:
                _set(0, f"{u['cur_pct']:.2f}%")
            if p.get("cur") is not None:
                _set(1, f"{int(p['cur']):,} partners")
            if b.get("cur_m") is not None:
                _set(2, f"${b['cur_m']:.2f}M")
            if lu.get("cur") is not None:
                _set(3, f"{int(lu['cur']):,} employees")
    except (TypeError, ValueError, KeyError) as e:
        # Defensive: if a data field is unexpectedly shaped, leave the LLM's
        # value in place rather than crash the whole orchestrator.
        import logging
        logging.warning(f"[stamp_canonical_kpi_values] {persona}: {type(e).__name__}: {e}")

    return payload


def extract_prose_from_payload(payload: dict) -> str:
    """Concatenate every prose field in a composed persona payload into one
    string suitable for regex-probing. Used by the inline retry path in
    `run_firmwide_persona` and by the standalone `validate_consistency`
    task to scan `cached_insights` rows (not just `cached_agent_payload`)."""
    if not isinstance(payload, dict):
        return ""
    parts: list[str] = []
    for ins in (payload.get("insights") or []):
        if not isinstance(ins, dict):
            continue
        for field in ("headline", "value", "narrative"):
            v = ins.get(field)
            if v:
                parts.append(str(v))
    for a in (payload.get("action_areas") or []):
        if not isinstance(a, dict):
            continue
        for field in ("headline", "narrative"):
            v = a.get(field)
            if v:
                parts.append(str(v))
    for c in (payload.get("bottom_chips") or []):
        if isinstance(c, dict):
            t = c.get("text") or c.get("question")
            if t:
                parts.append(str(t))
        elif isinstance(c, str):
            parts.append(c)
    return "\n".join(parts)


def compose_for_persona_with_filters(
    persona: str,
    filters: Optional[dict],
    warehouse_id: str,
    sdk_workspace_client: WorkspaceClient,
) -> dict:
    """Pull data, build prompt, call Haiku, return parsed JSON.

    Args:
        persona: 'admin' | 'finance' | 'hr'.
        filters: optional dict like {"region": "EMEA"}; values of "All" / empty are skipped.
        warehouse_id: SQL warehouse ID. If non-empty, exported to SQL_WAREHOUSE_ID
            so `_run_sql` picks it up.
        sdk_workspace_client: WorkspaceClient used for BOTH the SQL Statement
            Execution API AND the Haiku serving-endpoint call.

    Returns:
        Parsed Haiku JSON: {'insights': [...], 'action_areas': [...], 'bottom_chips': [...]}.

    Raises:
        ValueError on unknown persona, json.JSONDecodeError / ValueError on bad
        Haiku output, RuntimeError on Haiku HTTP failure.
    """
    if persona not in PERSONA_PIPELINES:
        raise ValueError(f"Unknown persona: {persona!r}; expected one of {list(PERSONA_PIPELINES)}")

    # If caller passes an explicit warehouse_id, make sure the SQL helper picks
    # it up. This keeps the contract simple: pass warehouse_id, get filtered data.
    if warehouse_id:
        os.environ.setdefault("SQL_WAREHOUSE_ID", warehouse_id)
        if not os.environ.get("SQL_WAREHOUSE_ID"):
            os.environ["SQL_WAREHOUSE_ID"] = warehouse_id

    pull_fn, prompt_fn = PERSONA_PIPELINES[persona]
    data = pull_fn(sdk_workspace_client, filters)
    prompt = prompt_fn(data, filters)
    raw_text = call_haiku(sdk_workspace_client, prompt)
    return parse_json_with_fence_strip(raw_text)


# ----------------------------------------------------------------------------
# Page-scoped chip generation for the deepdive pages (Admin Overview, Finance
# Overview). These chips are stored under persona='_shared' and slot_type
# 'bottom_chip_admin' / 'bottom_chip_finance' — see persona_insights_reader's
# get_bottom_chips_for_page() and app.py's /api/get-page-chips endpoint.
#
# The deepdive pages embed Lakeview dashboards that drill into the same
# underlying data as the persona Executive Summary tiles, but at greater
# detail. So we reuse the persona pull functions and just swap in a
# page-scoped chip prompt that explicitly bans the failure modes we saw:
# compound multi-part questions, forecasting tails, and questions that
# can't be answered by one Genie SQL query.
# ----------------------------------------------------------------------------

# Each deepdive page maps to the persona pull it reuses + the ACTUAL list of
# Lakeview dashboard tiles shown on that page. Chips must drill into one of
# these tiles — anchoring to real visible content, not arbitrary topics like
# "utilization" that don't appear on the admin dashboard. Source: the
# corresponding `dashboards/<page>_*.lvdash.json` widget displayNames.
PAGE_TILES = {
    "admin": [
        "Accrued Revenues", "Billable Revenue", "Projected Revenue",
        "Partnerships Revenue", "Products Revenue", "Other Revenue",
        "Industry Revenue Breakdown", "Regional Revenue Trends",
        "Billable Expenses", "Budgeted Expenses", "Corporate", "Marketing",
        "Regional Expense Trends", "Project Gross Margins",
        "Partner Headcount", "Revenue per Partner", "Practice Area Breakdown",
    ],
    "finance": [
        "Enterprise Revenues", "Enterprise Margins", "Billable Revenue",
        "Billable Revenue - Forecast", "Partnerships Revenue", "Products Revenue",
        "Other Revenue", "Days Sales Outstanding",
        "Firmwide Receivables by Aging", "Invoice Receivables", "Client Invoices",
        "Firmwide Payables by Status", "Billable Expenses", "Budgeted Expenses",
        "Corporate Expenses", "Marketing Expenses",
        "IT & Technology", "Contractors", "Non-FTE Vendors",
    ],
}

PAGE_TO_PERSONA_PULL = {
    "admin":   ("admin",   "Admin Overview deepdive page"),
    "finance": ("finance", "Finance Overview deepdive page"),
}


def build_page_chips_prompt(page: str, data: dict) -> str:
    """Prompt Haiku to generate 3 deepdive-page chip questions for `page`.

    These chips power the AI Assistant modal on the Admin Overview / Finance
    Overview pages. Each chip MUST anchor to a tile actually visible on that
    page — no inventing topics that aren't on the dashboard.
    """
    if page not in PAGE_TO_PERSONA_PULL:
        raise ValueError(f"Unknown page {page!r}; expected one of {list(PAGE_TO_PERSONA_PULL)}")
    _, page_description = PAGE_TO_PERSONA_PULL[page]
    tiles = PAGE_TILES[page]
    tile_list = "\n".join(f"  - {t}" for t in tiles)

    return f"""You are generating 3 drill-down "Try asking..." chip questions for the {page_description} in a Tier 1 / Big 4 consulting firm's CFO Operations Platform.

THE PAGE THE USER IS LOOKING AT SHOWS THESE TILES (and ONLY these tiles):
{tile_list}

Each chip MUST be anchored to ONE of these tiles. Do not invent topics that
aren't represented here. The user is looking AT this dashboard — chips are
the natural follow-up question for a tile they're already seeing.

HARD REQUIREMENTS — every chip MUST satisfy ALL of these:

1. SINGLE-SHOT, SQL-ANSWERABLE. ONE Genie SQL query answers it. NO compound
   multi-part questions stitched with "and" or "—". NO forecasting tails
   ("is it sustainable", "will it continue"). NO opinion questions ("should
   we", "is this a concern").

2. ANCHORED TO A REAL TILE. The chip must drill into the data behind one of
   the tiles listed above. Different chips must drill into different tiles.

3. ABSOLUTELY NO NAMED ENTITIES IN CHIP TEXT — ZERO EXCEPTIONS. Do NOT name
   any specific city, office, practice, partner, client, or industry in the
   chip text. The user is on a dashboard that already shows them WHICH
   entities are outliers — the chip's job is to ask the CATEGORY of question,
   and Genie surfaces the named entities when it runs the SQL.

   GOOD: "Which offices have the largest expense overage versus budget?"
   BAD:  "Which cost categories within London and Sao Paulo are responsible
          for the 30%+ expense overages?" — locks Genie into ONE specific
          two-city subset; if Munich or Tokyo also have 30%+ overages they're
          invisible. Pre-cached chip text is FROZEN across the entire demo
          window — naming entities means the chip can't gracefully adapt as
          the data shifts.

   GOOD: "Which clients account for the largest share of 90+ day receivables?"
   BAD:  "What's Tencent's exposure to 90+ day aging?" — single-client chip,
          dies if Tencent isn't the relevant story this cycle.

   This rule has NO EXCEPTIONS. Even if the supporting data dict surfaces a
   single clear outlier, leave it for Genie to discover at query time — the
   chip stays generic and reusable across demo cycles. The ONLY entity-class
   words allowed in chip text are category-level: "offices", "practices",
   "regions", "industries", "clients", "partners", "projects" — never the
   specific names of those entities.

4. ONE SENTENCE, ≤ 22 WORDS, ENDS IN `?`. Terse. No preamble like "Given that
   revenue grew..." — just the question.

5. NO COLLOQUIALISMS. Use measured language. Prefer "Which", "What", "How",
   "Where" question openings.

DATA CONTEXT (use ONLY if it surfaces a clear, narrative-anchored outlier
worth naming in chip text — otherwise keep chips category-level):
{_compact_data_for_prompt(data)}

EXAMPLES OF GOOD CHIPS (model after these — category-level, tile-anchored):
  "Which offices have the largest revenue shortfall against annual budget?"
  "How is partner headcount split between lateral hires, internal promotions, and departures over the last 6 fiscal months?"
  "What is the margin gap between fixed-price and time-and-materials engagements by practice area?"
  "Which practices show the worst project gross margin compression QoQ?"
  "Which clients account for the largest share of 90+ day receivables?"

EXAMPLES OF BAD CHIPS (do NOT generate):
  "How do utilization rates compare between Sao Paulo, Mumbai, and Shanghai for consultant levels?" — utilization isn't on this page AND city picks are arbitrary
  "Which practices account for the largest expense variance in Bangkok, London, and Sao Paulo offices?" — arbitrary city picks
  "EMEA Strategy & Consulting beat forecast by $44M — is it sustainable into next month?" — compound + forecasting
  "Should we increase headcount in Bangkok?" — opinion-based

Return ONLY raw JSON, no markdown fences:
{{"bottom_chips": [{{"question_text": "..."}}, {{"question_text": "..."}}, {{"question_text": "..."}}]}}
"""


def _compact_data_for_prompt(data: dict) -> str:
    """Compact the persona pull data into a short anchor section for the chip
    prompt. We surface only the lists of supporting top-N tables (top offices
    by overage, top practice margins) because those are the only places clear
    outliers could justify naming a specific entity in a chip."""
    import json as _json
    keep = {}
    for k, v in (data or {}).items():
        if isinstance(v, list) and v and isinstance(v[0], (list, tuple)) and len(v) <= 5:
            keep[k] = v
    return _json.dumps(keep, default=str)[:1500] or "(no supporting tables surfaced)"


def compose_page_chips(
    page: str,
    warehouse_id: str,
    sdk_workspace_client: WorkspaceClient,
) -> list[dict]:
    """Generate 3 deepdive-page chip questions for `page` (admin | finance).

    Returns: list of {'question_text': '...'} dicts (exactly 3, or whatever
    Haiku returned if it deviated). Caller writes them to gold_persona_insights
    with persona='_shared', slot_type=f'bottom_chip_{page}'.
    """
    if page not in PAGE_TO_PERSONA_PULL:
        raise ValueError(f"Unknown page {page!r}")
    persona_for_pull, _ = PAGE_TO_PERSONA_PULL[page]

    if warehouse_id:
        os.environ.setdefault("SQL_WAREHOUSE_ID", warehouse_id)
        if not os.environ.get("SQL_WAREHOUSE_ID"):
            os.environ["SQL_WAREHOUSE_ID"] = warehouse_id

    pull_fn, _ = PERSONA_PIPELINES[persona_for_pull]
    data = pull_fn(sdk_workspace_client, None)  # firmwide pull, no filters
    prompt = build_page_chips_prompt(page, data)
    raw_text = call_haiku(sdk_workspace_client, prompt)
    parsed = parse_json_with_fence_strip(raw_text)
    return [c for c in (parsed.get("bottom_chips") or []) if isinstance(c, dict)][:3]


# ----------------------------------------------------------------------------
# Orchestrator-time chip pre-caching: Opus decomposition + parallel Genie firing
# ----------------------------------------------------------------------------
# For each chip we want a deterministic, demo-stable cache of supporting Genie
# answers. Strategy:
#   1. Opus decomposes the chip text into exactly 2 supporting sub-queries
#      (single SQL each, different angles on the chip's underlying "why").
#   2. Both sub-queries fire through Genie IN PARALLEL — captures sql + raw_data
#      + narrative + row_count from each.
#   3. The resulting bundle is serialized into `cached_agent_payload`. Click-time
#      reads this back and Haiku synthesizes a multi-section root-cause analysis
#      from the 2 sub-results.
# Determinism: this all happens ONCE per chip per orchestrator run. The cache is
# stable for the entire demo window between job reruns.
# Matches the conceptual model of the original PRESET_QUESTION_MAPPINGS dict
# (2 hand-authored sub-queries per chip), generalized so it scales to
# LLM-generated chip texts.

def _orchestrator_genie_query(workspace_client: WorkspaceClient, space_id: str, question: str,
                              max_poll_sec: int = 120) -> dict:
    """Fire ONE Genie question via SDK, poll to completion, return
    {sub_question, sql, narrative, raw_data, row_count}.

    Uses the Databricks SDK GenieAPI (not the app's REST helper) since this
    runs from a notebook context where the app-runtime helpers aren't loaded.
    """
    out = {"sub_question": question, "sql": "", "narrative": "", "raw_data": [], "row_count": None}
    try:
        conv = workspace_client.genie.start_conversation(space_id=space_id, content=question)
        conversation_id = conv.conversation_id
        message_id = conv.message_id

        start = time.time()
        msg = None
        while time.time() - start < max_poll_sec:
            time.sleep(2.0)
            msg = workspace_client.genie.get_message(
                space_id=space_id, conversation_id=conversation_id, message_id=message_id,
            )
            status = getattr(msg.status, "value", str(msg.status)) if msg.status else ""
            if status in ("COMPLETED", "FAILED", "CANCELLED"):
                break

        if msg is None:
            return out

        attachments = msg.attachments or []
        narrative_parts = []
        for att in attachments:
            text = getattr(att, "text", None)
            query = getattr(att, "query", None)
            if text and getattr(text, "content", None):
                narrative_parts.append(text.content)
            if query and getattr(query, "query", None):
                out["sql"] = query.query
                try:
                    result = workspace_client.genie.get_message_query_result_by_attachment(
                        space_id=space_id,
                        conversation_id=conversation_id,
                        message_id=message_id,
                        attachment_id=att.attachment_id,
                    )
                    if result and result.statement_response and result.statement_response.result:
                        data_array = result.statement_response.result.data_array or []
                        out["raw_data"] = data_array[:50]  # cap to keep payload reasonable
                        out["row_count"] = len(data_array)
                except Exception as _e:
                    pass  # SQL captured even if data extraction fails
        out["narrative"] = "\n".join(narrative_parts)
    except Exception as e:
        out["narrative"] = f"Genie query error: {type(e).__name__}: {e}"
    return out


def decompose_chip_to_subqueries(workspace_client: WorkspaceClient, chip_text: str,
                                  page_context_tiles: list[str],
                                  agent_model_endpoint_url: str,
                                  agent_model_name: str,
                                  n_subqueries: int = 3) -> list[str]:
    """Use the Opus agent model to decompose a chip into 2-3 supporting sub-queries.

    The agent's job is purely planning: given the chip and the page context,
    generate 2-3 concrete Genie data-lookup questions whose answers, together,
    give Haiku enough data to write a root-cause synthesis. Each sub-query
    targets a different angle. `n_subqueries` caps the maximum returned.
    """
    tile_list = "\n".join(f"  - {t}" for t in page_context_tiles)
    # Mirrors the prompt pattern of the original orchestrator (cross-referenced
    # from the cfo-app-customer notebook): emphasizes GROUNDED DATA LOOKUPS, not
    # philosophical rephrasings of the chip question. Allows the model to choose
    # 2 or 3 sub-queries based on what the question genuinely needs.
    decomp_prompt = f"""You are planning sub-queries for a Genie agent.

The user clicked this question on the CFO Operations Platform:
"{chip_text}"

The user is on a dashboard showing these tiles:
{tile_list}

Plan 2-3 focused Genie sub-questions that, taken together, will surface the data needed for Haiku to compose a multi-section root-cause analysis answering the user's question.

Each sub-question MUST:
- Be a single, focused investigation Genie can answer with ONE SQL query against gold tables (regional P&L by month, project profitability, employees + utilization, AR/AP aging, partner economics)
- Reference NAMED ENTITIES + COMPLETE fiscal months only (never include the in-progress current month)
- Be GROUNDED — phrase as DATA LOOKUPS, not philosophical (e.g. "What is the monthly DSO trend for the US region over the last 6 complete fiscal months, by office?" NOT "Why is DSO struggling?")
- Cover a DIFFERENT angle from the others (don't ask 2 versions of the same thing)
- Specify the time window explicitly so Genie doesn't guess

Return ONLY raw JSON, no markdown fences. Pick 2 sub-queries if 2 angles suffice; 3 if a third angle materially adds to the root-cause story.

Example shape:
{{"sub_queries": ["What is the monthly DSO trend for the US region over the last 6 complete fiscal months, by office?", "Which 5 named clients have the largest aged AR balances in the US region for the most recent complete fiscal month?", "What is the AR aging bucket distribution for these top 5 clients in the last complete month?"]}}
"""
    # Use AI Gateway /chat/completions — auto-provisioned in every workspace,
    # customer-portable. Body shape matches the starter-code example exactly:
    # messages + model + max_tokens. No temperature / extra fields (some
    # gateway model wrappers reject anything not in the OpenAI core schema).
    base = agent_model_endpoint_url.rstrip("/")
    endpoint = base if base.endswith("/chat/completions") else f"{base}/chat/completions"
    body = {
        "messages": [{"role": "user", "content": decomp_prompt}],
        "model": agent_model_name,
        "max_tokens": 800,
    }
    headers = workspace_client.config.authenticate()
    headers["Content-Type"] = "application/json"
    try:
        resp = requests.post(endpoint, headers=headers, json=body, timeout=120)
        if resp.status_code != 200:
            # Include the response body in the error so we can diagnose 400s
            # like "model not enabled in gateway" or unexpected param errors.
            raise RuntimeError(
                f"AI Gateway error ({resp.status_code}) at {endpoint}: {resp.text[:500]}"
            )
        text = (resp.json().get("choices") or [{}])[0].get("message", {}).get("content", "") or ""
        parsed = parse_json_with_fence_strip(text)
        sub_qs = [s for s in (parsed.get("sub_queries") or []) if isinstance(s, str) and s.strip()]
        return sub_qs[:n_subqueries]
    except Exception as e:
        # Decomposition failure is non-fatal — caller logs and skips this chip.
        raise RuntimeError(f"Opus decomposition failed: {type(e).__name__}: {e}")


def generate_chip_followup_texts(workspace_client: WorkspaceClient, chip_text: str,
                                  main_payload: dict, agent_model_endpoint_url: str,
                                  agent_model_name: str, n: int = 2) -> list[str]:
    """Use the COMPOSE model (Haiku) to generate N follow-up question texts based on
    the chip + its cached sub-results. The follow-ups should drill INTO specific
    patterns the main payload surfaced (named entities, deltas, anomalies).

    These follow-up texts are pre-fired at Genie by precache_chip_followups so that
    when the user clicks one, it's a cache hit instead of a live ~20-30s round-trip.
    """
    import json as _json
    sub_summary = []
    for sub in (main_payload.get("sub_question_results") or [])[:3]:
        sub_summary.append({
            "sub_question": (sub.get("sub_question") or "")[:200],
            "narrative": (sub.get("narrative") or "")[:500],
            "row_count": sub.get("row_count"),
        })
    context_json = _json.dumps(sub_summary, default=str)[:2500]

    prompt = f"""A CFO clicked this chip question on the operations platform:

CHIP QUESTION: {chip_text}

The supporting Genie sub-queries returned this context:
{context_json}

Generate exactly {n} drill-down follow-up questions a CFO would naturally ask AFTER seeing the answer to this chip. Each follow-up MUST:
- Reference a SPECIFIC named entity the sub-results revealed (office name, practice name, partner level, client name, project name)
- Be answerable by ONE Genie SQL query — NO compound/multi-part, NO forecasting tails
- Be ≤ 18 words, end in `?`
- Cover a DIFFERENT angle from the other follow-up(s)
- Be a GROUNDED data lookup, not philosophical

NUMBER FIDELITY (HARD STOP — most common failure mode):
- If a follow-up cites a SPECIFIC number (percentage, dollar amount, count, days, ratio, delta), that EXACT number — same digits, same units — MUST appear verbatim in one of the sub-result narratives above.
- DO NOT include numbers that "sound right," derived numbers, averaged numbers, or numbers that feel like they fit the topic. Only verbatim-from-narrative numbers.
- When in doubt, REMOVE the number from the follow-up entirely. A number-free question that names an entity and a direction is always safe; a number-laden question with a fabricated value will be answered by Genie pulling the real data, the real data won't match, and the synthesis will (correctly) call out the inconsistency — making the assistant say "your premise is wrong" to the customer.

NUMBER-ENTITY ATTRIBUTION (HARD STOP — subtle failure mode):
- If you cite a number in a follow-up AND attach it to a specific entity (office,
  practice, region, customer, partner), the entity-number pairing in the chip
  MUST match the entity-number pairing in the source narrative.
- WRONG: source narrative says "firmwide Senior Partner utilization is 34.34%",
  chip says "Why is Senior Partner utilization at 34.34% **in Strategy & Consulting**
  versus other practices?" — the 34.34% is firmwide, not S&C-specific. Reattaching
  a firmwide number to a specific practice misleads the user into thinking that's
  the per-practice number, which Genie's drill-down will (correctly) contradict.
- RIGHT: if the narrative says "the firmwide rate is 34.34% while Strategy & Consulting
  is 38.5%", then the chip can say "Why is S&C SP utilization at 38.5% above
  firmwide 34.34%?" (both numbers + their entities match the narrative).
- Safest pattern: when in doubt about an entity-number pairing, omit the number
  and just name the entity. "Why does Strategy & Consulting have the highest
  Senior Partner utilization?" is always safe.

GROUNDING RULES — answerability matters more than novelty:
- PREFER follow-ups that drill into entities/dimensions named in the sub-result NARRATIVES (those came from actual SQL results and will reproduce cleanly).
- PREFER follow-ups that stay within the SAME data slice the chip already exercised. If the chip cut by region, follow-ups should drill into a specific region — NOT pivot to a different aggregation grain (e.g., cost-center, sub-practice) that the source tables may not support.
- AVOID follow-ups that probe a number a sub-result CAVEATED. Look in the narratives for phrases like "could not reconcile", "no rows", "zero rows", "data limitation", "should be validated", "sub-query returned no", "thin result set", "limited matches", "no defensible answer". If a narrative flagged its number as unreliable, a follow-up about that number will dead-end on the same caveat — and users hate that.
- AVOID follow-ups that ask "why X" when X is a difference between two sub-query results at DIFFERENT grains (e.g., one query returned firmwide totals, another returned office-level rows — a "why is firmwide X% different from office Y%" question hits a grain mismatch).
- If you can't find a clean drill-down that satisfies the rules above, prefer a single broader question (e.g., "How does <entity> compare on <same metric> over a longer window?") over fabricating a sharp-looking question that won't reproduce.

Return ONLY raw JSON, no markdown fences:
{{"followups": ["...", "..."]}}
"""
    # Route through call_haiku() so we inherit:
    #   - Retry-with-backoff on 429 (Haiku rate limit)
    #   - Fallback cascade to Sonnet 4.5 → Sonnet 4.6 when Haiku exhausts retries
    #   - Consistent error formatting
    # The previous direct requests.post() bypassed all of this and silently
    # returned [] on any non-200, which is why most chip follow-ups came back
    # as "cached 0 follow-up(s)" with no diagnostic info during the recent
    # rate-limit bursts. Diagnostic prints below also distinguish the 4
    # failure modes (API error / exception / JSON parse / empty LLM output)
    # so we can tell which case produced the empty list.
    try:
        text = call_haiku(workspace_client, prompt)
    except Exception as e:
        print(f"      ⚠ followup-text LLM call FAILED ({type(e).__name__}: {str(e)[:200]}) — returning empty list")
        return []
    if not text or not text.strip():
        print(f"      ⚠ followup-text LLM returned EMPTY content (model honored prompt but said nothing) — returning empty list")
        return []
    try:
        parsed = parse_json_with_fence_strip(text)
    except Exception as e:
        print(f"      ⚠ followup-text JSON PARSE failed ({type(e).__name__}: {str(e)[:120]}); raw preview: {text[:200]!r} — returning empty list")
        return []
    fus = [s for s in (parsed.get('followups') or []) if isinstance(s, str) and s.strip()]
    if not fus:
        print(f"      ⚠ followup-text LLM RETURNED VALID JSON but 'followups' list was empty/invalid; raw: {text[:200]!r}")
        return []
    return fus[:n]


def regenerate_followup_with_feedback(
    workspace_client: WorkspaceClient,
    chip_text: str,
    main_payload: dict,
    rejected_followup: str,
    rejection_reason: str,
    keep_followups: list[str],
    agent_model_endpoint_url: str,
    agent_model_name: str,
) -> str:
    """Generate ONE replacement follow-up for a previously-rejected hollow one,
    staying on-topic with the parent chip but avoiding the entity/grain that
    caused the rejection.

    This is the first tier of the validate-and-regenerate fallback chain.
    Goal: preserve conversational flow (follow-up should still feel like a
    natural drill-in from the parent chip's findings) while sidestepping the
    specific problem that made the previous attempt return zero rows.

    `keep_followups` are the OTHER follow-ups for the same parent chip that
    DID validate — the regenerator should suggest something distinct from
    those, not a duplicate angle.

    Returns "" on failure; caller falls through to topic-tagged / generic backups.
    """
    import json as _json
    sub_summary = []
    for sub in (main_payload.get("sub_question_results") or [])[:3]:
        sub_summary.append({
            "sub_question": (sub.get("sub_question") or "")[:200],
            "narrative": (sub.get("narrative") or "")[:500],
            "row_count": sub.get("row_count"),
        })
    context_json = _json.dumps(sub_summary, default=str)[:2500]
    keep_block = "\n".join(f"  - {q}" for q in keep_followups) if keep_followups else "  (none)"

    prompt = f"""A CFO clicked this chip question on the operations platform:

CHIP QUESTION: {chip_text}

The supporting Genie sub-queries returned this context:
{context_json}

A previous follow-up was generated but had to be REJECTED because:
  REJECTED FOLLOW-UP: "{rejected_followup}"
  REASON: {rejection_reason}

Other follow-ups that we'll keep alongside the new one (suggest something DIFFERENT from these):
{keep_block}

Generate exactly 1 REPLACEMENT follow-up. The replacement MUST:
- Stay topically connected to the parent chip's findings (the CFO should not feel
  the conversation jumped to an unrelated topic).
- AVOID the entity / grain / time-window that caused the previous rejection. If
  the previous follow-up referenced a specific named entity that had no data at
  the drill-down grain, your replacement must reference a DIFFERENT entity OR
  pivot to a more aggregate dimension that the sub-result narratives confirm
  has data.
- PREFER drilling into entities/numbers that appear verbatim in the sub-result
  narratives above — those came from actual SQL results and will reproduce
  cleanly when Genie fires them.
- Be ≤ 18 words, end with "?".
- Be a single, answerable, grounded question — no compound/multi-part.

Return ONLY raw JSON, no markdown fences:
{{"followup": "..."}}
"""
    compose_model = (
        os.environ.get("CFO_CLAUDE_MODEL_COMPOSE", "").strip()
        or "databricks-claude-haiku-4-5"
    )
    base = agent_model_endpoint_url.rstrip("/")
    endpoint = base if base.endswith("/chat/completions") else f"{base}/chat/completions"
    body = {
        "messages": [{"role": "user", "content": prompt}],
        "model": compose_model,
        "max_tokens": 250,
    }
    headers = workspace_client.config.authenticate()
    headers["Content-Type"] = "application/json"
    try:
        resp = requests.post(endpoint, headers=headers, json=body, timeout=60)
        if resp.status_code != 200:
            return ""
        text = (resp.json().get("choices") or [{}])[0].get("message", {}).get("content", "") or ""
        parsed = parse_json_with_fence_strip(text)
        fu = (parsed.get("followup") or "").strip()
        return fu
    except Exception:
        return ""


def precache_chip_followups(workspace_client: WorkspaceClient, genie_space_id: str,
                             chip_text: str, main_payload: dict,
                             page_context_tiles: list[str],
                             agent_model_endpoint_url: str, agent_model_name: str,
                             n: int = 2, n_subqueries: int = 3,
                             backup_followups: list[str] | None = None) -> list[dict]:
    """For one parent chip: generate N follow-up question texts (Haiku) + for each
    follow-up, do FULL Opus-decomposition + parallel Genie sub-queries (same depth
    as the parent chip). Returns a list of `{question_text, cached_payload}` dicts.

    Each follow-up's cached_payload has the same multi-subquery shape as the parent
    chip — so when a user clicks the follow-up, click-time synthesis sees the same
    richness of supporting data, not a single-shot Genie answer.

    Side effect: `main_payload['suggested_questions']` is set IN-PLACE to the N
    follow-up texts, so the parent chip's `_replay_with_synthesis` surfaces them
    after rendering the answer (instead of falling back to live Claude generation).

    Cost: ~2x what the old single-shot path used to be (each follow-up now does an
    Opus decompose + 3 parallel Genie sub-queries vs 1 single-shot Genie). The
    follow-ups themselves still run in parallel, so per-chip wall time is roughly
    the same as one full chip's cache build.
    """
    from concurrent.futures import ThreadPoolExecutor

    fu_texts = generate_chip_followup_texts(
        workspace_client=workspace_client,
        chip_text=chip_text,
        main_payload=main_payload,
        agent_model_endpoint_url=agent_model_endpoint_url,
        agent_model_name=agent_model_name,
        n=n,
    )
    if not fu_texts:
        return []

    out: list[dict] = []
    # Fire each follow-up's full multi-subquery cache build in parallel. Each
    # follow-up = one Opus decompose call + n_subqueries Genie sub-queries.
    with ThreadPoolExecutor(max_workers=len(fu_texts)) as executor:
        future_map = {
            executor.submit(
                precache_chip_payload,
                workspace_client,
                genie_space_id,
                ft,
                page_context_tiles,
                agent_model_endpoint_url,
                agent_model_name,
                n_subqueries,
            ): ft
            for ft in fu_texts
        }
        for future, ft in future_map.items():
            try:
                cached_payload = future.result(timeout=420)
            except Exception as e:
                cached_payload = {
                    "sub_question_results": [{
                        "sub_question": ft,
                        "sql": "",
                        "narrative": f"Follow-up pre-cache error: {e}",
                        "raw_data": [],
                        "row_count": 0,
                    }],
                    "suggested_questions": [],
                }
            out.append({
                "question_text": ft,
                "cached_payload": cached_payload,
            })

    # ─── Hollow-response validation + tiered fallback ─────────────────────
    # For each follow-up whose pre-cache returned a hollow payload, fall back
    # in this order:
    #   1. LLM REGENERATION WITH FEEDBACK — re-call Haiku with the parent chip's
    #      results + the rejected follow-up + rejection reason, ask for a
    #      replacement that stays on-topic but avoids the dead-end entity.
    #      Up to MAX_REGEN_ATTEMPTS tries per hollow follow-up.
    #   2. TOPIC-RELEVANT BACKUP — pick from the provided backup pool, since
    #      these are aggregate-framed questions in the same persona/page domain.
    #   3. DROP — if all fallbacks exhaust, omit the follow-up rather than ship
    #      a "no data was returned" cached payload.
    # The key UX property: every PERSISTED follow-up either (a) is topically
    # related to the parent chip (regeneration succeeded) or (b) is a generic
    # but in-domain question from the backup pool. NEVER a hollow cached row.
    MAX_REGEN_ATTEMPTS = 2
    backups_queue = list(backup_followups or [])
    used_backups: set[str] = set()
    used_regen_texts: set[str] = set()

    def _kept_followups(skip_idx: int) -> list[str]:
        """Return texts of the OTHER follow-ups currently in `out` (not at skip_idx).
        Passed to the LLM regenerator so it suggests something distinct."""
        return [
            (out[j] or {}).get("question_text", "")
            for j in range(len(out))
            if j != skip_idx and out[j] is not None
        ]

    for idx in range(len(out)):
        item = out[idx]
        if item is None:
            continue
        hollow, reason = is_hollow_payload(item.get("cached_payload") or {})
        if not hollow:
            continue
        original_ft = item["question_text"]
        print(f"      ⚠ follow-up hollow: '{original_ft[:80]}...' ({reason})")

        # ── Tier 1: LLM regeneration with rejection feedback ─────────────
        replaced = False
        rejected_so_far = original_ft
        last_reason = reason
        for attempt in range(1, MAX_REGEN_ATTEMPTS + 1):
            try:
                new_ft = regenerate_followup_with_feedback(
                    workspace_client=workspace_client,
                    chip_text=chip_text,
                    main_payload=main_payload,
                    rejected_followup=rejected_so_far,
                    rejection_reason=last_reason,
                    keep_followups=_kept_followups(idx),
                    agent_model_endpoint_url=agent_model_endpoint_url,
                    agent_model_name=agent_model_name,
                )
            except Exception as e:
                print(f"      ⚠ regen attempt {attempt} error: {type(e).__name__}: {e}")
                break
            if not new_ft or new_ft in used_regen_texts or new_ft == rejected_so_far:
                print(f"      ⚠ regen attempt {attempt} returned empty/duplicate; trying next tier")
                break
            used_regen_texts.add(new_ft)
            print(f"      ↻ regen attempt {attempt}: '{new_ft[:80]}...'")
            try:
                new_payload = precache_chip_payload(
                    workspace_client=workspace_client,
                    genie_space_id=genie_space_id,
                    chip_text=new_ft,
                    page_context_tiles=page_context_tiles,
                    agent_model_endpoint_url=agent_model_endpoint_url,
                    agent_model_name=agent_model_name,
                    n_subqueries=n_subqueries,
                )
            except Exception as e:
                print(f"      ⚠ regen pre-cache error: {type(e).__name__}: {e}")
                rejected_so_far = new_ft
                last_reason = f"pre-cache error: {e}"
                continue
            new_hollow, new_reason = is_hollow_payload(new_payload)
            if not new_hollow:
                out[idx] = {"question_text": new_ft, "cached_payload": new_payload}
                print(f"      ✓ regen tier succeeded ('{new_ft[:80]}...')")
                replaced = True
                break
            print(f"      ⚠ regen attempt {attempt} also hollow: {new_reason}")
            rejected_so_far = new_ft
            last_reason = new_reason
        if replaced:
            continue

        # ── Tier 2: topic-relevant backup from the provided pool ─────────
        while backups_queue:
            candidate = backups_queue.pop(0)
            if not candidate or candidate in used_backups:
                continue
            used_backups.add(candidate)
            try:
                candidate_payload = precache_chip_payload(
                    workspace_client=workspace_client,
                    genie_space_id=genie_space_id,
                    chip_text=candidate,
                    page_context_tiles=page_context_tiles,
                    agent_model_endpoint_url=agent_model_endpoint_url,
                    agent_model_name=agent_model_name,
                    n_subqueries=n_subqueries,
                )
            except Exception as e:
                print(f"      ⚠ backup '{candidate[:60]}...' pre-cache error: {e}")
                continue
            c_hollow, c_reason = is_hollow_payload(candidate_payload)
            if c_hollow:
                print(f"      ⚠ backup '{candidate[:60]}...' also hollow: {c_reason}")
                continue
            print(f"      ↻ substituted with backup pool: '{candidate[:80]}...'")
            out[idx] = {"question_text": candidate, "cached_payload": candidate_payload}
            replaced = True
            break

        # ── Tier 3: drop the follow-up entirely ──────────────────────────
        if not replaced:
            out[idx] = None
            print(f"      ✗ regen + backups exhausted; dropping hollow follow-up")

    out = [x for x in out if x is not None]
    if not out:
        # All follow-ups were hollow and we had no working backups/regens.
        # The caller will skip persisting follow-up rows for this chip.
        main_payload["suggested_questions"] = []
        return []

    # Attach the (possibly substituted/regenerated) follow-up texts to the
    # main chip's payload so the parent chip's _replay_with_synthesis surfaces
    # them (no live Claude generation needed).
    main_payload["suggested_questions"] = [x["question_text"] for x in out]

    return out


def precache_chip_payload(workspace_client: WorkspaceClient, genie_space_id: str,
                          chip_text: str, page_context_tiles: list[str],
                          agent_model_endpoint_url: str, agent_model_name: str,
                          n_subqueries: int = 2) -> dict:
    """For one chip: Opus decompose → fire N Genie sub-queries IN PARALLEL → return
    a `cached_agent_payload` dict ready to serialize into gold_persona_insights.

    The returned shape matches what `_replay_with_synthesis` expects:
        {"sub_question_results": [{sub_question, sql, narrative, raw_data, row_count}, ...],
         "suggested_questions": []}
    """
    from concurrent.futures import ThreadPoolExecutor

    sub_queries = decompose_chip_to_subqueries(
        workspace_client=workspace_client,
        chip_text=chip_text,
        page_context_tiles=page_context_tiles,
        agent_model_endpoint_url=agent_model_endpoint_url,
        agent_model_name=agent_model_name,
        n_subqueries=n_subqueries,
    )
    if not sub_queries:
        return {"sub_question_results": [], "suggested_questions": []}

    sub_results: list[dict] = []
    with ThreadPoolExecutor(max_workers=len(sub_queries)) as executor:
        futures = [
            executor.submit(_orchestrator_genie_query, workspace_client, genie_space_id, sq)
            for sq in sub_queries
        ]
        for f in futures:
            try:
                sub_results.append(f.result(timeout=180))
            except Exception as e:
                sub_results.append({"sub_question": "", "sql": "", "narrative": f"Genie error: {e}", "raw_data": [], "row_count": 0})

    return {"sub_question_results": sub_results, "suggested_questions": []}


# ─── Hollow-response detector ───────────────────────────────────────────────
# Used by the validate-and-regenerate loop wrapping chip + follow-up pre-caching.
# A "hollow" payload is one where Genie ran sub-queries but the underlying data
# doesn't actually answer the question — typically because the LLM-generated
# question references an entity (project, customer, vendor) that has no records
# at the drill-down grain. Without this check, hollow payloads get persisted and
# the user clicks a chip → instantly gets back vague "no data was returned"
# prose. That UX is worse than live computation.

_HOLLOW_PROSE_PATTERNS = (
    "no data was returned",
    "no records",
    "no employees logged",
    "no billable",
    "none recorded",
    "no recorded",
    "no available data",
    "data was not returned",
    "no financial activity",
)


def is_hollow_payload(payload: dict, min_total_rows: int = 5,
                      min_nonempty_subqueries: int = 2) -> tuple[bool, str]:
    """Returns (is_hollow, reason). A payload is hollow if:
       - It has zero sub-query results, OR
       - The total row count across sub-queries is below min_total_rows, OR
       - Fewer than min_nonempty_subqueries sub-queries returned > 0 rows, OR
       - Any sub-query's narrative matches a "no data" prose pattern.

    Reason string is suitable for feeding back to the LLM as rejection
    feedback ("previous suggestion was rejected because ...") so it can
    regenerate a different candidate.
    """
    if not isinstance(payload, dict):
        return True, "payload not a dict"
    sub_results = payload.get("sub_question_results") or []
    if not sub_results:
        return True, "no sub-query results"

    total_rows = 0
    nonempty = 0
    for r in sub_results:
        if not isinstance(r, dict):
            continue
        rc = r.get("row_count") or 0
        try:
            rc = int(rc)
        except (TypeError, ValueError):
            rc = len(r.get("raw_data") or [])
        if rc > 0:
            nonempty += 1
            total_rows += rc
        narrative = (r.get("narrative") or "").lower()
        for pat in _HOLLOW_PROSE_PATTERNS:
            if pat in narrative:
                return True, f"sub-query narrative contains '{pat}' — data is empty at this grain"

    if total_rows < min_total_rows:
        return True, f"total rows across sub-queries = {total_rows} (< {min_total_rows} minimum)"
    if nonempty < min_nonempty_subqueries:
        return True, f"only {nonempty} of {len(sub_results)} sub-queries returned rows (< {min_nonempty_subqueries} minimum)"

    return False, "ok"


def precache_chip_payload_validated(
    workspace_client: WorkspaceClient,
    genie_space_id: str,
    chip_text: str,
    page_context_tiles: list[str],
    agent_model_endpoint_url: str,
    agent_model_name: str,
    n_subqueries: int = 2,
    max_attempts: int = 3,
    chip_regenerator=None,
) -> tuple[str, dict]:
    """Pre-cache a chip with hollow-response validation + regeneration.

    On a hollow response, asks `chip_regenerator(rejected_text, reason)` for a
    new chip candidate and tries again. Returns (final_chip_text, payload). If
    all attempts return hollow payloads, returns the LAST attempt anyway — the
    caller can fall back to backup_questions.yml.

    `chip_regenerator` is optional: when None, we return after the first attempt
    without retrying. Set it to a callable that returns the new chip string.
    """
    current_text = chip_text
    last_payload = None
    for attempt in range(1, max_attempts + 1):
        payload = precache_chip_payload(
            workspace_client=workspace_client,
            genie_space_id=genie_space_id,
            chip_text=current_text,
            page_context_tiles=page_context_tiles,
            agent_model_endpoint_url=agent_model_endpoint_url,
            agent_model_name=agent_model_name,
            n_subqueries=n_subqueries,
        )
        last_payload = payload
        hollow, reason = is_hollow_payload(payload)
        if not hollow:
            return current_text, payload
        print(f"      ⚠ attempt {attempt}/{max_attempts} hollow: {reason}")
        if chip_regenerator is None or attempt == max_attempts:
            break
        try:
            new_text = chip_regenerator(current_text, reason)
        except Exception as e:
            print(f"      ⚠ chip_regenerator failed ({type(e).__name__}: {e}); stopping retries")
            break
        if not new_text or new_text == current_text:
            print(f"      ⚠ regenerator returned empty / unchanged text; stopping retries")
            break
        print(f"      ↻ regenerated chip: '{new_text[:80]}...'")
        current_text = new_text
    return current_text, (last_payload or {"sub_question_results": [], "suggested_questions": []})
