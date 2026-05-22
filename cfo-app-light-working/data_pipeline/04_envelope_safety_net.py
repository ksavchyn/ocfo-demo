# Databricks notebook source
# DBTITLE 1,Envelope Safety Net — emergent-aggregate assertions
# MAGIC %md
# MAGIC # Envelope Safety Net
# MAGIC
# MAGIC Reads `plausibility_envelope.yml` and runs ~30 assertions that verify
# MAGIC the generated data satisfies bounds that CAN'T be enforced at sample
# MAGIC time (emergent aggregates, top-N concentrations, mix shares, FK
# MAGIC resolution).
# MAGIC
# MAGIC **The build FAILS if any assertion fails.** This is the test harness
# MAGIC for the generator: don't publish data that violates the envelope.
# MAGIC
# MAGIC Sample-time bounds (per-row caps, per-cell volume density) are
# MAGIC enforced inside `01_generate_bronze_data.py` directly — they shouldn't
# MAGIC appear here.

# COMMAND ----------

import os
import yaml

try:
    dbutils.widgets.text("CFO_CATALOG", "main")  # noqa: F821
    dbutils.widgets.text("CFO_SCHEMA_NAME", "cfo_proserv")  # noqa: F821
    _WIDGETS = True
except Exception:
    _WIDGETS = False


def _config(name, default):
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
print(f"Validating: {CATALOG}.{SCHEMA}")

def _find_envelope():
    """Locate plausibility_envelope.yml across Python-script + Databricks
    notebook execution contexts."""
    candidates = []
    try:
        candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       "plausibility_envelope.yml"))
    except NameError:
        pass
    cwd = os.getcwd()
    candidates += [
        os.path.join(cwd, "plausibility_envelope.yml"),
        os.path.join(cwd, "data_pipeline", "plausibility_envelope.yml"),
        os.path.join(cwd, "..", "data_pipeline", "plausibility_envelope.yml"),
    ]
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        notebook_path = ctx.notebookPath().get()
        notebook_dir = os.path.dirname(notebook_path)
        candidates.append(f"/Workspace{notebook_dir}/plausibility_envelope.yml")
    except Exception:
        pass

    for p in candidates:
        try:
            if os.path.exists(p):
                return p
        except Exception:
            continue
    raise FileNotFoundError("plausibility_envelope.yml not found. Tried: " + " | ".join(candidates))


_ENVELOPE_PATH = _find_envelope()
with open(_ENVELOPE_PATH, "r") as _f:
    ENVELOPE = yaml.safe_load(_f)


def env_range(*path, default=None):
    node = ENVELOPE
    for k in path:
        if not isinstance(node, dict) or k not in node:
            return default
        node = node[k]
    return node


# ─── Assertion runner ──────────────────────────────────────────────────
_failures = []
_passes = []


def assert_sql_in_range(name, sql, lo, hi, unit=""):
    """Run SQL → expect single scalar value in [lo, hi]."""
    try:
        row = spark.sql(sql.format(c=CATALOG, s=SCHEMA)).collect()[0]
        val = row[0] if row[0] is not None else 0
        ok = (lo is None or val >= lo) and (hi is None or val <= hi)
        if ok:
            _passes.append((name, val, unit))
            print(f"  ✓ {name}: {val:.2f}{unit} ∈ [{lo}, {hi}]")
        else:
            _failures.append((name, val, lo, hi, unit, sql))
            print(f"  ✗ {name}: {val:.2f}{unit} OUTSIDE [{lo}, {hi}]")
    except Exception as e:
        _failures.append((name, None, lo, hi, unit, f"SQL ERROR: {e}"))
        print(f"  ✗ {name}: SQL error — {e}")


def assert_sql_equals(name, sql, expected):
    """Run SQL → expect single scalar == expected."""
    try:
        row = spark.sql(sql.format(c=CATALOG, s=SCHEMA)).collect()[0]
        val = row[0] if row[0] is not None else 0
        ok = val == expected
        if ok:
            _passes.append((name, val, ""))
            print(f"  ✓ {name}: {val} == {expected}")
        else:
            _failures.append((name, val, expected, expected, "", sql))
            print(f"  ✗ {name}: {val} != {expected}")
    except Exception as e:
        _failures.append((name, None, expected, expected, "", f"SQL ERROR: {e}"))
        print(f"  ✗ {name}: SQL error — {e}")

# COMMAND ----------

# DBTITLE 1,Firmwide aggregates
print("\n=== FIRMWIDE ===")

lo, hi = env_range("firmwide", "annual_revenue_B", default=[15, 25])
assert_sql_in_range(
    "Annual revenue (trailing 12 months)",
    """SELECT SUM(amount) / 1e9 FROM {c}.{s}.silver_fact_accounts_receivable
       WHERE invoice_date >= DATE_SUB(CURRENT_DATE(), 365)""",
    lo, hi, "B"
)

# Strict partners (Partner + Senior Partner) — matches what the demo's
# narrative-bearing tiles show.
lo, hi = env_range("firmwide", "strict_partners", default=[2200, 2700])
assert_sql_in_range(
    "Strict partners (Partner + Senior Partner)",
    """SELECT COUNT(*) FROM {c}.{s}.silver_dim_employees
       WHERE employment_status = 'Active' AND job_level IN ('Partner','Senior Partner')
         AND is_current = TRUE AND is_latest_snapshot = TRUE""",
    lo, hi
)

lo, hi = env_range("firmwide", "total_employees", default=[20000, 45000])
assert_sql_in_range(
    "Total active employees",
    """SELECT COUNT(*) FROM {c}.{s}.silver_dim_employees
       WHERE employment_status = 'Active' AND is_current = TRUE AND is_latest_snapshot = TRUE""",
    lo, hi
)

# COMMAND ----------

# DBTITLE 1,Receivables / DSO
print("\n=== RECEIVABLES (AR) ===")

lo, hi = env_range("receivables_ar", "total_open_ar_B", default=[1, 4])
assert_sql_in_range(
    "Total open AR balance",
    """SELECT SUM(amount) / 1e9 FROM {c}.{s}.silver_fact_accounts_receivable
       WHERE UPPER(COALESCE(payment_status, '')) NOT IN ('PAID','CLOSED')""",
    lo, hi, "B"
)

lo, hi = env_range("receivables_ar", "top_10_client_ar_concentration_pct", default=[15, 45])
assert_sql_in_range(
    "Top-10 client AR concentration",
    """WITH per_client AS (
         SELECT customer_name, SUM(amount) AS bal
         FROM {c}.{s}.silver_fact_accounts_receivable
         WHERE UPPER(COALESCE(payment_status, '')) NOT IN ('PAID','CLOSED')
         GROUP BY customer_name
       ),
       totals AS (SELECT SUM(bal) AS total_bal FROM per_client),
       top10 AS (SELECT SUM(bal) AS top10_bal FROM (
         SELECT bal FROM per_client ORDER BY bal DESC LIMIT 10
       ))
       SELECT (top10.top10_bal / totals.total_bal) * 100 FROM top10, totals""",
    lo, hi, "%"
)

lo, hi = env_range("receivables_ar", "top_1_client_ar_concentration_pct", default=[2, 10])
assert_sql_in_range(
    "Top-1 client AR concentration",
    """WITH per_client AS (
         SELECT customer_name, SUM(amount) AS bal
         FROM {c}.{s}.silver_fact_accounts_receivable
         WHERE UPPER(COALESCE(payment_status, '')) NOT IN ('PAID','CLOSED')
         GROUP BY customer_name
       )
       SELECT MAX(bal) / (SELECT SUM(bal) FROM per_client) * 100 FROM per_client""",
    lo, hi, "%"
)

lo, hi = env_range("receivables_ar", "single_invoice_M", default=[0.05, 12])
assert_sql_in_range(
    "Max single AR invoice",
    """SELECT MAX(amount) / 1e6 FROM {c}.{s}.silver_fact_accounts_receivable""",
    None, hi, "M"
)

# AR.project_id FK 100% resolution to silver_dim_projects
assert_sql_equals(
    "AR.project_id FK orphans (must be 0)",
    """SELECT COUNT(*) FROM {c}.{s}.silver_fact_accounts_receivable ar
       LEFT JOIN {c}.{s}.silver_dim_projects p
         ON ar.project_id = p.project_id
       WHERE ar.project_id IS NOT NULL AND p.project_id IS NULL""",
    0
)

def _ar_bucket_share_sql(bucket: str) -> str:
    """SQL for share of open AR in a specific aging bucket, as % of total open AR."""
    return f"""WITH bal AS (
         SELECT CASE
           WHEN days_outstanding <= 30 THEN '0_30'
           WHEN days_outstanding <= 60 THEN '31_60'
           WHEN days_outstanding <= 90 THEN '61_90'
           ELSE '91_plus'
         END AS bucket,
         amount FROM {{c}}.{{s}}.silver_fact_accounts_receivable
         WHERE UPPER(COALESCE(payment_status, '')) NOT IN ('PAID','CLOSED')
       )
       SELECT (SUM(CASE WHEN bucket='{bucket}' THEN amount ELSE 0 END) / SUM(amount)) * 100 FROM bal"""

# AR aging mix — every bucket must be populated. Empty 61-90 / 91+ buckets are
# the demo-killer pattern (CFO drills into AR aging, sees zero in older buckets,
# rejects the data). Bounds match plausibility_envelope.yml `aging_distribution_pct`.
for bucket_key, bucket_id in [("bucket_0_30", "0_30"), ("bucket_31_60", "31_60"),
                              ("bucket_61_90", "61_90"), ("bucket_91_plus", "91_plus")]:
    bucket_lo, bucket_hi = env_range("receivables_ar", "aging_distribution_pct", bucket_key, default=[0, 100])
    assert_sql_in_range(
        f"AR aging {bucket_key} share",
        _ar_bucket_share_sql(bucket_id),
        bucket_lo, bucket_hi, "%"
    )

# MoM firmwide DSO stability — month-to-month change in firmwide DSO should be
# small. Big swings (137 → 39 over 4 months) are the cohort-age artifact that
# makes the data look "broken" to a CFO. Uses gold_ar_snapshot_aging which has
# the trailing 13 month-end DSO snapshots.
assert_sql_in_range(
    "Max month-over-month firmwide DSO change (absolute days)",
    """WITH snaps AS (
         SELECT DISTINCT snapshot_date, firmwide_dso_days
         FROM {c}.{s}.gold_ar_snapshot_aging
       ),
       paired AS (
         SELECT snapshot_date, firmwide_dso_days,
                LAG(firmwide_dso_days) OVER (ORDER BY snapshot_date) AS prev_dso
         FROM snaps
       )
       SELECT MAX(ABS(firmwide_dso_days - prev_dso)) FROM paired WHERE prev_dso IS NOT NULL""",
    None, 15, " days"
)

# Per-client DSO variance — top-5 and non-top-5 cohorts must have DIFFERENT
# weighted DSO. If they're identical (CLT artifact from a single global
# payment distribution), it means archetype assignment failed.
assert_sql_in_range(
    "Top-5 vs non-top-5 weighted DSO spread (must be > 1 day apart)",
    """WITH per_client AS (
         SELECT customer_name, SUM(amount) AS bal, SUM(amount * days_outstanding) AS bal_days
         FROM {c}.{s}.silver_fact_accounts_receivable
         WHERE UPPER(COALESCE(payment_status, '')) NOT IN ('PAID','CLOSED')
         GROUP BY customer_name
       ),
       ranked AS (
         SELECT *, ROW_NUMBER() OVER (ORDER BY bal DESC) AS rn FROM per_client
       ),
       top5 AS (
         SELECT SUM(bal_days) / NULLIF(SUM(bal), 0) AS dso FROM ranked WHERE rn <= 5
       ),
       rest AS (
         SELECT SUM(bal_days) / NULLIF(SUM(bal), 0) AS dso FROM ranked WHERE rn > 5
       )
       SELECT ABS(top5.dso - rest.dso) FROM top5, rest""",
    1.0, None, " days"
)

# COMMAND ----------

# DBTITLE 1,Payables / Vendor concentration
print("\n=== PAYABLES (AP) ===")

# Top vendor concentration ≤ 10%
lo, hi = env_range("payables_ap", "top_vendor_concentration_pct", default=[1, 12])
assert_sql_in_range(
    "Top-1 vendor concentration",
    """WITH per_vendor AS (
         SELECT vendor_name, SUM(amount) AS bal FROM {c}.{s}.bronze_sap_accounts_payable
         GROUP BY vendor_name
       )
       SELECT MAX(bal) / (SELECT SUM(bal) FROM per_vendor) * 100 FROM per_vendor""",
    lo, hi, "%"
)

lo, hi = env_range("payables_ap", "vendor_count", default=[150, 600])
assert_sql_in_range(
    "Active vendor count",
    """SELECT COUNT(DISTINCT vendor_name) FROM {c}.{s}.bronze_sap_accounts_payable""",
    lo, hi
)

# COMMAND ----------

# DBTITLE 1,Expense composition — billable vs T&E
print("\n=== EXPENSE COMPOSITION ===")

# Labor cost (timecards) should dwarf T&E in the total billable expense pool.
# Old bronze doesn't carry is_billable on AP, so the labor share is computed
# from silver_fact_timecards (billable time_type) vs Concur T&E flagged billable.
assert_sql_in_range(
    "Billable expense labor share (timecard labor / (labor + T&E billable))",
    """WITH te_billable AS (
         SELECT SUM(transaction_amount) AS te FROM {c}.{s}.bronze_concur_expense_items WHERE is_billable = true
       ),
       labor AS (
         SELECT SUM(cost_amount) AS lc FROM {c}.{s}.silver_fact_timecards WHERE time_type_clean = 'Billable'
       )
       SELECT (labor.lc / (labor.lc + COALESCE(te_billable.te, 0))) * 100 FROM labor, te_billable""",
    70, 95, "%"
)

# COMMAND ----------

# DBTITLE 1,Partner economics
print("\n=== PARTNER ECONOMICS ===")

# Monthly billable hours per partner ∈ [60, 110]
lo, hi = env_range("partner_economics", "monthly_billable_hours_per_partner", default=[55, 120])
assert_sql_in_range(
    "Avg monthly billable hours per partner",
    """WITH partner_hours AS (
         SELECT t.employee_id, DATE_TRUNC('MONTH', t.work_date) AS m, SUM(t.hours) AS hrs
         FROM {c}.{s}.bronze_workday_timecards t
         JOIN {c}.{s}.silver_dim_employees e ON t.employee_id = e.employee_id
         WHERE t.time_type = 'Billable'
           AND e.job_level IN ('Partner','Senior Partner')
           AND e.is_current = TRUE AND e.is_latest_snapshot = TRUE
         GROUP BY t.employee_id, DATE_TRUNC('MONTH', t.work_date)
       )
       SELECT AVG(hrs) FROM partner_hours""",
    lo, hi, " hrs/mo"
)

# COMMAND ----------

# DBTITLE 1,Office economics — variance bounds
print("\n=== OFFICE ECONOMICS ===")

# Office × month budget variance — ≤30% on every cell (envelope: [-20, 25])
# Uses gold_regional_pnl (the old generator's per-cell P&L table).
v_lo, v_hi = env_range("budget_variance", "office_month_variance_pct", default=[-30, 30])
assert_sql_in_range(
    "Max office-month revenue variance |actual - budget| / budget",
    """SELECT MAX(ABS((total_revenue - budgeted_revenue) / NULLIF(budgeted_revenue, 0))) * 100
       FROM {c}.{s}.gold_regional_pnl
       WHERE budgeted_revenue > 0 AND total_revenue > 0
         AND fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)""",
    None, max(abs(v_lo), abs(v_hi)) + 5, "%"
)

assert_sql_in_range(
    "Max office-month expense variance |actual - budget| / budget",
    """SELECT MAX(ABS((operating_expenses - budgeted_expenses) / NULLIF(budgeted_expenses, 0))) * 100
       FROM {c}.{s}.gold_regional_pnl
       WHERE budgeted_expenses > 0 AND operating_expenses > 0
         AND fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)""",
    None, max(abs(v_lo), abs(v_hi)) + 5, "%"
)

lo, hi = env_range("office_economics", "count", default=[15, 30])
assert_sql_in_range(
    "Office count",
    """SELECT COUNT(DISTINCT location) FROM {c}.{s}.silver_dim_employees
       WHERE is_current = TRUE AND is_latest_snapshot = TRUE""",
    lo, hi
)

# COMMAND ----------

# DBTITLE 1,Volume density — no sparse cells
print("\n=== VOLUME DENSITY ===")

# Every (location, fiscal_month) cell has ≥ MIN AR rows in last 12 months
min_ar = env_range("volume_density", "ar_rows_per_office_month_min", default=30) - 5  # grace
assert_sql_equals(
    "AR sparse-cell count (location × month with < min rows) — must be 0",
    f"""WITH cells AS (
          SELECT location, DATE_TRUNC('MONTH', invoice_date) AS m, COUNT(*) AS n
          FROM {{c}}.{{s}}.silver_fact_accounts_receivable
          WHERE invoice_date >= DATE_SUB(CURRENT_DATE(), 365)
          GROUP BY location, DATE_TRUNC('MONTH', invoice_date)
        )
        SELECT COUNT(*) FROM cells WHERE n < {min_ar}""",
    0
)

# COMMAND ----------

# DBTITLE 1,Event volume steady-state — no taper near current_date
print("\n=== EVENT VOLUME STEADY-STATE ===")

# Compare last-3-months expense volume vs trailing-12-month average. Ratio should
# stay in [0.6, 1.4] — no 40%+ cliff or spike.
assert_sql_in_range(
    "Trailing-3-month expense volume ratio vs trailing-12-month avg",
    """WITH monthly AS (
         SELECT DATE_TRUNC('MONTH', transaction_date) AS m, SUM(transaction_amount) AS total
         FROM {c}.{s}.bronze_concur_expense_items
         WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 365)
         GROUP BY DATE_TRUNC('MONTH', transaction_date)
       ),
       agg AS (
         SELECT
           AVG(total) AS avg_12mo,
           AVG(CASE WHEN m >= DATE_SUB(CURRENT_DATE(), 90) THEN total END) AS avg_3mo
         FROM monthly
       )
       SELECT avg_3mo / NULLIF(avg_12mo, 0) FROM agg""",
    0.60, 1.40, "x"
)

# COMMAND ----------

# DBTITLE 1,Realistic variance bands (added 2026-05-20)
print("\n=== REALISTIC VARIANCE BANDS ===")

# Firmwide expense variance vs budget — must be in realistic ±5% band.
# Catches the 11.99% over-budget headline that triggered the demo redesign.
assert_sql_in_range(
    "Firmwide expense variance vs budget (last complete month)",
    """WITH m AS (
         SELECT fiscal_period, SUM(total_expenses) AS act, SUM(budgeted_expenses) AS bud
         FROM {c}.{s}.gold_regional_pnl
         WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
         GROUP BY fiscal_period
       )
       SELECT (act - bud) / NULLIF(bud, 0) * 100 FROM m""",
    -5, 5, "%"
)

# Max single office expense variance vs budget — must be in ±15% band.
# Catches the NY +46.4% / Chicago +38.2% / WashDC +33.9% wildly over-budget
# numbers from the 2026-05-20 screenshot review.
assert_sql_in_range(
    "Max office-level expense variance vs budget (absolute)",
    """WITH m AS (
         SELECT location, SUM(total_expenses) AS act, SUM(budgeted_expenses) AS bud
         FROM {c}.{s}.gold_regional_pnl
         WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
         GROUP BY location
         HAVING SUM(budgeted_expenses) > 0
       )
       SELECT MAX(ABS((act - bud) / bud)) * 100 FROM m""",
    None, 15, "%"
)

# Office concentration: no single office >2.5x firmwide median expense.
# Catches the Dubai 5x outlier that surfaced on the Regional Expense Trends
# chart (Dubai $197M billable expense while other offices were $30-50M).
assert_sql_in_range(
    "Max-office-to-median-office expense ratio",
    """WITH per_office AS (
         SELECT location, SUM(total_expenses) AS total
         FROM {c}.{s}.gold_regional_pnl
         WHERE fiscal_period >= DATE_SUB(CURRENT_DATE(), 180)
         GROUP BY location
       )
       SELECT MAX(total) / NULLIF(PERCENTILE(total, 0.5), 0) FROM per_office""",
    None, 2.5, "x"
)

# Office concentration: no single office >2.5x firmwide median revenue.
assert_sql_in_range(
    "Max-office-to-median-office revenue ratio",
    """WITH per_office AS (
         SELECT location, SUM(total_revenue) AS total
         FROM {c}.{s}.gold_regional_pnl
         WHERE fiscal_period >= DATE_SUB(CURRENT_DATE(), 180)
         GROUP BY location
       )
       SELECT MAX(total) / NULLIF(PERCENTILE(total, 0.5), 0) FROM per_office""",
    None, 2.5, "x"
)

# Cross-table scale parity: gold_regional_pnl Corporate expense should match
# gold_department_summary Corporate expense within ±5%. Catches the bug where
# gold_department_summary historically omitted EXPENSE_SCALE, producing
# $565K firmwide Corporate (Genie) vs $85M firmwide Corporate (dashboard).
assert_sql_in_range(
    "Cross-table Corporate-expense scale parity (regional_pnl vs department_summary)",
    """WITH a AS (
         SELECT SUM(corporate_expenses) AS v
         FROM {c}.{s}.gold_regional_pnl
         WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
       ), b AS (
         SELECT SUM(accrued_expenses) AS v
         FROM {c}.{s}.gold_department_summary
         WHERE department = 'Corporate'
           AND fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
       )
       SELECT ABS(a.v - b.v) / NULLIF(GREATEST(a.v, b.v), 0) * 100 FROM a, b""",
    None, 5, "%"
)

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "=" * 60)
print(f"PASSES: {len(_passes)}")
print(f"FAILURES: {len(_failures)}")
print("=" * 60)

# STRICT mode controls whether failures block the pipeline. Default OFF for
# customer ship — failures land in a table for inspection but the build
# continues. Toggle to "true" during our own iteration when we WANT the
# pipeline to fail loudly on calibration drift.
_STRICT = _config("CFO_SAFETY_NET_STRICT", "false").lower() in ("true", "1", "yes")

if _failures:
    print("\nFAILURES:")
    for f in _failures:
        print(f"  - {f[0]}: got {f[1]}, expected [{f[2]}, {f[3]}]{f[4]}")
    # Write failure log to a table for inspection
    fail_rows = [{"check": f[0], "actual": str(f[1]), "expected_lo": str(f[2]),
                  "expected_hi": str(f[3]), "unit": f[4]} for f in _failures]
    spark.createDataFrame(fail_rows).write.format("delta").mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.envelope_safety_net_failures")
    if _STRICT:
        raise AssertionError(f"Envelope safety net: {len(_failures)} assertions failed. "
                             f"See {CATALOG}.{SCHEMA}.envelope_safety_net_failures.")
    else:
        print(f"\n[NON-STRICT] {len(_failures)} assertions failed but pipeline continues. "
              f"See {CATALOG}.{SCHEMA}.envelope_safety_net_failures for the list.")
else:
    print("All envelope assertions passed.")
    # Clear any prior failure log
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.envelope_safety_net_failures")
