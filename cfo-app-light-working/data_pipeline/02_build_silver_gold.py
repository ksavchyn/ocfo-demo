# Databricks notebook source
# MAGIC %md
# MAGIC # Silver & Gold Layer Data Preparation
# MAGIC ## CFO Analytics Demo — Modeled after an elite consulting firm Financials
# MAGIC
# MAGIC This notebook transforms bronze-layer tables (23 tables from Salesforce, Workday, Concur, SAP)
# MAGIC into 9 silver-layer tables and 14 gold-layer tables for CFO analytics dashboards.
# MAGIC
# MAGIC **Silver Layer:** Cleansed, deduplicated, enriched dimensions and facts
# MAGIC **Gold Layer:** Aggregated, business-ready metrics and summaries

# COMMAND ----------

# DBTITLE 1,Configuration & Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# UC table + column descriptions for the 12 tables that don't have metadata
# inline below. Source-of-truth lives in uc_metadata.py so the customer
# bundle deploy carries the same docs through.
import sys
sys.path.insert(0, ".")
from uc_metadata import TABLE_DOCS, COLUMN_DOCS

import os

# Widget declarations — bundle's notebook_task.base_parameters flow through here.
try:
    dbutils.widgets.text("CFO_CATALOG", "main")  # noqa: F821
    dbutils.widgets.text("CFO_SCHEMA_NAME", "cfo_proserv_dev")  # noqa: F821
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
# Customer overrides via bundle var or job UI.
SCHEMA = _config("CFO_SCHEMA_NAME", "cfo_proserv_dev")
print(f"Target: {CATALOG}.{SCHEMA}")
BRONZE_PREFIX = "bronze_"
SILVER_PREFIX = "silver_"
GOLD_PREFIX = "gold_"

# Bronze data is generated as employee-level T&E and individual customer invoices,
# which sums to magnitudes far below realistic firmwide totals. These multipliers
# scale fact rows so dashboard rollups land per CFO demo spec (elite-firm scale):
#   - Expenses ≈ 30% of revenue (SG&A above-the-line; delivery_cost separately ~47%)
#   - Net operating margin lands at ~22-23% (matches elite consulting firm benchmarks)
#   - DSO ≈ 28 days per spec ("Current liquidity (DSO) position: 28 Days")
# EXPENSE_SCALE applied at gold rollup (NOT silver) so per-project expenses in
# gold_project_profitability stay small and project margins stay believable.
# Scale factors retuned 2026-05-19 after the event-driven generator landed.
# Each surface needs a different scale because event-driven coverage is
# uneven across bronze tables:
#   - AR: simulator produces one full-scale invoice per engagement-month.
#     Open AR = $4.82B which matches a 50-day DSO on $19B/yr revenue. KEEP 1.
#   - AP: simulator only produces project direct-expense + firm-overhead AP.
#     Bronze AP = $3.67B / 3yr = $1.2B/yr; real firm spends ~$14B/yr non-labor.
#     SCALE 10 brings annual AP to ~$12B and open balance to ~$1.5B
#     (DPO ~40d), matching industry norms.
#   - OpEx (Concur-driven silver_fact_expenses): STILL random in bronze, not
#     yet event-driven. Bronze Concur = $0.06B; real OpEx for the firm is
#     ~18% revenue = ~$10B over 3 yrs. SCALE 150 brings it to ~$9B.
#     2026-05-21 LATER: restored to 150 after the bronze-boost approach
#     (BRONZE_TE_BOOST=150 in 01_generate_bronze_data.py) caused non-engineered
#     project T&E:contract ratios to balloon to 200-555% on the Finance Overview
#     "Top T&E Outliers" table. The correct fix is to keep EXPENSE_SCALE at the
#     gold rollup AND apply it consistently to gold_te_contract_audit so app.py
#     can query that table directly (data-agnostic, no scale knowledge in app
#     code). Customer-mapping path will require gating EXPENSE_SCALE on the
#     synthetic/customer mode flag in a later cycle.
EXPENSE_SCALE = 150
AR_SCALE = 1
AP_SCALE = 10

# Practice-area names are now demo-facing throughout (bronze, silver, gold, UI, Genie).
# The historical bronze→silver remap was removed when bronze switched to demo names.
# _practice_area_case() is kept as a NO-OP wrapper so existing callers continue working;
# they just project the column directly. Safe to inline-remove on a future cleanup pass.
def _practice_area_case(col: str) -> str:
    """No-op: practice area names are already demo-facing in bronze. Returns column unchanged."""
    return col

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Run timestamp: {datetime.now().isoformat()}")


def _esc(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "''")


def apply_table_metadata(table_short_name: str, table_doc: str, column_docs: dict) -> None:
    """Apply table-level COMMENT + per-column COMMENT to a table in {CATALOG}.{SCHEMA}.

    Called immediately after each CREATE OR REPLACE TABLE so that descriptions are
    re-applied on every refresh (CREATE OR REPLACE drops table metadata).

    Critical for Genie accuracy per Databricks best practices:
    "Quality table and column descriptions in Unity Catalog are critical for Genie."
    """
    full_name = f"{CATALOG}.{SCHEMA}.{table_short_name}"
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{_esc(table_doc)}'")
    for col, desc in column_docs.items():
        spark.sql(
            f"ALTER TABLE {full_name} ALTER COLUMN {col} COMMENT '{_esc(desc)}'"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Silver Layer Tables (9 total)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. silver_dim_clients
# MAGIC Source: `bronze_sfdc_accounts`
# MAGIC - Deduplicate by account_id (latest modified wins)

# COMMAND ----------

# DBTITLE 1,Silver: Dim Clients
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_clients AS
WITH ranked_accounts AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY last_modified_date DESC
        ) AS rn
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}sfdc_accounts
)
SELECT
    account_id                  AS client_id,
    account_name                AS client_name,
    industry,
    account_type                AS client_type,
    parent_account_id           AS parent_client_id,
    billing_street,
    billing_city,
    billing_state,
    billing_postal_code,
    billing_country,
    phone,
    website,
    annual_revenue,
    number_of_employees,
    owner_id                    AS account_manager_id,
    COALESCE(account_status, 'Active') AS client_status,
    region,
    location,
    {_practice_area_case('practice_area')} AS practice_area,
    customer,
    created_date,
    last_modified_date,
    CURRENT_TIMESTAMP()         AS silver_load_timestamp
FROM ranked_accounts
WHERE rn = 1
""")

print("silver_dim_clients created.")
apply_table_metadata("silver_dim_clients", TABLE_DOCS["silver_dim_clients"], COLUMN_DOCS["silver_dim_clients"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_clients").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. silver_dim_employees
# MAGIC Source: `bronze_workday_employees` LEFT JOIN `bronze_workday_organizations`
# MAGIC - SCD Type 2 style with effective dates
# MAGIC - Includes snapshot_date for period-over-period comparisons

# COMMAND ----------

# DBTITLE 1,Silver: Dim Employees (Monthly Snapshots)
# Generate monthly snapshots from 2023-03 through 2026-02 so that the Admin Dashboard
# can compare headcount across periods (current vs. previous month/quarter/year).
# Each snapshot reflects the employee state as of the last day of that month.
# NOTE: An `is_latest_snapshot` column is included so downstream queries that
# previously used only `is_current = TRUE` can add `AND is_latest_snapshot = TRUE`
# to avoid duplicate rows from multiple snapshot months.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees AS
WITH snapshot_months AS (
    -- Generate one row per month from 36 months ago through last complete month.
    -- End bound is the FIRST day of the current month (exclusive of in-progress
    -- month) so we never emit a snapshot dated in the future. Previously the
    -- end was DATE_TRUNC('MONTH', CURRENT_DATE()) (inclusive) which combined
    -- with LAST_DAY() below produced a snapshot_date of e.g. May 31 2026 even
    -- on May 17 — a forward-dated record that broke any Genie aggregation
    -- comparing snapshot_date against CURRENT_DATE().
    SELECT EXPLODE(SEQUENCE(
        ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -36),
        ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1),
        INTERVAL 1 MONTH
    )) AS snapshot_month_start
),
snapshots AS (
    SELECT
        snapshot_month_start,
        LAST_DAY(snapshot_month_start) AS snapshot_date
    FROM snapshot_months
),
-- Deduplicate organizations: one row per cost_center to prevent fanout
org_deduped AS (
    SELECT cost_center, organization_name, country,
           ROW_NUMBER() OVER (PARTITION BY cost_center ORDER BY organization_name) AS rn
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}workday_organizations
),
base AS (
    SELECT
        e.employee_id,
        e.first_name,
        e.last_name,
        CONCAT(e.first_name, ' ', e.last_name)  AS full_name,
        e.hire_date,
        e.termination_date,
        e.job_title                                   AS position_title,
        e.job_level,
        e.job_profile                                 AS job_family,
        -- Silver carries `location` forward as the canonical office identifier.
        -- (`bronze_workday_employees.office` was a duplicate column and is not projected.)
        e.location,
        o.organization_name                       AS office_name,
        e.region,
        o.country,
        {_practice_area_case('e.practice_area')} AS practice_area,
        e.industry,
        e.customer,
        e.manager_id,
        e.cost_center,
        COALESCE(e.employee_type, 'Regular')     AS employee_type,
        -- effective_date dropped — was always identical to hire_date (COALESCE
        -- with created_date never fired since hire_date is always populated) and
        -- had zero downstream consumers.
        COALESCE(e.termination_date,
                 DATE('9999-12-31'))              AS effective_end_date
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}workday_employees e
    LEFT JOIN org_deduped o
        ON e.cost_center = o.cost_center AND o.rn = 1
)
SELECT
    b.employee_id,
    b.first_name,
    b.last_name,
    b.full_name,
    b.hire_date,
    b.termination_date,
    CASE
        WHEN b.termination_date IS NOT NULL AND b.termination_date <= s.snapshot_date THEN 'Terminated'
        WHEN b.hire_date > s.snapshot_date THEN 'Pre-Hire'
        ELSE 'Active'
    END                                       AS employment_status,
    b.position_title,
    -- Q4/Q10 demo narrative: Early-2026 partner promotion class.
    -- Promote 3% of Directors + 6% of Associate Partners to Partner in 2026 snapshots
    -- so partner-count grows faster than revenue → revenue-per-partner declines ~5% YoY.
    -- Story: "Firm elevated 50+ Directors and 330+ Associate Partners to Partner in Q1 2026."
    CASE
        -- Tuned to land ~76 firmwide new partners. Previous 1%/1.7% gave 128; the
        -- silver_dim_employees snapshot has more APs/Dirs than predicted so dial down.
        -- 0.6% Directors + 1.0% APs → ~7 + 60 ≈ 67. Slightly low but in target band.
        WHEN b.job_level = 'Director'
             AND s.snapshot_date >= DATE('2026-01-01')
             AND ABS(HASH(b.employee_id)) % 1000 < 6
        THEN 'Partner'
        WHEN b.job_level = 'Associate Partner'
             AND s.snapshot_date >= DATE('2026-01-01')
             AND ABS(HASH(b.employee_id)) % 1000 < 10
        THEN 'Partner'
        ELSE b.job_level
    END                                       AS job_level,
    b.job_family,
    b.location,
    b.office_name,
    b.region,
    b.country,
    b.practice_area,
    b.industry,
    b.customer,
    b.manager_id,
    b.cost_center,
    b.employee_type,
    b.effective_end_date,
    CASE
        WHEN (b.termination_date IS NULL OR b.termination_date > s.snapshot_date)
             AND b.hire_date <= s.snapshot_date THEN TRUE
        ELSE FALSE
    END                                       AS is_current,
    s.snapshot_date,
    CASE
        WHEN s.snapshot_date = (SELECT MAX(snapshot_date) FROM snapshots) THEN TRUE
        ELSE FALSE
    END                                       AS is_latest_snapshot,
    CURRENT_TIMESTAMP()                       AS silver_load_timestamp
FROM base b
CROSS JOIN snapshots s
-- Include employees who were hired on or before the snapshot date AND either:
-- (a) were not terminated before the snapshot month started, OR
-- (b) this is the latest snapshot (so terminated employees appear with is_current=FALSE
--     and downstream joins on is_latest_snapshot still find them)
WHERE b.hire_date <= s.snapshot_date
  AND (
    b.termination_date IS NULL
    OR b.termination_date >= s.snapshot_month_start
    OR s.snapshot_date = (SELECT MAX(snapshot_date) FROM snapshots)
  )
""")

print("silver_dim_employees created.")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees").show()

apply_table_metadata(
    f"{SILVER_PREFIX}dim_employees",
    "Employee dimension with monthly snapshots (one row per employee × snapshot_date). "
    "Q1 2026 partner promotion class engineered: ~3% of Directors and ~6% of Associate Partners "
    "promoted to Partner from Jan 2026 forward (Q4/Q10 demo narrative — drives revenue-per-partner YoY decline).",
    {
        "job_level": "Job level — values: Senior Partner, Partner, Associate Partner, Director, Engagement Manager, Associate, Business Analyst. Partners promoted from Director (~3%) and Associate Partner (~6%) starting Jan 2026.",
        "employment_status": "Employment state at snapshot — values: Active, Terminated, Pre-Hire. Use Active for current headcount.",
        "snapshot_date": "Last day of month for this snapshot row. Use MAX(snapshot_date) for current view; ADD_MONTHS(MAX, -12) for YoY comparison.",
        "is_current": "TRUE if employee was active as of the snapshot_date row.",
        "is_latest_snapshot": "TRUE only on rows from the most recent snapshot_date. Filter to avoid duplicate rows from multi-month snapshots.",
        "location": "Employee's assigned office city (NYC, SF, Chicago, London, etc.).",
        "office_name": "Display name of the office organization unit (from bronze_workday_organizations).",
        "region": "High-level region — Americas, EMEA, Asia Pacific.",
        "practice_area": "Service line — values: Strategy & Consulting, Technology, Operations, Managed Services: Tech, Managed Services: Ops, Audit, Tax, Accounting.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. silver_dim_projects
# MAGIC Source: `bronze_sfdc_engagements` LEFT JOIN `bronze_sfdc_contracts`
# MAGIC - Deduplicate by engagement_id
# MAGIC - Realistic planned_hours (500-5000), planned_revenue from bronze, planned_cost = revenue * 0.68

# COMMAND ----------

# DBTITLE 1,Silver: Dim Projects
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects AS
WITH ranked_engagements AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY engagement_id
            ORDER BY last_modified_date DESC
        ) AS rn
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}sfdc_engagements
),
-- Derive staffing metrics from assignments
project_staffing AS (
    SELECT
        project_id,
        COUNT(DISTINCT employee_id)                             AS assigned_headcount,
        SUM(CASE WHEN assignment_status = 'Active' THEN 1 ELSE 0 END) AS active_assignments,
        SUM(CASE WHEN assignment_status = 'Planned' THEN 1 ELSE 0 END) AS planned_assignments,
        ROUND(AVG(allocation_percentage), 1)                    AS avg_allocation_pct
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}workday_assignments
    GROUP BY project_id
)
SELECT
    eng.engagement_id                                           AS project_id,
    eng.engagement_name                                         AS project_name,
    eng.account_id                                              AS client_id,
    eng.opportunity_id,
    eng.engagement_type                                         AS project_type,
    {_practice_area_case('eng.practice_area')} AS practice_area,
    -- `office` projection dropped — was always identical to `location` (both come
    -- from bronze_sfdc_engagements). Standardize on `location` to align with
    -- silver_dim_employees naming.
    eng.lead_partner                                              AS lead_partner_id,
    eng.engagement_manager                                         AS project_manager_id,
    eng.start_date                                              AS project_start_date,
    eng.end_date                                                AS project_end_date,
    -- Realistic planned hours: hash-based deterministic value between 500-5000
    CAST(500 + ABS(HASH(eng.engagement_id)) % 4500 AS DOUBLE)  AS planned_hours,
    COALESCE(eng.forecasted_revenue, eng.budget_amount, 0)      AS planned_revenue,
    COALESCE(eng.forecasted_revenue, eng.budget_amount, 0) * 0.68 AS planned_cost,
    COALESCE(eng.forecasted_revenue, eng.budget_amount, 0)
        - (COALESCE(eng.forecasted_revenue, eng.budget_amount, 0) * 0.68) AS planned_margin,
    CASE
        WHEN COALESCE(eng.forecasted_revenue, eng.budget_amount, 0) > 0
        THEN ROUND(
            (1.0 - 0.68) * 100, 2
        )
        ELSE 0.0
    END                                                         AS planned_margin_pct,
    c.contract_id,
    c.contract_number,
    c.total_contract_value,
    c.billing_frequency,
    c.payment_terms,
    c.company_signed_date,
    c.customer_signed_date,
    c.status                                                    AS contract_status,
    eng.status                                                  AS project_status,
    CASE
        WHEN eng.status IN ('Active', 'In Progress', 'Open') THEN TRUE
        ELSE FALSE
    END                                                         AS is_active,
    -- Staffing columns from assignments
    COALESCE(ps.assigned_headcount, 0)                          AS assigned_headcount,
    COALESCE(ps.active_assignments, 0)                          AS active_assignments,
    COALESCE(ps.planned_assignments, 0)                         AS planned_assignments,
    COALESCE(ps.avg_allocation_pct, 0)                          AS avg_allocation_pct,
    -- Required headcount: higher than assigned to create realistic understaffing (ProServ is chronically understaffed)
    -- Use planned_hours with a 2.0-2.6x uplift so ~55-65% of projects are understaffed
    CEIL(CAST(500 + ABS(HASH(eng.engagement_id)) % 4500 AS DOUBLE)
        * (2.0 + (ABS(HASH(CONCAT(eng.engagement_id, 'req'))) % 60) / 100.0)
        / GREATEST(MONTHS_BETWEEN(COALESCE(eng.end_date, DATE_ADD(eng.start_date, 365)), eng.start_date) / 12.0 * 2080, 1))
                                                                AS required_headcount,
    eng.region,
    eng.location,
    eng.industry,
    eng.customer,
    eng.created_date,
    eng.last_modified_date,
    CURRENT_TIMESTAMP()                                         AS silver_load_timestamp
FROM ranked_engagements eng
LEFT JOIN {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}sfdc_contracts c
    ON eng.contract_id = c.contract_id
LEFT JOIN project_staffing ps
    ON eng.engagement_id = ps.project_id
WHERE eng.rn = 1
""")

print("silver_dim_projects created.")
apply_table_metadata("silver_dim_projects", TABLE_DOCS["silver_dim_projects"], COLUMN_DOCS["silver_dim_projects"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. silver_fact_timecards
# MAGIC Source: `bronze_workday_timecards` LEFT JOIN billing/cost rates
# MAGIC - Includes revenue_category derivation (Billable, Products, Partnerships, Other) — spec-compliant per CFO Demo Spec
# MAGIC - utilization scaling by seniority and practice billing premiums

# COMMAND ----------

# DBTITLE 1,Silver: Fact Timecards
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards AS
WITH
-- Deduplicate billing rates: pick the most recent effective_date per employee per work_date
billing_rate_ranked AS (
    SELECT t.timecard_id, br.billing_rate,
           ROW_NUMBER() OVER (PARTITION BY t.timecard_id ORDER BY br.effective_date DESC) AS rn
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}workday_timecards t
    INNER JOIN {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}workday_billing_rates br
        ON t.employee_id = br.employee_id
        AND t.work_date BETWEEN br.effective_date AND COALESCE(br.end_date, DATE('9999-12-31'))
),
billing_rate_deduped AS (
    SELECT timecard_id, billing_rate FROM billing_rate_ranked WHERE rn = 1
),
-- Deduplicate cost rates: pick the most recent effective_date per employee per work_date
cost_rate_ranked AS (
    SELECT t.timecard_id, cr.hourly_cost_rate,
           ROW_NUMBER() OVER (PARTITION BY t.timecard_id ORDER BY cr.effective_date DESC) AS rn
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}workday_timecards t
    INNER JOIN {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}workday_cost_rates cr
        ON t.employee_id = cr.employee_id
        AND t.work_date BETWEEN cr.effective_date AND COALESCE(cr.end_date, DATE('9999-12-31'))
),
cost_rate_deduped AS (
    SELECT timecard_id, hourly_cost_rate FROM cost_rate_ranked WHERE rn = 1
),
-- Demo narrative anomalies: layered per (location, practice, period) so dimensional
-- drill-downs find a coherent business story behind each headline variance:
--   Q7 Nov 2025 firmwide rev dip — Thanksgiving + post-election billable-day shortfall.
--     DIFFERENTIAL by practice + region so the firmwide miss tells a real story (delivery-
--     heavy practices hit hardest, Q4-cyclical practices like Audit hold up). Replaces the
--     prior uniform 0.82 multiplier that produced an unrealistic ~-34% miss in every cell.
--   Q5 Technology Q1 2026 underperform — 3-4 large enterprise clients paused (-15%)
--   Q1 NY Ops + Marketing/Sales last complete month — specific client engagement losses (-40%)
bronze_timecards_narrative AS (
    SELECT
        bt.*,
        bt.hours * (
            -- Nov 2025 differential seasonality. Firmwide ~-15-20% miss; per-cell variance
            -- ranges from -25%+ (Operations Americas) to near-zero (Audit). Story: delivery-
            -- heavy practices crushed by Thanksgiving PTO + project transitions; Audit
            -- ramps for Q4 client work; advisory practices weather the calendar shift.
            -- Practice names are demo-facing throughout (bronze, silver, gold, UI).
            CASE WHEN YEAR(bt.work_date) = 2025 AND MONTH(bt.work_date) = 11 THEN
                CASE bt.practice_area
                    WHEN 'Operations'             THEN 0.78  -- delivery-heavy, hit hardest by Thanksgiving
                    WHEN 'Managed Services: Ops'  THEN 0.82  -- delivery shutdown impact
                    WHEN 'Managed Services: Tech' THEN 0.85  -- partial holiday impact
                    WHEN 'Technology'             THEN 0.88  -- moderate holiday impact
                    WHEN 'Accounting'             THEN 0.93  -- Q4 dip during holiday weeks
                    WHEN 'Tax'                    THEN 0.96  -- year-end planning sustains volume
                    WHEN 'Strategy & Consulting'  THEN 0.99  -- project-based, weathers calendar
                    WHEN 'Audit'                  THEN 1.18  -- Q4 audit ramp — ACTUALLY GROWS
                    ELSE 0.92
                END
                -- Americas Thanksgiving effect (additional ~5% drag); EMEA/APAC unaffected.
                * CASE WHEN bt.region = 'Americas' THEN 0.95 ELSE 1.0 END
            ELSE 1.0 END
            * CASE WHEN bt.practice_area = 'Technology'
                    AND bt.work_date BETWEEN DATE('2026-01-01') AND DATE('2026-03-31') THEN 0.85 ELSE 1.0 END
            * CASE WHEN bt.location = 'New York' AND bt.practice_area IN ('Operations', 'Tax')
                    AND DATE_TRUNC('MONTH', bt.work_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1) THEN 0.60 ELSE 1.0 END
        ) AS hours_adjusted
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}workday_timecards bt
),
base_timecards AS (
    SELECT
        t.timecard_id,
        t.employee_id,
        t.project_id,
        t.work_date,
        t.week_ending_date                                        AS time_period_end,
        -- Clean time types
        -- ~5% of employees are "on the bench" in recent months (the firm is leaner): reclassify their billable as Non-Billable
        CASE
            WHEN UPPER(t.time_type) IN ('BILLABLE', 'CLIENT BILLABLE', 'BILL')
                 AND t.work_date >= DATE_SUB(CURRENT_DATE(), 90)
                 AND ABS(HASH(t.employee_id)) % 100 < 5
                THEN 'Non-Billable'
            WHEN UPPER(t.time_type) IN ('BILLABLE', 'CLIENT BILLABLE', 'BILL')
                THEN 'Billable'
            WHEN UPPER(t.time_type) IN ('NON-BILLABLE', 'NON BILLABLE', 'INTERNAL', 'ADMIN')
                THEN 'Non-Billable'
            WHEN UPPER(t.time_type) IN ('PTO', 'VACATION', 'SICK', 'LEAVE', 'HOLIDAY')
                THEN 'Time Off'
            ELSE 'Other'
        END                                                       AS time_type_clean,
        -- Scale billable hours by seniority: utilization ranges
        -- Senior Partner: 0.10-0.20, Partner: 0.18-0.30, Associate Partner: 0.38-0.50
        -- Director: 0.50-0.65, Engagement Manager: 0.60-0.72, Associate: 0.78-0.86, Business Analyst: 0.85-0.93
        CASE
            WHEN UPPER(t.time_type) IN ('BILLABLE', 'CLIENT BILLABLE', 'BILL') THEN
                t.hours_adjusted * COALESCE(
                    CASE emp_lvl.job_level
                        WHEN 'Senior Partner'      THEN 0.10 + (ABS(HASH(t.timecard_id)) % 10) / 100.0
                        WHEN 'Partner'             THEN 0.18 + (ABS(HASH(t.timecard_id)) % 12) / 100.0
                        WHEN 'Associate Partner'   THEN 0.38 + (ABS(HASH(t.timecard_id)) % 12) / 100.0
                        WHEN 'Director'            THEN 0.50 + (ABS(HASH(t.timecard_id)) % 15) / 100.0
                        WHEN 'Engagement Manager'  THEN 0.60 + (ABS(HASH(t.timecard_id)) % 12) / 100.0
                        WHEN 'Associate'           THEN 0.78 + (ABS(HASH(t.timecard_id)) % 8)  / 100.0
                        WHEN 'Business Analyst'    THEN 0.85 + (ABS(HASH(t.timecard_id)) % 8)  / 100.0
                        ELSE 1.0
                    END, 1.0)
                -- practice billing premiums
                * COALESCE(
                    CASE t.practice_area
                        WHEN 'Strategy & Consulting' THEN 1.20
                        WHEN 'Technology'          THEN 1.10
                        WHEN 'Managed Services: Tech'      THEN 1.05
                        WHEN 'Audit'            THEN 1.00
                        WHEN 'Operations'                   THEN 0.90
                        WHEN 'Managed Services: Ops'        THEN 0.95
                        WHEN 'Accounting'               THEN 1.00
                        WHEN 'Tax'              THEN 0.95
                        ELSE 1.00
                    END, 1.0)
            ELSE t.hours_adjusted
        END                                                           AS hours_worked,
        t.task_id,
        CASE WHEN UPPER(t.time_type) IN ('BILLABLE', 'CLIENT BILLABLE', 'BILL') THEN COALESCE(brd.billing_rate, t.billing_rate, 0) ELSE 0 END AS billing_rate,
        COALESCE(crd.hourly_cost_rate, t.cost_rate, 0)               AS cost_rate,
        -- billing_amount uses the adjusted hours. ALSO applies the DEMO_OVERRIDES
        -- office revenue multipliers (Munich +30%, NY +5%, SF -8%, Sao Paulo -15%)
        -- so actual revenue at gold_regional_pnl shows engineered office narratives
        -- vs uniform budgets.
        CASE
            WHEN UPPER(t.time_type) IN ('BILLABLE', 'CLIENT BILLABLE', 'BILL') THEN
                t.hours_adjusted * COALESCE(
                    CASE emp_lvl.job_level
                        WHEN 'Senior Partner'      THEN 0.10 + (ABS(HASH(t.timecard_id)) % 10) / 100.0
                        WHEN 'Partner'             THEN 0.18 + (ABS(HASH(t.timecard_id)) % 12) / 100.0
                        WHEN 'Associate Partner'   THEN 0.38 + (ABS(HASH(t.timecard_id)) % 12) / 100.0
                        WHEN 'Director'            THEN 0.50 + (ABS(HASH(t.timecard_id)) % 15) / 100.0
                        WHEN 'Engagement Manager'  THEN 0.60 + (ABS(HASH(t.timecard_id)) % 12) / 100.0
                        WHEN 'Associate'           THEN 0.78 + (ABS(HASH(t.timecard_id)) % 8)  / 100.0
                        WHEN 'Business Analyst'    THEN 0.85 + (ABS(HASH(t.timecard_id)) % 8)  / 100.0
                        ELSE 1.0
                    END, 1.0)
                * COALESCE(
                    CASE t.practice_area
                        WHEN 'Strategy & Consulting' THEN 1.20
                        WHEN 'Technology'          THEN 1.10
                        WHEN 'Managed Services: Tech'      THEN 1.05
                        WHEN 'Audit'            THEN 1.00
                        WHEN 'Operations'                   THEN 0.90
                        WHEN 'Managed Services: Ops'        THEN 0.95
                        WHEN 'Accounting'               THEN 1.00
                        WHEN 'Tax'              THEN 0.95
                        ELSE 1.00
                    END, 1.0)
                * CASE t.location
                    WHEN 'Munich'         THEN 1.30
                    WHEN 'New York'       THEN 1.05
                    WHEN 'San Francisco'  THEN 0.92
                    WHEN 'Sao Paulo'      THEN 0.85
                    ELSE 1.00
                  END
                * COALESCE(brd.billing_rate, t.billing_rate, 0)
            ELSE 0
        END                                                           AS billing_amount,
        -- Cost amount: scale billable cost proportionally to billable hours (non-billable cost stays at full hours)
        CASE
            WHEN UPPER(t.time_type) IN ('BILLABLE', 'CLIENT BILLABLE', 'BILL') THEN
                t.hours_adjusted * COALESCE(
                    CASE emp_lvl.job_level
                        WHEN 'Senior Partner'      THEN 0.10 + (ABS(HASH(t.timecard_id)) % 10) / 100.0
                        WHEN 'Partner'             THEN 0.18 + (ABS(HASH(t.timecard_id)) % 12) / 100.0
                        WHEN 'Associate Partner'   THEN 0.38 + (ABS(HASH(t.timecard_id)) % 12) / 100.0
                        WHEN 'Director'            THEN 0.50 + (ABS(HASH(t.timecard_id)) % 15) / 100.0
                        WHEN 'Engagement Manager'  THEN 0.60 + (ABS(HASH(t.timecard_id)) % 12) / 100.0
                        WHEN 'Associate'           THEN 0.78 + (ABS(HASH(t.timecard_id)) % 8)  / 100.0
                        WHEN 'Business Analyst'    THEN 0.85 + (ABS(HASH(t.timecard_id)) % 8)  / 100.0
                        ELSE 1.0
                    END, 1.0)
                * COALESCE(
                    CASE t.practice_area
                        WHEN 'Strategy & Consulting' THEN 1.20
                        WHEN 'Technology'          THEN 1.10
                        WHEN 'Managed Services: Tech'      THEN 1.05
                        WHEN 'Audit'            THEN 1.00
                        WHEN 'Operations'                   THEN 0.90
                        WHEN 'Managed Services: Ops'        THEN 0.95
                        WHEN 'Accounting'               THEN 1.00
                        WHEN 'Tax'              THEN 0.95
                        ELSE 1.00
                    END, 1.0)
                * COALESCE(crd.hourly_cost_rate, t.cost_rate, 0)
            -- Non-billable timecards (PTO, training, BD, internal) book to firmwide
            -- overhead at gold_regional_pnl.total_expenses, NOT to project P&L.
            -- Setting project-level cost_amount to 0 for non-billable so that
            -- gold_project_profitability.actual_margin reflects billable economics only.
            -- Bench-cost questions compute non-billable cost on-the-fly via
            -- (hours_worked × cost_rate) in their queries — see Genie q08 trusted
            -- query and insights_compose.py's pull_hr_data() bench computation.
            ELSE 0
        END                                                           AS cost_amount,
        t.approval_status,
        t.submitted_date,
        t.approved_date,
        t.region,
        t.location,
        {_practice_area_case('t.practice_area')} AS practice_area,
        t.industry,
        t.customer,
        t.created_date,
        CURRENT_TIMESTAMP()                                       AS silver_load_timestamp
    FROM bronze_timecards_narrative t
    LEFT JOIN billing_rate_deduped brd
        ON t.timecard_id = brd.timecard_id
    LEFT JOIN cost_rate_deduped crd
        ON t.timecard_id = crd.timecard_id
    LEFT JOIN {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}workday_employees emp_lvl
        ON t.employee_id = emp_lvl.employee_id
),
-- Map project to engagement type for revenue category derivation
project_engagement_type AS (
    SELECT project_id, project_type, client_id
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects
)
SELECT
    bt.*,
    CASE
        -- Products revenue: ~9% of customers (software/IP licensing — consulting solutions / BCG GAMMA equivalent)
        WHEN bt.time_type_clean = 'Billable'
             AND ABS(HASH(COALESCE(bt.customer, ''))) % 100 < 9
            THEN 'Products'
        -- Partnerships revenue: ~13% of customers (joint ventures and alliances)
        -- Boosted from spec ~6% to compensate for customer-volume skew (smaller customers underweight the bucket)
        WHEN bt.time_type_clean = 'Billable'
             AND ABS(HASH(COALESCE(bt.customer, ''))) % 100 >= 9
             AND ABS(HASH(COALESCE(bt.customer, ''))) % 100 < 22
            THEN 'Partnerships'
        -- Other revenue: ~4% of customers from billable cohort (training, recruiting, misc revenue)
        -- Without this bucket, "Other" stack is empty in the chart since non-billable timecards get filtered upstream
        WHEN bt.time_type_clean = 'Billable'
             AND ABS(HASH(COALESCE(bt.customer, ''))) % 100 >= 22
             AND ABS(HASH(COALESCE(bt.customer, ''))) % 100 < 26
            THEN 'Other'
        -- Billable revenue: ~74% of customers (bulk of consulting fees, ~80%+ of revenue $)
        WHEN bt.time_type_clean = 'Billable'
            THEN 'Billable'
        -- Non-billable timecards (vacation, training, internal work)
        ELSE 'Other'
    END AS revenue_category
FROM base_timecards bt
LEFT JOIN project_engagement_type pet
    ON bt.project_id = pet.project_id
""")

print("silver_fact_timecards created.")

apply_table_metadata(
    f"{SILVER_PREFIX}fact_timecards",
    "Per-employee, per-day, per-project timecard fact table. Source of revenue (billing_amount) and "
    "delivery cost (cost_amount) for all financial rollups. Engineered narrative anomalies via "
    "hours_adjusted: Q7 Nov 2025 firmwide -18%, Q5 Technology Q1 2026 -15%, Q1 NY Ops/Tax last "
    "complete month -40%.",
    {
        "time_type_clean": "Cleaned time type — values: Billable, Non-Billable, Time Off, Other. Filter Billable for revenue-generating hours.",
        "hours_worked": "Hours worked (already adjusted for seniority utilization, practice premium, and demo narrative anomalies). Use this as the canonical hours metric.",
        "billing_rate": "Hourly billing rate in USD (charged to client). Zero for non-billable hours.",
        "cost_rate": "Hourly cost rate in USD (consultant compensation cost).",
        "billing_amount": "Revenue generated by this timecard in USD = hours_worked × billing_rate (billable rows only). Sum to get period revenue.",
        "cost_amount": "Cost incurred by this timecard in USD = hours_worked × cost_rate. Sum to get delivery cost.",
        "approval_status": "Approval state — typically Approved or Pending. Filter Approved for finalized financials.",
        "revenue_category": "Revenue type classification — values: Billable, Products, Partnerships, Other. Drives revenue stack on regional revenue chart.",
        "work_date": "Date the work was performed. Use for monthly aggregation: DATE_TRUNC('MONTH', work_date).",
        "practice_area": "Service line. Technology practice underperformed Q1 2026 due to enterprise client engagement pauses.",
        "location": "Office city. NY Operations + Marketing/Sales practices have engineered revenue dampener for last complete month.",
    },
)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards").show()
spark.sql(f"SELECT revenue_category, COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards GROUP BY revenue_category ORDER BY cnt DESC").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. silver_fact_expenses
# MAGIC Source: `bronze_concur_expense_items` INNER JOIN `bronze_concur_expense_reports`
# MAGIC - Includes expense_category_business and budgeted_amount

# COMMAND ----------

# DBTITLE 1,Silver: Fact Expenses
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses AS
WITH cost_center_dept AS (
    -- Use cost center mapping table to map cost_center codes to department categories
    SELECT
        cost_center_code,
        department_category
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}cost_center_mapping
),
expense_base_raw AS (
    SELECT
        ei.expense_id                                                AS expense_item_id,
        ei.report_id                                                 AS expense_report_id,
        er.employee_id,
        er.report_name,
        er.submit_date                                               AS submission_date,
        er.approved_date                                             AS approval_date,
        er.approval_status,
        ei.transaction_date                                          AS expense_date,
        ei.expense_type,
        ei.transaction_amount                                        AS amount,
        ei.transaction_currency                                      AS currency,
        ei.is_billable,
        ei.vendor_name                                               AS merchant_name,
        ei.comments                                                  AS expense_description,
        CASE WHEN ei.has_receipt THEN 'Received' ELSE 'Missing' END  AS receipt_status,
        er.cost_center,
        ei.project_id,
        ei.region,
        ei.location,
        {_practice_area_case('ei.practice_area')} AS practice_area,
        ei.industry,
        ei.customer,
        ei.created_date,
        CURRENT_TIMESTAMP()                                         AS silver_load_timestamp,
        ccd.department_category
    FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}concur_expense_items ei
    INNER JOIN {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}concur_expense_reports er
        ON ei.report_id = er.report_id
    LEFT JOIN cost_center_dept ccd
        ON er.cost_center = ccd.cost_center_code
),
-- Demo narrative anomaly multiplier on expense amounts:
--   Q6 Chicago 2025 office buildout — Tech + Other expense categories spike Jul-Dec 2025 (+60% / +30%)
--     Story: Chicago opened expanded office floor + IT modernization mid-2025
--   Q1 NY Ops + Marketing/Sales last complete month — paired with timecards dampener
--     so NY's expense-to-revenue ratio stays preserved when revenue drops (-50% on those cells)
expense_base AS (
    SELECT
        eb.expense_item_id, eb.expense_report_id, eb.employee_id, eb.report_name,
        eb.submission_date, eb.approval_date, eb.approval_status,
        eb.expense_date, eb.expense_type,
        eb.amount * (
            CASE WHEN eb.location = 'Chicago'
                  AND eb.expense_date BETWEEN DATE('2025-07-01') AND DATE('2025-12-31')
                  AND UPPER(COALESCE(eb.department_category, '')) IN ('IT', 'R&D', 'TECHNOLOGY', 'ENGINEERING') THEN 1.60
                 WHEN eb.location = 'Chicago'
                  AND eb.expense_date BETWEEN DATE('2025-07-01') AND DATE('2025-12-31')
                  AND UPPER(COALESCE(eb.department_category, '')) NOT IN ('IT', 'R&D', 'TECHNOLOGY', 'ENGINEERING', 'FINANCE', 'HR', 'LEGAL', 'EXECUTIVE', 'MARKETING', 'SALES') THEN 1.30
                 ELSE 1.0
            END
            * CASE WHEN eb.location = 'New York' AND eb.practice_area IN ('Operations', 'Tax')
                    AND DATE_TRUNC('MONTH', eb.expense_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1) THEN 0.50
                   ELSE 1.0
              END
        ) AS amount,
        eb.currency, eb.is_billable, eb.merchant_name, eb.expense_description, eb.receipt_status,
        eb.cost_center, eb.project_id, eb.region, eb.location, eb.practice_area, eb.industry,
        eb.customer, eb.created_date, eb.silver_load_timestamp, eb.department_category
    FROM expense_base_raw eb
)
SELECT
    eb.*,
    CASE
        WHEN eb.is_billable = TRUE
            THEN 'Billable'
        WHEN UPPER(COALESCE(eb.department_category, '')) IN ('FINANCE', 'HR', 'LEGAL', 'EXECUTIVE')
            THEN 'Corporate'
        WHEN UPPER(COALESCE(eb.department_category, '')) IN ('MARKETING', 'SALES')
            THEN 'Marketing'
        WHEN UPPER(COALESCE(eb.department_category, '')) IN ('IT', 'R&D', 'TECHNOLOGY', 'ENGINEERING')
            THEN 'Technology'
        ELSE 'Other'
    END                                                             AS expense_category_business,
    -- Budgeted amount: REALISTIC variance calibration (CFO demo 2026-05-20 rewrite).
    -- Tier-1 firmwide budget variance is ±3-5%; office-level ±6-8%. Prior formula
    -- stacked 4 multipliers that compounded to ±30% per cell — produced impossible
    -- "NY +46% over budget" headlines. New approach: budgeted = actual / (1 + target_variance)
    -- where target_variance is a sum of small structural offsets per dimension.
    -- Firmwide weighted average of target_variance ≈ +2-3% (slightly over plan, realistic).
    --
    -- Engineered narrative outliers (all within Tier-1-credible bounds):
    --   NY:    +6%  (headline overage office, biggest in narrative)
    --   Chicago: +4%  (secondary overage)
    --   Washington DC: +3%  (tertiary)
    --   San Francisco: +2%  (mild)
    --   London: -3% (well-managed, under budget — contrast story)
    --   Bangkok: -2% (well-managed)
    --   Frankfurt, Mumbai, Singapore, Sydney: small under (-1% to -2%)
    --   All other offices: neutral with ±1.5% jitter
    -- Marketing dept skews +1.5%, IT/Tech skews -1%. Q4 seasonal +1%. Per-cell
    -- jitter ±1.5%. Result: realistic distribution where firmwide variance is
    -- ~+2.5% (in-band per envelope), office max ±8%, no impossible outliers.
    ROUND(
        eb.amount / (
            1.0
            + CASE eb.location
                WHEN 'New York'      THEN  0.060
                WHEN 'Chicago'       THEN  0.040
                WHEN 'Washington DC' THEN  0.030
                WHEN 'San Francisco' THEN  0.020
                WHEN 'London'        THEN -0.030
                WHEN 'Bangkok'       THEN -0.020
                WHEN 'Frankfurt'     THEN -0.015
                WHEN 'Mumbai'        THEN -0.010
                WHEN 'Singapore'     THEN -0.010
                WHEN 'Sydney'        THEN -0.005
                WHEN 'Sao Paulo'     THEN  0.000
                ELSE ((ABS(HASH(COALESCE(eb.location, ''))) % 31) - 15) / 1000.0
            END
            + CASE
                WHEN UPPER(COALESCE(eb.department_category, '')) IN ('MARKETING', 'SALES')
                    THEN  0.015
                WHEN UPPER(COALESCE(eb.department_category, '')) IN ('IT', 'R&D', 'TECHNOLOGY', 'ENGINEERING')
                    THEN -0.010
                WHEN UPPER(COALESCE(eb.department_category, '')) IN ('FINANCE', 'HR', 'LEGAL', 'EXECUTIVE')
                    THEN  0.005
                ELSE 0.0
            END
            + CASE
                WHEN MONTH(eb.expense_date) IN (11, 12) THEN  0.010
                WHEN MONTH(eb.expense_date) IN ( 1,  2) THEN -0.005
                ELSE 0.0
            END
            + ((ABS(HASH(eb.expense_item_id)) % 31) - 15) / 1000.0
        ),
        2
    )                                                               AS budgeted_amount
FROM expense_base eb
""")

print("silver_fact_expenses created.")

# ─── T&E OUTLIER ENGINEERING (Tier-1 scale) ─────────────────────────────────
# The "Top T&E Outliers (>6% of Contract Value)" table on Finance Overview
# expects 5+ engagements where billable T&E exceeds 6% of contract value.
# Natural per-project T&E in this dataset is ~$10-15K — too small relative
# to Tier-1 consulting contracts to ever cross 6% without intervention.
#
# Earlier approach: shrink total_contract_value × 0.05 for the top-8 T&E
# projects. Math worked but produced $11K T&E on $58K contracts, which reads
# to a CFO as micro-deals, not Tier-1 outliers. Removed in favor of this
# T&E-boost approach which keeps contracts at natural Tier-1 magnitudes.
#
# Boost: multiply billable expense rows by 30× for the top-8 T&E projects.
# Result: ~$300K-$500K T&E on $5-10M contracts → 6-10% ratio, surfacing as
# legitimate-looking outliers at recognizable enterprise magnitudes.
# Side effect: actual_cost in gold_project_profitability grows by ~$3M total
# firmwide (0.02% of revenue), and margins on these 8 projects compress
# slightly — both narratively consistent with "T&E-heavy outlier engagement".
# Idempotent because silver_fact_expenses is CREATE OR REPLACE'd above.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _top_te_projects AS
-- Select 8 mid-sized Active engagements ($2M-$15M contract) that already have
-- at least 3 billable expense rows. Mid-sized so the 7-10% T&E target is
-- visually credible ($150K-$1.5M T&E spend); ≥3 rows so the boost preserves
-- some natural per-row variance instead of looking flat.
--
-- 2026-05-20 FIX: original version picked top 8 by contract value with no
-- geographic distribution constraint. This concentrated all 8 boosted
-- projects in a handful of offices (e.g., Dubai had 3 boosted projects ×
-- ~$1M T&E × 150 EXPENSE_SCALE = $450M of inflated billable expense, which
-- made Dubai a 5x outlier on the Regional Expense Trends chart).
-- Fix: enforce ROW_NUMBER OVER (PARTITION BY location) = 1 so each office
-- contributes AT MOST ONE outlier project. Distributes the 8 outliers across
-- 8 distinct offices, eliminating single-office concentration.
WITH eligible AS (
    SELECT
        proj.project_id,
        proj.location,
        proj.total_contract_value,
        -- 2026-05-21 (later) — removed /EXPENSE_SCALE. gold_te_contract_audit
        -- no longer scales billable_expenses; per-project ratios use silver
        -- values directly. So the engineering target is contract × 6.5-9.5%
        -- in SILVER. App.py /api/te-outliers reads gold_te_contract_audit.
        -- billable_expenses (= silver), divides by total_contract_value, gets
        -- 6.5-9.5% for the 8 engineered, sub-6% for everyone else.
        proj.total_contract_value * (0.065 + (ABS(HASH(proj.project_id)) % 30) / 1000.0) AS target_total_te,
        ce.current_te,
        ce.row_count
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects proj
    JOIN (
        SELECT project_id, SUM(amount) AS current_te, COUNT(*) AS row_count
        FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
        WHERE expense_category_business = 'Billable' AND project_id IS NOT NULL
        GROUP BY project_id
        HAVING COUNT(*) >= 3 AND SUM(amount) > 0
    ) ce ON ce.project_id = proj.project_id
    WHERE proj.project_status IN ('Active', 'In Progress', 'Open')
        AND proj.total_contract_value BETWEEN 2000000 AND 15000000
),
ranked_per_office AS (
    SELECT
        project_id, location, total_contract_value, target_total_te,
        current_te, row_count,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY total_contract_value DESC) AS rn
    FROM eligible
)
SELECT
    project_id, total_contract_value, target_total_te, current_te, row_count
FROM ranked_per_office
WHERE rn = 1
ORDER BY total_contract_value DESC
LIMIT 8
""")

# Engineered T&E outliers: multiply each project's billable expense rows by
# (target_te / current_te) so the project's total T&E lands at exactly 6.5-9.5%
# of contract value. Previous fixed-multiplier approach (12x, 30x) didn't
# reliably hit the threshold because natural T&E varied wildly across projects.
# This ratio approach is grain-correct and deterministic — same 8 projects
# always end up in the 6.5-9.5% band regardless of underlying T&E magnitudes.
spark.sql(f"""
MERGE INTO {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses AS target
USING _top_te_projects AS source
ON target.project_id = source.project_id
WHEN MATCHED AND target.expense_category_business = 'Billable'
    THEN UPDATE SET amount = ROUND(target.amount * (source.target_total_te / source.current_te), 2)
""")

n_boosted = spark.sql(f"""
SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses exp
JOIN _top_te_projects tp ON exp.project_id = tp.project_id
WHERE exp.expense_category_business = 'Billable'
""").collect()[0]["n"]
print(f"T&E outlier engineering applied — 8 mid-sized projects scaled to 6.5-9.5% T&E:contract ratio ({n_boosted} expense rows touched).")
spark.sql("DROP VIEW IF EXISTS _top_te_projects")
# ─────────────────────────────────────────────────────────────────────────────

apply_table_metadata(
    f"{SILVER_PREFIX}fact_expenses",
    "Per-expense-item fact table from Concur (T&E, software, contractor, travel). Source of "
    "operating_expenses for all financial rollups. Engineered narrative anomalies on amount: "
    "Q6 Chicago Tech/Other +60%/+30% Jul-Dec 2025; Q1 NY Ops/Tax -50% last complete month "
    "(paired with timecards dampener).",
    {
        "expense_date": "Date the expense was incurred. Use for monthly aggregation.",
        "amount": "Expense amount in USD (post-narrative-anomaly adjustment, scaled at gold rollup by EXPENSE_SCALE=300 for firmwide totals).",
        "budgeted_amount": "Budgeted amount for this expense in USD. Tuned so firmwide actuals run ~6-8% over budget for normal months, with Q4 spike (Nov-Dec ~22% over).",
        "expense_type": "Concur expense type (Lodging, Meals, Per Diem, Air Travel, Mileage, Training, Software, Other).",
        "expense_category_business": "Business category — values: Billable (client-rebillable), Corporate, Marketing, Technology, Other. Drives expense category stack on charts.",
        "department_category": "Department mapped from cost_center — values: FINANCE, HR, LEGAL, EXECUTIVE, MARKETING, SALES, IT, R&D, TECHNOLOGY, ENGINEERING, OPERATIONS, etc.",
        "is_billable": "TRUE if expense is rebillable to a client; FALSE if firm overhead.",
        "approval_status": "Concur report approval state — typically Submitted, Approved, Rejected.",
        "receipt_status": "Receipt presence — values: Received, Missing.",
        "location": "Office where expense was incurred. Chicago has engineered Q4 2025 Tech/Other category spike.",
    },
)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses").show()
spark.sql(f"SELECT expense_category_business, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total_amount FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses GROUP BY expense_category_business ORDER BY total_amount DESC").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. silver_wip_unbilled
# MAGIC Source: silver_fact_timecards joined with silver_dim_employees, silver_dim_projects, silver_dim_clients
# MAGIC - Ensures lead_partner_name is populated via proper JOIN path

# COMMAND ----------

# DBTITLE 1,Silver: WIP Unbilled
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{SILVER_PREFIX}wip_unbilled AS
SELECT
    t.timecard_id,
    t.employee_id,
    e.full_name                                                   AS employee_name,
    e.job_level,
    e.job_family,
    t.project_id,
    p.project_name,
    p.client_id,
    cl.client_name,
    p.lead_partner_id,
    lp.full_name                                                  AS lead_partner_name,
    p.project_manager_id,
    pm.full_name                                                  AS project_manager_name,
    t.work_date,
    t.hours_worked,
    t.billing_rate,
    t.cost_rate,
    t.billing_amount,
    t.cost_amount,
    t.billing_amount - t.cost_amount                              AS margin_amount,
    t.time_type_clean,
    t.revenue_category,
    t.approval_status,
    -- WIP = approved billable time not yet invoiced
    CASE
        WHEN t.time_type_clean = 'Billable'
             AND UPPER(COALESCE(t.approval_status, '')) IN ('APPROVED', 'SUBMITTED')
        THEN TRUE
        ELSE FALSE
    END                                                           AS is_wip,
    t.region,
    t.location,
    t.practice_area,
    t.industry,
    t.customer,
    CURRENT_TIMESTAMP()                                           AS silver_load_timestamp
FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards t
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees e
    ON t.employee_id = e.employee_id
    AND e.is_current = TRUE
    AND e.is_latest_snapshot = TRUE
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects p
    ON t.project_id = p.project_id
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_clients cl
    ON p.client_id = cl.client_id
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees lp
    ON p.lead_partner_id = lp.employee_id
    AND lp.is_current = TRUE
    AND lp.is_latest_snapshot = TRUE
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees pm
    ON p.project_manager_id = pm.employee_id
    AND pm.is_current = TRUE
    AND pm.is_latest_snapshot = TRUE
WHERE t.time_type_clean = 'Billable'
  AND t.work_date >= DATE_SUB(CURRENT_DATE(), 45)
""")

print("silver_wip_unbilled created.")
apply_table_metadata("silver_wip_unbilled", TABLE_DOCS["silver_wip_unbilled"], COLUMN_DOCS["silver_wip_unbilled"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}wip_unbilled").show()
spark.sql(f"SELECT COUNT(*) AS has_partner_name FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}wip_unbilled WHERE lead_partner_name IS NOT NULL").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. silver_fact_accounts_payable (NEW)
# MAGIC Source: `bronze_sap_accounts_payable`
# MAGIC - Aging buckets based on days past due

# COMMAND ----------

# DBTITLE 1,Silver: Fact Accounts Payable
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_payable AS
SELECT
    invoice_id,
    vendor_id,
    vendor_name,
    invoice_number,
    invoice_date,
    due_date,
    amount * {AP_SCALE} AS amount,
    currency,
    payment_terms,
    payment_status,
    payment_date,
    gl_account,
    cost_center,
    department,
    -- days_outstanding: stable distribution per invoice (NOT anchored to CURRENT_DATE).
    -- Prior formula made monthly DPO trend artificial — recent invoices had 0-15 days
    -- (no time to age) while older ones had 30+ days (accumulated). Now PAID invoices use
    -- their actual delay; non-PAID invoices use a deterministic hash-based projection
    -- centered on ~30-35 days (matches DPO Insight band) with realistic long tail.
    CASE
        WHEN UPPER(COALESCE(payment_status, '')) = 'PAID' THEN GREATEST(DATEDIFF(COALESCE(payment_date, due_date), due_date), 0)
        WHEN UPPER(COALESCE(payment_status, '')) = 'OVERDUE' THEN 60 + (ABS(HASH(invoice_id)) % 35)
        WHEN UPPER(COALESCE(payment_status, '')) = 'PARTIALLY PAID' THEN 35 + (ABS(HASH(invoice_id)) % 25)
        ELSE  -- OPEN / unknown: 70% in 0-30 day band, 25% in 31-60, 5% in 61-90
            CASE
                WHEN ABS(HASH(invoice_id)) % 100 < 70 THEN ABS(HASH(invoice_id)) % 31
                WHEN ABS(HASH(invoice_id)) % 100 < 95 THEN 31 + (ABS(HASH(invoice_id)) % 30)
                ELSE 61 + (ABS(HASH(invoice_id)) % 30)
            END
    END                                                           AS days_outstanding,
    CASE
        WHEN UPPER(COALESCE(payment_status, '')) = 'PAID' THEN
            CASE
                WHEN DATEDIFF(COALESCE(payment_date, due_date), due_date) <= 30  THEN '0-30 days'
                WHEN DATEDIFF(COALESCE(payment_date, due_date), due_date) <= 60  THEN '31-60 days'
                WHEN DATEDIFF(COALESCE(payment_date, due_date), due_date) <= 90  THEN '61-90 days'
                ELSE '90+ days'
            END
        WHEN UPPER(COALESCE(payment_status, '')) = 'OVERDUE' THEN
            CASE WHEN (60 + (ABS(HASH(invoice_id)) % 35)) <= 90 THEN '61-90 days' ELSE '90+ days' END
        WHEN UPPER(COALESCE(payment_status, '')) = 'PARTIALLY PAID' THEN
            CASE
                WHEN (35 + (ABS(HASH(invoice_id)) % 25)) <= 60 THEN '31-60 days'
                ELSE '61-90 days'
            END
        ELSE
            CASE
                WHEN ABS(HASH(invoice_id)) % 100 < 70 THEN '0-30 days'
                WHEN ABS(HASH(invoice_id)) % 100 < 95 THEN '31-60 days'
                ELSE '61-90 days'
            END
    END                                                           AS aging_bucket,
    region,
    location,
    {_practice_area_case('practice_area')} AS practice_area,
    industry,
    customer,
    created_date,
    CURRENT_TIMESTAMP()                                           AS silver_load_timestamp
FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}sap_accounts_payable
""")

print("silver_fact_accounts_payable created.")
apply_table_metadata("silver_fact_accounts_payable", TABLE_DOCS["silver_fact_accounts_payable"], COLUMN_DOCS["silver_fact_accounts_payable"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_payable").show()
spark.sql(f"SELECT aging_bucket, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_payable GROUP BY aging_bucket ORDER BY aging_bucket").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. silver_fact_accounts_receivable (NEW)
# MAGIC Source: `bronze_sap_accounts_receivable`
# MAGIC - Aging buckets based on days outstanding

# COMMAND ----------

# DBTITLE 1,Silver: Fact Accounts Receivable
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_receivable AS
SELECT
    ar.invoice_id,
    ar.customer_id,
    ar.customer_name,
    ar.invoice_number,
    ar.invoice_date,
    ar.due_date,
    ar.amount * {AR_SCALE}                                        AS amount,
    ar.currency,
    ar.payment_terms,
    ar.payment_status,
    ar.payment_date,
    -- DSO: realistic 30-60 day range with variation by industry and payment terms.
    -- For PAID invoices: days from invoice_date to payment_date (not due_date).
    -- For unpaid: days from invoice_date to reference date, CAPPED AT 365.
    -- Cap rationale: real invoices >365 days old would be written off as bad debt,
    -- not still "Open". Without the cap, 3-year-old synthetic invoices produced
    -- 1000+ day DSO outliers that broke drill-down aggregations.
    CASE
        WHEN UPPER(COALESCE(ar.payment_status, '')) = 'PAID'
            THEN LEAST(GREATEST(DATEDIFF(COALESCE(ar.payment_date, DATE_ADD(ar.invoice_date, 45)), ar.invoice_date), 0), 365)
        ELSE LEAST(GREATEST(DATEDIFF(CURRENT_DATE(), ar.invoice_date), 0), 365)
    END                                                           AS days_outstanding,
    CASE
        WHEN UPPER(COALESCE(ar.payment_status, '')) = 'PAID' THEN
            CASE
                WHEN DATEDIFF(COALESCE(ar.payment_date, DATE_ADD(ar.invoice_date, 45)), ar.invoice_date) <= 30  THEN '0-30 days'
                WHEN DATEDIFF(COALESCE(ar.payment_date, DATE_ADD(ar.invoice_date, 45)), ar.invoice_date) <= 60  THEN '31-60 days'
                WHEN DATEDIFF(COALESCE(ar.payment_date, DATE_ADD(ar.invoice_date, 45)), ar.invoice_date) <= 90  THEN '61-90 days'
                ELSE '90+ days'
            END
        ELSE
            CASE
                WHEN DATEDIFF(CURRENT_DATE(), ar.invoice_date) <= 30  THEN '0-30 days'
                WHEN DATEDIFF(CURRENT_DATE(), ar.invoice_date) <= 60  THEN '31-60 days'
                WHEN DATEDIFF(CURRENT_DATE(), ar.invoice_date) <= 90  THEN '61-90 days'
                ELSE '90+ days'
            END
    END                                                           AS aging_bucket,
    ar.project_id,
    -- Lead partner from project
    COALESCE(
        CONCAT(lp.first_name, ' ', lp.last_name),
        'Unassigned'
    )                                                             AS lead_partner_name,
    ar.region,
    ar.location,
    {_practice_area_case('ar.practice_area')} AS practice_area,
    ar.industry,
    ar.customer,
    ar.created_date,
    CURRENT_TIMESTAMP()                                           AS silver_load_timestamp
FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}sap_accounts_receivable ar
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects p
    ON ar.project_id = p.project_id
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees lp
    ON p.lead_partner_id = lp.employee_id
    AND lp.is_current = TRUE
    AND lp.is_latest_snapshot = TRUE
""")

print("silver_fact_accounts_receivable created.")
apply_table_metadata("silver_fact_accounts_receivable", TABLE_DOCS["silver_fact_accounts_receivable"], COLUMN_DOCS["silver_fact_accounts_receivable"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_receivable").show()
spark.sql(f"SELECT aging_bucket, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_receivable GROUP BY aging_bucket ORDER BY aging_bucket").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. silver_fact_general_ledger (NEW)
# MAGIC Source: `bronze_sap_general_ledger`
# MAGIC - Account classification into Revenue, COGS, OpEx, etc.

# COMMAND ----------

# DBTITLE 1,Silver: Fact General Ledger
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_general_ledger AS
SELECT
    entry_id,
    posting_date,
    document_number,
    gl_account,
    gl_account_name,
    account_type,
    CASE
        WHEN UPPER(account_type) IN ('REVENUE', 'INCOME', 'SALES')
            THEN 'Revenue'
        WHEN UPPER(account_type) IN ('COGS', 'COST OF GOODS SOLD', 'COST OF SALES', 'COST OF REVENUE', 'DIRECT COST')
            THEN 'COGS'
        WHEN UPPER(account_type) IN ('EXPENSE', 'OPERATING EXPENSE', 'OPEX', 'SGA')
            THEN 'Operating Expense'
        WHEN UPPER(account_type) IN ('OTHER INCOME', 'INTEREST INCOME', 'NON-OPERATING INCOME')
            THEN 'Other Income'
        WHEN UPPER(account_type) IN ('TAX', 'INCOME TAX', 'TAX EXPENSE')
            THEN 'Tax'
        WHEN UPPER(account_type) IN ('ASSET', 'CURRENT ASSET', 'FIXED ASSET', 'NON-CURRENT ASSET')
            THEN 'Asset'
        WHEN UPPER(account_type) IN ('LIABILITY', 'CURRENT LIABILITY', 'NON-CURRENT LIABILITY', 'LONG-TERM LIABILITY')
            THEN 'Liability'
        WHEN UPPER(account_type) IN ('EQUITY', 'STOCKHOLDERS EQUITY', 'RETAINED EARNINGS')
            THEN 'Equity'
        ELSE 'Other'
    END                                                           AS account_category,
    debit_amount,
    credit_amount,
    COALESCE(debit_amount, 0) - COALESCE(credit_amount, 0)       AS net_amount,
    cost_center,
    profit_center,
    fiscal_year,
    fiscal_period,
    region,
    location,
    practice_area,
    industry,
    customer,
    created_date,
    CURRENT_TIMESTAMP()                                           AS silver_load_timestamp
FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}sap_general_ledger
""")

print("silver_fact_general_ledger created.")
apply_table_metadata("silver_fact_general_ledger", TABLE_DOCS["silver_fact_general_ledger"], COLUMN_DOCS["silver_fact_general_ledger"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_general_ledger").show()
spark.sql(f"SELECT account_category, COUNT(*) AS cnt, ROUND(SUM(net_amount), 2) AS net_total FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_general_ledger GROUP BY account_category ORDER BY net_total DESC").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Gold Layer Tables (14 total)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. gold_regional_pnl (FIXED)
# MAGIC - P&L by region and practice area with budget columns and year-over-year
# MAGIC - elite-firm leaner SG&A overhead multipliers

# COMMAND ----------

# DBTITLE 1,Gold: Regional P&L
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}regional_pnl AS
WITH revenue_by_period AS (
    SELECT
        region,
        location,
        practice_area,
        industry,
        customer,
        DATE_TRUNC('month', work_date)                            AS fiscal_period,
        YEAR(work_date)                                           AS fiscal_year,
        MONTH(work_date)                                          AS fiscal_month,
        SUM(billing_amount)                                       AS total_revenue,
        SUM(cost_amount)                                          AS total_cost_of_delivery,
        SUM(hours_worked)                                         AS total_hours
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    WHERE time_type_clean = 'Billable'
    GROUP BY region, location, practice_area, industry, customer, DATE_TRUNC('month', work_date), YEAR(work_date), MONTH(work_date)
),
-- Revenue type breakdown from revenue_category
revenue_type_by_period AS (
    SELECT
        region,
        location,
        practice_area,
        industry,
        customer,
        DATE_TRUNC('month', work_date)                            AS fiscal_period,
        -- Dominant revenue type for each dimension combo per period
        FIRST_VALUE(revenue_category) OVER (
            PARTITION BY region, location, practice_area, industry, customer, DATE_TRUNC('month', work_date)
            ORDER BY SUM(billing_amount) DESC
        )                                                         AS revenue_type
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    WHERE time_type_clean = 'Billable'
    GROUP BY region, location, practice_area, industry, customer, DATE_TRUNC('month', work_date), revenue_category
),
revenue_type_deduped AS (
    SELECT DISTINCT
        region, location, practice_area, industry, customer, fiscal_period,
        FIRST_VALUE(revenue_type) OVER (
            PARTITION BY region, location, practice_area, industry, customer, fiscal_period
            ORDER BY revenue_type
        ) AS revenue_type
    FROM revenue_type_by_period
),
expenses_by_period AS (
    SELECT
        region,
        location,
        practice_area,
        industry,
        customer,
        DATE_TRUNC('month', expense_date)                         AS fiscal_period,
        -- EXPENSE_SCALE bumps T&E silver totals to firmwide-OpEx magnitudes
        -- (~70% of revenue: payroll + facilities + tools, not just T&E).
        SUM(amount) * {EXPENSE_SCALE}                             AS total_expenses,
        SUM(budgeted_amount) * {EXPENSE_SCALE}                    AS total_budgeted_expenses,
        -- Expense category breakdown (spec: Billable / Corporate / Marketing / Technology / Other)
        SUM(CASE WHEN expense_category_business = 'Billable'   THEN amount ELSE 0 END) * {EXPENSE_SCALE} AS billable_expenses,
        SUM(CASE WHEN expense_category_business = 'Corporate'  THEN amount ELSE 0 END) * {EXPENSE_SCALE} AS corporate_expenses,
        SUM(CASE WHEN expense_category_business = 'Marketing'  THEN amount ELSE 0 END) * {EXPENSE_SCALE} AS marketing_expenses,
        SUM(CASE WHEN expense_category_business = 'Technology' THEN amount ELSE 0 END) * {EXPENSE_SCALE} AS tech_expenses,
        SUM(CASE WHEN expense_category_business NOT IN ('Billable', 'Corporate', 'Marketing', 'Technology') THEN amount ELSE 0 END) * {EXPENSE_SCALE} AS other_expenses,
        -- Per-category budget breakdown (same CASE pattern as actuals). Required
        -- so Genie can answer "which expense categories show the largest variance
        -- vs budget?" — without these, every category gets compared to the
        -- firmwide budget pool and the variance %s are nonsensical (all "88-100%
        -- under budget" because the category actual is small vs the pool total).
        SUM(CASE WHEN expense_category_business = 'Billable'   THEN budgeted_amount ELSE 0 END) * {EXPENSE_SCALE} AS budgeted_billable_expenses,
        SUM(CASE WHEN expense_category_business = 'Corporate'  THEN budgeted_amount ELSE 0 END) * {EXPENSE_SCALE} AS budgeted_corporate_expenses,
        SUM(CASE WHEN expense_category_business = 'Marketing'  THEN budgeted_amount ELSE 0 END) * {EXPENSE_SCALE} AS budgeted_marketing_expenses,
        SUM(CASE WHEN expense_category_business = 'Technology' THEN budgeted_amount ELSE 0 END) * {EXPENSE_SCALE} AS budgeted_tech_expenses,
        SUM(CASE WHEN expense_category_business NOT IN ('Billable', 'Corporate', 'Marketing', 'Technology') THEN budgeted_amount ELSE 0 END) * {EXPENSE_SCALE} AS budgeted_other_expenses
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
    GROUP BY region, location, practice_area, industry, customer, DATE_TRUNC('month', expense_date)
),
planned_rev AS (
    SELECT
        region,
        location,
        practice_area,
        industry,
        customer,
        SUM(planned_revenue)                                      AS total_planned_revenue
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects
    WHERE is_active = TRUE
    GROUP BY region, location, practice_area, industry, customer
),
combined AS (
    SELECT
        r.region,
        r.location,
        r.practice_area,
        r.industry,
        r.customer,
        r.fiscal_period,
        r.fiscal_year,
        r.fiscal_month,
        r.total_revenue,
        r.total_cost_of_delivery,
        COALESCE(e.total_expenses, 0)                             AS total_expenses,
        r.total_cost_of_delivery * (
            -- the consulting firm SG&A: based on cost_of_delivery, scaled to yield ~12-18% of revenue
            -- (region multipliers halved from prior 0.75-0.90 band to land net margin in
            -- realistic +15-20% range for top-tier consulting after expenses also flow through).
            CASE r.region
                WHEN 'EMEA'         THEN 0.42
                WHEN 'Asia Pacific' THEN 0.35
                ELSE 0.40
            END
            * CASE r.practice_area
                WHEN 'Strategy & Consulting'   THEN 1.15
                WHEN 'Technology'              THEN 0.90
                WHEN 'Managed Services: Tech'  THEN 0.85
                WHEN 'Tax'                     THEN 1.00
                WHEN 'Operations'              THEN 0.95
                WHEN 'Managed Services: Ops'   THEN 1.05
                WHEN 'Audit'                   THEN 1.10
                WHEN 'Accounting'              THEN 0.90
                ELSE 1.00
            END
            * CASE r.industry
                WHEN 'Financial Services'              THEN 1.12
                WHEN 'Health & Public Service'          THEN 1.08
                WHEN 'Resources'                        THEN 1.15
                WHEN 'Communications Media & Technology' THEN 0.92
                ELSE 1.00
            END
            -- Location adjustment: NY/SF anchored per demo narrative (NY runs leaner, SF heavier).
            -- Other offices get deterministic hash-based variance so different filters tell different stories.
            * CASE r.location
                WHEN 'New York'      THEN 0.92
                WHEN 'San Francisco' THEN 1.05
                ELSE (0.88 + (ABS(HASH(COALESCE(r.location, ''))) % 20) / 100.0)   -- 0.88-1.07 spread for other offices
            END
        )                                                         AS sga_overhead,
        -- Operating income: revenue - cost_of_delivery - SG&A allocated as % of revenue
        -- (NOT using total_expenses here because EXPENSE_SCALE=300 inflates it for DC-question
        -- magnitude purposes; double-subtracting it pushed operating margins to -30/-40% which
        -- isn't credible for top-tier consulting offices). SG&A targets 15-25% of revenue,
        -- yielding operating margins in the realistic 18-30% range with practice/location variance.
        r.total_revenue - r.total_cost_of_delivery
            - r.total_revenue * (
                CASE r.region WHEN 'EMEA' THEN 0.20 WHEN 'Asia Pacific' THEN 0.18 ELSE 0.22 END
                * CASE r.practice_area WHEN 'Strategy & Consulting' THEN 0.95 WHEN 'Technology' THEN 0.92 WHEN 'Managed Services: Tech' THEN 0.90 WHEN 'Tax' THEN 1.05 WHEN 'Operations' THEN 1.00 WHEN 'Managed Services: Ops' THEN 1.02 WHEN 'Audit' THEN 1.10 WHEN 'Accounting' THEN 0.95 ELSE 1.00 END
                * CASE r.location WHEN 'New York' THEN 0.95 WHEN 'San Francisco' THEN 1.05 ELSE (0.92 + (ABS(HASH(COALESCE(r.location, ''))) % 16) / 100.0) END
            )                                                     AS operating_income,
        CASE
            WHEN r.total_revenue > 0
            THEN ROUND((r.total_revenue - r.total_cost_of_delivery
                - r.total_revenue * (
                    CASE r.region WHEN 'EMEA' THEN 0.20 WHEN 'Asia Pacific' THEN 0.18 ELSE 0.22 END
                    * CASE r.practice_area WHEN 'Strategy & Consulting' THEN 0.95 WHEN 'Technology' THEN 0.92 WHEN 'Managed Services: Tech' THEN 0.90 WHEN 'Tax' THEN 1.05 WHEN 'Operations' THEN 1.00 WHEN 'Managed Services: Ops' THEN 1.02 WHEN 'Audit' THEN 1.10 WHEN 'Accounting' THEN 0.95 ELSE 1.00 END
                    * CASE r.location WHEN 'New York' THEN 0.95 WHEN 'San Francisco' THEN 1.05 ELSE (0.92 + (ABS(HASH(COALESCE(r.location, ''))) % 16) / 100.0) END
                )) / r.total_revenue * 100, 2)
            ELSE 0
        END                                                       AS operating_margin_pct,
        r.total_revenue - r.total_cost_of_delivery                AS gross_profit,
        CASE
            WHEN r.total_revenue > 0
            THEN ROUND((r.total_revenue - r.total_cost_of_delivery) / r.total_revenue * 100, 2)
            ELSE 0
        END                                                       AS gross_margin_pct,
        r.total_hours,
        -- Budget columns
        -- REVERTED to original formula. The "anchor budget to actual" approach
        -- I tried earlier killed the engineered office narrative (NYC/Chicago/DC
        -- over budget per DEMO_OVERRIDES) AND used the wrong column basis,
        -- producing 70% UNDER-budget variances that broke the demo talk track.
        -- This formula preserves the per-office narrative anchors.
        COALESCE(e.total_budgeted_expenses, 0)                    AS budgeted_expenses,
        -- Expense breakdown columns (spec: Billable / Corporate / Marketing / Technology / Other)
        ROUND(COALESCE(e.billable_expenses, 0), 2)                AS billable_expenses,
        ROUND(COALESCE(e.corporate_expenses, 0), 2)               AS corporate_expenses,
        ROUND(COALESCE(e.marketing_expenses, 0), 2)               AS marketing_expenses,
        ROUND(COALESCE(e.tech_expenses, 0), 2)                    AS tech_expenses,
        ROUND(COALESCE(e.other_expenses, 0), 2)                   AS other_expenses,
        -- Per-category budgets (paired with the actuals above). Required so
        -- per-category variance vs budget actually compares like-to-like.
        ROUND(COALESCE(e.budgeted_billable_expenses, 0), 2)       AS budgeted_billable_expenses,
        ROUND(COALESCE(e.budgeted_corporate_expenses, 0), 2)      AS budgeted_corporate_expenses,
        ROUND(COALESCE(e.budgeted_marketing_expenses, 0), 2)      AS budgeted_marketing_expenses,
        ROUND(COALESCE(e.budgeted_tech_expenses, 0), 2)           AS budgeted_tech_expenses,
        ROUND(COALESCE(e.budgeted_other_expenses, 0), 2)          AS budgeted_other_expenses,
        -- Revenue type
        COALESCE(rt.revenue_type, 'Other')                        AS revenue_type,
        -- Previous year revenue via LAG
        LAG(r.total_revenue, 12) OVER (
            PARTITION BY r.region, r.location, r.practice_area, r.industry, r.customer
            ORDER BY r.fiscal_period
        )                                                         AS prev_year_revenue
    FROM revenue_by_period r
    LEFT JOIN expenses_by_period e
        ON r.region = e.region
        AND r.location = e.location
        AND r.practice_area = e.practice_area
        AND r.industry = e.industry
        AND r.customer = e.customer
        AND r.fiscal_period = e.fiscal_period
    LEFT JOIN revenue_type_deduped rt
        ON r.region = rt.region
        AND r.location = rt.location
        AND r.practice_area = rt.practice_area
        AND r.industry = rt.industry
        AND r.customer = rt.customer
        AND r.fiscal_period = rt.fiscal_period
)
SELECT
    c.*,
    -- Budgeted revenue: REALISTIC variance calibration (CFO demo 2026-05-20 rewrite + 2026-05-20 evening extension).
    -- Tier-1 firmwide revenue variance is ±3-5%; office-level ±6-12%. Prior version
    -- engineered only 13 offices at ±2-7% — too subtle to read on the Regional Revenue
    -- Trends chart, so chart showed every office hitting target. This version covers
    -- ALL 23 offices at ±5-12% so the chart bars visibly separate from the projected
    -- revenue line.
    --
    -- Engineered narrative outliers (each office has an explicit baseline):
    --   UNDER plan (revenue miss — bars below projected line):
    --     NY -8%, San Francisco -5%, Munich -4%, Paris -3%, Milan -3%, Sao Paulo -4%,
    --     Seoul -2%, Tokyo -2%
    --   OVER plan (revenue beat — bars above projected line):
    --     Frankfurt +7%, Mumbai +6%, Dubai +5%, Singapore +5%, Sydney +4%, Houston +3%,
    --     Toronto +2%
    --   AT plan ±1% (mild noise so chart stays naturalistic):
    --     Chicago, Washington DC, London, Amsterdam, Bangkok, Atlanta, Hong Kong,
    --     Zurich, Shanghai
    -- Practice variance overlay: Strategy & Consulting +1.5%, Technology +1%, Operations -1.5%, Tax -0.5%.
    -- Per-cell jitter ±1.5%.
    -- 2026-05-21 — moved 5 of the 9 "at-plan" offices into visible-deviation
    -- territory (±3% range). Prior version had 9 offices at ±0.5-1.5% which
    -- looked uniformly flat on the chart — implausible that this many offices
    -- hit plan exactly. 4 offices stay at ±1% (Washington DC, London, Atlanta,
    -- Hong Kong) as legitimately on-plan; everyone else visibly deviates.
    ROUND(c.total_revenue / (
        1.0
        + CASE c.location
            -- Under-plan offices (revenue miss)
            WHEN 'New York'      THEN -0.080
            WHEN 'San Francisco' THEN -0.050
            WHEN 'Munich'        THEN -0.040
            WHEN 'Sao Paulo'     THEN -0.040
            WHEN 'Paris'         THEN -0.030
            WHEN 'Milan'         THEN -0.030
            WHEN 'Seoul'         THEN -0.020
            WHEN 'Tokyo'         THEN -0.020
            -- Over-plan offices (revenue beat)
            WHEN 'Frankfurt'     THEN  0.070
            WHEN 'Mumbai'        THEN  0.060
            WHEN 'Dubai'         THEN  0.050
            WHEN 'Singapore'     THEN  0.050
            WHEN 'Sydney'        THEN  0.040
            WHEN 'Houston'       THEN  0.030
            WHEN 'Toronto'       THEN  0.020
            -- Previously at-plan — 5 now visibly deviating (±2.5-3.5%):
            WHEN 'Chicago'       THEN -0.035
            WHEN 'Amsterdam'     THEN -0.030
            WHEN 'Bangkok'       THEN  0.025
            WHEN 'Zurich'        THEN  0.030
            WHEN 'Shanghai'      THEN -0.025
            -- 4 offices remain legitimately on-plan (±1%):
            WHEN 'Washington DC' THEN  0.010
            WHEN 'London'        THEN  0.005
            WHEN 'Atlanta'       THEN -0.005
            WHEN 'Hong Kong'     THEN  0.010
            ELSE 0.0
        END
        + CASE c.practice_area
            WHEN 'Strategy & Consulting' THEN  0.015
            WHEN 'Technology'            THEN  0.010
            WHEN 'Operations'            THEN -0.015
            WHEN 'Tax'                   THEN -0.005
            WHEN 'Audit'                 THEN  0.000
            ELSE 0.0
        END
        + ((ABS(HASH(
            COALESCE(c.practice_area, '') || '|' ||
            COALESCE(c.region, '') || '|' ||
            COALESCE(c.location, '') || '|' ||
            DATE_FORMAT(c.fiscal_period, 'yyyy-MM')
          )) % 31) - 15) / 1000.0
    ), 2)  AS budgeted_revenue,
    CASE
        WHEN c.prev_year_revenue > 0
        THEN LEAST(GREATEST(
            ROUND((c.total_revenue - c.prev_year_revenue) / c.prev_year_revenue * 100, 2),
            -100), 500)
        ELSE NULL
    END                                                           AS revenue_yoy_growth_pct,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM combined c
LEFT JOIN planned_rev pr
    ON c.region = pr.region
    AND c.location = pr.location
    AND c.practice_area = pr.practice_area
    AND c.industry = pr.industry
    AND c.customer = pr.customer
""")

print("gold_regional_pnl created.")

apply_table_metadata(
    f"{GOLD_PREFIX}regional_pnl",
    "Office/practice/industry/customer-level P&L by fiscal month. Primary source for: regional "
    "revenue/expense charts, expense-category breakdown (billable/corporate/marketing/tech/other), "
    "budget variance (actual vs budget), and revenue YoY trend. Used for Q1 (NY underperform), "
    "Q5 (Technology practice), Q6 (Chicago expenses), Q11 (firmwide expenses over budget) demo questions.",
    {
        "fiscal_period": "First day of fiscal month (timestamp). Filter < DATE_TRUNC('MONTH', CURRENT_DATE()) to exclude partial in-progress month.",
        "total_revenue": "Monthly revenue in USD from billable timecards. Same definition as gold_enterprise_metrics.revenue but at this finer dimensional grain.",
        "budgeted_revenue": "Monthly revenue forecast/budget in USD. Compare against total_revenue for actual vs target analysis.",
        "total_cost_of_delivery": "Cost of delivery in USD (labor cost from billable timecards).",
        "total_expenses": "Total SG&A operating expenses in USD. Equals billable_expenses + corporate_expenses + marketing_expenses + tech_expenses + other_expenses.",
        "budgeted_expenses": "Monthly expense budget in USD. Spec: actuals run ~6-8% over budget firmwide for normal months, with a Q4 spike (Nov/Dec ~22% over).",
        "billable_expenses": "Billable T&E expenses in USD (client-rebillable consultant T&E).",
        "corporate_expenses": "Corporate overhead expenses in USD (Finance, HR, Legal, Executive cost centers).",
        "marketing_expenses": "Marketing & Sales expenses in USD.",
        "tech_expenses": "Technology / IT expenses in USD. Spike in Chicago Jul-Dec 2025 due to office buildout + IT modernization (Q6 demo narrative).",
        "other_expenses": "Other expense categories (catch-all bucket).",
        "budgeted_billable_expenses": "Monthly budget for the Billable category in USD. Pair with billable_expenses to compute per-category variance vs budget.",
        "budgeted_corporate_expenses": "Monthly budget for the Corporate category in USD. Pair with corporate_expenses for category-level variance.",
        "budgeted_marketing_expenses": "Monthly budget for the Marketing category in USD. Pair with marketing_expenses for category-level variance.",
        "budgeted_tech_expenses": "Monthly budget for the Technology category in USD. Pair with tech_expenses for category-level variance.",
        "budgeted_other_expenses": "Monthly budget for the Other category (catch-all) in USD. Pair with other_expenses for category-level variance.",
        "sga_overhead": "SG&A allocated overhead applied to office/practice level (used in EBIT margin calculations).",
        "operating_income": "Operating income in USD = revenue - cost_of_delivery - total_expenses - sga_overhead.",
        "operating_margin_pct": "Operating margin percentage = operating_income / total_revenue × 100.",
        "gross_profit": "Gross profit in USD = total_revenue - total_cost_of_delivery.",
        "gross_margin_pct": "Gross margin percentage = gross_profit / total_revenue × 100.",
        "revenue_type": "Primary revenue category for this row — values: Billable, Products, Partnerships, Other.",
        "prev_year_revenue": "Same fiscal_period in prior year for YoY comparison. May be NULL for periods earlier than 12 months of history.",
        "revenue_yoy_growth_pct": "YoY revenue growth percentage = (total_revenue - prev_year_revenue) / prev_year_revenue × 100.",
    },
)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}regional_pnl").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. gold_talent_supply_demand (FIXED)
# MAGIC - Workforce utilization and supply/demand metrics by region and practice area

# COMMAND ----------

# DBTITLE 1,Gold: Talent Supply & Demand
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}talent_supply_demand AS
WITH active_headcount AS (
    SELECT
        region,
        location,
        practice_area,
        industry,
        customer,
        job_level,
        job_family,
        COUNT(*)                                                  AS headcount
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees
    WHERE is_current = TRUE
      AND is_latest_snapshot = TRUE
    GROUP BY region, location, practice_area, industry, customer, job_level, job_family
),
utilization AS (
    SELECT
        e.region,
        e.location,
        e.practice_area,
        e.industry,
        e.customer,
        e.job_level,
        e.job_family,
        DATE_TRUNC('month', t.work_date)                          AS fiscal_period,
        COUNT(DISTINCT t.employee_id)                             AS active_billers,
        SUM(t.hours_worked)                                       AS total_hours,
        SUM(CASE WHEN t.time_type_clean = 'Billable' THEN t.hours_worked ELSE 0 END) AS billable_hours,
        SUM(CASE WHEN t.time_type_clean = 'Billable' THEN t.hours_worked ELSE 0 END)
            / NULLIF(SUM(t.hours_worked), 0) * 100               AS utilization_pct
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards t
    LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees e
        ON t.employee_id = e.employee_id
        AND e.is_current = TRUE
        AND e.is_latest_snapshot = TRUE
    GROUP BY e.region, e.location, e.practice_area, e.industry, e.customer, e.job_level, e.job_family,
             DATE_TRUNC('month', t.work_date)
),
demand AS (
    SELECT
        region,
        location,
        practice_area,
        industry,
        customer,
        COUNT(*)                                                  AS active_projects,
        SUM(planned_hours)                                        AS total_demand_hours
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects
    WHERE is_active = TRUE
    GROUP BY region, location, practice_area, industry, customer
)
SELECT
    COALESCE(ah.region, u.region)                                 AS region,
    COALESCE(ah.location, u.location)                             AS location,
    COALESCE(ah.practice_area, u.practice_area)                   AS practice_area,
    COALESCE(ah.industry, u.industry)                             AS industry,
    COALESCE(ah.customer, u.customer)                             AS customer,
    COALESCE(ah.job_level, u.job_level)                           AS job_level,
    COALESCE(ah.job_family, u.job_family)                         AS job_family,
    u.fiscal_period,
    COALESCE(ah.headcount, 0)                                     AS supply_headcount,
    COALESCE(u.active_billers, 0)                                 AS active_billers,
    COALESCE(u.total_hours, 0)                                    AS total_hours,
    COALESCE(u.billable_hours, 0)                                 AS billable_hours,
    ROUND(COALESCE(u.utilization_pct, 0), 2)                      AS utilization_pct,
    COALESCE(d.active_projects, 0)                                AS demand_projects,
    COALESCE(d.total_demand_hours, 0)                             AS demand_hours,
    CASE
        WHEN COALESCE(ah.headcount, 0) > 0
        THEN ROUND(COALESCE(d.total_demand_hours, 0) / (COALESCE(ah.headcount, 1) * 160) * 100, 2)
        ELSE 0
    END                                                           AS demand_coverage_pct,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM active_headcount ah
FULL OUTER JOIN utilization u
    ON ah.region = u.region
    AND ah.location = u.location
    AND ah.practice_area = u.practice_area
    AND ah.industry = u.industry
    AND ah.customer = u.customer
    AND ah.job_level = u.job_level
    AND ah.job_family = u.job_family
LEFT JOIN demand d
    ON COALESCE(ah.region, u.region) = d.region
    AND COALESCE(ah.location, u.location) = d.location
    AND COALESCE(ah.practice_area, u.practice_area) = d.practice_area
    AND COALESCE(ah.industry, u.industry) = d.industry
    AND COALESCE(ah.customer, u.customer) = d.customer
""")

print("gold_talent_supply_demand created.")
apply_table_metadata("gold_talent_supply_demand", TABLE_DOCS["gold_talent_supply_demand"], COLUMN_DOCS["gold_talent_supply_demand"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}talent_supply_demand").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. gold_project_profitability (FIXED)
# MAGIC - Project-level profitability with realistic planned_revenue and populated partner names

# COMMAND ----------

# DBTITLE 1,Gold: Project Profitability
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}project_profitability AS
WITH deduped_employees AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY silver_load_timestamp DESC) AS _rn
        FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees
        WHERE is_current = TRUE
          AND is_latest_snapshot = TRUE
    ) WHERE _rn = 1
),
-- Fallback partner pool keyed by (region, practice_area). ~19% of project lead_partner_ids
-- in dim_projects don't match any employee_id in dim_employees (orphaned references).
-- This pool gives us a deterministic same-region-and-practice partner to fall back to,
-- so every project ends up with a populated lead_partner_name in the gold table.
fallback_partner_pool AS (
    SELECT
        region,
        practice_area,
        full_name,
        employee_id,
        ROW_NUMBER() OVER (
            PARTITION BY region, practice_area
            ORDER BY employee_id
        ) AS rn
    FROM deduped_employees
    WHERE (UPPER(COALESCE(position_title, '')) LIKE '%PARTNER%'
           OR job_level = 'Partner')
),
project_actuals AS (
    SELECT
        project_id,
        SUM(billing_amount)                                       AS actual_revenue,
        SUM(cost_amount)                                          AS actual_cost,
        SUM(hours_worked)                                         AS actual_hours,
        SUM(CASE WHEN time_type_clean = 'Billable' THEN hours_worked ELSE 0 END) AS billable_hours,
        MIN(work_date)                                            AS first_timecard_date,
        MAX(work_date)                                            AS last_timecard_date,
        COUNT(DISTINCT employee_id)                               AS team_size
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    GROUP BY project_id
),
project_expenses AS (
    SELECT
        project_id,
        SUM(amount)                                               AS total_expenses
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
    WHERE project_id IS NOT NULL
    GROUP BY project_id
)
SELECT
    p.project_id,
    p.project_name,
    p.client_id,
    cl.client_name,
    p.project_type,
    p.practice_area,
    p.lead_partner_id,
    COALESCE(lp.full_name, fp.full_name)                          AS lead_partner_name,
    p.project_manager_id,
    pm.full_name                                                  AS project_manager_name,
    p.project_start_date,
    p.project_end_date,
    p.project_status,
    p.is_active,
    -- Planned
    p.planned_hours,
    p.planned_revenue,
    p.planned_cost,
    p.planned_margin,
    p.planned_margin_pct,
    p.contract_id,
    p.total_contract_value,
    -- Actuals
    COALESCE(pa.actual_revenue, 0)                                AS actual_revenue,
    COALESCE(pa.actual_cost, 0)                                   AS actual_cost,
    COALESCE(pe.total_expenses, 0)                                AS actual_expenses,
    -- Introduce realistic margin variance: some projects lose money, some are highly profitable
    -- Apply a project-specific cost adjustment factor (0.7x to 1.5x) to create spread.
    -- Q2 demo narrative: NY projects ending last complete month run ~25% over plan on cost
    -- (story: contractor rate increases + scope creep on 6-8 engagements).
    -- Bumped from 10% → 25% on 2026-05-04 to make individual project compression visible
    -- so Genie can list specific underperforming projects by name in drill-down.
    (COALESCE(pa.actual_revenue, 0)
        - COALESCE(pa.actual_cost, 0) * (
            CASE
                -- ~12% of projects are loss-makers (cost inflated 1.2-1.5x)
                WHEN ABS(HASH(p.project_id)) % 100 < 12
                    THEN 1.20 + (ABS(HASH(CONCAT(p.project_id, 'margin'))) % 30) / 100.0
                -- ~15% are high-margin (cost deflated 0.7-0.85x)
                WHEN ABS(HASH(p.project_id)) % 100 >= 85
                    THEN 0.70 + (ABS(HASH(CONCAT(p.project_id, 'margin'))) % 15) / 100.0
                -- ~73% are normal range with some variance
                ELSE 0.90 + (ABS(HASH(CONCAT(p.project_id, 'margin'))) % 20) / 100.0
            END)
        * CASE WHEN p.location = 'New York'
                AND p.project_end_date >= ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
                AND p.project_end_date < DATE_TRUNC('MONTH', CURRENT_DATE()) THEN 1.25 ELSE 1.0 END
        - COALESCE(pe.total_expenses, 0))                         AS actual_margin,
    CASE
        WHEN COALESCE(pa.actual_revenue, 0) > 0
        THEN ROUND(
            (COALESCE(pa.actual_revenue, 0)
                - COALESCE(pa.actual_cost, 0) * (
                    CASE
                        WHEN ABS(HASH(p.project_id)) % 100 < 12
                            THEN 1.20 + (ABS(HASH(CONCAT(p.project_id, 'margin'))) % 30) / 100.0
                        WHEN ABS(HASH(p.project_id)) % 100 >= 85
                            THEN 0.70 + (ABS(HASH(CONCAT(p.project_id, 'margin'))) % 15) / 100.0
                        ELSE 0.90 + (ABS(HASH(CONCAT(p.project_id, 'margin'))) % 20) / 100.0
                    END)
                * CASE WHEN p.location = 'New York'
                        AND p.project_end_date >= ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
                        AND p.project_end_date < DATE_TRUNC('MONTH', CURRENT_DATE()) THEN 1.25 ELSE 1.0 END
                - COALESCE(pe.total_expenses, 0))
            / COALESCE(pa.actual_revenue, 1) * 100, 2)
        ELSE 0
    END                                                           AS actual_margin_pct,
    COALESCE(pa.actual_hours, 0)                                  AS actual_hours,
    COALESCE(pa.billable_hours, 0)                                AS billable_hours,
    COALESCE(pa.team_size, 0)                                     AS team_size,
    -- Variance — NULL the % when planned_revenue < $1K (engineered "trickle"
    -- engagement budgets produced 4,000%+ variance on tiny absolute values,
    -- which is data-quality noise not a real finding). Cap rest to [-100, 500].
    COALESCE(pa.actual_revenue, 0) - p.planned_revenue            AS revenue_variance,
    CASE
        WHEN p.planned_revenue >= 1000
        THEN LEAST(GREATEST(
            ROUND((COALESCE(pa.actual_revenue, 0) - p.planned_revenue) / p.planned_revenue * 100, 2),
            -100), 500)
        ELSE NULL
    END                                                           AS revenue_variance_pct,
    COALESCE(pa.actual_hours, 0) - p.planned_hours                AS hours_variance,
    pa.first_timecard_date,
    pa.last_timecard_date,
    -- Completion
    CASE
        WHEN p.planned_hours > 0
        THEN ROUND(COALESCE(pa.actual_hours, 0) / p.planned_hours * 100, 2)
        ELSE 0
    END                                                           AS pct_hours_consumed,
    CASE
        WHEN p.planned_revenue > 0
        THEN ROUND(COALESCE(pa.actual_revenue, 0) / p.planned_revenue * 100, 2)
        ELSE 0
    END                                                           AS pct_revenue_recognized,
    p.region,
    p.location,
    p.industry,
    p.customer,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects p
LEFT JOIN project_actuals pa
    ON p.project_id = pa.project_id
LEFT JOIN project_expenses pe
    ON p.project_id = pe.project_id
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_clients cl
    ON p.client_id = cl.client_id
LEFT JOIN deduped_employees lp
    ON p.lead_partner_id = lp.employee_id
LEFT JOIN deduped_employees pm
    ON p.project_manager_id = pm.employee_id
-- Fallback partner: pick a deterministic partner from same region+practice when lp.* is NULL
LEFT JOIN fallback_partner_pool fp
    ON p.region = fp.region
    AND p.practice_area = fp.practice_area
    AND fp.rn = (ABS(HASH(p.project_id)) % 5) + 1
""")

print("gold_project_profitability created.")

apply_table_metadata(
    f"{GOLD_PREFIX}project_profitability",
    "One row per project with planned vs actual revenue, cost, hours, margin. Used for Project "
    "Gross Margins KPIs (CFO + Admin dashboards) and Q2 (NY project margins reduce) + Q9 "
    "(projects over plan) demo questions. Filter on actual_margin_pct BETWEEN 10 AND 50 for "
    "healthy projects, or BETWEEN -50 AND 70 for broader analysis.",
    {
        "project_name": "Project name in format 'Client - Engagement Type' (e.g., 'Google - Performance Transformation Engagement').",
        "client_name": "Client/customer name. May differ from project_name first segment because some projects have multiple stakeholders.",
        "project_type": "Engagement type — Strategy Review, Operations Improvement, Performance Transformation, Innovation Lab, etc.",
        "location": "Office where lead partner sits (city). Use to filter NY vs SF, etc.",
        "lead_partner_name": "Full name of lead partner accountable for the engagement.",
        "project_start_date": "Project kickoff date.",
        "project_end_date": "Project completion or expected end date. Use to filter projects ending in last complete month for monthly margin analysis.",
        "project_status": "Lifecycle status — Active, Completed, Cancelled, On Hold.",
        "is_active": "TRUE if project is currently in progress (status=Active and end_date in future).",
        "planned_hours": "Planned consultant hours per the engagement plan. Synthetic data has timecard hours that often greatly exceed planned_hours; for over-plan questions, filter by actual_hours/planned_hours ratio.",
        "planned_revenue": "Planned project revenue in USD per contract.",
        "planned_cost": "Planned project cost in USD per the engagement model.",
        "planned_margin_pct": "Planned margin percentage = (planned_revenue - planned_cost) / planned_revenue × 100. Typically 30-35%.",
        "actual_revenue": "Actual revenue billed to date in USD (from approved billable timecards).",
        "actual_cost": "Actual cost incurred to date in USD (consultant labor cost). NY projects ending last complete month run ~25% over plan on cost (Q2 demo narrative: contractor rate increases + scope creep).",
        "actual_margin_pct": "Actual margin percentage. Realistic 20-50% range. NY April projects average ~42-43% (vs March 53%) — reflects engineered margin compression.",
        "actual_hours": "Total hours worked on project to date.",
        "team_size": "Distinct count of employees who logged time on this project.",
        "revenue_variance_pct": "Revenue variance percentage vs plan = (actual_revenue - planned_revenue) / planned_revenue × 100.",
        "hours_variance": "Hours variance vs plan = actual_hours - planned_hours. Positive = running over plan.",
        "pct_hours_consumed": "Percentage of planned hours consumed = actual_hours / planned_hours × 100.",
        "pct_revenue_recognized": "Percentage of planned revenue recognized = actual_revenue / planned_revenue × 100.",
    },
)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}project_profitability").show()
spark.sql(f"SELECT COUNT(*) AS has_partner_name FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}project_profitability WHERE lead_partner_name IS NOT NULL").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. gold_project_profitability_monthly (NEW)
# MAGIC - One row per **project × fiscal_month** while a project was active
# MAGIC - Fixes the drill-down failure where "monthly trend for project X" returned 1 row
# MAGIC - Sourced from silver_fact_timecards (already monthly-grain) and silver_fact_expenses

# COMMAND ----------

# DBTITLE 1,Gold: Project Profitability — Monthly Time Series
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}project_profitability_monthly AS
WITH project_monthly_actuals AS (
    SELECT
        project_id,
        DATE_TRUNC('month', work_date)                            AS fiscal_month,
        SUM(billing_amount)                                       AS actual_revenue,
        SUM(cost_amount)                                          AS actual_cost,
        SUM(hours_worked)                                         AS actual_hours,
        SUM(CASE WHEN time_type_clean = 'Billable' THEN hours_worked ELSE 0 END) AS billable_hours,
        COUNT(DISTINCT employee_id)                               AS team_size
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    GROUP BY project_id, DATE_TRUNC('month', work_date)
),
project_monthly_expenses AS (
    SELECT
        project_id,
        DATE_TRUNC('month', expense_date)                         AS fiscal_month,
        SUM(amount)                                               AS actual_expenses
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
    WHERE project_id IS NOT NULL
    GROUP BY project_id, DATE_TRUNC('month', expense_date)
)
SELECT
    p.project_id,
    p.project_name,
    p.client_id,
    cl.client_name,
    p.project_type,
    p.practice_area,
    p.lead_partner_id,
    p.project_start_date,
    p.project_end_date,
    p.project_status,
    a.fiscal_month,
    YEAR(a.fiscal_month)                                          AS fiscal_year,
    MONTH(a.fiscal_month)                                         AS fiscal_month_num,
    -- Actuals
    COALESCE(a.actual_revenue, 0)                                 AS actual_revenue,
    COALESCE(a.actual_cost, 0)                                    AS actual_cost,
    COALESCE(e.actual_expenses, 0)                                AS actual_expenses,
    -- 2026-05-21 — actual_margin / actual_margin_pct redefined as PROJECT
    -- GROSS MARGIN = revenue − direct labor cost only. T&E is below the gross-
    -- margin line in Tier-1 services accounting and was producing -3000% to
    -- -7000% per-project margins for small Audit engagements where monthly
    -- T&E (engineered narratives) exceeded monthly revenue. For NET margin
    -- analysis subtract actual_expenses separately at the query layer.
    COALESCE(a.actual_revenue, 0) - COALESCE(a.actual_cost, 0)    AS actual_margin,
    CASE
        WHEN COALESCE(a.actual_revenue, 0) > 0
        THEN ROUND(
            (COALESCE(a.actual_revenue, 0) - COALESCE(a.actual_cost, 0))
            / a.actual_revenue * 100, 2)
        ELSE NULL
    END                                                           AS actual_margin_pct,
    COALESCE(a.actual_hours, 0)                                   AS actual_hours,
    COALESCE(a.billable_hours, 0)                                 AS billable_hours,
    COALESCE(a.team_size, 0)                                      AS team_size,
    -- Pro-rated monthly plan (planned_revenue / project_duration_months)
    -- so monthly variance is meaningful, not "actuals vs total contract value".
    CASE
        WHEN p.project_start_date IS NOT NULL AND p.project_end_date IS NOT NULL
            AND p.project_end_date > p.project_start_date
            AND p.planned_revenue > 0
        THEN
            p.planned_revenue
            / GREATEST(
                MONTHS_BETWEEN(
                    DATE_TRUNC('month', p.project_end_date),
                    DATE_TRUNC('month', p.project_start_date)
                ) + 1, 1
            )
        ELSE NULL
    END                                                           AS planned_revenue_monthly,
    -- Region/location/industry/customer from project dim
    p.region,
    p.location,
    p.industry,
    p.customer,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM project_monthly_actuals a
JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects p
    ON a.project_id = p.project_id
LEFT JOIN project_monthly_expenses e
    ON a.project_id = e.project_id AND a.fiscal_month = e.fiscal_month
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_clients cl
    ON p.client_id = cl.client_id
""")

print("gold_project_profitability_monthly created.")

apply_table_metadata(
    f"{GOLD_PREFIX}project_profitability_monthly",
    "Project profitability with MONTHLY time-series grain (one row per project × fiscal_month "
    "while project was active). Use this table when drilling into a single project's revenue / "
    "cost / margin trajectory over time. The companion gold_project_profitability table has "
    "one row per project (lifetime aggregate) — use that for project-level comparisons, "
    "this monthly table for project drill-downs.",
    {
        "project_id": "Project identifier — joins to gold_project_profitability and silver_dim_projects.",
        "fiscal_month": "Month boundary (first day of month, DATE type). Use DATE_TRUNC('month', ...) for grouping or filtering.",
        "actual_revenue": "Billable revenue recognized this fiscal_month for this project (from approved billable timecards).",
        "actual_cost": "Labor cost incurred this fiscal_month (from timecard cost_amount).",
        "actual_expenses": "Direct project expenses incurred this fiscal_month (from silver_fact_expenses).",
        "actual_margin_pct": "(actual_revenue - actual_cost - actual_expenses) / actual_revenue × 100. NULL when actual_revenue is 0.",
        "planned_revenue_monthly": "Pro-rated monthly plan = planned_revenue / project_duration_months. Use for monthly variance vs. plan.",
        "billable_hours": "Hours logged as Billable this fiscal_month.",
        "team_size": "Distinct employees who billed time this fiscal_month.",
    },
)
spark.sql(f"SELECT COUNT(*) AS row_count, COUNT(DISTINCT project_id) AS unique_projects, COUNT(DISTINCT fiscal_month) AS unique_months FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}project_profitability_monthly").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. gold_receivables_wip_aging
# MAGIC - Receivables by category: Client Invoice, Contractor, Non-FTE Vendor, Other
# MAGIC - Standardized aging buckets: 0-30, 31-60, 61-90, 90+ days

# COMMAND ----------

# DBTITLE 1,Gold: Receivables & WIP Aging
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}receivables_wip_aging AS
WITH client_invoices AS (
    -- Direct client invoices (the bulk of AR)
    SELECT
        'Client Invoice'                                          AS category,
        invoice_id                                                AS record_id,
        customer_name                                             AS counterparty_name,
        invoice_number,
        invoice_date,
        due_date,
        amount,
        GREATEST(COALESCE(days_outstanding, 0), 0)               AS days_outstanding,
        CASE
            WHEN ABS(HASH(invoice_id)) % 100 < 70 THEN '0-30 days'
            WHEN ABS(HASH(invoice_id)) % 100 < 85 THEN '31-60 days'
            WHEN ABS(HASH(invoice_id)) % 100 < 93 THEN '61-90 days'
            ELSE '90+ days'
        END                                                       AS aging_bucket,
        payment_status,
        project_id,
        region,
        location,
        practice_area,
        industry,
        customer
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_receivable
    WHERE UPPER(COALESCE(payment_status, '')) NOT IN ('PAID', 'CLOSED')
),
contractor_receivables AS (
    -- Contractor hours billed back to clients (subset of WIP)
    SELECT
        'Contractor'                                              AS category,
        timecard_id                                               AS record_id,
        client_name                                               AS counterparty_name,
        NULL                                                      AS invoice_number,
        work_date                                                 AS invoice_date,
        NULL                                                      AS due_date,
        billing_amount * 0.35                                     AS amount,
        DATEDIFF(CURRENT_DATE(), work_date)                       AS days_outstanding,
        CASE
            WHEN ABS(HASH(timecard_id)) % 100 < 70 THEN '0-30 days'
            WHEN ABS(HASH(timecard_id)) % 100 < 85 THEN '31-60 days'
            WHEN ABS(HASH(timecard_id)) % 100 < 93 THEN '61-90 days'
            ELSE '90+ days'
        END                                                       AS aging_bucket,
        'Unbilled'                                                AS payment_status,
        project_id,
        region,
        location,
        practice_area,
        industry,
        customer
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}wip_unbilled
    WHERE is_wip = TRUE
      AND ABS(HASH(timecard_id)) % 5 = 0
),
vendor_reimbursables AS (
    -- Billable expenses passed through to clients
    SELECT
        'Non-FTE Vendor'                                          AS category,
        expense_item_id                                           AS record_id,
        COALESCE(merchant_name, 'Unknown Vendor')                 AS counterparty_name,
        expense_report_id                                         AS invoice_number,
        expense_date                                              AS invoice_date,
        DATE_ADD(expense_date, 45)                                AS due_date,
        amount,
        GREATEST(DATEDIFF(CURRENT_DATE(), DATE_ADD(expense_date, 45)), 0) AS days_outstanding,
        CASE
            WHEN ABS(HASH(expense_item_id)) % 100 < 70 THEN '0-30 days'
            WHEN ABS(HASH(expense_item_id)) % 100 < 85 THEN '31-60 days'
            WHEN ABS(HASH(expense_item_id)) % 100 < 93 THEN '61-90 days'
            ELSE '90+ days'
        END                                                       AS aging_bucket,
        CASE WHEN approval_status = 'Approved' THEN 'Open' ELSE 'Pending' END AS payment_status,
        project_id,
        region,
        location,
        practice_area,
        industry,
        customer
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
    WHERE is_billable = TRUE
      AND expense_date >= DATE_SUB(CURRENT_DATE(), 150)
),
other_receivables AS (
    -- Smaller miscellaneous receivables from remaining WIP
    SELECT
        'Other'                                                   AS category,
        timecard_id                                               AS record_id,
        client_name                                               AS counterparty_name,
        NULL                                                      AS invoice_number,
        work_date                                                 AS invoice_date,
        NULL                                                      AS due_date,
        billing_amount * 0.10                                     AS amount,
        DATEDIFF(CURRENT_DATE(), work_date)                       AS days_outstanding,
        CASE
            WHEN ABS(HASH(timecard_id)) % 100 < 70 THEN '0-30 days'
            WHEN ABS(HASH(timecard_id)) % 100 < 85 THEN '31-60 days'
            WHEN ABS(HASH(timecard_id)) % 100 < 93 THEN '61-90 days'
            ELSE '90+ days'
        END                                                       AS aging_bucket,
        'Unbilled'                                                AS payment_status,
        project_id,
        region,
        location,
        practice_area,
        industry,
        customer
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}wip_unbilled
    WHERE is_wip = TRUE
      AND ABS(HASH(timecard_id)) % 10 = 1
),
combined AS (
    SELECT * FROM client_invoices
    UNION ALL
    SELECT * FROM contractor_receivables
    UNION ALL
    SELECT * FROM vendor_reimbursables
    UNION ALL
    SELECT * FROM other_receivables
)
SELECT
    c.category,
    c.record_id,
    c.counterparty_name,
    c.invoice_number,
    c.invoice_date,
    c.due_date,
    c.amount,
    c.days_outstanding,
    c.aging_bucket,
    c.payment_status,
    c.project_id,
    p.project_name,
    p.lead_partner_id,
    lp.full_name                                                  AS lead_partner_name,
    p.project_manager_id,
    pm.full_name                                                  AS project_manager_name,
    -- Priority score: higher = more urgent
    ROUND(
        (COALESCE(c.days_outstanding, 0) / 30.0) * 40
        + (LOG10(GREATEST(c.amount, 1))) * 20
        + CASE c.category
            WHEN 'Client Invoice' THEN 15
            WHEN 'Contractor'     THEN 10
            WHEN 'Non-FTE Vendor' THEN 5
            ELSE 0
          END,
        2
    )                                                             AS collection_priority_score,
    c.region,
    c.location,
    c.practice_area,
    c.industry,
    c.customer,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM combined c
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects p
    ON c.project_id = p.project_id
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees lp
    ON p.lead_partner_id = lp.employee_id
    AND lp.is_current = TRUE
    AND lp.is_latest_snapshot = TRUE
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees pm
    ON p.project_manager_id = pm.employee_id
    AND pm.is_current = TRUE
    AND pm.is_latest_snapshot = TRUE
""")

print("gold_receivables_wip_aging created.")
apply_table_metadata("gold_receivables_wip_aging", TABLE_DOCS["gold_receivables_wip_aging"], COLUMN_DOCS["gold_receivables_wip_aging"])

apply_table_metadata(
    f"{GOLD_PREFIX}receivables_wip_aging",
    "Combined view of unpaid invoices (AR) and unbilled work-in-progress (WIP) with aging "
    "buckets. Used for receivables aging chart, top unpaid invoices workflow, and Q14 "
    "(unbilled work for NY) drill-down.",
    {
        "category": "Record type — values: AR (unpaid invoice) or WIP (unbilled work).",
        "counterparty_name": "Customer/client name owing the amount.",
        "amount": "Amount in USD outstanding.",
        "days_outstanding": "Days from invoice_date to today (for unpaid) or to payment_date (for paid).",
        "aging_bucket": "Aging bucket — values: 0-30 days, 31-60 days, 61-90 days, 90+ days.",
        "payment_status": "Payment state — values: Open, Partially Paid, Paid, Closed. Filter NOT IN (Paid, Closed) for outstanding only.",
        "collection_priority_score": "Calculated priority score for collection actions. Higher = more urgent.",
    },
)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}receivables_wip_aging").show()
spark.sql(f"SELECT category, aging_bucket, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}receivables_wip_aging GROUP BY category, aging_bucket ORDER BY category, aging_bucket").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4b. gold_ar_snapshot_aging (NEW)
# MAGIC One row per (month-end, region, location, customer, aging_bucket) for the
# MAGIC trailing 13 months. **This is the CFO-meaningful DSO trend table.**
# MAGIC
# MAGIC The receivables_wip_aging table above shows the CURRENT aging snapshot only.
# MAGIC When Genie or a user asks "how has DSO trended month-over-month" or "what
# MAGIC was AR aging in March vs April", the right grain is a **month-end snapshot**
# MAGIC of open invoices — NOT a slice of invoices issued IN that month (the
# MAGIC invoice-issue-cohort grain creates the artifact where older months show
# MAGIC higher DSO because their invoices have had more time to accrue days).
# MAGIC
# MAGIC `firmwide_dso_days` is denormalized per row so Genie can report the
# MAGIC headline DSO trend without re-aggregating.

# COMMAND ----------

# DBTITLE 1,Gold: AR Snapshot Aging (month-end series for DSO trend)
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}ar_snapshot_aging AS
WITH month_ends AS (
    -- Last 13 COMPLETED fiscal month-ends. We exclude the current month-end
    -- because today's date is usually mid-month, and aging computed against
    -- a future month-end double-counts the gap between today and end-of-month
    -- (creating a spurious DSO spike for the current month).
    SELECT LAST_DAY(ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -m - 1)) AS snapshot_date
    FROM (SELECT explode(SEQUENCE(0, 12)) AS m) t
),
ar_open AS (
    -- Open AR as of each month-end: invoice issued on/before snapshot AND
    -- either still unpaid OR paid AFTER the snapshot. Aging is from invoice
    -- issue to snapshot (not to today).
    SELECT
        m.snapshot_date,
        ar.invoice_id,
        ar.customer_name,
        ar.region,
        ar.location,
        ar.amount,
        DATEDIFF(m.snapshot_date, ar.invoice_date) AS days_out
    FROM month_ends m
    JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_receivable ar
      ON ar.invoice_date <= m.snapshot_date
     AND (ar.payment_date IS NULL OR ar.payment_date > m.snapshot_date)
    WHERE DATEDIFF(m.snapshot_date, ar.invoice_date) BETWEEN 0 AND 365
),
firmwide AS (
    SELECT
        snapshot_date,
        ROUND(SUM(amount * days_out) / NULLIF(SUM(amount), 0), 1) AS firmwide_dso_days,
        ROUND(SUM(amount), 2) AS firmwide_open_ar
    FROM ar_open
    GROUP BY snapshot_date
),
bucketed AS (
    SELECT
        snapshot_date,
        region,
        location,
        customer_name,
        CASE
            WHEN days_out <= 30 THEN '0-30 days'
            WHEN days_out <= 60 THEN '31-60 days'
            WHEN days_out <= 90 THEN '61-90 days'
            ELSE '90+ days'
        END AS aging_bucket,
        ROUND(SUM(amount), 2) AS open_ar_balance,
        COUNT(*) AS invoice_count,
        ROUND(SUM(amount * days_out) / NULLIF(SUM(amount), 0), 1) AS weighted_dso_days
    FROM ar_open
    GROUP BY snapshot_date, region, location, customer_name,
        CASE
            WHEN days_out <= 30 THEN '0-30 days'
            WHEN days_out <= 60 THEN '31-60 days'
            WHEN days_out <= 90 THEN '61-90 days'
            ELSE '90+ days'
        END
)
SELECT
    b.snapshot_date,
    b.region,
    b.location,
    b.customer_name,
    b.aging_bucket,
    b.open_ar_balance,
    b.invoice_count,
    b.weighted_dso_days,
    f.firmwide_dso_days,
    f.firmwide_open_ar,
    CURRENT_TIMESTAMP() AS gold_load_timestamp
FROM bucketed b
LEFT JOIN firmwide f USING (snapshot_date)
""")

print("gold_ar_snapshot_aging created.")
apply_table_metadata(
    "gold_ar_snapshot_aging",
    TABLE_DOCS["gold_ar_snapshot_aging"],
    COLUMN_DOCS["gold_ar_snapshot_aging"],
)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}ar_snapshot_aging").show()
spark.sql(
    f"SELECT snapshot_date, aging_bucket, COUNT(*) AS rows, "
    f"ROUND(SUM(open_ar_balance)/1e6, 1) AS open_ar_M "
    f"FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}ar_snapshot_aging "
    f"GROUP BY snapshot_date, aging_bucket ORDER BY snapshot_date DESC, aging_bucket"
).show(60, truncate=False)
spark.sql(
    f"SELECT DISTINCT snapshot_date, firmwide_dso_days "
    f"FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}ar_snapshot_aging "
    f"ORDER BY snapshot_date DESC"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. gold_te_contract_audit (FIXED)
# MAGIC - Time & expense vs. contract budget comparison with lead partner

# COMMAND ----------

# DBTITLE 1,Gold: T&E Contract Audit
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}te_contract_audit AS
WITH project_time AS (
    SELECT
        project_id,
        SUM(hours_worked)                                         AS total_hours,
        SUM(billing_amount)                                       AS total_billed,
        SUM(cost_amount)                                          AS total_cost,
        MIN(work_date)                                            AS earliest_work_date,
        MAX(work_date)                                            AS latest_work_date
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    WHERE time_type_clean = 'Billable'
    GROUP BY project_id
),
project_expense AS (
    SELECT
        project_id,
        SUM(amount)                                               AS total_expense_amount,
        SUM(CASE WHEN is_billable = TRUE THEN amount ELSE 0 END) AS billable_expense_amount,
        COUNT(*)                                                  AS expense_count
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
    WHERE project_id IS NOT NULL
    GROUP BY project_id
)
SELECT
    p.project_id,
    p.project_name,
    p.client_id,
    cl.client_name,
    p.project_type,
    p.practice_area,
    p.lead_partner_id,
    lp.full_name                                                  AS lead_partner_name,
    p.project_manager_id,
    pm.full_name                                                  AS project_manager_name,
    p.contract_id,
    p.contract_number,
    p.total_contract_value,
    p.planned_revenue,
    p.planned_hours,
    p.planned_cost,
    -- Time actuals
    COALESCE(pt.total_hours, 0)                                   AS actual_hours,
    COALESCE(pt.total_billed, 0)                                  AS actual_billed,
    COALESCE(pt.total_cost, 0)                                    AS actual_time_cost,
    -- 2026-05-21 (later) — REVERTED the EXPENSE_SCALE multiplication on
    -- gold_te_contract_audit expense columns. The math is geometrically
    -- incompatible: silver per-project T&E (~$30-45K natural) × EXPENSE_SCALE
    -- (=150) = $4.5-6.75M, which against a $1-2M contract gives 200-500%
    -- T&E:contract ratios — implausible. EXPENSE_SCALE stays at the firmwide
    -- rollup (gold_regional_pnl) where it's needed for OpEx magnitudes; at
    -- the per-project level, silver values are realistic on their own.
    -- The silver MERGE that engineers the 8 T&E outliers (line ~928) has its
    -- /EXPENSE_SCALE removed in parallel, so engineered silver T&E lands at
    -- contract × 6.5-9.5% directly and app.py's ratio computation works.
    COALESCE(pe.total_expense_amount, 0)                          AS actual_expenses,
    COALESCE(pe.billable_expense_amount, 0)                       AS billable_expenses,
    COALESCE(pe.expense_count, 0)                                 AS expense_line_items,
    -- Combined T&E (timecard $ + expense $, both at silver scale)
    COALESCE(pt.total_billed, 0) + COALESCE(pe.billable_expense_amount, 0) AS total_te_billed,
    COALESCE(pt.total_cost, 0) + COALESCE(pe.total_expense_amount, 0)      AS total_te_cost,
    -- Contract utilization
    CASE
        WHEN COALESCE(p.total_contract_value, 0) > 0
        THEN ROUND(
            (COALESCE(pt.total_billed, 0) + COALESCE(pe.billable_expense_amount, 0))
            / p.total_contract_value * 100, 2)
        ELSE 0
    END                                                           AS contract_utilization_pct,
    COALESCE(p.total_contract_value, 0)
        - COALESCE(pt.total_billed, 0)
        - COALESCE(pe.billable_expense_amount, 0)                 AS contract_remaining,
    -- Budget variance — NULL the % when planned_revenue < $1K (noise filter,
    -- see gold_project_profitability for rationale). Cap to [-100, 500].
    CASE
        WHEN p.planned_revenue >= 1000
        THEN LEAST(GREATEST(
            ROUND((COALESCE(pt.total_billed, 0) - p.planned_revenue) / p.planned_revenue * 100, 2),
            -100), 500)
        ELSE NULL
    END                                                           AS revenue_variance_pct,
    CASE
        WHEN p.planned_hours >= 10
        THEN LEAST(GREATEST(
            ROUND((COALESCE(pt.total_hours, 0) - p.planned_hours) / p.planned_hours * 100, 2),
            -100), 500)
        ELSE NULL
    END                                                           AS hours_variance_pct,
    -- Risk flags
    CASE
        WHEN COALESCE(p.total_contract_value, 0) > 0
             AND (COALESCE(pt.total_billed, 0) + COALESCE(pe.billable_expense_amount, 0))
                 > p.total_contract_value * 0.9
        THEN 'High'
        WHEN COALESCE(p.total_contract_value, 0) > 0
             AND (COALESCE(pt.total_billed, 0) + COALESCE(pe.billable_expense_amount, 0))
                 > p.total_contract_value * 0.75
        THEN 'Medium'
        ELSE 'Low'
    END                                                           AS budget_risk_level,
    pt.earliest_work_date,
    pt.latest_work_date,
    p.project_status,
    p.is_active,
    p.region,
    p.location,
    p.industry,
    p.customer,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects p
LEFT JOIN project_time pt
    ON p.project_id = pt.project_id
LEFT JOIN project_expense pe
    ON p.project_id = pe.project_id
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_clients cl
    ON p.client_id = cl.client_id
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees lp
    ON p.lead_partner_id = lp.employee_id
    AND lp.is_current = TRUE
    AND lp.is_latest_snapshot = TRUE
LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees pm
    ON p.project_manager_id = pm.employee_id
    AND pm.is_current = TRUE
    AND pm.is_latest_snapshot = TRUE
""")

print("gold_te_contract_audit created.")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}te_contract_audit").show()
spark.sql(f"SELECT COUNT(*) AS has_partner_name FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}te_contract_audit WHERE lead_partner_name IS NOT NULL").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. Engineered T&E outliers — NOW HANDLED IN SILVER
# MAGIC
# MAGIC The "Top T&E Outliers" engineering moved to silver_fact_expenses (see the
# MAGIC T&E OUTLIER ENGINEERING block in the silver_fact_expenses section above).
# MAGIC We now boost billable T&E by 30× for the top-8 T&E projects rather than
# MAGIC shrinking their contract value by × 0.05. The new approach yields
# MAGIC ~$300K-$500K T&E on multi-$M contracts (Tier-1 magnitudes) instead of
# MAGIC $11K T&E on $58K contracts (micro-deal magnitudes that read wrong to a CFO).
# MAGIC
# MAGIC The gold_project_profitability and gold_te_contract_audit tables built
# MAGIC above already pick up the boosted T&E values from silver, so no
# MAGIC additional engineering is required here. Leaving this section header in
# MAGIC place for navigability of the notebook structure.

# COMMAND ----------

# DBTITLE 1,Gold: T&E Outliers — engineering moved to silver
print("T&E outlier engineering — handled at silver layer (boost in silver_fact_expenses); no gold-layer adjustment required.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. gold_enterprise_metrics (FIXED)
# MAGIC - High-level enterprise KPIs with region and practice area breakdowns

# COMMAND ----------

# DBTITLE 1,Gold: Enterprise Metrics
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}enterprise_metrics AS
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', work_date)                            AS fiscal_period,
        region,
        location,
        practice_area,
        industry,
        customer,
        SUM(billing_amount)                                       AS revenue,
        SUM(cost_amount)                                          AS delivery_cost,
        SUM(hours_worked)                                         AS total_hours,
        SUM(CASE WHEN time_type_clean = 'Billable' THEN hours_worked ELSE 0 END) AS billable_hours,
        COUNT(DISTINCT employee_id)                               AS active_employees
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    GROUP BY DATE_TRUNC('month', work_date), region, location, practice_area, industry, customer
),
monthly_expenses AS (
    SELECT
        DATE_TRUNC('month', expense_date)                         AS fiscal_period,
        region,
        location,
        practice_area,
        industry,
        customer,
        -- EXPENSE_SCALE bumps T&E rollup to firmwide-OpEx magnitude (~30% of revenue).
        -- Applied here so Enterprise Margins KPI reflects realistic services-firm net margin.
        SUM(amount) * {EXPENSE_SCALE}                             AS expenses
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
    GROUP BY DATE_TRUNC('month', expense_date), region, location, practice_area, industry, customer
),
headcount AS (
    SELECT
        region,
        location,
        practice_area,
        industry,
        customer,
        COUNT(*)                                                  AS total_headcount
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees
    WHERE is_current = TRUE
      AND is_latest_snapshot = TRUE
    GROUP BY region, location, practice_area, industry, customer
),
-- DSO computed as MONTH-END SNAPSHOT, not invoice cohort age.
-- Per plausibility_envelope.yml metric_definitions.dso:
--   DSO = (open_ar_balance_at_month_end / monthly_revenue) * days_in_month
-- The cohort-age approach (AVG of days_outstanding at invoice-issue-month
-- grain) produces declining values toward recent periods (newer invoices
-- haven't had time to age) and produces extreme tails like Seoul 4 days /
-- Seoul 170 days that aren't real DSO. Month-end snapshot bounds DSO to
-- the realistic 25-50 day range.
ar_monthly_billing AS (
    SELECT
        DATE_TRUNC('month', invoice_date)                       AS fiscal_period,
        region, location, practice_area, industry, customer,
        SUM(amount)                                             AS monthly_revenue
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_receivable
    GROUP BY DATE_TRUNC('month', invoice_date), region, location, practice_area, industry, customer
),
ar_open_at_month_end AS (
    -- For each fiscal_period (month), sum invoices issued ≤ month-end and
    -- still unpaid as of month-end (no payment OR paid AFTER month-end).
    SELECT
        DATE_TRUNC('month', mb.fiscal_period)                   AS fiscal_period,
        ar.region, ar.location, ar.practice_area, ar.industry, ar.customer,
        SUM(ar.amount)                                          AS open_ar_balance,
        COUNT(*)                                                AS unpaid_invoice_count
    FROM ar_monthly_billing mb
    JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_receivable ar
      ON ar.invoice_date <= LAST_DAY(mb.fiscal_period)
      AND (ar.payment_date IS NULL OR ar.payment_date > LAST_DAY(mb.fiscal_period))
      AND ar.region = mb.region
      AND ar.location = mb.location
      AND ar.practice_area = mb.practice_area
      AND ar.industry = mb.industry
      AND ar.customer = mb.customer
    GROUP BY DATE_TRUNC('month', mb.fiscal_period),
             ar.region, ar.location, ar.practice_area, ar.industry, ar.customer
),
ar_metrics AS (
    SELECT
        mb.fiscal_period,
        mb.region, mb.location, mb.practice_area, mb.industry, mb.customer,
        COALESCE(oar.open_ar_balance, 0)                            AS total_ar,
        CASE
            WHEN mb.monthly_revenue > 0 AND oar.open_ar_balance IS NOT NULL
            THEN ROUND((oar.open_ar_balance / mb.monthly_revenue)
                       * DAY(LAST_DAY(mb.fiscal_period)), 1)
            ELSE NULL
        END                                                         AS avg_dso,
        COALESCE(oar.unpaid_invoice_count, 0)                       AS unpaid_invoice_count
    FROM ar_monthly_billing mb
    LEFT JOIN ar_open_at_month_end oar
        ON mb.fiscal_period = oar.fiscal_period
       AND mb.region = oar.region
       AND mb.location = oar.location
       AND mb.practice_area = oar.practice_area
       AND mb.industry = oar.industry
       AND mb.customer = oar.customer
),
wip_metrics AS (
    SELECT
        DATE_TRUNC('month', work_date)                            AS fiscal_period,
        region,
        location,
        practice_area,
        industry,
        customer,
        SUM(billing_amount)                                       AS total_wip
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}wip_unbilled
    WHERE is_wip = TRUE
    GROUP BY DATE_TRUNC('month', work_date), region, location, practice_area, industry, customer
),
pipeline AS (
    SELECT
        region,
        location,
        practice_area,
        industry,
        customer,
        SUM(planned_revenue)                                      AS pipeline_revenue
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects
    WHERE is_active = TRUE
    GROUP BY region, location, practice_area, industry, customer
)
SELECT
    mr.fiscal_period,
    mr.region,
    mr.location,
    mr.practice_area,
    mr.industry,
    mr.customer,
    mr.revenue,
    mr.delivery_cost,
    COALESCE(me.expenses, 0)                                      AS operating_expenses,
    mr.revenue - mr.delivery_cost - COALESCE(me.expenses, 0)     AS profit,
    CASE
        WHEN mr.revenue > 0
        THEN ROUND((mr.revenue - mr.delivery_cost - COALESCE(me.expenses, 0)) / mr.revenue * 100, 2)
        ELSE 0
    END                                                           AS margin_pct,
    ROUND(mr.revenue - mr.delivery_cost, 2)                       AS gross_profit,
    CASE
        WHEN mr.revenue > 0
        THEN ROUND((mr.revenue - mr.delivery_cost) / mr.revenue * 100, 2)
        ELSE 0
    END                                                           AS gross_margin_pct,
    mr.total_hours,
    mr.billable_hours,
    CASE
        WHEN mr.total_hours > 0
        THEN ROUND(mr.billable_hours / mr.total_hours * 100, 2)
        ELSE 0
    END                                                           AS utilization_pct,
    mr.active_employees,
    COALESCE(hc.total_headcount, 0)                               AS headcount,
    COALESCE(ar.total_ar, 0)                                      AS accounts_receivable,
    -- DSO: leave NULL for slices with no unpaid AR. If we COALESCE to 0, any
    -- downstream AVG() across slices (e.g. "regional DSO trend") gets dragged
    -- toward 0 by all the empty-AR slices, producing 0.27-day "DSO" headlines
    -- that contradict the unpaid-only Top Unpaid Clients table. NULL means
    -- SQL's AVG() correctly ignores empty slices.
    ROUND(ar.avg_dso, 1)                                          AS avg_days_sales_outstanding,
    COALESCE(ar.unpaid_invoice_count, 0)                          AS unpaid_invoice_count,
    COALESCE(wip.total_wip, 0)                                    AS wip_value,
    COALESCE(pl.pipeline_revenue, 0)                              AS pipeline_revenue,
    -- Cash proxy: revenue - cost - expenses + paid AR
    ROUND(mr.revenue - mr.delivery_cost - COALESCE(me.expenses, 0) * 0.8, 2) AS estimated_cash_flow,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM monthly_revenue mr
LEFT JOIN monthly_expenses me
    ON mr.fiscal_period = me.fiscal_period
    AND mr.region = me.region
    AND mr.location = me.location
    AND mr.practice_area = me.practice_area
    AND mr.industry = me.industry
    AND mr.customer = me.customer
LEFT JOIN headcount hc
    ON mr.region = hc.region
    AND mr.location = hc.location
    AND mr.practice_area = hc.practice_area
    AND mr.industry = hc.industry
    AND mr.customer = hc.customer
LEFT JOIN ar_metrics ar
    ON mr.fiscal_period = ar.fiscal_period
    AND mr.region = ar.region
    AND mr.location = ar.location
    AND mr.practice_area = ar.practice_area
    AND mr.industry = ar.industry
    AND mr.customer = ar.customer
LEFT JOIN wip_metrics wip
    ON mr.fiscal_period = wip.fiscal_period
    AND mr.region = wip.region
    AND mr.location = wip.location
    AND mr.practice_area = wip.practice_area
    AND mr.industry = wip.industry
    AND mr.customer = wip.customer
LEFT JOIN pipeline pl
    ON mr.region = pl.region
    AND mr.location = pl.location
    AND mr.practice_area = pl.practice_area
    AND mr.industry = pl.industry
    AND mr.customer = pl.customer
""")

print("gold_enterprise_metrics created.")

apply_table_metadata(
    f"{GOLD_PREFIX}enterprise_metrics",
    "Firmwide financial KPIs by fiscal month and full dimensional cut "
    "(region/location/practice_area/industry/customer). Primary source for: Enterprise Revenue, "
    "Enterprise Margins, DSO, Invoice Receivables (with WIP), and Pipeline Revenue tiles on the "
    "CFO dashboard. One row per (fiscal_period × dimension combination). Always filter to last "
    "complete month for KPI MoM/YoY comparisons — never use partial in-progress month.",
    {
        "fiscal_period": "First day of fiscal month (timestamp). Use ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1) for last complete month.",
        "region": "High-level region — values: Americas, EMEA, Asia Pacific. Always title-case (not all-uppercase).",
        "location": "Office city — e.g., New York, San Francisco, Chicago, London, Frankfurt, Tokyo. Used by NY-vs-SF and Chicago-expense demo narratives.",
        "practice_area": "Service line — values: Strategy & Consulting, Technology, Operations, Managed Services: Tech, Managed Services: Ops, Audit, Tax, Accounting.",
        "industry": "Customer industry vertical (Financial Services, Healthcare, Retail, Manufacturing, etc.).",
        "customer": "Customer name (top accounts: LVMH, Siemens, Federal Republic of Germany, Novartis, etc.).",
        "revenue": "Monthly billable revenue in USD, sourced from approved billable timecards × billing rate. Sum across the dimensional grain when grouping by fiscal_period only.",
        "delivery_cost": "Monthly cost of delivery (consultant labor cost) in USD, from billable timecards × cost rate. Excludes SG&A overhead.",
        "operating_expenses": "Monthly SG&A operating expenses in USD (corporate overhead, marketing, tech, billable T&E, other). Scaled to ~30% of revenue to reflect realistic services-firm OpEx ratio. Does NOT include delivery_cost.",
        "profit": "Operating profit in USD = revenue - delivery_cost - operating_expenses. Used as numerator for Enterprise Margins KPI.",
        "margin_pct": "Operating margin percentage = profit / revenue × 100. Realistic services-firm range 18-25%.",
        "gross_profit": "Gross profit in USD = revenue - delivery_cost (before SG&A). Realistic range 50-55% of revenue.",
        "gross_margin_pct": "Gross margin percentage = gross_profit / revenue × 100.",
        "total_hours": "Total consultant hours worked in the period (billable + non-billable + time-off).",
        "billable_hours": "Hours classified as billable (client-billable timecards only).",
        "utilization_pct": "Utilization percentage = billable_hours / total_hours × 100. Realistic services range 65-75%. Use to identify bench growth.",
        "active_employees": "Distinct count of employees with at least one timecard in the period.",
        "headcount": "Total active headcount per dim_employees as of period end (NOT just employees with timecards).",
        "accounts_receivable": "Outstanding AR balance for invoices issued in this period that are still unpaid (USD). Used in DSO calculation.",
        "avg_days_sales_outstanding": "Average days outstanding across UNPAID invoices in this slice. NULL when the slice has zero unpaid invoices (so SQL AVG() correctly skips empty slices instead of being dragged to 0). DO NOT compute firmwide or regional DSO via AVG(avg_days_sales_outstanding) — query silver_fact_accounts_receivable directly with the unpaid filter instead.",
        "unpaid_invoice_count": "Count of unpaid invoices in this slice (matches the denominator behind avg_days_sales_outstanding). Use as the weight in any weighted DSO aggregation across slices.",
        "wip_value": "Work-in-progress (WIP) value in USD — billable work performed but not yet invoiced. Used in Invoice Receivables KPI alongside accounts_receivable.",
        "pipeline_revenue": "Forecasted revenue from active but unsold projects in the pipeline. NOT the same as actual revenue — represents future opportunity.",
        "estimated_cash_flow": "Calculated cash flow proxy = revenue - delivery_cost - operating_expenses × 0.8. Use as directional indicator only.",
    },
)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}enterprise_metrics").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. gold_payables_aging
# MAGIC - Vendor payables with vendor_classification and standardized aging buckets
# MAGIC - Categories: Contractors, IT & Technology, Marketing & Brand Management, Professional & Legal Services, Real Estate & Facilities

# COMMAND ----------

# DBTITLE 1,Gold: Payables Aging
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}payables_aging AS
SELECT
    ap.vendor_name,
    ap.invoice_number,
    ap.invoice_date,
    ap.due_date,
    ap.amount                                                     AS amount_due,
    GREATEST(COALESCE(ap.days_outstanding, 0), 0)                 AS days_outstanding,
    -- Standardized aging buckets (no 'Current')
    CASE
        WHEN GREATEST(COALESCE(ap.days_outstanding, 0), 0) <= 30  THEN '0-30 days'
        WHEN GREATEST(COALESCE(ap.days_outstanding, 0), 0) <= 60  THEN '31-60 days'
        WHEN GREATEST(COALESCE(ap.days_outstanding, 0), 0) <= 90  THEN '61-90 days'
        ELSE '90+ days'
    END                                                           AS aging_bucket,
    ap.department,
    -- Vendor classification based on vendor name
    CASE
        WHEN ap.vendor_name IN ('Deloitte', 'EY', 'PwC', 'KPMG')
            THEN 'Contractors'
        WHEN ap.vendor_name IN ('AWS', 'Microsoft Azure', 'Google Cloud', 'Dell Technologies',
                                'HP', 'Lenovo', 'Apple', 'Databricks', 'Snowflake',
                                'Salesforce', 'ServiceNow', 'Workday')
            THEN 'IT & Technology'
        WHEN ap.vendor_name IN ('Gartner', 'Forrester', 'IDC')
            THEN 'Marketing & Brand Management'
        WHEN ap.vendor_name IN ('Oracle', 'SAP')
            THEN 'Professional & Legal Services'
        WHEN ap.vendor_name IN ('WeWork', 'Regus', 'JLL', 'CBRE', 'Iron Mountain')
            THEN 'Real Estate & Facilities'
        ELSE 'IT & Technology'
    END                                                           AS vendor_classification,
    -- Payment priority score: age-based (0-60 pts) + amount-based (0-40 pts)
    ROUND(
        LEAST(GREATEST(COALESCE(ap.days_outstanding, 0), 0), 180) / 3.0
        + LOG10(GREATEST(ap.amount, 1)) * 8,
        2
    )                                                             AS payment_priority_score,
    CONCAT(
        YEAR(ap.invoice_date), '-',
        LPAD(CAST(MONTH(ap.invoice_date) AS STRING), 2, '0')
    )                                                             AS fiscal_period,
    ap.payment_status,
    ap.payment_terms,
    ap.gl_account,
    ap.cost_center,
    ap.region,
    ap.location,
    ap.practice_area,
    ap.industry,
    ap.customer,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_payable ap
WHERE UPPER(COALESCE(ap.payment_status, '')) NOT IN ('PAID', 'CLOSED')
""")

print("gold_payables_aging created.")
apply_table_metadata("gold_payables_aging", TABLE_DOCS["gold_payables_aging"], COLUMN_DOCS["gold_payables_aging"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}payables_aging").show()
spark.sql(f"SELECT vendor_classification, aging_bucket, COUNT(*) AS cnt, ROUND(SUM(amount_due), 2) AS total FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}payables_aging GROUP BY vendor_classification, aging_bucket ORDER BY vendor_classification, aging_bucket").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. gold_enterprise_summary (NEW)
# MAGIC - Top-level KPI metrics as name/value rows for executive dashboard cards
# MAGIC - utilization target: 60% (vs. Accenture 75%)

# COMMAND ----------

# DBTITLE 1,Gold: Enterprise Summary
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}enterprise_summary AS
WITH latest_period AS (
    SELECT MAX(DATE_TRUNC('month', work_date)) AS current_period
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
),
prev_period AS (
    SELECT ADD_MONTHS((SELECT current_period FROM latest_period), -1) AS prev_period_date
),
-- Dimension spine: union of all dimension combos present in EITHER period (revenue + expenses)
dim_spine AS (
    SELECT DISTINCT region, location, practice_area, industry, customer
    FROM (
        SELECT region, location, practice_area, industry, customer
        FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
        WHERE DATE_TRUNC('month', work_date) IN (
            (SELECT current_period FROM latest_period),
            (SELECT prev_period_date FROM prev_period)
        ) AND time_type_clean = 'Billable'
        UNION
        SELECT region, location, practice_area, industry, customer
        FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
        WHERE DATE_TRUNC('month', expense_date) IN (
            (SELECT current_period FROM latest_period),
            (SELECT prev_period_date FROM prev_period)
        )
    )
),
-- Dimensional current period revenue
curr_rev AS (
    SELECT region, location, practice_area, industry, customer,
           SUM(billing_amount) AS revenue, SUM(cost_amount) AS cost
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    WHERE DATE_TRUNC('month', work_date) = (SELECT current_period FROM latest_period)
      AND time_type_clean = 'Billable'
    GROUP BY region, location, practice_area, industry, customer
),
prev_rev AS (
    SELECT region, location, practice_area, industry, customer,
           SUM(billing_amount) AS revenue, SUM(cost_amount) AS cost
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    WHERE DATE_TRUNC('month', work_date) = (SELECT prev_period_date FROM prev_period)
      AND time_type_clean = 'Billable'
    GROUP BY region, location, practice_area, industry, customer
),
curr_exp AS (
    SELECT region, location, practice_area, industry, customer,
           SUM(amount) AS expenses
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
    WHERE DATE_TRUNC('month', expense_date) = (SELECT current_period FROM latest_period)
    GROUP BY region, location, practice_area, industry, customer
),
prev_exp AS (
    SELECT region, location, practice_area, industry, customer,
           SUM(amount) AS expenses
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses
    WHERE DATE_TRUNC('month', expense_date) = (SELECT prev_period_date FROM prev_period)
    GROUP BY region, location, practice_area, industry, customer
),
utilization AS (
    SELECT region, location, practice_area, industry, customer,
        SUM(CASE WHEN time_type_clean = 'Billable' THEN hours_worked ELSE 0 END)
            / NULLIF(SUM(hours_worked), 0) * 100 AS util_pct
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    WHERE DATE_TRUNC('month', work_date) = (SELECT current_period FROM latest_period)
    GROUP BY region, location, practice_area, industry, customer
),
prev_util AS (
    SELECT region, location, practice_area, industry, customer,
        SUM(CASE WHEN time_type_clean = 'Billable' THEN hours_worked ELSE 0 END)
            / NULLIF(SUM(hours_worked), 0) * 100 AS util_pct
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    WHERE DATE_TRUNC('month', work_date) = (SELECT prev_period_date FROM prev_period)
    GROUP BY region, location, practice_area, industry, customer
),
headcount AS (
    SELECT region, location, practice_area, industry, customer,
           COUNT(*) AS hc
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees
    WHERE is_current = TRUE
      AND is_latest_snapshot = TRUE
    GROUP BY region, location, practice_area, industry, customer
),
prev_headcount AS (
    -- Previous month snapshot for headcount change KPI
    SELECT region, location, practice_area, industry, customer,
           COUNT(*) AS hc
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees
    WHERE is_current = TRUE
      AND snapshot_date = LAST_DAY(ADD_MONTHS((SELECT current_period FROM latest_period), -1))
    GROUP BY region, location, practice_area, industry, customer
),
-- DSO = (open AR balance today) / (last-30-day revenue) * 30
-- MONTH-END SNAPSHOT GRAIN. See plausibility_envelope.yml metric_definitions.dso.
-- Cohort-age (AVG(days_outstanding)) decays toward recent periods and produces
-- extreme tails (Seoul 4 days / 170 days) that aren't real DSO.
dso_open_ar_today AS (
    SELECT region, location, practice_area, industry, customer,
           SUM(amount) AS open_balance
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_receivable
    WHERE UPPER(COALESCE(payment_status, '')) NOT IN ('PAID', 'CLOSED')
    GROUP BY region, location, practice_area, industry, customer
),
dso_recent_revenue AS (
    SELECT region, location, practice_area, industry, customer,
           SUM(amount) AS last_30d_revenue
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_accounts_receivable
    WHERE invoice_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY region, location, practice_area, industry, customer
),
dso AS (
    SELECT
        COALESCE(oa.region, rr.region)                 AS region,
        COALESCE(oa.location, rr.location)             AS location,
        COALESCE(oa.practice_area, rr.practice_area)   AS practice_area,
        COALESCE(oa.industry, rr.industry)             AS industry,
        COALESCE(oa.customer, rr.customer)             AS customer,
        CASE
            WHEN rr.last_30d_revenue > 0
            THEN ROUND((oa.open_balance / rr.last_30d_revenue) * 30, 1)
            ELSE NULL
        END                                            AS avg_dso
    FROM dso_open_ar_today oa
    FULL OUTER JOIN dso_recent_revenue rr
        ON oa.region = rr.region
       AND oa.location = rr.location
       AND oa.practice_area = rr.practice_area
       AND oa.industry = rr.industry
       AND oa.customer = rr.customer
),
wip AS (
    SELECT region, location, practice_area, industry, customer,
           SUM(billing_amount) AS wip_val
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}wip_unbilled
    WHERE is_wip = TRUE
    GROUP BY region, location, practice_area, industry, customer
),
pipeline AS (
    SELECT region, location, practice_area, industry, customer,
           SUM(planned_revenue) AS pipeline_val
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects
    WHERE is_active = TRUE
    GROUP BY region, location, practice_area, industry, customer
),
-- Period-aware budget at (region, practice_area) level — projects are sparse at finer granularity
budget_rev_agg AS (
    SELECT region, practice_area,
           SUM(
               planned_revenue
               / GREATEST(MONTHS_BETWEEN(COALESCE(project_end_date, DATE_ADD(project_start_date, 365)), project_start_date), 1)
               * 1.05
           ) AS monthly_budget_rev
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects
    WHERE is_active = TRUE
      AND project_start_date <= (SELECT current_period FROM latest_period)
      AND COALESCE(project_end_date, DATE('2099-12-31')) >= (SELECT current_period FROM latest_period)
    GROUP BY region, practice_area
),
-- Distribute budget proportionally to each dimension combo based on current revenue share
rev_share AS (
    SELECT region, location, practice_area, industry, customer, revenue,
           revenue / NULLIF(SUM(revenue) OVER (PARTITION BY region, practice_area), 0) AS share
    FROM curr_rev
),
budget_rev AS (
    SELECT rs.region, rs.location, rs.practice_area, rs.industry, rs.customer,
           ROUND(COALESCE(ba.monthly_budget_rev, 0) * COALESCE(rs.share, 0), 2) AS monthly_budget_rev
    FROM rev_share rs
    LEFT JOIN budget_rev_agg ba ON rs.region <=> ba.region AND rs.practice_area <=> ba.practice_area
)
SELECT metric_name, current_value, previous_value, target_value,
       -- Capped period-over-period change to prevent extreme outliers on dashboards
       CASE
           WHEN previous_value > 0
           THEN LEAST(GREATEST(ROUND((current_value - previous_value) / previous_value * 100, 2), -100), 500)
           WHEN current_value > 0 THEN NULL
           ELSE 0
       END AS change_pct,
       region, location, practice_area, industry, customer,
       (SELECT CAST(current_period AS STRING) FROM latest_period) AS fiscal_period,
       CURRENT_TIMESTAMP() AS last_updated
FROM (
    -- Revenue: use dimension spine with LEFT JOINs, filter out rows where both current and previous are 0
    SELECT 'revenue' AS metric_name,
           COALESCE(cr.revenue, 0) AS current_value,
           COALESCE(pr.revenue, 0) AS previous_value,
           COALESCE(br.monthly_budget_rev, 0) AS target_value,
           ds.region, ds.location, ds.practice_area, ds.industry, ds.customer
    FROM dim_spine ds
    LEFT JOIN curr_rev cr ON ds.region <=> cr.region AND ds.location <=> cr.location AND ds.practice_area <=> cr.practice_area AND ds.industry <=> cr.industry AND ds.customer <=> cr.customer
    LEFT JOIN prev_rev pr ON ds.region <=> pr.region AND ds.location <=> pr.location AND ds.practice_area <=> pr.practice_area AND ds.industry <=> pr.industry AND ds.customer <=> pr.customer
    LEFT JOIN budget_rev br ON ds.region <=> br.region AND ds.location <=> br.location AND ds.practice_area <=> br.practice_area AND ds.industry <=> br.industry AND ds.customer <=> br.customer
    WHERE COALESCE(cr.revenue, 0) > 0 OR COALESCE(pr.revenue, 0) > 0
    UNION ALL
    -- Expenses: anchor on current period expenses, LEFT JOIN previous to avoid false -100% declines
    SELECT 'expenses',
           ce.expenses,
           COALESCE(pe.expenses, 0),
           ce.expenses * 1.30,
           ce.region, ce.location, ce.practice_area, ce.industry, ce.customer
    FROM curr_exp ce
    LEFT JOIN prev_exp pe ON ce.region <=> pe.region AND ce.location <=> pe.location AND ce.practice_area <=> pe.practice_area AND ce.industry <=> pe.industry AND ce.customer <=> pe.customer
    UNION ALL
    -- Utilization: anchor on current period, LEFT JOIN previous to avoid false zero declines
    -- utilization target: 60% (vs. Accenture 75%)
    SELECT 'utilization',
           ROUND(u.util_pct, 2),
           ROUND(COALESCE(pu.util_pct, 0), 2),
           60.0,
           u.region, u.location, u.practice_area, u.industry, u.customer
    FROM utilization u
    LEFT JOIN prev_util pu ON u.region <=> pu.region AND u.location <=> pu.location AND u.practice_area <=> pu.practice_area AND u.industry <=> pu.industry AND u.customer <=> pu.customer
    UNION ALL
    SELECT 'headcount', CAST(h.hc AS DOUBLE), CAST(COALESCE(ph.hc, 0) AS DOUBLE), CAST(NULL AS DOUBLE),
           h.region, h.location, h.practice_area, h.industry, h.customer
    FROM headcount h
    LEFT JOIN prev_headcount ph ON h.region <=> ph.region AND h.location <=> ph.location AND h.practice_area <=> ph.practice_area AND h.industry <=> ph.industry AND h.customer <=> ph.customer
    UNION ALL
    SELECT 'dso', avg_dso, CAST(NULL AS DOUBLE), 45.0,
           region, location, practice_area, industry, customer
    FROM dso
    UNION ALL
    SELECT 'wip_value', wip_val, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
           region, location, practice_area, industry, customer
    FROM wip
    UNION ALL
    SELECT 'pipeline', pipeline_val, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
           region, location, practice_area, industry, customer
    FROM pipeline
)
""")

print("gold_enterprise_summary created.")
apply_table_metadata("gold_enterprise_summary", TABLE_DOCS["gold_enterprise_summary"], COLUMN_DOCS["gold_enterprise_summary"])
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}enterprise_summary").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. gold_practice_area_summary (NEW)
# MAGIC - Practice area revenue performance with pipeline and variance

# COMMAND ----------

# DBTITLE 1,Gold: Practice Area Summary
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}practice_area_summary AS
WITH accrued AS (
    SELECT
        practice_area,
        DATE_TRUNC('month', work_date)                            AS fiscal_period,
        region,
        location,
        industry,
        customer,
        SUM(billing_amount)                                       AS accrued_revenue,
        SUM(cost_amount)                                          AS accrued_cost
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards
    WHERE time_type_clean = 'Billable'
    GROUP BY practice_area, DATE_TRUNC('month', work_date), region, location, industry, customer
),
-- pipeline_revenue intentionally NOT computed here. Same column exists on
-- gold_enterprise_metrics as the canonical source (snapshot backlog of active
-- projects; sums by practice/region cleanly to the firmwide total). A second
-- "annualized monthly run-rate" computation here produced a 30-42x scale
-- mismatch against the dashboard tile + enterprise_metrics, since the same
-- dimensional slice cannot legitimately have two different "pipeline" numbers.
-- Realistic budget: anchor target to trailing 6-month average of actual revenue per grain
-- (smooths month-to-month noise), apply mild deterministic seasonality, then a 10% stretch
-- so the office consistently aims slightly above its run-rate. Misses land in the credible
-- 0-25% range instead of the prior 50%+ swings caused by per-period project-pool churn.
budgeted AS (
    SELECT
        a.practice_area,
        a.region,
        a.location,
        a.industry,
        a.customer,
        a.fiscal_period,
        COALESCE(
            AVG(a.accrued_revenue) OVER (
                PARTITION BY a.practice_area, a.region, a.location, a.industry, a.customer
                ORDER BY a.fiscal_period
                ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
            ),
            a.accrued_revenue
        )
            * CASE EXTRACT(MONTH FROM a.fiscal_period)
                WHEN 1  THEN 0.96  WHEN 2  THEN 0.96  WHEN 3  THEN 1.02
                WHEN 4  THEN 1.00  WHEN 5  THEN 1.02  WHEN 6  THEN 1.06
                WHEN 7  THEN 0.94  WHEN 8  THEN 0.94  WHEN 9  THEN 1.04
                WHEN 10 THEN 1.06  WHEN 11 THEN 1.04  WHEN 12 THEN 1.08
                ELSE 1.00
              END
            * 1.10                                                AS target_revenue
    FROM accrued a
),
top_partner AS (
    SELECT
        practice_area,
        region,
        lead_partner_name,
        ROW_NUMBER() OVER (
            PARTITION BY practice_area, region
            ORDER BY SUM(actual_revenue) DESC
        ) AS rn
    FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}project_profitability
    WHERE lead_partner_name IS NOT NULL
    GROUP BY practice_area, region, lead_partner_name
)
SELECT
    a.practice_area,
    a.fiscal_period,
    ROUND(a.accrued_revenue, 2)                                   AS accrued_revenue,
    ROUND(COALESCE(b.target_revenue, 0), 2)                       AS target_revenue,
    -- Capped variance to prevent extreme outliers
    CASE
        WHEN COALESCE(b.target_revenue, 0) > 0
        THEN LEAST(GREATEST(
            ROUND((a.accrued_revenue - b.target_revenue) / b.target_revenue * 100, 2),
            -100), 500)
        ELSE 0
    END                                                           AS revenue_variance_pct,
    tp.lead_partner_name,
    a.region,
    a.location,
    a.industry,
    a.customer,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM accrued a
LEFT JOIN budgeted b
    ON a.practice_area = b.practice_area
    AND a.region = b.region
    AND a.location = b.location
    AND a.industry = b.industry
    AND a.customer = b.customer
    AND a.fiscal_period = b.fiscal_period
LEFT JOIN top_partner tp
    ON a.practice_area = tp.practice_area
    AND a.region = tp.region
    AND tp.rn = 1
""")

print("gold_practice_area_summary created.")
apply_table_metadata("gold_practice_area_summary", TABLE_DOCS["gold_practice_area_summary"], COLUMN_DOCS["gold_practice_area_summary"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}practice_area_summary").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. gold_department_summary (NEW)
# MAGIC - Department-level expense tracking vs. budget

# COMMAND ----------

# DBTITLE 1,Gold: Department Summary
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}department_summary AS
WITH dept_expenses AS (
    SELECT
        COALESCE(e.expense_category_business, 'Other')            AS department,
        DATE_TRUNC('month', e.expense_date)                       AS fiscal_period,
        e.region,
        e.location,
        e.practice_area,
        e.industry,
        e.customer,
        -- EXPENSE_SCALE applied consistently with gold_regional_pnl + gold_enterprise_metrics
        -- so Genie answers reading from this table match dashboard answers reading from
        -- gold_regional_pnl. Prior to 2026-05-20 this table omitted the multiplier,
        -- causing $565K vs $85M scale mismatches in expense-variance chats.
        SUM(e.amount) * {EXPENSE_SCALE}                           AS accrued_expenses,
        SUM(e.budgeted_amount) * {EXPENSE_SCALE}                  AS budgeted_expenses
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_expenses e
    GROUP BY
        COALESCE(e.expense_category_business, 'Other'),
        DATE_TRUNC('month', e.expense_date),
        e.region,
        e.location,
        e.practice_area,
        e.industry,
        e.customer
),
dept_leads AS (
    -- Derive department lead from cost center mapping and employee data
    SELECT
        department_category AS department,
        FIRST(manager_name, TRUE)                                 AS department_lead
    FROM (
        SELECT
            ccm.department_category,
            CONCAT(emp.first_name, ' ', emp.last_name) AS manager_name
        FROM {CATALOG}.{SCHEMA}.{BRONZE_PREFIX}cost_center_mapping ccm
        LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees emp
            ON ccm.cost_center_code = emp.cost_center
            AND emp.is_current = TRUE
            AND emp.is_latest_snapshot = TRUE
            AND UPPER(emp.job_level) IN ('DIRECTOR', 'VP', 'SVP', 'EXECUTIVE', 'SENIOR PARTNER', 'PARTNER', 'ASSOCIATE PARTNER')
    )
    GROUP BY department_category
)
SELECT
    de.department,
    de.fiscal_period,
    ROUND(de.accrued_expenses, 2)                                 AS accrued_expenses,
    ROUND(de.budgeted_expenses, 2)                                AS budgeted_expenses,
    -- Variance % is NULL when budget < $1000 (silver-scale; cells too small to
    -- produce meaningful percentages — otherwise rows like $11,572 actual on
    -- $261 budget surface as "4,329% over budget", which is data-quality noise
    -- not a real finding). Cap the rest to [-100, 500] to bound outliers.
    CASE
        WHEN de.budgeted_expenses >= 1000
        THEN LEAST(GREATEST(
            ROUND((de.accrued_expenses - de.budgeted_expenses) / de.budgeted_expenses * 100, 2),
            -100), 500)
        ELSE NULL
    END                                                           AS expense_variance_pct,
    dl.department_lead,
    de.region,
    de.location,
    de.practice_area,
    de.industry,
    de.customer,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM dept_expenses de
LEFT JOIN dept_leads dl
    ON UPPER(de.department) = UPPER(dl.department)
""")

print("gold_department_summary created.")
apply_table_metadata("gold_department_summary", TABLE_DOCS["gold_department_summary"], COLUMN_DOCS["gold_department_summary"])
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}department_summary").show()
spark.sql(f"SELECT department, ROUND(SUM(accrued_expenses), 2) AS total_actual, ROUND(SUM(budgeted_expenses), 2) AS total_budget FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}department_summary GROUP BY department ORDER BY total_actual DESC").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. gold_partner_metrics (NEW)
# MAGIC - Partner-level performance: revenue, margin, clients, projects, utilization

# COMMAND ----------

# DBTITLE 1,Gold: Partner Metrics
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{GOLD_PREFIX}partner_metrics AS
WITH partner_projects AS (
    SELECT
        p.lead_partner_id                                         AS partner_id,
        lp.full_name                                              AS partner_name,
        lp.region                                                 AS region,
        lp.location                                               AS location,
        lp.practice_area                                          AS practice_area,
        lp.industry                                               AS industry,
        lp.customer                                               AS customer,
        DATE_TRUNC('month', t.work_date)                          AS fiscal_period,
        COUNT(DISTINCT p.project_id)                              AS project_count,
        COUNT(DISTINCT p.client_id)                               AS client_count,
        SUM(t.billing_amount)                                     AS revenue_managed,
        SUM(t.cost_amount)                                        AS cost_managed,
        SUM(t.hours_worked)                                       AS total_hours,
        SUM(CASE WHEN t.time_type_clean = 'Billable' THEN t.hours_worked ELSE 0 END) AS billable_hours
    FROM {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_projects p
    INNER JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}fact_timecards t
        ON p.project_id = t.project_id
    LEFT JOIN {CATALOG}.{SCHEMA}.{SILVER_PREFIX}dim_employees lp
        ON p.lead_partner_id = lp.employee_id
        AND lp.is_current = TRUE
        AND lp.is_latest_snapshot = TRUE
    WHERE p.lead_partner_id IS NOT NULL
    GROUP BY
        p.lead_partner_id,
        lp.full_name,
        lp.region,
        lp.location,
        lp.practice_area,
        lp.industry,
        lp.customer,
        DATE_TRUNC('month', t.work_date)
)
SELECT
    partner_name,
    partner_id,
    fiscal_period,
    ROUND(revenue_managed, 2)                                     AS revenue_managed,
    CASE
        WHEN revenue_managed > 0
        THEN ROUND((revenue_managed - cost_managed) / revenue_managed * 100, 2)
        ELSE 0
    END                                                           AS profit_margin_pct,
    client_count,
    project_count,
    CASE
        WHEN total_hours > 0
        THEN ROUND(billable_hours / total_hours * 100, 2)
        ELSE 0
    END                                                           AS utilization_rate,
    region,
    location,
    practice_area,
    industry,
    customer,
    CURRENT_TIMESTAMP()                                           AS last_updated
FROM partner_projects
""")

print("gold_partner_metrics created.")

apply_table_metadata(
    f"{GOLD_PREFIX}partner_metrics",
    "Per-partner monthly performance: revenue managed, profit margin, client/project counts, "
    "utilization. Used for Revenue per Partner KPIs and partner-level filtering. One row per "
    "(partner_id × fiscal_period × dimension cut).",
    {
        "partner_name": "Full name of partner.",
        "revenue_managed": "Revenue attributed to this partner's book of business in USD for the period.",
        "profit_margin_pct": "Profit margin percentage on partner's book of business.",
        "client_count": "Distinct clients managed by this partner in the period.",
        "project_count": "Distinct projects managed by this partner in the period.",
        "utilization_rate": "Partner utilization percentage. Below 80% = on bench.",
    },
)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}partner_metrics").show()
spark.sql(f"SELECT partner_name, ROUND(SUM(revenue_managed), 2) AS total_revenue, ROUND(AVG(profit_margin_pct), 2) AS avg_margin FROM {CATALOG}.{SCHEMA}.{GOLD_PREFIX}partner_metrics WHERE partner_name IS NOT NULL GROUP BY partner_name ORDER BY total_revenue DESC LIMIT 10").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Persona Insights Cache Table (empty shell)
# MAGIC The orchestrator notebook (`generate_insights.py`) populates this table.
# MAGIC Creating the empty shell here ensures the app can read from it on a clean
# MAGIC bundle deploy even before the orchestrator's first run (returns no rows
# MAGIC rather than 'table not found').

# COMMAND ----------

# DBTITLE 1,Gold: Persona Insights Cache (shell)
persona_insights_fqn = f"{CATALOG}.{SCHEMA}.{GOLD_PREFIX}persona_insights"
# filter_axis + filter_value are part of the canonical schema (filter-aware insight pre-caching).
# Including them directly in CREATE TABLE avoids Spark ALTER syntax pitfalls.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {persona_insights_fqn} (
    persona              STRING NOT NULL,
    slot_type            STRING NOT NULL,
    slot_id              INT    NOT NULL,
    parent_slot_id       INT,
    parent_slot_type     STRING,
    headline             STRING,
    value                STRING,
    comparison           STRING,
    trend                STRING,
    trend_direction      STRING,
    status_color         STRING,
    narrative            STRING,
    question_text        STRING,
    routed_subqueries    STRING,
    cached_agent_payload STRING,
    target_entity_type   STRING,
    target_entity_value  STRING,
    last_refreshed       TIMESTAMP NOT NULL,
    fiscal_period_anchor DATE,
    filter_axis          STRING,
    filter_value         STRING
) USING DELTA""")

# Migration path for tables created before filter_axis / filter_value were
# added to the canonical schema. ALTER one column at a time (multi-column ADD
# with DEFAULT clauses is not reliably supported across Spark versions).
for col_name in ("filter_axis", "filter_value"):
    try:
        spark.sql(f"ALTER TABLE {persona_insights_fqn} ADD COLUMN IF NOT EXISTS {col_name} STRING")
    except Exception as _e:
        # Already exists or unsupported — fall through. The CREATE TABLE above
        # is the primary path; this is just belt-and-suspenders.
        print(f"  ALTER ADD COLUMN {col_name}: {type(_e).__name__}: {str(_e)[:160]}")

# Backfill defaults for any NULLs (safe whether columns are new or old).
try:
    spark.sql(f"UPDATE {persona_insights_fqn} SET filter_axis = 'firmwide' WHERE filter_axis IS NULL")
    spark.sql(f"UPDATE {persona_insights_fqn} SET filter_value = 'all' WHERE filter_value IS NULL")
except Exception as _e:
    print(f"  Backfill filter defaults: {type(_e).__name__}: {str(_e)[:160]}")

print(f"Ensured {persona_insights_fqn} exists with 21 columns (shell — orchestrator populates rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary: All Silver & Gold Tables with Row Counts

# COMMAND ----------

# DBTITLE 1,Table Summary & Row Counts
from pyspark.sql import Row

silver_tables = [
    "silver_dim_clients",
    "silver_dim_employees",
    "silver_dim_projects",
    "silver_fact_timecards",
    "silver_fact_expenses",
    "silver_wip_unbilled",
    "silver_fact_accounts_payable",
    "silver_fact_accounts_receivable",
    "silver_fact_general_ledger",
]

gold_tables = [
    "gold_regional_pnl",
    "gold_talent_supply_demand",
    "gold_project_profitability",
    "gold_receivables_wip_aging",
    "gold_ar_snapshot_aging",
    "gold_te_contract_audit",
    "gold_enterprise_metrics",
    "gold_payables_aging",
    "gold_enterprise_summary",
    "gold_practice_area_summary",
    "gold_department_summary",
    "gold_partner_metrics",
    "gold_persona_insights",
]

all_tables = silver_tables + gold_tables
results = []

print("=" * 65)
print(f"{'Layer':<10} {'Table Name':<40} {'Row Count':>12}")
print("=" * 65)

for table_name in all_tables:
    layer = "Silver" if table_name.startswith("silver_") else "Gold"
    try:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.{table_name}").collect()[0]["cnt"]
        results.append(Row(layer=layer, table_name=table_name, row_count=count))
        print(f"{layer:<10} {table_name:<40} {count:>12,}")
    except Exception as ex:
        results.append(Row(layer=layer, table_name=table_name, row_count=-1))
        print(f"{layer:<10} {table_name:<40} {'ERROR':>12}  ({str(ex)[:50]})")

print("=" * 65)

total_silver = sum(r.row_count for r in results if r.layer == "Silver" and r.row_count >= 0)
total_gold = sum(r.row_count for r in results if r.layer == "Gold" and r.row_count >= 0)
print(f"\nSilver total rows: {total_silver:,}")
print(f"Gold total rows:   {total_gold:,}")
print(f"Grand total rows:  {total_silver + total_gold:,}")
print(f"\nSilver tables: {len(silver_tables)}")
print(f"Gold tables:   {len(gold_tables)}")
print(f"Total tables:  {len(all_tables)}")
print(f"\nCompleted at: {datetime.now().isoformat()}")