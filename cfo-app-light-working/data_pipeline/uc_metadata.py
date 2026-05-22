"""Unity Catalog table and column descriptions for main.cfo_proserv tables.

Imported by silver_gold_data_prep_consulting.py — descriptions are applied via
the existing apply_table_metadata() helper. Source-of-truth lives here so
the customer bundle deploy carries the same docs through.

Style mirrors the gold-standard tables (gold_enterprise_metrics, gold_regional_pnl):
1 sentence per column with units, ranges, valid values, and demo-narrative
disambiguation when relevant. Skips self-explanatory timestamps and surrogate IDs.
"""

# =============================================================================
# TABLE-LEVEL DESCRIPTIONS
# =============================================================================
TABLE_DOCS: dict[str, str] = {
    # ---- Tier 1 (in Genie space) ----
    "silver_dim_clients": (
        "Master client/customer dimension — one row per client (deduplicated by "
        "client_id, latest modified wins). Source of client_name, industry, region, "
        "and parent-account hierarchy used for AR drill-downs and customer-named "
        "narrative answers."
    ),
    "silver_dim_projects": (
        "Project/engagement dimension — one row per project. Source of planned_hours, "
        "planned_revenue, planned_cost, contract value, lead partner / project manager "
        "assignments, and status. Joined to fact_timecards / fact_expenses for plan-vs-"
        "actual analysis. is_active = TRUE filter feeds active project lists, pipeline "
        "rollups, and Q9 'projects over plan' demo questions."
    ),
    "silver_fact_accounts_receivable": (
        "Per-invoice fact table for outstanding and paid client invoices (AR). "
        "Amount is post-AR_SCALE=12 magnitude bump so firmwide AR balance lands at "
        "realistic services-firm magnitude. Source for DSO calculation (spec target: "
        "28 days), aging analysis, and Q14 (NY unbilled work) drill-down. Filter "
        "payment_status NOT IN ('PAID','CLOSED') for outstanding receivables."
    ),
    "silver_fact_accounts_payable": (
        "Per-invoice fact table for vendor payables (AP) sourced from SAP. Amount is "
        "post-AP_SCALE=12 bump so total unpaid AP lands at the right magnitude vs "
        "monthly cost (~$1.09B), driving DPO to ~30-40 days (industry norm for "
        "elite consulting). days_outstanding uses a stable hash-based projection for "
        "non-PAID invoices so monthly DPO trend is not artificially anchored to today."
    ),
    "gold_receivables_wip_aging": (
        "Combined view of unpaid invoices (AR) and unbilled work-in-progress (WIP) "
        "with aging buckets. Categories: Client Invoice (the bulk of AR), Contractor, "
        "Non-FTE Vendor, Other. Used for receivables aging chart, top unpaid invoices "
        "workflow, and Q14 (unbilled work for NY) drill-down."
    ),
    "gold_ar_snapshot_aging": (
        "Month-end AR aging snapshot for the trailing 13 months. One row per "
        "(snapshot_date, region, location, customer_name, aging_bucket). "
        "**This is the CFO-meaningful DSO trend table — use it for ALL DSO trend, "
        "DSO MoM, and AR aging trend questions.** Aging is computed as "
        "DATEDIFF(snapshot_date, invoice_date) for invoices that were open as of "
        "that snapshot (issued <= snapshot AND (unpaid OR paid > snapshot)). The "
        "snapshot grain produces a stable MoM trend; do NOT group AR by "
        "invoice-issue-month — that produces a 'cohort age' artifact where older "
        "issue months show higher DSO because their invoices had more time to "
        "accrue. firmwide_dso_days is denormalized per row for headline reporting."
    ),
    "gold_payables_aging": (
        "Open payables fact, one row per outstanding vendor invoice (filtered "
        "payment_status NOT IN ('PAID','CLOSED')) with aging buckets and vendor "
        "classification. Source for vendor concentration analysis, payables aging "
        "chart, and DPO-by-category questions. vendor_classification rolls vendors "
        "into 5 categories: Contractors, IT & Technology, Marketing & Brand "
        "Management, Professional & Legal Services, Real Estate & Facilities."
    ),
    "gold_practice_area_summary": (
        "Practice-area-level monthly summary with accrued revenue, pipeline, "
        "monthly stretch target, and variance vs target. One row per "
        "(practice_area × fiscal_period × dimension cut). Primary source for "
        "monthly-target miss questions ('why did we miss target this month?') — "
        "target_revenue here is the AMBITIOUS monthly stretch target (set ~10% above "
        "trailing 6-month run-rate with seasonality), distinct from "
        "gold_regional_pnl.budgeted_revenue (the conservative annual budget)."
    ),
    "gold_department_summary": (
        "Department-level monthly expense tracking — accrued vs budgeted by "
        "expense_category_business (Billable, Corporate, Marketing, Technology, "
        "Other). Used for department-level expense variance KPIs on the Admin "
        "dashboard and Q11 firmwide-expenses-over-budget drill-downs. Department "
        "lead populated from cost_center mapping joined to senior employees."
    ),
    # ---- Tier 2 (not in Genie space — abbreviated coverage) ----
    "silver_wip_unbilled": (
        "Approved billable timecards within the last 45 days that have not yet been "
        "invoiced (work-in-progress). Joined to dim_employees, dim_projects, and "
        "dim_clients so lead_partner_name / project_manager_name / client_name are "
        "all populated for direct rendering. is_wip = TRUE filter feeds the WIP "
        "rollup used in Invoice Receivables KPI (AR + WIP) and gold_receivables_wip_aging."
    ),
    "silver_fact_general_ledger": (
        "Per-journal-entry GL fact from SAP. Each row is a debit/credit posting "
        "with account_type classified into account_category (Revenue, COGS, "
        "Operating Expense, Other Income, Tax, Asset, Liability, Equity, Other). "
        "Used for trial-balance and account-category rollups; not the primary "
        "source for revenue/cost on dashboards (those come from fact_timecards + "
        "fact_expenses)."
    ),
    "gold_talent_supply_demand": (
        "Workforce supply-vs-demand matrix by region, location, practice_area, "
        "industry, customer, job_level, job_family, and fiscal_period. Combines "
        "active headcount (supply), monthly utilization (active billers + "
        "billable/total hours), and project demand (active project hours). "
        "Used for utilization analysis, bench growth, and capacity-gap questions."
    ),
    "gold_enterprise_summary": (
        "Top-level executive KPIs as one row per (metric_name × dimension cut) for "
        "the latest complete month. metric_name values: revenue, expenses, "
        "utilization, headcount, dso, wip_value, pipeline. Each row carries "
        "current_value, previous_value (prior month), target_value, and capped "
        "change_pct. utilization target: 60% (vs Accenture 75%). DSO "
        "target: 45 days. Drives the executive dashboard KPI tiles."
    ),
}


# =============================================================================
# COLUMN-LEVEL DESCRIPTIONS
# =============================================================================
COLUMN_DOCS: dict[str, dict[str, str]] = {
    # -------------------------------------------------------------------------
    # silver_dim_clients
    # -------------------------------------------------------------------------
    "silver_dim_clients": {
        "client_id": "Unique client identifier (Salesforce account_id). Joined from silver_dim_projects.client_id and silver_fact_accounts_receivable.customer_id.",
        "client_name": "Client/customer display name. Use this when answering 'which clients...' questions.",
        "industry": "Client industry vertical — e.g., Financial Services, Healthcare, Retail, Manufacturing, Communications Media & Technology, Resources, Health & Public Service. Drives industry-level rollups in regional P&L.",
        "client_type": "Salesforce account type — typically Customer, Prospect, Partner. Filter to Customer for revenue-bearing clients.",
        "parent_client_id": "Parent account client_id for hierarchical groupings (e.g., subsidiary roll-ups to a holding company). NULL when the client has no parent.",
        "billing_country": "Billing address country. Use for country-level segmentation when region/location are too coarse.",
        "billing_city": "Billing address city. Distinct from `location` (which is the firm's office city, not the client's billing address).",
        "billing_state": "Billing address state / province.",
        "annual_revenue": "Client's reported annual revenue in USD (from Salesforce). Use for client-size segmentation (Enterprise vs Mid-Market). Not the firm's revenue — see fact_timecards.billing_amount for that.",
        "number_of_employees": "Client's reported employee count. Use for client-size segmentation.",
        "account_manager_id": "Salesforce owner_id for the firm employee managing this client relationship. Joins to silver_dim_employees.employee_id.",
        "client_status": "Client lifecycle status — values: Active, Inactive (defaulted to Active when bronze is null). Filter Active for current book of business.",
        "region": "Client's high-level region — values: Americas, EMEA, Asia Pacific (title-case). Same enum as fact tables.",
        "location": "Office city associated with this client (the firm's office that owns the relationship). Used by NY-vs-SF demo narratives.",
        "practice_area": "Lead service line for this client — values: Strategy & Consulting, Technology, Operations, Managed Services: Tech, Managed Services: Ops, Audit, Tax, Accounting (post-remap from bronze practice area enum).",
        "customer": "Customer name (denormalized; mirrors client_name on most rows). Used as the canonical filter column across fact tables.",
    },

    # -------------------------------------------------------------------------
    # silver_dim_projects
    # -------------------------------------------------------------------------
    "silver_dim_projects": {
        "project_id": "Unique project / engagement identifier (Salesforce engagement_id). Joins to fact_timecards, fact_expenses, fact_accounts_receivable, gold_project_profitability.",
        "project_name": "Project name in format 'Client - Engagement Type' (e.g., 'Google - Performance Transformation Engagement'). Surface in drill-down lists.",
        "client_id": "Client owning the project. Joins to silver_dim_clients.client_id.",
        "opportunity_id": "Salesforce opportunity_id from which this engagement was sold. Link to upstream pipeline.",
        "project_type": "Engagement type — values include Strategy Review, Operations Improvement, Performance Transformation, Innovation Lab, Implementation, etc.",
        "practice_area": "Service line — values: Strategy & Consulting, Technology, Operations, Managed Services: Tech, Managed Services: Ops, Audit, Tax, Accounting.",
        "location": "Office city where the lead partner sits (NY, SF, Chicago, London, etc.). Used to filter NY-vs-SF demo narratives.",
        "lead_partner_id": "Employee ID of accountable partner. Joins to silver_dim_employees.employee_id. ~19% of projects have orphaned partner refs that don't match dim_employees — gold_project_profitability uses a fallback partner pool to backfill names.",
        "project_manager_id": "Employee ID of project manager / engagement manager. Joins to silver_dim_employees.employee_id.",
        "project_start_date": "Project kickoff date.",
        "project_end_date": "Project completion or expected end date. NULL for open-ended engagements. Used to filter projects ending in last complete month for monthly margin analysis.",
        "planned_hours": "Planned consultant hours per the engagement plan. Synthetic value 500-5000 (deterministic by project_id). Active project book runs over plan on hours by design — frame as 'biggest overruns' (synthetic data quirk).",
        "planned_revenue": "Planned project revenue in USD per contract (forecasted_revenue or budget_amount from bronze).",
        "planned_cost": "Planned project cost in USD = planned_revenue × 0.68 (engagement model assumes 32% planned margin).",
        "planned_margin": "Planned margin in USD = planned_revenue - planned_cost.",
        "planned_margin_pct": "Planned margin percentage. Constant 32% by construction (1 - 0.68); use as the baseline for actual_margin_pct comparisons.",
        "contract_id": "Contract identifier from bronze_sfdc_contracts. Joins to silver_dim_clients via project.client_id only — no direct contract dim.",
        "contract_number": "Human-readable contract number (e.g., MSA-2024-0178).",
        "total_contract_value": "Total contract value (TCV) in USD at Tier-1 magnitudes (mostly $1M-$15M, larger enterprise deals run higher). The 'Top T&E Outliers' surfacing now comes from boosted billable T&E in silver_fact_expenses (× 30 for 8 top-T&E projects) rather than shrunken TCV, so TCV here is the un-engineered value.",
        "billing_frequency": "Contract billing cadence — values: Monthly, Quarterly, Milestone, Time & Materials.",
        "payment_terms": "Contract payment terms — values: Net 30, Net 45, Net 60, Net 90.",
        "contract_status": "Contract lifecycle state — values: Active, Expired, Cancelled, Pending.",
        "project_status": "Project lifecycle status — values: Active, In Progress, Open, Completed, Cancelled, On Hold.",
        "is_active": "TRUE if project_status IN ('Active','In Progress','Open'). Filter to TRUE for active project book and pipeline rollups.",
        "assigned_headcount": "Distinct count of employees assigned to this project (from bronze_workday_assignments).",
        "active_assignments": "Count of currently-active staffing assignments on the project.",
        "planned_assignments": "Count of planned (not yet started) staffing assignments on the project.",
        "avg_allocation_pct": "Average % allocation across all assignments on the project (0-100).",
        "required_headcount": "Required headcount derived from planned_hours × 2.0-2.6x uplift / project lifetime. ~55-65% of projects are understaffed by design (ProServ understaffing narrative).",
        "region": "High-level region — Americas, EMEA, Asia Pacific (title-case).",
        "location": "Office city where engagement is delivered.",
        "industry": "Client industry vertical.",
        "customer": "Customer/client name — canonical filter column across fact tables.",
    },

    # -------------------------------------------------------------------------
    # silver_fact_accounts_receivable
    # -------------------------------------------------------------------------
    "silver_fact_accounts_receivable": {
        "invoice_id": "Unique invoice identifier from SAP AR.",
        "customer_id": "Client identifier. Joins to silver_dim_clients.client_id.",
        "customer_name": "Client/customer display name on the invoice.",
        "invoice_number": "Human-readable invoice number.",
        "invoice_date": "Date the invoice was issued. Use for monthly aggregation: DATE_TRUNC('MONTH', invoice_date).",
        "due_date": "Date payment is due per contract terms.",
        "amount": "Invoice amount in USD (post-AR_SCALE=12 magnitude bump). Sum across unpaid invoices for outstanding AR balance.",
        "currency": "ISO currency code. Demo data is USD-only; column kept for schema completeness.",
        "payment_terms": "Payment terms — values: Net 30, Net 45, Net 60, Net 90.",
        "payment_status": "Payment state — values: PAID, OPEN, OVERDUE, PARTIALLY PAID, CLOSED. Filter NOT IN ('PAID','CLOSED') for outstanding receivables.",
        "payment_date": "Date payment was received (NULL for unpaid invoices).",
        "days_outstanding": "Days outstanding — for PAID invoices = days from invoice_date to payment_date; for unpaid = days from invoice_date to today. Driver of DSO. Spec target: 28 days. NY Operations practice has elevated DSO last complete month (demo narrative).",
        "aging_bucket": "Aging bucket — values: '0-30 days', '31-60 days', '61-90 days', '90+ days'. Drives receivables aging chart.",
        "project_id": "Project the invoice was generated from. Joins to silver_dim_projects.project_id.",
        "lead_partner_name": "Full name of partner accountable for the underlying engagement (sourced via project → dim_employees join). 'Unassigned' when partner ref doesn't resolve.",
        "region": "High-level region — Americas, EMEA, Asia Pacific.",
        "location": "Office city. NY clients in Operations practice drive recent DSO increase (demo narrative).",
        "practice_area": "Service line of the underlying engagement.",
        "industry": "Client industry vertical.",
        "customer": "Customer name — canonical filter column. Aligns with customer_name on this row.",
    },

    # -------------------------------------------------------------------------
    # silver_fact_accounts_payable
    # -------------------------------------------------------------------------
    "silver_fact_accounts_payable": {
        "invoice_id": "Unique vendor invoice identifier from SAP AP.",
        "vendor_id": "Vendor identifier.",
        "vendor_name": "Vendor display name. Maps to vendor_classification in gold_payables_aging (Contractors: Deloitte/EY/PwC/KPMG; IT & Technology: AWS/Azure/GCP/Databricks/Snowflake/Salesforce/Workday/etc.; Real Estate & Facilities: WeWork/Regus/JLL/CBRE/Iron Mountain).",
        "invoice_number": "Human-readable vendor invoice number.",
        "invoice_date": "Date the vendor issued the invoice.",
        "due_date": "Date payment is due to the vendor per payment_terms.",
        "amount": "Invoice amount in USD (post-AP_SCALE=12 magnitude bump so firmwide AP balance lands at realistic magnitude relative to monthly cost ~$1.09B).",
        "currency": "ISO currency code. Demo data is USD-only.",
        "payment_terms": "Payment terms — values: Net 30, Net 45, Net 60, Net 90.",
        "payment_status": "Payment state — values: PAID, OPEN, OVERDUE, PARTIALLY PAID, CLOSED. Filter NOT IN ('PAID','CLOSED') for open payables.",
        "payment_date": "Date the invoice was paid (NULL for unpaid).",
        "gl_account": "GL account number the invoice posts to.",
        "cost_center": "Cost center charged for the invoice. Joins to bronze_cost_center_mapping for department_category.",
        "department": "Department that owns the spend (denormalized for direct filtering).",
        "days_outstanding": "Days outstanding. PAID invoices use actual payment delay; OVERDUE invoices use 60-95 day projection; PARTIALLY PAID 35-60; OPEN follows 70/25/5 distribution across 0-30/31-60/61-90 day bands. Centered on ~30-35 days to match DPO Insight band.",
        "aging_bucket": "Aging bucket — values: '0-30 days', '31-60 days', '61-90 days', '90+ days'.",
        "region": "High-level region — Americas, EMEA, Asia Pacific.",
        "location": "Office city where the spend is booked.",
        "practice_area": "Practice area allocation for the invoice.",
        "industry": "Industry tag (often denormalized from project).",
        "customer": "Associated customer/client when the spend is rebillable.",
    },

    # -------------------------------------------------------------------------
    # gold_receivables_wip_aging
    # -------------------------------------------------------------------------
    "gold_receivables_wip_aging": {
        "category": "Record type — values: 'Client Invoice' (unpaid AR — the bulk), 'Contractor' (contractor hours billed back to clients, subset of WIP), 'Non-FTE Vendor' (billable expenses passed through to clients), 'Other' (smaller misc receivables from WIP).",
        "record_id": "Source record ID (invoice_id for AR, timecard_id for WIP, expense_item_id for vendor reimbursables).",
        "counterparty_name": "Customer/client name owing the amount (or vendor name for Non-FTE Vendor category).",
        "invoice_number": "Invoice number for AR rows; NULL for WIP/contractor rows that haven't been invoiced yet.",
        "invoice_date": "Invoice issue date for AR; work_date or expense_date for WIP/vendor categories.",
        "due_date": "Payment due date for AR; NULL for unbilled WIP; expense_date+45 for vendor reimbursables.",
        "amount": "Amount in USD outstanding. Sum across rows (filtered to outstanding) for total AR + WIP exposure (Invoice Receivables KPI).",
        "days_outstanding": "Days from invoice_date / work_date to today (for unpaid) or to payment_date (for paid).",
        "aging_bucket": "Aging bucket — values: '0-30 days', '31-60 days', '61-90 days', '90+ days'.",
        "payment_status": "Payment state — values: Open, Partially Paid, Paid, Closed, Unbilled, Pending. Filter NOT IN ('Paid','Closed') for outstanding only.",
        "project_id": "Project the receivable was generated from.",
        "project_name": "Project name (denormalized from dim_projects).",
        "lead_partner_name": "Full name of accountable partner (from dim_projects via dim_employees).",
        "project_manager_name": "Full name of project manager.",
        "collection_priority_score": "Calculated priority score for collection actions = (days_outstanding/30)×40 + LOG10(amount)×20 + category_weight (Client Invoice:15, Contractor:10, Non-FTE Vendor:5, Other:0). Higher = more urgent. Used to rank top unpaid invoices.",
        "region": "High-level region — Americas, EMEA, Asia Pacific.",
        "location": "Office city.",
        "practice_area": "Service line.",
        "industry": "Client industry vertical.",
        "customer": "Customer name — canonical filter column.",
    },

    # -------------------------------------------------------------------------
    # gold_ar_snapshot_aging
    # -------------------------------------------------------------------------
    "gold_ar_snapshot_aging": {
        "snapshot_date": "Month-end date the aging snapshot was computed for. Filter to a specific month for point-in-time aging; group by snapshot_date for DSO trend.",
        "region": "High-level region — Americas, EMEA, Asia Pacific.",
        "location": "Office city.",
        "customer_name": "Client name. The customer's invoices may appear across multiple offices when a global engagement is led out of one office and billed to others.",
        "aging_bucket": "Aging bucket relative to snapshot_date — values: '0-30 days', '31-60 days', '61-90 days', '90+ days'.",
        "open_ar_balance": "Sum of open invoice amounts at the snapshot. SUM across rows for total open AR; aggregate without aging_bucket for customer/office totals.",
        "invoice_count": "Number of open invoices at the snapshot in this aging bucket / customer / office cell.",
        "weighted_dso_days": "Amount-weighted average days outstanding for invoices in this cell. Use SUM(open_ar_balance × weighted_dso_days)/SUM(open_ar_balance) to roll up across cells.",
        "firmwide_dso_days": "Amount-weighted firmwide DSO as of snapshot_date — denormalized identically across every row of the same snapshot. SELECT DISTINCT (snapshot_date, firmwide_dso_days) for the headline DSO trend.",
        "firmwide_open_ar": "Total open AR firmwide at snapshot_date — denormalized identically across every row of the same snapshot.",
        "gold_load_timestamp": "Pipeline load timestamp.",
    },

    # -------------------------------------------------------------------------
    # gold_payables_aging
    # -------------------------------------------------------------------------
    "gold_payables_aging": {
        "vendor_name": "Vendor display name. Source for vendor concentration analysis (top 5 vendors by amount_due).",
        "invoice_number": "Human-readable vendor invoice number.",
        "invoice_date": "Date the vendor issued the invoice.",
        "due_date": "Date payment is due to the vendor.",
        "amount_due": "Outstanding payable amount in USD (post-AP_SCALE bump, filtered to NOT PAID/CLOSED).",
        "days_outstanding": "Days outstanding on the unpaid invoice. Centered on ~30-35 days to match DPO band.",
        "aging_bucket": "Aging bucket — values: '0-30 days', '31-60 days', '61-90 days', '90+ days'. Drives payables aging chart.",
        "department": "Department that owns the spend.",
        "vendor_classification": "Vendor category — values: 'Contractors' (Deloitte, EY, PwC, KPMG), 'IT & Technology' (AWS, Azure, GCP, Databricks, Snowflake, Salesforce, Workday, etc. — also default bucket), 'Marketing & Brand Management' (Gartner, Forrester, IDC), 'Professional & Legal Services' (Oracle, SAP), 'Real Estate & Facilities' (WeWork, Regus, JLL, CBRE, Iron Mountain).",
        "payment_priority_score": "Payment priority score = LEAST(days_outstanding,180)/3 + LOG10(amount)×8. Higher = more urgent. Used to rank vendors for AP cycles.",
        "fiscal_period": "Fiscal period as 'YYYY-MM' string derived from invoice_date.",
        "payment_status": "Payment state — values: OPEN, OVERDUE, PARTIALLY PAID. PAID/CLOSED rows are filtered out at the gold layer.",
        "payment_terms": "Payment terms — values: Net 30, Net 45, Net 60, Net 90.",
        "gl_account": "GL account number the invoice posts to.",
        "cost_center": "Cost center charged.",
        "region": "High-level region — Americas, EMEA, Asia Pacific.",
        "location": "Office city where spend is booked.",
        "practice_area": "Practice area allocation.",
        "industry": "Industry tag.",
        "customer": "Associated customer when rebillable.",
    },

    # -------------------------------------------------------------------------
    # gold_practice_area_summary
    # -------------------------------------------------------------------------
    "gold_practice_area_summary": {
        "practice_area": "Service line — values: Strategy & Consulting, Technology, Operations, Managed Services: Tech, Managed Services: Ops, Audit, Tax, Accounting. Technology (D&A) practice underperformed Q1 2026 due to enterprise client engagement pauses (demo narrative).",
        "fiscal_period": "First day of fiscal month (timestamp). Filter < DATE_TRUNC('MONTH', CURRENT_DATE()) to exclude partial in-progress month.",
        "accrued_revenue": "Monthly accrued billable revenue in USD (sum of billing_amount from billable timecards). Same definition as gold_enterprise_metrics.revenue at this finer grain.",
        "target_revenue": "Monthly STRETCH target in USD — set ~10% above trailing 6-month rolling average of accrued_revenue with mild seasonal adjustment (Q4 +6-8%, summer -6%). Firm misses these in 5-7 of every 12 months by 5-30%. NOT the annual budget — for annual budget see gold_regional_pnl.budgeted_revenue.",
        "revenue_variance_pct": "Variance vs monthly stretch target = (accrued_revenue - target_revenue) / target_revenue × 100, capped to [-100, 500].",
        "lead_partner_name": "Top partner by revenue in this practice/region (rank 1 from project_profitability). May be NULL when no partner record resolves.",
        "region": "High-level region — Americas, EMEA, Asia Pacific.",
        "location": "Office city. NY Operations + Marketing/Sales practices have engineered revenue dampener for last complete month (demo narrative).",
        "industry": "Client industry vertical.",
        "customer": "Customer name — canonical filter column.",
    },

    # -------------------------------------------------------------------------
    # gold_department_summary
    # -------------------------------------------------------------------------
    "gold_department_summary": {
        "department": "Department / expense_category_business — values: Billable, Corporate, Marketing, Technology, Other. Same enum as silver_fact_expenses.expense_category_business.",
        "fiscal_period": "First day of fiscal month (timestamp). Filter < DATE_TRUNC('MONTH', CURRENT_DATE()) to exclude partial in-progress month.",
        "accrued_expenses": "Monthly actual expenses in USD at silver scale (NOT yet × EXPENSE_SCALE). Sum across all rows for department-level monthly actual.",
        "budgeted_expenses": "Monthly expense budget in USD at silver scale. Spec: actuals run ~6-8% over budget for normal months; Q4 (Oct-Dec) spikes to ~12-18% over for Q4 'December spike' demo beat.",
        "expense_variance_pct": "Variance vs budget = (accrued_expenses - budgeted_expenses) / budgeted_expenses × 100. Positive = over budget.",
        "department_lead": "Senior employee mapped from cost_center_mapping → dim_employees (Director, VP, SVP, Executive, Senior Partner, Partner, or Associate Partner). May be NULL when mapping doesn't resolve.",
        "region": "High-level region — Americas, EMEA, Asia Pacific.",
        "location": "Office city. Chicago has engineered Q4 2025 Tech/Other category spike (demo narrative).",
        "practice_area": "Service line of the underlying engagement (when expense was rebillable).",
        "industry": "Client industry vertical (when applicable).",
        "customer": "Customer name (when expense was rebillable).",
    },

    # -------------------------------------------------------------------------
    # silver_wip_unbilled (Tier 2 — abbreviated)
    # -------------------------------------------------------------------------
    "silver_wip_unbilled": {
        "timecard_id": "Unique timecard identifier from silver_fact_timecards.",
        "employee_id": "Consultant who logged the time. Joins to silver_dim_employees.employee_id.",
        "employee_name": "Consultant full name.",
        "job_level": "Consultant job level — Senior Partner, Partner, Associate Partner, Director, Engagement Manager, Associate, Business Analyst.",
        "client_name": "Client/customer name (denormalized from dim_clients).",
        "lead_partner_name": "Full name of lead partner on the underlying engagement.",
        "project_manager_name": "Full name of project manager on the underlying engagement.",
        "work_date": "Date the work was performed. Filtered to last 45 days.",
        "hours_worked": "Hours logged on this timecard (already adjusted for seniority utilization, practice premium, and demo narrative anomalies).",
        "billing_amount": "Revenue value of the unbilled work in USD = hours_worked × billing_rate. Sum across is_wip=TRUE for total WIP value.",
        "cost_amount": "Cost incurred = hours_worked × cost_rate.",
        "margin_amount": "Margin contribution = billing_amount - cost_amount.",
        "is_wip": "TRUE if time_type_clean='Billable' AND approval_status IN ('APPROVED','SUBMITTED'). Canonical filter for WIP rollups.",
        "approval_status": "Approval state — values: APPROVED, SUBMITTED, PENDING.",
    },

    # -------------------------------------------------------------------------
    # silver_fact_general_ledger (Tier 2 — abbreviated)
    # -------------------------------------------------------------------------
    "silver_fact_general_ledger": {
        "entry_id": "Unique GL entry identifier.",
        "posting_date": "Date the entry was posted to GL.",
        "gl_account": "GL account number.",
        "gl_account_name": "GL account human-readable name.",
        "account_type": "Raw account type from SAP — Revenue, Income, Sales, COGS, Expense, Tax, Asset, Liability, Equity, etc.",
        "account_category": "Normalized account category — values: Revenue, COGS, Operating Expense, Other Income, Tax, Asset, Liability, Equity, Other. Use this column (not account_type) for category rollups.",
        "debit_amount": "Debit amount in USD (positive). NULL or 0 when row is a credit-only entry.",
        "credit_amount": "Credit amount in USD (positive). NULL or 0 when row is a debit-only entry.",
        "net_amount": "Net amount = debit_amount - credit_amount. Use signed sum for trial balance rollups.",
        "fiscal_year": "Fiscal year (integer, e.g., 2026).",
        "fiscal_period": "Fiscal period within the year (1-12 integer).",
    },

    # -------------------------------------------------------------------------
    # gold_talent_supply_demand (Tier 2 — abbreviated)
    # -------------------------------------------------------------------------
    "gold_talent_supply_demand": {
        "fiscal_period": "First day of fiscal month (timestamp). Filter < DATE_TRUNC('MONTH', CURRENT_DATE()) to exclude partial month.",
        "job_level": "Job level — Senior Partner, Partner, Associate Partner, Director, Engagement Manager, Associate, Business Analyst.",
        "job_family": "Job family classification (Consulting, Technology, Operations, Corporate Functions, etc.).",
        "supply_headcount": "Active headcount in this dimension cut as of latest snapshot.",
        "active_billers": "Distinct count of employees who logged any timecard in the period.",
        "total_hours": "Total hours worked in the period (billable + non-billable + time-off).",
        "billable_hours": "Hours classified as billable.",
        "utilization_pct": "Utilization percentage = billable_hours / total_hours × 100. Realistic services range 65-75%; demo target 60% (vs Accenture 75%). D&A practice is typically the outlier.",
        "demand_projects": "Count of active projects in this dimension cut.",
        "demand_hours": "Sum of planned_hours across active projects.",
        "demand_coverage_pct": "Demand-coverage ratio = demand_hours / (supply_headcount × 160) × 100. Above 100% indicates the practice is undersupplied.",
    },

    # -------------------------------------------------------------------------
    # gold_enterprise_summary (Tier 2 — abbreviated)
    # -------------------------------------------------------------------------
    "gold_enterprise_summary": {
        "metric_name": "KPI identifier — values: 'revenue', 'expenses', 'utilization', 'headcount', 'dso', 'wip_value', 'pipeline'. One row per (metric × dimension cut).",
        "current_value": "Current-period value of the metric (last complete month for revenue/expenses/utilization; latest for headcount/dso/wip/pipeline). USD for revenue/expenses/wip/pipeline; days for dso; %% for utilization; integer for headcount.",
        "previous_value": "Prior-period value of the metric (NULL for dso/wip/pipeline which are point-in-time only).",
        "target_value": "Target/budget value for the metric. Revenue: monthly_budget_rev distributed by current revenue share. Expenses: current × 1.30. Utilization: 60.0 (demo target). DSO: 45.0. NULL for headcount/wip/pipeline.",
        "change_pct": "Period-over-period change = (current - previous) / previous × 100, capped to [-100, 500] to prevent extreme outliers on dashboard tiles.",
        "region": "High-level region — Americas, EMEA, Asia Pacific.",
        "location": "Office city. NY Operations + Marketing/Sales practices have engineered revenue dampener for last complete month.",
        "practice_area": "Service line. D&A practice underperformed Q1 2026 due to enterprise client engagement pauses.",
        "industry": "Client industry vertical.",
        "customer": "Customer name — canonical filter column.",
        "fiscal_period": "Latest complete month as a string (e.g., '2026-04-01 00:00:00').",
    },
}
