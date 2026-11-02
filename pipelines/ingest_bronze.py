# Lakeflow Spark Declarative Pipeline: raw source extracts -> governed bronze tables.
#
# data_pipeline/01_generate_bronze_data.py lands one folder of raw Parquet per
# source table in the UC Volume /Volumes/<catalog>/<schema>/raw_landing/ (it
# stands in for the SAP, Salesforce, Workday, Concur, and AI Gateway system-table
# extracts). This pipeline ingests each folder incrementally with Auto Loader
# into a bronze_<source> streaming table in Unity Catalog. The silver/gold build
# (data_pipeline/02_build_silver_gold.py) reads these tables.
#
# Data quality: every table carries an expectation that no rows landed in
# Auto Loader's _rescued_data column (i.e. the file matched the inferred schema).
# Violations are logged to the pipeline event log, not dropped, so a schema drift
# in a source extract is visible without losing rows.

from pyspark import pipelines as dp

LANDING_ROOT = spark.conf.get("cfo.landing_root")  # noqa: F821  (set in the pipeline configuration)

# source folder -> (source system, description)
SOURCES = {
    "workday_employees": ("Workday", "Employees and job levels"),
    "workday_positions": ("Workday", "Open and filled positions"),
    "workday_timecards": ("Workday", "Timecard entries (billable and non-billable hours)"),
    "workday_billing_rates": ("Workday", "Billing rates by level and practice"),
    "workday_cost_rates": ("Workday", "Cost rates by level"),
    "workday_assignments": ("Workday", "Staffing assignments to engagements"),
    "workday_organizations": ("Workday", "Practice and office hierarchy"),
    "sfdc_accounts": ("Salesforce", "Client accounts"),
    "sfdc_opportunities": ("Salesforce", "Pipeline opportunities"),
    "sfdc_contracts": ("Salesforce", "Client contracts and contract types"),
    "sfdc_engagements": ("Salesforce", "Engagements (projects) with budgets"),
    "sfdc_forecasts": ("Salesforce", "Revenue forecasts"),
    "concur_expense_reports": ("Concur", "T&E expense reports"),
    "concur_expense_items": ("Concur", "T&E expense line items"),
    "concur_travel_bookings": ("Concur", "Travel bookings"),
    "concur_approvals": ("Concur", "Expense approvals"),
    "sap_accounts_payable": ("SAP", "Accounts payable"),
    "sap_accounts_receivable": ("SAP", "Accounts receivable and aging"),
    "sap_general_ledger": ("SAP", "General ledger entries"),
    "sap_cost_centers": ("SAP", "Cost centers"),
    "sap_profit_centers": ("SAP", "Profit centers"),
    "sap_purchase_orders": ("SAP", "Purchase orders"),
    "cost_center_mapping": ("SAP", "Cost center to practice mapping"),
    "ai_budget_policy": ("Databricks", "AI budget policies by cost center"),
    "ai_list_prices": ("Databricks system tables", "system.billing.list_prices look-alike"),
    "ai_gateway_usage": ("Databricks system tables", "system.ai_gateway.usage look-alike (per-request tokens and tags)"),
    "billing_usage": ("Databricks system tables", "system.billing.usage look-alike (DBU usage with custom tags)"),
    "ai_gateway_external_model_spend": ("Databricks system tables", "system.ai_gateway.external_model_spend look-alike"),
}


def _bronze_table(source: str, system: str, description: str):
    @dp.table(
        name=f"bronze_{source}",
        comment=f"Bronze: raw {system} extract, {description}. Ingested by Auto Loader from {LANDING_ROOT}/{source}.",
        table_properties={"quality": "bronze", "source_system": system},
    )
    @dp.expect("no_rescued_data", "_rescued_data IS NULL")
    def _ingest():
        return (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "parquet")
            .load(f"{LANDING_ROOT}/{source}")
        )

    return _ingest


for _source, (_system, _description) in SOURCES.items():
    _bronze_table(_source, _system, _description)
