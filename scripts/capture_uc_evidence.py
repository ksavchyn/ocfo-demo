"""Capture text evidence from the Unity Catalog tables built by the job (row counts,
safety-net results, consistency-validation results). Prints plain-text tables.

  DATABRICKS_CONFIG_PROFILE=kat-personal python scripts/capture_uc_evidence.py <catalog>.<schema> <warehouse_id>
"""
import sys
from databricks.sdk import WorkspaceClient

FQ, WH = sys.argv[1], sys.argv[2]
w = WorkspaceClient()


def q(sql):
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=WH, wait_timeout="50s")
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"{r.status.state}: {r.status.error}")
    cols = [c.name for c in r.manifest.schema.columns]
    rows = [[(" ".join(str(v).split()) if v is not None else "")[:90] for v in row] for row in (r.result.data_array or [])]
    return cols, rows


def show(title, sql):
    cols, rows = q(sql)
    print(f"\n== {title}\n-- {' '.join(sql.split())[:160]}{'...' if len(sql) > 160 else ''}")
    widths = [max(len(c), *(len(str(r[i])) for r in rows)) if rows else len(c) for i, c in enumerate(cols)]
    print("  " + "  ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
    for r in rows:
        print("  " + "  ".join(str(v).ljust(widths[i]) for i, v in enumerate(r)))
    print(f"  ({len(rows)} rows)")


tables = ["bronze_workday_timecards", "bronze_ai_gateway_usage", "silver_dim_clients", "silver_dim_projects",
          "silver_fact_timecards", "silver_fact_accounts_receivable", "silver_fact_expenses",
          "gold_project_profitability", "gold_regional_pnl", "gold_ai_cost_observability", "gold_ai_cost_monthly",
          "gold_ai_cost_by_engagement", "gold_ai_cost_by_cost_center", "gold_persona_insights"]
print(f"Unity Catalog schema {FQ}: medallion built on the Lakeflow bronze streaming tables")
show("Row counts across the medallion", " UNION ALL ".join(
    f"SELECT '{t}' AS table_name, COUNT(*) AS row_count FROM {FQ}.{t}" for t in tables))
show("Table types (bronze = Lakeflow streaming tables, metric views for Genie)",
     f"SELECT table_name, table_type FROM {FQ.split('.')[0]}.information_schema.tables "
     f"WHERE table_schema = '{FQ.split('.')[1]}' AND (table_name LIKE 'bronze_ai%' OR table_name LIKE '%_mv' "
     f"OR table_name = 'ai_budget_serving') ORDER BY table_name")
show("Plausibility safety net (04_envelope_safety_net): failures recorded",
     f"SELECT COUNT(*) AS failed_assertions FROM {FQ}.envelope_safety_net_failures")
show("Plausibility safety net: failed assertions (job runs non-strict, so they are logged, not blocking)",
     f"SELECT * FROM {FQ}.envelope_safety_net_failures LIMIT 20")
show("Consistency-validation harness (03_validate_consistency): violations by type",
     f"SELECT * FROM {FQ}.consistency_violations LIMIT 20")
show("Generated persona insights (Claude via AI Gateway), count by persona and slot",
     f"SELECT persona, slot_type, COUNT(*) AS n FROM {FQ}.gold_persona_insights GROUP BY ALL ORDER BY persona, slot_type")
