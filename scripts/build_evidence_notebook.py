"""Build and EXECUTE evidence/EXECUTION_EVIDENCE.ipynb against the live workspace.

Every output cell is produced by actually running the code below against
fevm-kat-personal (job run, Lakeflow pipeline event log, UC tables, Lakebase,
deployed app, Genie agent). Outputs are kept short (<2,000 chars per cell).

  ~/.venvs/fe-bar-cfo/bin/python scripts/build_evidence_notebook.py
"""
import nbformat as nbf
from nbclient import NotebookClient

md = nbf.v4.new_markdown_cell
code = nbf.v4.new_code_cell

cells = [
md("""# Execution evidence: CFO Operations Platform + AI Cost & Governance

Executed against Databricks workspace `fevm-kat-personal` (AWS us-west-2), catalog `kat_personal_catalog.cfo_febar`. Every output below was produced by running the cell; nothing was typed by hand.

## How I built it (workflow and AI tools)
- **Order:** synthetic source extracts (SAP, Salesforce, Workday, Concur, AI Gateway and billing system-table look-alikes), then Lakeflow Auto Loader ingestion into bronze, then the Unity Catalog medallion (silver/gold, metric views, a ~30-assertion plausibility safety net), then the AI cost spine and attribution, then the Genie space, then Claude insight generation with a consistency-validation harness, then the Databricks App, and finally Lakebase for operational serving and action write-back. All of it deploys as one Declarative Automation Bundle.
- **AI tools:** built with **Claude Code** as a pair programmer, using the **Databricks AI Dev Kit** skills (pipelines, Lakebase, Genie, Apps, bundles) and the Databricks CLI. A running project log carried decisions and dead ends between sessions.
- **Where AI was the force multiplier:** (1) researching the AI Gateway and billing system-table schemas and verifying the attribution mechanics (request tags, endpoint tags, budget policies) before modeling, so the synthetic tables match the real schemas; (2) generating 28 interlocking synthetic tables with engineered, discoverable business patterns plus the assertions that keep them plausible; (3) the runtime AI itself, with Claude Opus for agent and judgment calls and Haiku for composing from computed values, checked against gold-table totals; (4) this iteration, where adding the Lakeflow pipeline and the Lakebase layer, deploying, and capturing this evidence took a single Claude Code session.
- **What stayed human:** the scope (GenAI inference cost, not ML training), the personas, and the rule that every visual comes from a system table or a printed formula.
"""),
code("""import os, json, time, datetime
os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "kat-personal")
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
FQ, WH = "kat_personal_catalog.cfo_febar", "f1bf4bce59e8eee2"
JOB_RUN_ID, PIPELINE_ID = 595756286301571, "e79de7c5-7300-44d2-8a8a-b9c8ecee1d0e"
APP_URL = "https://cfo-febar-7474659787238214.aws.databricksapps.com"
def q(sql):
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=WH, wait_timeout="50s")
    assert r.status.state.value == "SUCCEEDED", r.status
    return [c.name for c in r.manifest.schema.columns], (r.result.data_array or [])
def table(cols, rows):
    wd = [max(len(c), *(len(str(r[i])) for r in rows)) if rows else len(c) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(wd[i]) for i, c in enumerate(cols)))
    for r in rows: print("  ".join(str(v).ljust(wd[i]) for i, v in enumerate(r)))
print("workspace:", w.config.host, "| user:", w.current_user.me().user_name, "| run at:", datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))"""),
md("## 1. Orchestration: the bundle job ran end to end (9/9 tasks)"),
code("""run = w.jobs.get_run(JOB_RUN_ID)
print(f"job run {run.run_id}: {run.state.result_state.value}  ({run.run_name})")
for t in sorted(run.tasks, key=lambda t: t.start_time or 0):
    kind = "lakeflow_pipeline" if t.pipeline_task else "notebook"
    print(f"  {t.task_key:24} {kind:18} {t.state.result_state.value:8} {((t.end_time or 0)-(t.start_time or 0))/1000:6.0f}s")"""),
md("## 2. Lakeflow: Spark Declarative Pipeline, Auto Loader into bronze streaming tables"),
code("""evs = list(w.pipelines.list_pipeline_events(pipeline_id=PIPELINE_ID, max_results=250))
upd = sorted([e for e in evs if e.event_type == "update_progress"], key=lambda e: e.timestamp)
print("pipeline", PIPELINE_ID, "| update states:", " -> ".join(e.message.split(" is ")[-1].rstrip(".") for e in upd))
flows = {}
for e in evs:
    if e.event_type == "flow_progress" and e.message.endswith("has COMPLETED."):
        flows[e.origin.flow_name.split(".")[-1]] = True
print(f"{len(flows)} Auto Loader flows COMPLETED:", ", ".join(sorted(flows))[:1500])"""),
code("""cols, rows = q(f\"\"\"SELECT table_name, table_type FROM kat_personal_catalog.information_schema.tables
  WHERE table_schema='cfo_febar' AND table_name LIKE 'bronze_%' ORDER BY table_name LIMIT 6\"\"\")
table(cols, rows); print("...")
cols, rows = q(f"SELECT COUNT(*) AS bronze_streaming_tables FROM kat_personal_catalog.information_schema.tables WHERE table_schema='cfo_febar' AND table_name LIKE 'bronze_%' AND table_type='STREAMING_TABLE'")
table(cols, rows)"""),
md("## 3. Unity Catalog medallion: row counts bronze to gold"),
code("""ts = ["bronze_workday_timecards","bronze_ai_gateway_usage","silver_fact_timecards","silver_dim_projects",
      "gold_project_profitability","gold_ai_cost_observability","gold_ai_cost_by_engagement","gold_ai_cost_by_cost_center","gold_persona_insights"]
table(*q(" UNION ALL ".join(f"SELECT '{t}' AS table_name, COUNT(*) AS row_count FROM {FQ}.{t}" for t in ts)))"""),
md("## 4. Guardrails: plausibility safety net and the Gen AI consistency-validation harness"),
code("""cols, rows = q(f"SELECT COUNT(*) AS failed_assertions FROM {FQ}.envelope_safety_net_failures"); table(cols, rows)
print("(non-strict mode: failures are logged for review, not blocking; top 3 below)")
table(*q(f"SELECT `check`, ROUND(TRY_CAST(actual AS DOUBLE),1) AS actual, expected_lo, expected_hi, unit FROM {FQ}.envelope_safety_net_failures LIMIT 3"))
print()
table(*q(f"SELECT persona, slot_type, type, LEFT(message, 95) AS message FROM {FQ}.consistency_violations"))"""),
md("## 5. Gen AI output: Claude-generated insights grounded in gold tables"),
code("""cols, rows = q(f\"\"\"SELECT persona, LEFT(headline, 60) AS headline, LEFT(value, 20) AS value
  FROM {FQ}.gold_persona_insights WHERE slot_type='insight' ORDER BY persona LIMIT 6\"\"\")
table(cols, rows)"""),
md("## 6. Lakebase: synced-table read + OLTP write through the app's own module"),
code("""import sys; sys.path.insert(0, ".")
os.environ["CFO_LAKEBASE_ENDPOINT"] = "projects/cfo-ops/branches/production/endpoints/primary"
os.environ["CFO_LAKEBASE_HOST"] = w.postgres.get_endpoint(name=os.environ["CFO_LAKEBASE_ENDPOINT"]).status.hosts.host
import lakebase_store as lb
t0 = time.time(); rows = lb.read_cost_centers(limit=3)
print(f"READ {lb.SERVING_TABLE} (Lakebase synced table) in {(time.time()-t0)*1000:.0f} ms")
for r in rows: print(f"  {r[0]}  {r[1]:<11} ttm_ai_cost={float(r[3]):>10,.0f}  budget={float(r[4]):>9,.0f}  util={float(r[5]):.0f}%  {r[6]}")
aid = lb.log_action("nudge_dept_head", "cost_center", rows[0][0], amount_usd=float(rows[0][3]), actor="evidence-notebook",
                    note=f"AI budget at {float(rows[0][5]):.0f}% of annual policy")
print("WRITE cfo_ops.ai_actions ->", aid)
print("READ BACK:", json.dumps(lb.recent_actions(limit=1)[0], default=str))"""),
md("## 7. Deployed Databricks App: live API responses"),
code("""import requests
H = w.config.authenticate()
r = requests.get(APP_URL + "/health", headers=H, timeout=60); print("GET /health", r.status_code, r.text[:160])
r = requests.get(APP_URL + "/api/ai-cost/kpis", headers=H, timeout=120); k = r.json()
print("GET /api/ai-cost/kpis", r.status_code, json.dumps({x: k[x] for x in list(k)[:8] if x != "by_dim"}, default=str)[:600])
r = requests.get(APP_URL + "/api/ai-cost/engagements", headers=H, timeout=120); e = r.json()
print("GET /api/ai-cost/engagements", r.status_code, f"{len(e)} rows; top 3:")
for x in e[:3]: print(f"  {x['client']:<14} {x['project'][:38]:<38} ai_cost={x['ai_cost']:>10,.0f}  {x['contract_type']:<16} -> {x['recoverability']}")"""),
md("## 8. Genie agent: a real question answered over the governed tables"),
code("""Q = "Which cost centers are furthest over their AI budget, and why?"
t0 = time.time()
r = requests.post(APP_URL + "/api/chat", headers={**H, "Content-Type": "application/json"}, json={"message": Q}, timeout=400)
a = r.json()
print(f"POST /api/chat {r.status_code} in {time.time()-t0:.0f}s | status={a.get('status')}")
print("Q:", Q); print("A:", a.get("message", "")[:1600])"""),
]

nb = nbf.v4.new_notebook(cells=cells, metadata={"kernelspec": {"name": "fe-bar-cfo", "display_name": "fe-bar-cfo", "language": "python"}})
NotebookClient(nb, timeout=600, kernel_name="fe-bar-cfo", resources={"metadata": {"path": "."}}).execute()
nbf.write(nb, "evidence/EXECUTION_EVIDENCE.ipynb")
for c in nb.cells:
    if c.cell_type == "code":
        n = sum(len(o.get("text", "")) for o in c.outputs)
        print(f"cell output chars: {n}" + ("  <-- over 2000" if n > 2000 else ""))
