# Execution evidence

Text output from a full run of this build on a personal Databricks workspace (`fevm-kat-personal`, AWS us-west-2, catalog `kat_personal_catalog.cfo_febar`) on 2026-11-02. All of it was captured from live runs; nothing was typed by hand. The scripts that produced it are in `scripts/`.

| File | What it shows |
|---|---|
| `logs/deploy_and_pipeline_run.log` | `deploy.sh`: bundle deploy (pipeline, job, app, dashboards), Genie space provisioning, then the full data job, **9/9 tasks SUCCESS** |
| `01_job_run_summary.txt` | Job run `595756286301571`: every task, its type (notebook vs. Lakeflow pipeline task), result, and duration |
| `02_lakeflow_pipeline_run.txt` | **Lakeflow Declarative Pipeline** event log: update states, 28 Auto Loader flows COMPLETED, **4,958,460 rows** ingested into `bronze_*` streaming tables, `no_rescued_data` expectation passed/failed per table |
| `03_unity_catalog_tables_and_validation.txt` | Row counts across bronze, silver, and gold; table types (streaming tables, metric views, the Lakebase synced table); plausibility safety-net results; the consistency-validation harness catching 2 real KPI-drift errors in generated prose; Claude-generated insights by persona |
| `lakebase_smoke_test_output.txt` | `scripts/lakebase_smoke_test.py`: **Lakebase** read from the synced table plus an OLTP insert into `cfo_ops.ai_actions` and read-back, through the app's `lakebase_store.py` |
| `logs/lakebase_setup.log` | Lakebase project `cfo-ops` (Postgres 17, autoscaling endpoint) and synced table creation, ending `SYNCED_TABLE_ONLINE` |
| `04_app_api_responses.txt` | The **deployed Databricks App**: `/health` plus the AI Cost & Governance APIs returning live gold-table data (HTTP 200) |
| `05_app_lakebase_read_write.txt` | The app's HTTP API with Lakebase configured: Spend Controls served from Lakebase (`X-Data-Source: lakebase`), governance actions written with `POST /api/ai-actions` and read back |
| `06_genie_agent_response.txt` | A **Genie agent** answer from the deployed app: Claude via Unity AI Gateway calling the Genie space over the governed tables |
| `logs/app_redeploy.log` | Redeploy after switching Claude to Unity Catalog model-service names (`system.ai.claude-*`); the legacy endpoint names returned "no longer available" on the gateway |

Known, logged findings from this run (left as is, since they show the guardrails working): the plausibility safety net (non-strict) recorded 13 calibration misses for this run's as-of date, and the consistency harness flagged 2 insights whose stated DSO drifted from the canonical KPI.
