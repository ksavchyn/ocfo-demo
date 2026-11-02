# How this was built

**CFO Operations Platform for Professional Services, with AI Cost & Governance.** A Databricks App for the CFO and FP&A team of a global Tier 1 consulting firm. It unifies SAP, Salesforce, Workday, and Concur data with Databricks AI usage telemetry, so AI spend is attributed to clients, engagements, practices, and cost centers like every other line in the P&L. On top of that sit governance actions: recover T&M AI spend, reprice fixed-fee erosion, enforce cost-center budgets, and plan against guardrails.

## The data journey

| Stage | Databricks component | Where in the repo |
|---|---|---|
| Raw source extracts (synthetic) | Parquet files in a UC Volume, one folder per source table | `data_pipeline/01_generate_bronze_data.py` |
| Ingest | **Lakeflow Spark Declarative Pipeline**: Auto Loader into 28 `bronze_*` streaming tables, with a data-quality expectation on each | `pipelines/ingest_bronze.py`, `databricks.yml` (`cfo_bronze_ingest`) |
| Govern and model | **Unity Catalog**: medallion silver/gold, table and column documentation, UC metric views, plausibility safety net (~30 assertions) | `data_pipeline/02_build_silver_gold.py`, `data_pipeline/uc_metadata.py`, `genie_config/`, `data_pipeline/04_envelope_safety_net.py` |
| AI cost spine | AI Gateway usage plus billing system tables (synthetic look-alikes with the real schemas), priced with list prices, attributed via request tags, endpoint tags, and budget policies | `bronze_ai_*` / `bronze_billing_usage`, `gold_ai_cost_*` |
| Operational serving | **Lakebase**: synced table of cost-center AI budgets for the Spend Controls view (read), and an `ai_actions` OLTP table for every governance action taken in the app (write) | `lakebase_store.py`, `scripts/setup_lakebase.sh`, `app.py` (`/api/ai-cost/cost-centers`, `/api/ai-actions`) |
| Intelligence | **Gen AI**: Claude via the AI Gateway writes the executive insights, grounded in canonical gold-table SQL and checked by a consistency-validation harness; `ai_query` drafts the nudge emails; `ai_forecast` projects AI spend | `insights_compose.py`, `genie_insights/generate_insights.py`, `data_pipeline/03_validate_consistency.py` |
| Natural-language Q&A | **Genie agent**: programmatically provisioned space over the gold tables and metric views, called from the app | `genie_insights/provision_genie_space.py`, `genie_agent.py` |
| Business surface | **Databricks App** (Flask): Exec Summary, Finance, Admin, AI Cost & Governance | `app.py`, `templates/`, `static/` |

Everything is deployed as one Declarative Automation Bundle (`databricks.yml`, `deploy.sh`). The job runs: land raw extracts, Lakeflow ingest, silver/gold, safety net, metric views, Genie provisioning, insight generation, consistency validation, and dashboard refresh.

## Build workflow and AI tools

I built this with **Claude Code** as a pair programmer, plus the **Databricks AI Dev Kit** skills (pipelines, Lakebase, Genie, Apps, bundles) and the Databricks CLI. I kept a running project log so each session started from the previous one's decisions and known dead ends.

Where AI was the force multiplier:
- **Research before modeling.** Claude Code read the AI Gateway and billing system-table docs and verified the attribution mechanics (request tags, endpoint tags, budget policies) before any data modeling. The synthetic bronze tables match the real system-table schemas column for column, so a customer deploy repoints the gold views at the real `system.*` tables.
- **Synthetic data with a story.** A generator for 28 source tables engineers specific, discoverable business patterns: AR concentration, an office over budget, fixed-fee AI erosion. A plausibility envelope (~30 assertions) keeps the data credible under a CFO's questions.
- **Runtime AI with guardrails.** Two model tiers: Opus for agent and judgment calls, Haiku for composing narratives from values already computed. Every generated insight is checked against the gold-table totals (sum-of-parts, subset-greater-than-total, KPI drift) before it ships.
- **This FE Bar iteration.** I added the Lakeflow ingestion pipeline and the Lakebase serving layer, deployed to a personal workspace, and captured the run evidence in a single Claude Code session using the AI Dev Kit pipelines and Lakebase skills.

What stayed human: the scope (GenAI inference cost, not ML training), the personas, and one rule for every visual: it comes straight from a system table or carries a printed formula.

## Key decisions and trade-offs

- **Extended the existing finance model instead of rebuilding it.** AI cost joins the client, engagement, practice, and cost-center dimensions finance already trusts.
- **Attribution from multiple signals.** Gateway request tags give per-call precision for custom agents. Budget policies cover AI Functions, AI Search, and Genie, where request tags can't ride along. Untagged spend shows up as a Governance Coverage KPI rather than being hidden.
- **Progressive friction over hard caps.** Visibility, then warnings, then approval, then downshift. The heaviest AI users are often the most productive.
- **Solid core vs. modeled edge.** Cost, attribution, and chargeback come from system tables. Engagement value, ROI, and fee compression are modeled and labeled as such.
- **Lakebase for the operational path only.** Analytics stay on Unity Catalog and the SQL warehouse. Lakebase serves the low-latency Spend Controls read and records governance actions, which are OLTP writes the lakehouse isn't meant for.
- **Snapshot sync for the Lakebase table.** The gold table is rebuilt on each pipeline run, so a snapshot sync is simpler and more reliable than change-data-feed sync.

## How to run

```
./deploy.sh --profile <profile> --catalog <catalog> --schema <schema> --warehouse-id <id> --app-name <app> --refresh-data
./scripts/setup_lakebase.sh <profile> <catalog> <schema>
```

## Execution evidence

See [`evidence/`](evidence/README.md): text output from a full run on a personal Databricks workspace.
