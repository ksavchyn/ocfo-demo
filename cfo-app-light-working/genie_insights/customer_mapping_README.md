# Customer Schema Mapping

`customer_mapping.py` maps **your** raw data to the schema the CFO demo expects, so the
demo runs on your data instead of the synthetic dataset. It reads your source tables,
proposes a mapping to the demo's load-bearing bronze layer (15 tables / 126 columns),
flags anything it isn't sure about for your review, and then builds `bronze_*` **views**
over your data — no copying, no ETL.

It does **not** move or rewrite your data. It creates views in a new target schema; your
source tables are untouched.

## Prerequisites

1. **Foundation Model API + AI Gateway access** (the only hard requirement). The notebook
   calls two model endpoints in your workspace:
   - `databricks-bge-large-en` — embeddings, for similarity recall (Step 3)
   - `databricks-claude-opus-4-7` — reranking + rationale, via the AI Gateway (Step 5)

   Both must be enabled and callable in your workspace. The Claude endpoint goes through
   the **AI Gateway** (`https://<workspace-host>/ai-gateway/mlflow/v1`) — if AI Gateway is
   a Beta you have to opt into, enable it in the workspace admin console first. Without
   these, Steps 3 and 5 fail. (Both endpoints are overridable via the `CFO_*` config keys
   in the Config cell if your workspace uses different names.)
2. **A new, empty target schema** for the output views — it must be different from both the
   demo schema and your source schema(s). The notebook refuses to run otherwise, so it can
   never overwrite your real data or the demo data.
3. **Read** on your source catalog(s)/schema(s); **CREATE VIEW** on the target schema.
4. A **serverless** notebook cluster (the notebook self-installs `pyyaml` if missing).

## Inputs (widgets)

| Widget | What it is |
|---|---|
| `CFO_CUSTOMER_SOURCES` | Your source data as `catalog.schema` (several allowed, `;`-separated). **Required.** |
| `CFO_DEMO_SCHEMA` | The deployed demo to map against (default `main.cfo_proserv`). |
| `CFO_TARGET_SCHEMA` | Where the mapped views are written. Must be **new/empty** (≠ sources, ≠ demo). |
| `CFO_MAPPINGS_FILE` | *(Step 7)* Path to a mappings.yaml to apply. Leave blank to use the file this run wrote. |
| `CFO_ALLOW_GAPS` | *(Step 7)* `false` = refuse to build while anything is unresolved. `true` = build now with unresolved columns left blank (NULL). |

## How to run

Run top to bottom (**Run all**). Steps 1–6 profile, match, and write two files; Step 7
builds the views after you've reviewed; Step 8 validates row counts.

1. **Profile** your source schema — per-column type, description, sample values, null %, distinct count.
2. **Load** the demo's load-bearing bronze spec (the 126 columns the app actually needs), with sample values.
3. **Embed** every column on both sides — `name + type + description + sample values` — with `databricks-bge-large-en`.
4. **Recall** — blend embedding cosine similarity with a lexical name-overlap boost; keep the top 5 candidates per demo column.
5. **Rerank** — Claude picks the best candidate using name, description, stats, and sample values, and assigns a confidence + rationale.
6. **Emit** `mappings.yaml` (the proposed mapping) and `gaps.md` (what needs your review). **Review these before Step 7.**
7. **Apply** — build `CREATE OR REPLACE VIEW` per table (gated; see below).
8. **Validate** — row counts per built view.

## Matching methodology

For each column the demo needs, the order of operations is:

1. **Exact / lexical name match** — an exact column-name twin is boosted so it leads the
   candidate list even when descriptions are sparse.
2. **Semantic similarity** — both sides are embedded from the *same* shape
   (`name + type + description + sample values`), so a column matches on meaning and on the
   *shape of its values* even if it's named differently or has no description. Sample values
   are embedded on **both** sides — the demo's synthetic values are representative of the
   value shapes expected, so they help match against your real data.
3. **LLM rerank** — Claude makes the final call from the top candidates, using names,
   descriptions, stats, and samples together, and assigns `confidence` + a `rationale`.

**We only auto-map what we're sure about** — high confidence, from the table's primary
source. Everything else (medium/low confidence, a column sourced from a *different* table,
a derived expression, or no match at all) is **surfaced for your review** rather than
silently wired up. A wrong silent mapping produces nonsense numbers in the app and erodes
trust in every tile; a flagged column is recoverable.

## Reviewing the proposal

Open `gaps.md` first — it lists, grouped by source system, every field we recommend but
aren't sure about. Then open `mappings.yaml` and **search `⚠️`**. For each flagged column:

- **Confirm or fix** `source_column` (the `rationale` and `alternatives` explain the pick), then **delete the `action:` line** — that deletion is how you mark a column "reviewed."
- **Cross-table flag** ("comes from a different table than the primary source"): if it's the
  same entity, add a `joins:` block (alias, catalog, schema, table, `on`, type) so the view
  can JOIN it; if it isn't a real match, treat it as a gap (leave it blank).
- **No source found:** set a `source_column` / `sql_expression`, ingest the data, or leave it
  blank (the column becomes NULL).

### Step 7 gate

Step 7 will **not** build views while any column is unresolved — no source, an unreviewed
`⚠️` line, or a source table that isn't reachable without a join. It raises a clear message
naming what's left. To stand the app up immediately with the unresolved columns **blank**
(NULL, never a guess), set `CFO_ALLOW_GAPS=true`.

## After mapping — point the demo at your data

Step 8 prints the exact commands. In short:

- **First time:** one `deploy.sh` run with `--skip-bronze-hydrate --refresh-data` against your
  target schema. This repoints the app (`CFO_SCHEMA`) and the Genie space at your schema and
  runs the pipeline on your views. It's an incremental deploy, not a from-scratch rebuild.
- **Recurring refresh:** schedule the `cfo_data_pipeline` job with `skip_hydrate_bronze=true` —
  the app and Genie already point at your schema, so only the data refreshes. No redeploy.
