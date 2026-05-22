# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Schema Mapping Notebook
# MAGIC
# MAGIC **Purpose:** Map a customer's existing financial data schema to the CFO demo's expected
# MAGIC `bronze_*` shape via SQL VIEWS, so the bundle's silver→gold transformations can run on
# MAGIC top of the customer's real data without any data movement.
# MAGIC
# MAGIC ## Why we map to bronze (not silver/gold)
# MAGIC
# MAGIC The bundle's bronze layer is modeled after canonical SaaS source schemas
# MAGIC (`bronze_workday_*`, `bronze_sfdc_*`, `bronze_concur_*`, `bronze_sap_*`). Source SaaS
# MAGIC systems have stable, well-known schemas — customer's raw HR/Finance/CRM data
# MAGIC deterministically looks like a known shape per source system, so the mapping is
# MAGIC shallow and template-like. The silver→gold transformations (which define the
# MAGIC aggregations the app, dashboards, and Genie depend on) stay 100% under our
# MAGIC control. Same code runs against synthetic bronze OR customer-mapped bronze;
# MAGIC aggregations behave identically. That's the contract we make with customers.
# MAGIC
# MAGIC ## Customer 2-step deploy workflow
# MAGIC
# MAGIC 1. **Demo step.** `./deploy.sh ... --refresh-data` — synthetic bronze hydrate + full
# MAGIC    silver/gold + Genie + dashboards. App works against demo data immediately.
# MAGIC 2. **Customer-data step.** Run this notebook → emits views shaped like our bronze
# MAGIC    (`bronze_workday_employees`, `bronze_sap_accounts_receivable`, etc.) into a target
# MAGIC    catalog/schema. Then `./deploy.sh ... --refresh-data --skip-bronze-hydrate
# MAGIC    --catalog <theirs> --schema <theirs>` — silver/gold rebuild on top of customer
# MAGIC    bronze views; app now shows their data.
# MAGIC
# MAGIC ## Methodology — hybrid VS + LLM
# MAGIC 1. **Profile customer schema(s)** — for each column: name, type, top-K sample values, null %, cardinality
# MAGIC 2. **Load our spec** — introspect `bronze_*` columns + comments from the bundle's deployed schema
# MAGIC 3. **Embed both sides** via `databricks-bge-large-en`; cosine-similarity ranking
# MAGIC 4. **VS recall** → top-5 candidate customer columns per OUR column
# MAGIC 5. **LLM rerank + rationale** — Claude picks best match, assigns confidence, writes one-line rationale
# MAGIC 6. **Emit `mappings.yaml` + `gaps.md`** to Workspace Files
# MAGIC 7. **STOP — customer review** (cell marker; edit YAML in browser)
# MAGIC 8. **Apply** — read (possibly-edited) YAML, emit `CREATE OR REPLACE VIEW` statements
# MAGIC 9. **Validate** — sample row counts per view; surface compile failures
# MAGIC
# MAGIC ## Multi-catalog source support
# MAGIC
# MAGIC The notebook supports federated data across multiple Unity Catalog catalogs. Set the
# MAGIC `CFO_CUSTOMER_SOURCES` widget to a semicolon-separated list of `catalog.schema` pairs:
# MAGIC
# MAGIC ```
# MAGIC CFO_CUSTOMER_SOURCES = workday_catalog.hr_schema;sap_catalog.finance_schema;sfdc_catalog.crm_schema
# MAGIC ```
# MAGIC
# MAGIC **What happens with multi-source:**
# MAGIC - Step 1 profiles ALL sources and combines their columns into one pool (each column tagged with its origin)
# MAGIC - Steps 3-5 match a demo column against the union (Claude sees `catalogA.schema.table.column` FQNs in its candidate list)
# MAGIC - Each column in `mappings.yaml` carries its own `source_catalog`, `source_schema`, `source_table`, `source_column`
# MAGIC - Per logical demo table, the YAML has a `primary_source:` block + optional `joins:` block when columns span sources
# MAGIC - The view DDL builder emits `FROM <primary> primary LEFT JOIN <other> alias ON <on>` for cross-source tables
# MAGIC
# MAGIC **Overlapping tables (same name in different catalogs):**
# MAGIC If `catalogA.schema.employees` and `catalogB.schema.employees` both exist, the LLM picks one based on:
# MAGIC - Sample values that look more current/correct
# MAGIC - null_pct and distinct_count (data freshness signals)
# MAGIC - Column comments / descriptions
# MAGIC
# MAGIC It is NOT "first one wins". But if the two columns look indistinguishable, the LLM's choice
# MAGIC can be somewhat arbitrary. The notebook automatically detects this case and flags every
# MAGIC ambiguous match in `gaps.md` under **⚠️ CROSS-CATALOG AMBIGUITY**, showing:
# MAGIC - Which catalog the LLM chose
# MAGIC - All sibling candidates (same column name in other catalogs) with their stats
# MAGIC - Explicit ACTION to edit the YAML if the LLM picked wrong
# MAGIC
# MAGIC **If you want to UNION two catalogs** (instead of pick one), write a `sql_expression` in the
# MAGIC YAML like `(SELECT col FROM A UNION ALL SELECT col FROM B)` — the view DDL builder will
# MAGIC use it verbatim. Auto-UNION is intentionally not done; that's a semantic decision
# MAGIC (which source is canonical, dedup logic, etc.) the customer owns.
# MAGIC
# MAGIC ## Outputs
# MAGIC - `mappings.yaml` — proposed map + confidence + rationale + alternatives (editable; multi-catalog aware)
# MAGIC - `gaps.md` — categories: ❓ GAP, ⚠️ CROSS-CATALOG AMBIGUITY
# MAGIC - `<target_catalog>.<target_schema>.bronze_*` views over customer's data
# MAGIC   (single-source FROM or multi-source JOIN). After the views exist, redeploy
# MAGIC   the bundle with `--skip-bronze-hydrate --catalog <target_catalog>
# MAGIC   --schema <target_schema>` to rebuild silver+gold on top of them.

# COMMAND ----------

# DBTITLE 1,Configuration & Imports
import os
import json
import time
import yaml
import requests
import numpy as np
from pyspark.sql import functions as F
from datetime import datetime
from databricks.sdk import WorkspaceClient

# Widget declarations — bundle's notebook_task.base_parameters flow through here.
try:
    # Multi-catalog form: semicolon-separated `catalog.schema;catalog.schema;...` list of
    # source locations. Use this if your real source data spans multiple catalogs/schemas;
    # the notebook profiles all listed sources and the LLM matches against the union.
    dbutils.widgets.text("CFO_CUSTOMER_SOURCES", "")  # noqa: F821
    # Single-source form (most common): leave CFO_CUSTOMER_SOURCES empty and set
    # these two widgets to point at your real catalog + schema.
    dbutils.widgets.text("CFO_CUSTOMER_CATALOG", "")  # noqa: F821
    dbutils.widgets.text("CFO_CUSTOMER_SCHEMA", "")  # noqa: F821
    dbutils.widgets.text("CFO_DEMO_CATALOG", "main")  # noqa: F821
    dbutils.widgets.text("CFO_DEMO_SCHEMA", "cfo_proserv")  # noqa: F821
    dbutils.widgets.text("CFO_TARGET_VIEW_CATALOG", "main")  # noqa: F821
    dbutils.widgets.text("CFO_TARGET_VIEW_SCHEMA", "cfo_demo")  # noqa: F821
    dbutils.widgets.text("CFO_CLAUDE_GATEWAY_URL", "")  # noqa: F821
    dbutils.widgets.text("CFO_CLAUDE_MODEL", "databricks-claude-opus-4-7")  # noqa: F821
    dbutils.widgets.text("CFO_EMBEDDING_ENDPOINT", "databricks-bge-large-en")  # noqa: F821
    dbutils.widgets.text("CFO_MAPPING_OUTPUT_DIR", "/Workspace/Shared/cfo_demo_mapping")  # noqa: F821
    _WIDGETS = True
except Exception:
    _WIDGETS = False


def _config(name: str, default: str = "") -> str:
    if _WIDGETS:
        try:
            v = dbutils.widgets.get(name)  # noqa: F821
            if v:
                return v
        except Exception:
            pass
    return os.environ.get(name, default)


def _parse_customer_sources() -> list[tuple[str, str]]:
    """Resolve customer source catalog.schema pairs.

    Priority order:
      1. CFO_CUSTOMER_SOURCES (semicolon-separated `catalog.schema` pairs) — multi-catalog form
      2. CFO_CUSTOMER_CATALOG + CFO_CUSTOMER_SCHEMA — single-source form (most common)

    Returns a list of `(catalog, schema)` tuples. Raises if nothing is set.
    """
    raw = _config("CFO_CUSTOMER_SOURCES", "").strip()
    if raw:
        out = []
        for entry in raw.split(";"):
            entry = entry.strip()
            if not entry:
                continue
            if "." not in entry:
                raise ValueError(f"CFO_CUSTOMER_SOURCES entry {entry!r} must be in 'catalog.schema' form")
            catalog, schema = entry.split(".", 1)
            out.append((catalog.strip(), schema.strip()))
        if out:
            return out
    # Fallback to single-source widgets
    single_cat = _config("CFO_CUSTOMER_CATALOG").strip()
    single_schema = _config("CFO_CUSTOMER_SCHEMA").strip()
    if single_cat and single_schema:
        return [(single_cat, single_schema)]
    raise ValueError(
        "No customer source configured. Set CFO_CUSTOMER_SOURCES "
        "(semicolon-separated `catalog.schema` pairs) for multi-catalog, "
        "OR CFO_CUSTOMER_CATALOG + CFO_CUSTOMER_SCHEMA for single-catalog."
    )


CUSTOMER_SOURCES     = _parse_customer_sources()   # list of (catalog, schema) tuples
DEMO_CATALOG         = _config("CFO_DEMO_CATALOG", "main")
DEMO_SCHEMA          = _config("CFO_DEMO_SCHEMA", "cfo_proserv_customer")
TARGET_VIEW_CATALOG  = _config("CFO_TARGET_VIEW_CATALOG", "main")
TARGET_VIEW_SCHEMA   = _config("CFO_TARGET_VIEW_SCHEMA", "cfo_demo")
CLAUDE_GATEWAY_URL   = _config("CFO_CLAUDE_GATEWAY_URL")
CLAUDE_MODEL         = _config("CFO_CLAUDE_MODEL", "databricks-claude-opus-4-7")
EMBEDDING_ENDPOINT   = _config("CFO_EMBEDDING_ENDPOINT", "databricks-bge-large-en")
OUTPUT_DIR           = _config("CFO_MAPPING_OUTPUT_DIR", "/Workspace/Shared/cfo_demo_mapping")

# Build a per-run output directory so re-runs don't overwrite review-in-progress YAML
RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DIR = f"{OUTPUT_DIR}/{RUN_TS}"

print(f"Customer sources:  {len(CUSTOMER_SOURCES)} location(s)")
for cat, sch in CUSTOMER_SOURCES:
    print(f"                     - {cat}.{sch}")
print(f"Demo spec source:  {DEMO_CATALOG}.{DEMO_SCHEMA}")
print(f"Target view dest:  {TARGET_VIEW_CATALOG}.{TARGET_VIEW_SCHEMA}")
print(f"Output directory:  {RUN_DIR}")

# COMMAND ----------

# DBTITLE 1,Auth helper — Workspace SDK token for Foundation Model + Claude gateway calls
_w = WorkspaceClient()


def _get_token() -> str:
    """Per-call token retrieval. Never cache (see CFO app no-token-cache rule)."""
    return _w.config.authenticate()["Authorization"].replace("Bearer ", "")


def _fmapi_base() -> str:
    """Foundation Model API base for the embedding endpoint. Uses workspace host."""
    return _w.config.host.rstrip("/") + "/serving-endpoints"


# COMMAND ----------

# DBTITLE 1,Step 1 — Profile customer schema
def profile_customer_schema(catalog: str, schema: str, sample_k: int = 5) -> list[dict]:
    """For each table+column in a single customer (catalog, schema), capture profile metadata.

    Returns a list of column records each tagged with its origin:
      {source_catalog, source_schema, table, column, type, comment, null_pct, distinct_count, sample_values: [...]}
    """
    print(f"Profiling {catalog}.{schema}...")
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    table_names = [r["tableName"] for r in tables_df.collect() if not r.get("isTemporary")]
    print(f"  found {len(table_names)} tables")

    profile = []
    for tname in table_names:
        fqn = f"{catalog}.{schema}.{tname}"
        try:
            cols_df = spark.sql(f"DESCRIBE TABLE {fqn}")
            cols = [(r["col_name"], r["data_type"], r["comment"]) for r in cols_df.collect()
                    if r["col_name"] and not r["col_name"].startswith("#")]
        except Exception as e:
            print(f"  skipping {tname}: {e}")
            continue

        try:
            row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").collect()[0]["n"]
        except Exception:
            row_count = 0

        for cname, ctype, ccomment in cols:
            try:
                null_pct = 0.0
                distinct = 0
                samples = []
                if row_count > 0:
                    stats = spark.sql(f"""
                        SELECT
                          (COUNT(*) - COUNT({cname})) * 1.0 / COUNT(*) AS null_pct,
                          COUNT(DISTINCT {cname}) AS distinct_count
                        FROM {fqn}
                    """).collect()[0]
                    null_pct = float(stats["null_pct"] or 0)
                    distinct = int(stats["distinct_count"] or 0)
                    sample_rows = spark.sql(f"""
                        SELECT CAST({cname} AS STRING) AS v
                        FROM {fqn}
                        WHERE {cname} IS NOT NULL
                        GROUP BY {cname}
                        ORDER BY COUNT(*) DESC
                        LIMIT {sample_k}
                    """).collect()
                    samples = [r["v"] for r in sample_rows if r["v"] is not None]
            except Exception as e:
                null_pct, distinct, samples = -1.0, -1, [f"<profile failed: {e}>"]

            profile.append({
                "source_catalog": catalog,
                "source_schema": schema,
                "table": tname,
                "column": cname,
                "type": ctype,
                "comment": ccomment or "",
                "null_pct": round(null_pct, 4),
                "distinct_count": distinct,
                "sample_values": samples,
            })

    print(f"  profiled {len(profile)} columns total in {catalog}.{schema}")
    return profile


def profile_customer_sources(sources: list[tuple[str, str]], sample_k: int = 5) -> list[dict]:
    """Profile multiple customer (catalog, schema) sources and combine into one pool.

    Each column record carries `source_catalog` + `source_schema` so the
    downstream LLM rerank and view-DDL builder know which physical source the
    matching customer column lives in. Use this whenever the customer's data is
    federated across multiple catalogs (e.g., Workday HR in one catalog, SAP
    finance in another)."""
    pool = []
    for catalog, schema in sources:
        pool.extend(profile_customer_schema(catalog, schema, sample_k=sample_k))
    print(f"\nTotal columns profiled across {len(sources)} source(s): {len(pool)}")
    return pool


customer_profile = profile_customer_sources(CUSTOMER_SOURCES)

# COMMAND ----------

# DBTITLE 1,Step 2 — Load our spec (bronze_* columns + comments) from deployed demo schema
def load_demo_spec(catalog: str, schema: str, prefixes: tuple[str, ...] = ("bronze_",)) -> list[dict]:
    """Introspect the bundle-deployed bronze tables to get the canonical schema we need to map TO.

    We target BRONZE (not silver/gold) because bronze is modeled after canonical SaaS
    source schemas (Workday / SFDC / Concur / SAP). Customer raw data deterministically
    looks like those source shapes, so the mapping is shallow. The bundle's silver→gold
    build runs on top of these bronze views after the customer redeploys with
    `--skip-bronze-hydrate`.

    Returns: [{table, column, type, comment}, ...]
    """
    print(f"Loading demo spec from {catalog}.{schema}...")
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    target_tables = [r["tableName"] for r in tables_df.collect()
                     if any(r["tableName"].startswith(p) for p in prefixes)]

    spec = []
    for tname in target_tables:
        fqn = f"{catalog}.{schema}.{tname}"
        # Pull table comment too — feeds richer LLM context
        try:
            tcomment_row = spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect()
            tcomment = next((r["data_type"] for r in tcomment_row if r["col_name"] == "Comment"), "")
        except Exception:
            tcomment = ""

        cols_df = spark.sql(f"DESCRIBE TABLE {fqn}")
        for r in cols_df.collect():
            if not r["col_name"] or r["col_name"].startswith("#"):
                continue
            spec.append({
                "table": tname,
                "table_comment": tcomment or "",
                "column": r["col_name"],
                "type": r["data_type"],
                "comment": r["comment"] or "",
            })

    print(f"  loaded {len(spec)} target columns across {len(target_tables)} tables")
    return spec


demo_spec = load_demo_spec(DEMO_CATALOG, DEMO_SCHEMA)

# COMMAND ----------

# DBTITLE 1,Step 3 — Embed both sides via Foundation Model API (databricks-bge-large-en)
def embed_texts(texts: list[str], endpoint: str, batch_size: int = 32) -> np.ndarray:
    """Call the workspace's Foundation Model embedding endpoint. Returns (N, D) array."""
    url = f"{_fmapi_base()}/{endpoint}/invocations"
    out = []
    for i in range(0, len(texts), batch_size):
        chunk = texts[i:i + batch_size]
        headers = {"Authorization": f"Bearer {_get_token()}", "Content-Type": "application/json"}
        resp = requests.post(url, headers=headers, json={"input": chunk}, timeout=60)
        resp.raise_for_status()
        body = resp.json()
        # FM API embedding response: {"data": [{"embedding": [...], "index": 0}, ...]}
        data = body.get("data") or []
        # Sort by index just in case the endpoint returns out of order
        data_sorted = sorted(data, key=lambda x: x.get("index", 0))
        out.extend([d["embedding"] for d in data_sorted])
    return np.array(out, dtype=np.float32)


def _column_text_for_embedding(rec: dict, side: str) -> str:
    """Build a short, semantically-rich description string per column to embed.

    For customer-side columns we include source catalog + schema so cross-catalog
    matches surface clearly (e.g., 'workday_catalog.hr_schema.employees' vs
    'sap_catalog.finance_schema.cost_centers')."""
    parts = []
    if side == "customer" and rec.get("source_catalog"):
        parts.append(f"source={rec['source_catalog']}.{rec['source_schema']}")
    parts.extend([f"table={rec['table']}", f"column={rec['column']}", f"type={rec['type']}"])
    if rec.get("comment"):
        parts.append(f"description: {rec['comment']}")
    if side == "customer" and rec.get("sample_values"):
        sv = ", ".join(str(s)[:40] for s in rec["sample_values"][:5])
        parts.append(f"samples: {sv}")
    return " | ".join(parts)


print(f"Embedding {len(demo_spec)} demo columns + {len(customer_profile)} customer columns via {EMBEDDING_ENDPOINT}...")
demo_texts = [_column_text_for_embedding(r, "demo") for r in demo_spec]
cust_texts = [_column_text_for_embedding(r, "customer") for r in customer_profile]

demo_emb = embed_texts(demo_texts, EMBEDDING_ENDPOINT)
cust_emb = embed_texts(cust_texts, EMBEDDING_ENDPOINT)

# Normalize for cosine similarity
demo_emb_n = demo_emb / np.clip(np.linalg.norm(demo_emb, axis=1, keepdims=True), 1e-9, None)
cust_emb_n = cust_emb / np.clip(np.linalg.norm(cust_emb, axis=1, keepdims=True), 1e-9, None)
print(f"  demo embeddings: {demo_emb_n.shape}, customer embeddings: {cust_emb_n.shape}")

# COMMAND ----------

# DBTITLE 1,Step 4 — VS recall (top-5 candidates per demo column)
TOP_K = 5
sims = demo_emb_n @ cust_emb_n.T  # shape (n_demo, n_cust)
top_k_idx = np.argsort(-sims, axis=1)[:, :TOP_K]

candidates_per_demo_col = []
for i, demo_col in enumerate(demo_spec):
    cands = []
    for j_idx, j in enumerate(top_k_idx[i]):
        score = float(sims[i, j])
        cands.append({
            "rank": j_idx + 1,
            "score": round(score, 4),
            **customer_profile[int(j)],
        })
    candidates_per_demo_col.append({"demo": demo_col, "candidates": cands})

print(f"VS recall complete: top-{TOP_K} customer candidates retrieved per demo column")

# COMMAND ----------

# DBTITLE 1,Step 5 — LLM rerank + rationale (Claude)
RERANK_PROMPT = """You are mapping a customer's data schema to a CFO demo's expected schema. \
The demo needs a column with the following definition:

DEMO COLUMN
  Table:       {demo_table}
  Column:      {demo_column}
  Type:        {demo_type}
  Description: {demo_comment}

The customer schema has these candidate columns (ranked by semantic similarity):
{candidates_block}

Pick the BEST match (or "no_match" if none of the candidates would work). \
Output STRICT JSON with these keys:
{{
  "source_catalog":  string  (the source catalog of the chosen column, or null if no_match),
  "source_schema":   string  (the source schema of the chosen column, or null if no_match),
  "source_table":    string  (the customer's table name, or null if no_match),
  "source_column":   string  (the customer's column name, or null if no_match),
  "confidence":      "high" | "medium" | "low",
  "rationale":       string  (one sentence explaining the choice, grounded in column types/samples/descriptions),
  "alternatives":    array of up to 2 other plausible candidates (column names only),
  "flagged":         bool   (true if confidence is low or no match)
}}

`source_catalog` and `source_schema` come from the candidate's fully-qualified name (shown above
each candidate as `catalog.schema.table.column`). They are critical when the customer's data is
federated across multiple catalogs — the view DDL builder needs them to construct the right
`FROM` / `JOIN` clauses.

Output ONLY the JSON object — no prose, no code fences."""


def _claude_call(prompt: str, gateway_url: str, model: str) -> dict:
    """OpenAI-compatible /chat/completions call against the workspace AI gateway."""
    if not gateway_url:
        gateway_url = _w.config.host.rstrip("/") + "/ai-gateway/mlflow/v1"
    url = gateway_url.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {_get_token()}", "Content-Type": "application/json"}
    body = {"model": model, "messages": [{"role": "user", "content": prompt}], "max_tokens": 600}
    resp = requests.post(url, headers=headers, json=body, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _candidates_block(cands: list[dict]) -> str:
    lines = []
    for c in cands:
        sv = ", ".join(str(s)[:30] for s in (c.get("sample_values") or [])[:3])
        # Show full fully-qualified name when source_catalog is known (multi-catalog deploys)
        src = c.get("source_catalog") or ""
        sch = c.get("source_schema") or ""
        fqn = f"{src}.{sch}.{c['table']}.{c['column']}" if src else f"{c['table']}.{c['column']}"
        lines.append(
            f"  [rank {c['rank']}, sim={c['score']}] "
            f"{fqn} ({c['type']}) "
            f"— null_pct={c.get('null_pct', '?')}, distinct={c.get('distinct_count', '?')}, samples=[{sv}]"
            + (f", desc: {c['comment']}" if c.get('comment') else "")
        )
    return "\n".join(lines)


def rerank_one(item: dict) -> dict:
    demo = item["demo"]
    prompt = RERANK_PROMPT.format(
        demo_table=demo["table"],
        demo_column=demo["column"],
        demo_type=demo["type"],
        demo_comment=demo.get("comment") or "(no description)",
        candidates_block=_candidates_block(item["candidates"]),
    )
    try:
        result = _claude_call(prompt, CLAUDE_GATEWAY_URL, CLAUDE_MODEL)
        content = result["choices"][0]["message"]["content"].strip()
        # Tolerate fenced JSON if Claude ignores instructions
        if content.startswith("```"):
            content = content.strip("`").split("\n", 1)[-1].rsplit("```", 1)[0]
        parsed = json.loads(content)
    except Exception as e:
        parsed = {
            "source_catalog": None,
            "source_schema": None,
            "source_column": None,
            "source_table": None,
            "confidence": "low",
            "rationale": f"LLM rerank failed: {type(e).__name__}: {e}",
            "alternatives": [],
            "flagged": True,
        }
    parsed["alternatives"] = parsed.get("alternatives") or []
    # If the LLM forgot catalog/schema but we know the candidate it picked, infer from top-1
    if parsed.get("source_column") and not parsed.get("source_catalog"):
        # Try to backfill from the top candidate matching this source_column
        for c in item["candidates"]:
            if c.get("column") == parsed.get("source_column") and c.get("table") == parsed.get("source_table"):
                parsed.setdefault("source_catalog", c.get("source_catalog"))
                parsed.setdefault("source_schema", c.get("source_schema"))
                break
    return {"demo": demo, "decision": parsed, "candidates": item["candidates"]}


print(f"LLM rerank: {len(candidates_per_demo_col)} columns to process...")
decisions = []
for idx, item in enumerate(candidates_per_demo_col, 1):
    decisions.append(rerank_one(item))
    if idx % 10 == 0:
        print(f"  ...{idx}/{len(candidates_per_demo_col)}")
print(f"LLM rerank complete")

# COMMAND ----------

# DBTITLE 1,Step 6 — Emit mappings.yaml + gaps.md to Workspace Files
# Canonical derivation expressions for computed columns that customers usually
# don't have natively. Keyed by (target_table_name, target_column_name) — when
# the LLM finds no good source-column match for one of these, the mapping
# notebook pre-fills the YAML with the canonical sql_expression as a starter.
#
# INTENTIONALLY EMPTY for bronze-mapping mode.
# All silver/gold derivations (aging buckets, days_outstanding, billing_amount =
# hours * rate, full_name concat, etc.) are computed by the bundle's
# build_silver_gold notebook on top of customer bronze views — NOT by the
# customer's mapping. Bronze is intended to mirror raw SaaS source schemas
# (Workday / SFDC / Concur / SAP), which typically don't carry these derived
# fields anyway. If a customer's bronze does happen to expose a derived field
# directly, they can still map it via the YAML's `source_column` mechanism.
CANONICAL_DERIVATIONS: dict[tuple[str, str], str] = {}


def assemble_mappings_yaml(decisions: list[dict]) -> dict:
    """Group column-level decisions back into table-level structure for the YAML.

    Multi-catalog aware: each column carries its own `source_catalog` + `source_schema`
    so columns from different physical catalogs can populate the same logical demo
    table via JOINs at view-render time.

    For columns the LLM couldn't confidently map AND that have a canonical
    derivation expression in CANONICAL_DERIVATIONS, pre-fill `sql_expression`
    with a starter pattern.
    """
    by_table: dict[str, dict] = {}
    for d in decisions:
        demo = d["demo"]
        decision = d["decision"]
        tname = demo["table"]
        cname = demo["column"]
        if tname not in by_table:
            by_table[tname] = {
                # Primary source = first column's source. If columns come from
                # multiple sources, the customer fills in the optional `joins:` block
                # to wire them together. The view DDL builder treats the primary as
                # the FROM clause and emits LEFT JOINs for any extra sources referenced.
                "primary_source": {
                    "catalog": decision.get("source_catalog"),
                    "schema": decision.get("source_schema"),
                    "table": decision.get("source_table"),
                },
                "joins": [],  # customer fills in: [{alias, catalog, schema, table, on, type}]
                "confidence": "high",
                "rationale": "",
                "columns": {},
            }

        col_entry: dict = {
            "source_catalog": decision.get("source_catalog"),
            "source_schema": decision.get("source_schema"),
            "source_table": decision.get("source_table"),
            "source_column": decision.get("source_column"),
            "confidence": decision.get("confidence", "low"),
            "rationale": decision.get("rationale", ""),
            "alternatives": decision.get("alternatives", []) or [],
            "flagged": bool(decision.get("flagged")),
        }

        # If the LLM couldn't find a source AND we have a canonical derivation
        # for this (table, column), inject the starter sql_expression.
        if not decision.get("source_column") and (tname, cname) in CANONICAL_DERIVATIONS:
            col_entry["sql_expression"] = CANONICAL_DERIVATIONS[(tname, cname)]
            col_entry["flagged"] = True
            col_entry["rationale"] = (
                "DERIVED — no direct customer column found. Pre-filled with "
                "the canonical derivation expression. Review and adjust to "
                "match your schema (column names, thresholds, etc.). Remove "
                "this sql_expression if your source data has a direct match."
            )

        by_table[tname]["columns"][cname] = col_entry

        # Table-level confidence = min of column-level confidences
        rank = {"low": 0, "medium": 1, "high": 2}
        cur = rank.get(by_table[tname]["confidence"], 2)
        col_rank = rank.get(decision.get("confidence", "low"), 0)
        by_table[tname]["confidence"] = ["low", "medium", "high"][min(cur, col_rank)]

    # Detect cross-catalog tables and flag them with a top-level note for the customer
    for tname, tspec in by_table.items():
        primary = tspec.get("primary_source") or {}
        cross_catalog_columns = []
        for cname, cspec in (tspec.get("columns") or {}).items():
            if not cspec.get("source_column"):
                continue
            if (cspec.get("source_catalog") != primary.get("catalog") or
                cspec.get("source_schema") != primary.get("schema") or
                cspec.get("source_table") != primary.get("table")):
                cross_catalog_columns.append(cname)
        if cross_catalog_columns:
            tspec["_review_note"] = (
                f"CROSS-SOURCE: columns {cross_catalog_columns} come from a different "
                f"catalog.schema.table than the primary source. Add a `joins:` block "
                f"specifying how to JOIN them (alias, on-clause, type). The view DDL "
                f"builder will use it to produce a correct multi-source SELECT."
            )

    return {
        "target_schema": f"{TARGET_VIEW_CATALOG}.{TARGET_VIEW_SCHEMA}",
        "customer_sources": [
            {"catalog": c, "schema": s} for (c, s) in CUSTOMER_SOURCES
        ],
        "tables": by_table,
    }


def write_workspace_file(path: str, content: str) -> None:
    """Write to Workspace Files via the SDK so customer can edit in browser."""
    parent = path.rsplit("/", 1)[0]
    try:
        _w.workspace.mkdirs(parent)
    except Exception:
        pass
    _w.workspace.upload(
        path=path,
        content=content.encode("utf-8"),
        format="AUTO",
        overwrite=True,
    )


mappings = assemble_mappings_yaml(decisions)
mappings_yaml_str = yaml.safe_dump(mappings, sort_keys=False, default_flow_style=False, allow_unicode=True)


# In-notebook review surface — display proposed mappings as a Spark DataFrame
# so the customer can scan/sort/filter inline before opening the YAML.
def _build_review_rows(mappings_dict: dict) -> list[dict]:
    """Flatten table.columns into a tabular structure for display."""
    rows = []
    for tname, tspec in (mappings_dict.get("tables") or {}).items():
        src_table = tspec.get("source_table") or "—"
        for cname, cspec in (tspec.get("columns") or {}).items():
            has_expr = bool((cspec.get("sql_expression") or "").strip())
            src_col = cspec.get("source_column") or ("(derived expression)" if has_expr else "(no match)")
            rows.append({
                "our_table": tname,
                "our_column": cname,
                "source_table": src_table,
                "source_column": src_col,
                "confidence": cspec.get("confidence", "low"),
                "flagged": "⚠️" if cspec.get("flagged") else "",
                "derived": "✓" if has_expr else "",
                "rationale": (cspec.get("rationale") or "")[:140],
            })
    return rows


try:
    review_rows = _build_review_rows(mappings)
    review_df = spark.createDataFrame(review_rows)  # noqa: F821
    print("\n=== Proposed mapping — review inline before editing YAML ===\n")
    print("Sort by `flagged` desc or `confidence` asc to triage the rows that need attention.\n")
    display(review_df)  # noqa: F821
    flagged_count = sum(1 for r in review_rows if r["flagged"])
    derived_count = sum(1 for r in review_rows if r["derived"])
    print(f"\nSummary: {len(review_rows)} columns total, {flagged_count} flagged for review, {derived_count} pre-filled with a derivation expression.")
except Exception as _disp_err:
    # Fall through if `spark` or `display` aren't available (e.g., local run)
    print(f"(Skipped in-notebook display: {_disp_err})")


gaps_lines = ["# Customer schema mapping — gaps report\n",
              f"_Generated {datetime.now().isoformat(timespec='seconds')}_\n\n",
              "Columns flagged for review. Three categories:\n",
              "- **Derived columns** — pre-filled with a canonical `sql_expression` in `mappings.yaml`. ",
              "Customer should verify the expression matches their schema (column names, thresholds, etc.).\n",
              "- **Genuine gaps** — no source column matched. Customer either provides the column name ",
              "or leaves `source_column: null` (view emits NULL placeholder).\n",
              "- **Cross-catalog ambiguity** — two or more candidates from different catalogs look equally good. ",
              "The LLM picked one but the customer should explicitly confirm or override.\n\n"]


def _detect_cross_catalog_ambiguity(d: dict) -> list[dict]:
    """If top-K candidates include same column from 2+ different (catalog, schema, table)
    sources, return the ambiguous siblings (excluding the LLM's pick) so we can
    surface them to the customer for explicit review."""
    chosen = d["decision"]
    chosen_key = (chosen.get("source_catalog"), chosen.get("source_schema"),
                  chosen.get("source_table"), chosen.get("source_column"))
    if not all(chosen_key):
        return []
    chosen_column_name = chosen_key[3]
    chosen_table_name = chosen_key[2]
    siblings = []
    for c in d.get("candidates") or []:
        c_key = (c.get("source_catalog"), c.get("source_schema"), c.get("table"), c.get("column"))
        # Same column + table NAME but DIFFERENT catalog or schema → ambiguity
        if (c.get("column") == chosen_column_name and
            c.get("table") == chosen_table_name and
            (c.get("source_catalog") != chosen_key[0] or c.get("source_schema") != chosen_key[1])):
            siblings.append(c)
    return siblings


for d in decisions:
    flagged = d["decision"].get("flagged")
    ambig_siblings = _detect_cross_catalog_ambiguity(d)
    if not flagged and not ambig_siblings:
        continue
    demo = d["demo"]
    cands = d["candidates"]
    closest = cands[0] if cands else None
    tname = demo["table"]
    cname = demo["column"]
    is_derived = (tname, cname) in CANONICAL_DERIVATIONS

    if ambig_siblings and not flagged:
        # Emit ambiguity-only entry (not in flagged categories above)
        chosen = d["decision"]
        chosen_fqn = f"{chosen.get('source_catalog')}.{chosen.get('source_schema')}.{chosen.get('source_table')}.{chosen.get('source_column')}"
        gaps_lines.append(f"## `{tname}.{cname}` ({demo['type']})  ⚠️ CROSS-CATALOG AMBIGUITY\n\n")
        gaps_lines.append(
            f"_LLM picked:_ `{chosen_fqn}` (confidence: {chosen.get('confidence', '?')})\n\n"
            f"_Same column name also exists in:_\n"
        )
        for s in ambig_siblings:
            sib_fqn = f"{s.get('source_catalog')}.{s.get('source_schema')}.{s['table']}.{s['column']}"
            sv = ", ".join(str(x)[:30] for x in (s.get("sample_values") or [])[:3])
            gaps_lines.append(f"- `{sib_fqn}` — null_pct={s.get('null_pct')}, distinct={s.get('distinct_count')}, samples=[{sv}]\n")
        gaps_lines.append(
            "\n**ACTION:** verify the LLM's pick. If the wrong source was chosen, edit "
            f"`tables.{tname}.columns.{cname}.source_catalog` and `source_schema` in `mappings.yaml`.\n\n"
            "---\n\n"
        )
        continue

    gaps_lines.append(f"## `{tname}.{cname}` ({demo['type']})  {'🔧 DERIVED' if is_derived else '❓ GAP'}\n\n")
    if demo.get("comment"):
        gaps_lines.append(f"_Demo expects:_ {demo['comment']}\n\n")

    if is_derived:
        gaps_lines.append(
            "_Pre-filled with a canonical derivation expression in `mappings.yaml`. "
            "Review the SQL and adjust column references / thresholds to match your data._\n\n"
            "```sql\n"
            f"{CANONICAL_DERIVATIONS[(tname, cname)]}\n"
            "```\n\n"
        )
    else:
        gaps_lines.append(f"_Rationale:_ {d['decision'].get('rationale', '')}\n\n")
        if closest:
            gaps_lines.append(
                f"_Closest customer column:_ `{closest['table']}.{closest['column']}` "
                f"({closest['type']}, sim={closest['score']})\n\n"
            )
    gaps_lines.append("---\n\n")

gaps_md_str = "".join(gaps_lines)

mappings_path = f"{RUN_DIR}/mappings.yaml"
gaps_path = f"{RUN_DIR}/gaps.md"
write_workspace_file(mappings_path, mappings_yaml_str)
write_workspace_file(gaps_path, gaps_md_str)

print(f"Wrote mappings:  {mappings_path}")
print(f"Wrote gaps:      {gaps_path}")
print(f"\nReview these in Workspace Files browser, edit mappings.yaml as needed, ")
print(f"then run the APPLY cell below with CFO_MAPPINGS_FILE={mappings_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏸ STOP — Customer review checkpoint
# MAGIC
# MAGIC Open `mappings.yaml` in the Workspace Files browser at the path printed above.
# MAGIC
# MAGIC For each table/column:
# MAGIC - Verify `source_table` / `source_column` are correct
# MAGIC - Check `rationale` makes sense
# MAGIC - For `flagged: true` rows: either fix the mapping, or mark `source_column: null` to skip that column in the view
# MAGIC - For `confidence: low` or `medium` rows: walk through with the SA before applying
# MAGIC
# MAGIC Cross-reference `gaps.md` for the no-match summary.
# MAGIC
# MAGIC When ready, set the `CFO_MAPPINGS_FILE` widget to the mappings.yaml path and run the APPLY cell.

# COMMAND ----------

# DBTITLE 1,Step 7 — Apply (read mappings.yaml, generate CREATE OR REPLACE VIEW statements)
try:
    dbutils.widgets.text("CFO_MAPPINGS_FILE", "")  # noqa: F821
except Exception:
    pass

MAPPINGS_FILE = _config("CFO_MAPPINGS_FILE", mappings_path)


def read_workspace_file(path: str) -> str:
    """Workspace Files read via SDK."""
    resp = _w.workspace.download(path).read()
    return resp.decode("utf-8")


def render_view_ddl(target_table: str, table_map: dict, target_catalog: str, target_schema: str) -> str:
    """Build CREATE OR REPLACE VIEW DDL from a single table-level mapping entry.

    Supports both single-source and multi-source (cross-catalog JOIN) mappings.

    YAML shape:
      primary_source:
        catalog: <c>
        schema:  <s>
        table:   <t>            # aliased as `primary` in the FROM clause
      joins:                    # optional, only if columns span multiple sources
        - alias:   cc
          catalog: <c>
          schema:  <s>
          table:   <t>
          type:    LEFT          # or INNER / RIGHT / FULL
          on:      "primary.cost_center_id = cc.cost_center_id"
      columns:
        <demo_col>:
          source_catalog: <c>   # if multi-source: must match either primary or a join entry
          source_schema:  <s>
          source_table:   <t>
          source_column:  <col_name>     # OR sql_expression below
          sql_expression: ...            # takes priority over source_column

    Column resolution:
      - If source_column is set and matches primary source → `primary.<col>`
      - If source_column matches a join alias's source → `<alias>.<col>`
      - If sql_expression is set → emit verbatim
      - Otherwise → CAST(NULL AS STRING) AS <demo_col>
    """
    primary = table_map.get("primary_source") or {}
    # Compat: if the YAML uses the flat `source_table` style (no primary_source
    # block), synthesize one from the top-level fields.
    if not primary.get("table") and table_map.get("source_table"):
        primary = {
            "catalog": table_map.get("source_catalog") or "",
            "schema": table_map.get("source_schema") or "",
            "table": table_map.get("source_table"),
        }

    src_table = primary.get("table")
    if not src_table:
        return f"-- SKIPPED {target_table}: no primary_source.table mapped"

    primary_catalog = primary.get("catalog") or ""
    primary_schema = primary.get("schema") or ""
    if not primary_catalog or not primary_schema:
        return f"-- SKIPPED {target_table}: primary_source missing catalog or schema"

    primary_fqn = f"{primary_catalog}.{primary_schema}.{src_table}"

    joins = table_map.get("joins") or []
    # Map (catalog, schema, table) → alias for column resolution
    alias_lookup = {(primary_catalog, primary_schema, src_table): "primary"}
    for j in joins:
        key = (j.get("catalog"), j.get("schema"), j.get("table"))
        alias = j.get("alias")
        if not alias or not all(key):
            return f"-- SKIPPED {target_table}: join entry incomplete (need alias, catalog, schema, table). Got: {j!r}"
        alias_lookup[key] = alias

    def _col_ref(col_map: dict, col_name: str) -> str:
        """Return the SQL expression for this column, properly aliased."""
        sql_expr = (col_map.get("sql_expression") or "").strip()
        if sql_expr:
            expr_lines = sql_expr.splitlines()
            if len(expr_lines) > 1:
                indented = "\n    ".join(expr_lines)
                return f"  (\n    {indented}\n  ) AS {col_name}"
            return f"  ({sql_expr}) AS {col_name}"
        src_col = col_map.get("source_column")
        if not src_col:
            return f"  CAST(NULL AS STRING) AS {col_name}  -- gap: no source column mapped"
        # Resolve which alias this column comes from
        col_cat = col_map.get("source_catalog") or primary_catalog
        col_sch = col_map.get("source_schema") or primary_schema
        col_tab = col_map.get("source_table") or src_table
        alias = alias_lookup.get((col_cat, col_sch, col_tab))
        if not alias:
            # Column references an unknown source — note it but emit anyway as best effort
            return f"  /* unknown source {col_cat}.{col_sch}.{col_tab} */ {src_col} AS {col_name}"
        return f"  {alias}.{src_col} AS {col_name}"

    select_parts = [_col_ref(c_map, c_name)
                    for c_name, c_map in (table_map.get("columns") or {}).items()]
    select_clause = ",\n".join(select_parts) if select_parts else "  primary.*"

    # Build FROM + JOINs
    from_clause = f"FROM {primary_fqn} primary"
    for j in joins:
        jtype = (j.get("type") or "LEFT").upper()
        jfqn = f"{j['catalog']}.{j['schema']}.{j['table']}"
        on_clause = (j.get("on") or "").strip()
        if not on_clause:
            return f"-- SKIPPED {target_table}: join entry for {j.get('alias')} missing `on` clause"
        from_clause += f"\n{jtype} JOIN {jfqn} {j['alias']} ON {on_clause}"

    fqn_target = f"{target_catalog}.{target_schema}.{target_table}"
    return f"CREATE OR REPLACE VIEW {fqn_target} AS\nSELECT\n{select_clause}\n{from_clause}"


def apply_mappings(mappings_file: str) -> dict:
    print(f"Reading {mappings_file}")
    raw = read_workspace_file(mappings_file)
    parsed = yaml.safe_load(raw)
    target_catalog, target_schema = parsed["target_schema"].split(".", 1)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

    results = {"created": [], "failed": []}
    for tname, table_map in (parsed.get("tables") or {}).items():
        ddl = render_view_ddl(tname, table_map, target_catalog, target_schema)
        if ddl.startswith("-- SKIPPED"):
            print(ddl)
            results["failed"].append({"table": tname, "reason": ddl})
            continue
        try:
            spark.sql(ddl)
            results["created"].append(tname)
            print(f"  ✓ created {target_catalog}.{target_schema}.{tname}")
        except Exception as e:
            results["failed"].append({"table": tname, "reason": str(e)[:200]})
            print(f"  ✗ failed {tname}: {e}")
    return results


apply_results = apply_mappings(MAPPINGS_FILE)
print(f"\nCreated: {len(apply_results['created'])} views")
print(f"Failed:  {len(apply_results['failed'])} views")

# COMMAND ----------

# DBTITLE 1,Step 8 — Validate (sample row counts per view)
def validate_views(catalog: str, schema: str, view_names: list[str]) -> None:
    print(f"Validating views in {catalog}.{schema}...")
    print("=" * 70)
    print(f"{'View':<45} {'Row Count':>15}  {'Status'}")
    print("=" * 70)
    for v in view_names:
        fqn = f"{catalog}.{schema}.{v}"
        try:
            n = spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").collect()[0]["n"]
            status = "OK" if n > 0 else "EMPTY (review mapping)"
            print(f"{v:<45} {n:>15,}  {status}")
        except Exception as e:
            print(f"{v:<45} {'ERROR':>15}  {str(e)[:80]}")


validate_views(TARGET_VIEW_CATALOG, TARGET_VIEW_SCHEMA, apply_results["created"])

print(f"\nDone. Next steps to point the demo at your real data:")
print(f"")
print(f"  Re-run deploy.sh with --schema={TARGET_VIEW_SCHEMA} (the same flags as your first deploy,")
print(f"  just change --schema to the target-view schema and add --genie-space-id):")
print(f"")
print(f"    ./deploy.sh \\")
print(f"      --profile <your-cli-profile> \\")
print(f"      --target dev \\")
print(f"      --catalog {TARGET_VIEW_CATALOG} \\")
print(f"      --schema {TARGET_VIEW_SCHEMA} \\")
print(f"      --warehouse-id <your-warehouse-id> \\")
print(f"      --genie-space-id <existing-genie-space-id> \\")
print(f"      --genie-space-title <same-title-as-first-deploy>")
print(f"")
print(f"  This re-runs the bundle deploy against your mapped views — Genie space gets")
print(f"  re-provisioned against the new schema, app.yml is regenerated with the new")
print(f"  CFO_SCHEMA env, the app is restarted. No notebooks to run manually.")
print(f"")
print(f"  After that finishes, re-run the data pipeline to regenerate chip pre-caches")
print(f"  against your data (~25-30 min):")
print(f"")
print(f"    databricks bundle run cfo_data_pipeline --target dev --profile <p> \\")
print(f"      --var \"catalog={TARGET_VIEW_CATALOG}\" --var \"schema_name={TARGET_VIEW_SCHEMA}\" \\")
print(f"      --var \"warehouse_id=<your-warehouse-id>\" \\")
print(f"      --var \"app_name=<your-app-name>\" \\")
print(f"      --var \"workspace_root_path=/Workspace/Users/<your-user>/cfo-app\" \\")
print(f"      --var \"genie_space_id=<existing-genie-space-id>\" \\")
print(f"      --var \"genie_space_title=<same-title>\"")
print(f"")
print(f"  To schedule recurring insight refreshes: add a `schedule` block to the")
print(f"  cfo_data_pipeline job in databricks.yml and re-deploy, OR add the schedule")
print(f"  via the Jobs UI on the deployed job.")
