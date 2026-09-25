# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Schema Mapping
# MAGIC
# MAGIC Maps your existing data to the CFO demo's `bronze_*` shape as SQL **views**, so the demo's
# MAGIC silver→gold transforms run on your real data (no data movement). It maps only the
# MAGIC **load-bearing** columns the app actually uses, and writes `mappings.yaml` (the proposed
# MAGIC mapping) + `gaps.md` (what couldn't be matched) for you to review.
# MAGIC
# MAGIC ### Inputs
# MAGIC - **CFO_CUSTOMER_SOURCES** — your source data as `catalog.schema` (several allowed, `;`-separated). *Required.*
# MAGIC - **CFO_DEMO_SCHEMA** — the deployed demo to map against, `catalog.schema` (default `main.cfo_proserv`).
# MAGIC - **CFO_TARGET_SCHEMA** — where the mapped views are written, `catalog.schema`. Must be a **new/empty** schema (≠ your sources, ≠ the demo).
# MAGIC
# MAGIC ### Run order
# MAGIC 1. Run the **Widgets** cell, fill in the 3 inputs, then run through **Step 6** → produces `mappings.yaml` + `gaps.md`.
# MAGIC 2. **Review `gaps.md`**, then edit `mappings.yaml` for the flagged columns.
# MAGIC 3. Run **Step 7 (Apply)** → builds the `bronze_*` views. (Blocked while load-bearing columns are unmapped; set `CFO_ALLOW_GAPS=true` to proceed with NULLs.)
# MAGIC 4. Redeploy: `./deploy.sh --skip-bronze-hydrate --catalog <target_cat> --schema <target_schema> …` → silver/gold rebuild on your data.

# COMMAND ----------

# DBTITLE 1,Widgets
try:
    dbutils.widgets.text("CFO_CUSTOMER_SOURCES", "")  # noqa: F821
    dbutils.widgets.text("CFO_DEMO_SCHEMA", "main.cfo_proserv")  # noqa: F821
    dbutils.widgets.text("CFO_TARGET_SCHEMA", "main.cfo_demo")  # noqa: F821
    # Step 7 (Apply) inputs — defined here so they exist before that cell runs.
    dbutils.widgets.text("CFO_MAPPINGS_FILE", "")  # noqa: F821
    dbutils.widgets.dropdown("CFO_ALLOW_GAPS", "false", ["false", "true"])  # noqa: F821
    _WIDGETS = True
except Exception:
    _WIDGETS = False

# COMMAND ----------

# DBTITLE 1,Configuration & Imports
import os
import json
import time
import requests
import numpy as np
from pyspark.sql import functions as F
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

# pyyaml isn't in the serverless base image — self-install so cell run-order can't break it.
try:
    import yaml
except ModuleNotFoundError:
    import subprocess as _sp, sys as _sys, importlib
    _sp.check_call([_sys.executable, "-m", "pip", "install", "-q", "pyyaml"])
    importlib.invalidate_caches()
    import yaml


def _config(name: str, default: str = "") -> str:
    if _WIDGETS:
        try:
            v = dbutils.widgets.get(name)  # noqa: F821
            if v:
                return v
        except Exception:
            pass
    return os.environ.get(name, default)


def _split_cat_schema(value: str, label: str) -> tuple[str, str]:
    """Split a `catalog.schema` value into (catalog, schema), validating the form."""
    value = (value or "").strip()
    if value.count(".") != 1 or value.startswith(".") or value.endswith("."):
        raise ValueError(f"{label} must be in 'catalog.schema' form, got {value!r}")
    catalog, schema = value.split(".", 1)
    return catalog.strip(), schema.strip()


def _parse_customer_sources() -> list[tuple[str, str]]:
    """Resolve customer source (catalog, schema) pairs from CFO_CUSTOMER_SOURCES
    — one or more `catalog.schema` entries, `;`-separated (one is the common case)."""
    raw = _config("CFO_CUSTOMER_SOURCES", "").strip()
    out = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        out.append(_split_cat_schema(entry, "CFO_CUSTOMER_SOURCES entry"))
    if not out:
        raise ValueError(
            "No customer source configured. Set CFO_CUSTOMER_SOURCES to one or more "
            "`catalog.schema` entries (semicolon-separated)."
        )
    return out


CUSTOMER_SOURCES     = _parse_customer_sources()   # list of (catalog, schema) tuples
DEMO_CATALOG, DEMO_SCHEMA               = _split_cat_schema(_config("CFO_DEMO_SCHEMA", "main.cfo_proserv"), "CFO_DEMO_SCHEMA")
TARGET_VIEW_CATALOG, TARGET_VIEW_SCHEMA = _split_cat_schema(_config("CFO_TARGET_SCHEMA", "main.cfo_demo"), "CFO_TARGET_SCHEMA")
# Target must be a new/empty schema, separate from the demo and your source schema(s).
_target_fqn = f"{TARGET_VIEW_CATALOG}.{TARGET_VIEW_SCHEMA}"
_forbidden_fqns = {f"{DEMO_CATALOG}.{DEMO_SCHEMA}"} | {f"{c}.{s}" for c, s in CUSTOMER_SOURCES}
if _target_fqn in _forbidden_fqns:
    raise ValueError(
        f"CFO_TARGET_SCHEMA ({_target_fqn}) must be a NEW schema, different from the demo spec "
        f"({DEMO_CATALOG}.{DEMO_SCHEMA}) and your source schema(s) "
        f"({', '.join(f'{c}.{s}' for c, s in CUSTOMER_SOURCES)}). It holds generated bronze_* "
        f"VIEWS and cannot coexist with existing bronze_* tables."
    )
# Engine plumbing (not customer widgets) — gateway URL derived from this workspace.
_WS_HOST             = spark.conf.get("spark.databricks.workspaceUrl")
CLAUDE_GATEWAY_URL   = _config("CFO_CLAUDE_GATEWAY_URL") or f"https://{_WS_HOST}/ai-gateway/mlflow/v1"
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
    table_names = [r["tableName"] for r in tables_df.collect() if not r.asDict().get("isTemporary", False)]
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

        try:  # table-level description — helps match tables the customer named differently
            ext = spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect()
            tcomment = next((r["data_type"] for r in ext if r["col_name"] == "Comment"), "") or ""
        except Exception:
            tcomment = ""

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
                "table_comment": tcomment,
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

# DBTITLE 1,Step 2 — Load our spec (LOAD-BEARING bronze columns only) from deployed demo schema

# The bronze tables/columns the app actually uses — the only ones you need to map.
# (Maintainers: regenerate from the pipeline if the demo's silver/gold logic changes.)
REQUIRED_BRONZE = {
    "bronze_sfdc_accounts": ["account_id", "account_name", "industry", "region", "location", "practice_area", "customer", "last_modified_date"],
    "bronze_sfdc_engagements": ["engagement_id", "engagement_name", "account_id", "engagement_type", "practice_area", "lead_partner", "engagement_manager", "start_date", "end_date", "forecasted_revenue", "budget_amount", "contract_id", "status", "region", "location", "industry", "customer", "last_modified_date"],
    "bronze_sfdc_contracts": ["contract_id", "total_contract_value"],
    "bronze_workday_employees": ["employee_id", "first_name", "last_name", "hire_date", "termination_date", "job_title", "job_level", "job_profile", "location", "region", "practice_area", "industry", "customer", "cost_center"],
    "bronze_workday_organizations": ["cost_center", "organization_name"],
    "bronze_workday_assignments": ["project_id", "employee_id", "assignment_status", "allocation_percentage"],
    "bronze_workday_timecards": ["timecard_id", "employee_id", "project_id", "work_date", "week_ending_date", "time_type", "hours", "billing_rate", "cost_rate", "approval_status", "region", "location", "practice_area", "industry", "customer"],
    "bronze_workday_billing_rates": ["employee_id", "billing_rate", "effective_date", "end_date"],
    "bronze_workday_cost_rates": ["employee_id", "hourly_cost_rate", "effective_date", "end_date"],
    "bronze_concur_expense_items": ["expense_id", "report_id", "transaction_date", "expense_type", "transaction_amount", "transaction_currency", "is_billable", "vendor_name", "has_receipt", "project_id", "region", "location", "practice_area", "industry", "customer"],
    "bronze_concur_expense_reports": ["report_id", "employee_id", "approval_status", "cost_center"],
    "bronze_cost_center_mapping": ["cost_center_code", "department_category"],
    "bronze_sap_accounts_payable": ["invoice_id", "vendor_name", "invoice_number", "invoice_date", "due_date", "amount", "currency", "payment_status", "payment_date", "department", "region", "location", "practice_area", "industry", "customer"],
    "bronze_sap_accounts_receivable": ["invoice_id", "customer_id", "customer_name", "invoice_number", "invoice_date", "due_date", "amount", "currency", "payment_status", "payment_date", "project_id", "region", "location", "practice_area", "industry", "customer"],
    "bronze_sap_general_ledger": ["account_type", "debit_amount", "credit_amount"],
}


def load_demo_spec(catalog: str, schema: str) -> list[dict]:
    """Load the demo's load-bearing bronze spec (per REQUIRED_BRONZE) — the schema we map
    customer data TO. Returns: [{table, column, type, comment, table_comment}, ...]"""
    n_cols = sum(len(c) for c in REQUIRED_BRONZE.values())
    print(f"Loading LOAD-BEARING demo spec from {catalog}.{schema} "
          f"({n_cols} columns across {len(REQUIRED_BRONZE)} tables)...")
    spec = []
    missing = []
    for tname, required_cols in REQUIRED_BRONZE.items():
        fqn = f"{catalog}.{schema}.{tname}"
        try:
            tcomment_row = spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect()
            tcomment = next((r["data_type"] for r in tcomment_row if r["col_name"] == "Comment"), "")
            cols_df = spark.sql(f"DESCRIBE TABLE {fqn}")
        except Exception as e:
            missing.append(f"{tname} (table not found: {str(e)[:50]})")
            continue
        present = {r["col_name"]: r for r in cols_df.collect()
                   if r["col_name"] and not r["col_name"].startswith("#")}
        # Representative sample values per required column (ONE query per table). The demo's
        # synthetic values stand in for the value SHAPE we expect, and get embedded on the
        # demo side so matching works even when a customer has no descriptions.
        samples_by_col: dict = {}
        sel_cols = [c for c in required_cols if c in present]
        if sel_cols:
            try:
                sample_rows = spark.sql(
                    f"SELECT {', '.join('`' + c + '`' for c in sel_cols)} FROM {fqn} LIMIT 50"
                ).collect()
                for c in sel_cols:
                    vals = []
                    for row in sample_rows:
                        v = row[c]
                        if v is not None and str(v) not in vals:
                            vals.append(str(v))
                        if len(vals) >= 5:
                            break
                    samples_by_col[c] = vals
            except Exception:
                samples_by_col = {}
        for cname in required_cols:
            r = present.get(cname)
            if r is None:
                missing.append(f"{tname}.{cname}")
                continue
            spec.append({
                "table": tname,
                "table_comment": tcomment or "",
                "column": cname,
                "type": r["data_type"],
                "comment": r["comment"] or "",
                "sample_values": samples_by_col.get(cname, []),
            })
    if missing:
        print(f"  ⚠️ {len(missing)} manifest entries not found in the demo schema "
              f"(REQUIRED_BRONZE may be stale vs the deployed demo): {missing[:10]}")
    print(f"  loaded {len(spec)} load-bearing columns across "
          f"{len({s['table'] for s in spec})} tables")
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
    """SYMMETRIC per-column text for embedding — identical shape on BOTH sides:
    table + column + type + description + sample values. We embed samples on both sides
    because the demo's synthetic values are representative of the SHAPE expected in each
    column (dates, IDs, currency codes, names), so value-pattern similarity is real
    matching signal — essential when a customer has no column/table descriptions.
    (null%/distinct stay LLM-only; catalog/schema is tracked separately for the DDL.)"""
    parts = [f"table={rec['table']}", f"column={rec['column']}", f"type={rec['type']}"]
    if rec.get("comment"):
        parts.append(f"description: {rec['comment']}")
    if rec.get("sample_values"):
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
NAME_WEIGHT = 0.4  # hybrid retrieval: lexical column-name match blended with embedding cosine


def _name_sim(a: str, b: str) -> float:
    """1.0 for an exact column-name match, else token (underscore-split) Jaccard. Pure
    embeddings under-weight short column names, so this guarantees an exact/near name
    match always surfaces as a candidate (e.g. customer `account_name` for demo `account_name`)."""
    a, b = (a or "").lower(), (b or "").lower()
    if a == b:
        return 1.0
    at, bt = set(a.split("_")), set(b.split("_"))
    return len(at & bt) / len(at | bt) if (at and bt) else 0.0


sims = demo_emb_n @ cust_emb_n.T  # cosine, shape (n_demo, n_cust)

candidates_per_demo_col = []
for i, demo_col in enumerate(demo_spec):
    dname = demo_col["column"]
    blended = [(float(sims[i, j]) + NAME_WEIGHT * _name_sim(dname, customer_profile[j]["column"]), j)
               for j in range(len(customer_profile))]
    blended.sort(key=lambda x: -x[0])
    cands = [{"rank": rank, "score": round(float(sims[i, j]), 4), **customer_profile[int(j)]}
             for rank, (_b, j) in enumerate(blended[:TOP_K], 1)]
    candidates_per_demo_col.append({"demo": demo_col, "candidates": cands})

print(f"VS recall complete: top-{TOP_K} customer candidates retrieved per demo column")

# COMMAND ----------

# DBTITLE 1,Step 5 — LLM rerank + rationale (Claude)
RERANK_PROMPT = """You are mapping a customer's data schema to a CFO demo's expected schema. \
The demo needs a column with the following definition:

DEMO COLUMN
  Table:       {demo_table}
  Table is:    {demo_table_comment}
  Column:      {demo_column}
  Type:        {demo_type}
  Description: {demo_comment}

The customer schema has these candidate columns (ranked by semantic similarity), each with its
column and table descriptions:
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

Judge confidence on the column DESCRIPTIONS, TABLE descriptions, and sample VALUES — NOT just
name/type similarity. A same-named column with different meaning is not a high-confidence match.
Set confidence "low" and flagged=true when the only evidence is name/type, or when the best
candidate comes from a customer table whose other columns don't also fit this demo table (a
coherent table-to-table match is stronger than columns scavenged from unrelated tables).

Do NOT reject a candidate (no_match) or mark it low merely because its sample VALUES are empty or
mostly null. If a candidate's NAME and TYPE (and DESCRIPTION, if present) match the demo column,
treat it as a valid match and note the sparsity in the rationale — an empty source column is a
data-quality issue for the customer to populate, NOT a missing mapping.

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
            + (f", col_desc: {c['comment']}" if c.get('comment') else "")
            + (f", table_desc: {c['table_comment']}" if c.get('table_comment') else "")
        )
    return "\n".join(lines)


def rerank_one(item: dict) -> dict:
    demo = item["demo"]
    prompt = RERANK_PROMPT.format(
        demo_table=demo["table"],
        demo_table_comment=demo.get("table_comment") or "(no description)",
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
            "alternatives": list(dict.fromkeys(decision.get("alternatives") or [])),
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

        # Searchable review marker (Cmd+F "⚠️"). We auto-map only what we're sure about:
        # high confidence, from this view's primary source. Everything else is a
        # recommendation the customer must review and confirm by deleting the action line.
        prim_table = (by_table[tname]["primary_source"] or {}).get("table")
        sc = (col_entry.get("source_column") or "").strip()
        st = col_entry.get("source_table")
        conf = col_entry.get("confidence", "low")
        cross_source = bool(sc) and bool(st) and st != prim_table
        if col_entry.get("sql_expression"):
            col_entry = {"action": "⚠️ REVIEW DERIVED — we pre-filled a sql_expression; confirm it fits your schema, then delete this line", **col_entry}
        elif not sc:
            col_entry = {"action": "⚠️ FILL THIS GAP — no source column found; set source_column (see alternatives) or a sql_expression, or leave blank (column will be NULL)", **col_entry}
        elif cross_source:
            col_entry = {"action": f"⚠️ REVIEW — we recommend `{st}.{sc}`, but it's from a different table than this view's primary source (`{prim_table}`). Same entity? add a `joins:` block. Not a match? treat as a gap. Confirm, then delete this line.", **col_entry}
        elif conf != "high" or col_entry.get("flagged"):
            col_entry = {"action": f"⚠️ REVIEW — we recommend `{sc}` but we're not fully confident ({conf}). Confirm or change it, then delete this line.", **col_entry}

        by_table[tname]["columns"][cname] = col_entry

        # Table-level confidence = min of column-level confidences
        rank = {"low": 0, "medium": 1, "high": 2}
        cur = rank.get(by_table[tname]["confidence"], 2)
        col_rank = rank.get(decision.get("confidence", "low"), 0)
        by_table[tname]["confidence"] = ["low", "medium", "high"][min(cur, col_rank)]

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
        format=ImportFormat.AUTO,
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


def _source_system(table: str) -> str:
    t = (table or "").lower()
    if t.startswith("bronze_workday"):     return "Workday (HR / time tracking)"
    if t.startswith("bronze_sap"):         return "SAP (finance / ERP)"
    if t.startswith("bronze_sfdc"):        return "Salesforce (CRM)"
    if t.startswith("bronze_concur"):      return "Concur (expense / T&E)"
    if t.startswith("bronze_cost_center"): return "Finance master data (cost-center reference)"
    return "Other"


def _domain_label(table: str) -> str:
    t = (table or "").lower()
    for p in ("bronze_workday_", "bronze_sap_", "bronze_sfdc_", "bronze_concur_", "bronze_"):
        if t.startswith(p):
            t = t[len(p):]
            break
    return t.replace("_", " ").title() or table


def _detect_cross_catalog_ambiguity(d: dict) -> list[dict]:
    """Only meaningful with MULTIPLE source schemas: returns sibling candidates of the chosen
    column that live in a DIFFERENT (catalog, schema) — same column in >1 source, so the
    LLM's pick should be confirmed. Single-source runs can't be ambiguous."""
    if len(CUSTOMER_SOURCES) <= 1:
        return []
    chosen = d["decision"]
    cat, sch, tab, col = (chosen.get("source_catalog"), chosen.get("source_schema"),
                          chosen.get("source_table"), chosen.get("source_column"))
    if not all([cat, sch, tab, col]):
        return []
    return [c for c in (d.get("candidates") or [])
            if c.get("column") == col and c.get("table") == tab
            and c.get("source_catalog") and c.get("source_schema")
            and (c.get("source_catalog"), c.get("source_schema")) != (cat, sch)]


def _fqn(*parts: str) -> str:
    """Join parts into catalog.schema.table.column, dropping blanks and collapsing accidental
    consecutive duplicate segments (guards the doubled-table-name bug)."""
    segs: list[str] = []
    for p in parts:
        for seg in str(p or "").split("."):
            seg = seg.strip()
            if seg and (not segs or segs[-1] != seg):
                segs.append(seg)
    return ".".join(segs)


# Primary source table per demo table (first decision's source — mirrors assemble_mappings_yaml).
primary_table_by_tname: dict = {}
for d in decisions:
    t = d["demo"]["table"]
    if t not in primary_table_by_tname:
        primary_table_by_tname[t] = d["decision"].get("source_table")

# Surface everything we're NOT sure about: anything below high confidence, from a different
# table than the view's primary source, derived, flagged, ambiguous, or with no source at all.
# High-confidence primary-source matches auto-map silently and don't appear here.
gap_items = []
for d in decisions:
    demo = d["demo"]
    dec = d["decision"]
    tname, cname = demo["table"], demo["column"]
    sc = (dec.get("source_column") or "").strip()
    st = dec.get("source_table")
    conf = dec.get("confidence", "low")
    is_derived = (tname, cname) in CANONICAL_DERIVATIONS or bool((dec.get("sql_expression") or "").strip())
    cross = bool(sc) and bool(st) and st != primary_table_by_tname.get(tname)
    flagged = bool(dec.get("flagged"))
    ambig = _detect_cross_catalog_ambiguity(d)
    no_source = not sc and not is_derived
    if not (no_source or is_derived or cross or conf != "high" or flagged or ambig):
        continue
    closest = (d.get("candidates") or [None])[0]
    rationale = (dec.get("rationale") or "").strip()
    alts = list(dict.fromkeys(dec.get("alternatives") or []))
    if no_source:
        status = "❓ NO SOURCE FOUND"
        bits = [rationale] if rationale else []
        if closest:
            bits.append(f"closest: `{closest['table']}.{closest['column']}` (sim={closest.get('score')})")
        if alts:
            bits.append(f"alternatives: {', '.join(alts)}")
        detail = " — ".join(bits) or "no candidate found — ingest this data, or leave blank (column will be NULL)"
    elif is_derived:
        status = "🔧 REVIEW DERIVED"
        detail = "we pre-filled a `sql_expression` in mappings.yaml — confirm it matches your schema"
    elif cross:
        status = "⚠️ REVIEW (different table)"
        detail = (f"we recommend `{st}.{sc}`, but it's from a different table than `{primary_table_by_tname.get(tname)}`. "
                  f"Same entity? add a `joins:` block. Not a match? treat as a gap")
        if alts:
            detail += f". alternatives: {', '.join(alts)}"
    elif ambig:
        status = "⚠️ CONFIRM SOURCE"
        chosen_fqn = _fqn(dec.get("source_catalog"), dec.get("source_schema"), st, sc)
        sibs = ", ".join(_fqn(s.get("source_catalog"), s.get("source_schema"), s["table"], s["column"]) for s in ambig)
        detail = f"same column exists in multiple sources — we chose `{chosen_fqn}`; also in: {sibs}"
    else:
        status = "⚠️ REVIEW (low confidence)"
        detail = f"we recommend `{sc}` ({conf})" + (f" — {rationale}" if rationale else "")
        if alts:
            detail += f"; alternatives: {', '.join(alts)}"
    gap_items.append({"system": _source_system(tname), "table": tname, "column": cname,
                      "type": demo.get("type", ""), "status": status, "detail": detail,
                      "edit_path": f"tables.{tname}.columns.{cname}"})

gaps_lines = ["# Customer schema mapping — what to wire up\n",
              f"_Generated {datetime.now().isoformat(timespec='seconds')}_\n\n"]
if not gap_items:
    gaps_lines.append("✅ Every load-bearing column mapped to your data with high confidence. Run Step 7 to build the views.\n")
else:
    by_system: dict = {}
    for it in gap_items:
        by_system.setdefault(it["system"], {}).setdefault(it["table"], []).append(it)
    gaps_lines.append(
        f"We auto-mapped the columns we're confident about. The **{len(gap_items)} field(s)** below, across "
        f"**{len(by_system)} source domain(s)**, are ones where **we have a recommendation but we're not sure** — "
        "please review and confirm each. They're grouped by the source system the data *typically* comes from — "
        "wherever you actually keep it. In `mappings.yaml` (search `⚠️`), confirm or fix each one and **delete its "
        "`action:` line** to mark it reviewed; then run Step 7. (To stand the app up now with these left blank, set "
        "the `CFO_ALLOW_GAPS` widget to `true`.)\n\n")
    gaps_lines.append("**Summary:**\n")
    for system, tables in by_system.items():
        gaps_lines.append(f"- {system}: {sum(len(v) for v in tables.values())} field(s)\n")
    gaps_lines.append("\n")
    for system, tables in by_system.items():
        gaps_lines.append(f"## {system}\n\n")
        for tname, items in tables.items():
            gaps_lines.append(f"### {_domain_label(tname)}  _(`{tname}`)_\n\n")
            for it in items:
                gaps_lines.append(f"- **{it['column']}** ({it['type']}) — {it['status']}"
                                  + (f": {it['detail']}" if it["detail"] else "")
                                  + f"  → edit `{it['edit_path']}`\n")
            gaps_lines.append("\n")

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
# CFO_MAPPINGS_FILE and CFO_ALLOW_GAPS are defined in the top Widgets cell. Empty
# CFO_MAPPINGS_FILE falls back to this run's mappings_path. CFO_ALLOW_GAPS='false'
# refuses to build while columns are unresolved; 'true' proceeds with NULL placeholders.
# Empty CFO_MAPPINGS_FILE falls back to this run's file if Steps 1-6 ran in this session;
# otherwise (fresh kernel) set the widget to your mappings.yaml path explicitly.
_this_run_mappings = mappings_path if "mappings_path" in globals() else ""
MAPPINGS_FILE = _config("CFO_MAPPINGS_FILE", _this_run_mappings)
if not (MAPPINGS_FILE or "").strip():
    raise ValueError(
        "No mappings file to apply. Set the CFO_MAPPINGS_FILE widget to your mappings.yaml path "
        "(e.g. /Workspace/Shared/cfo_demo_mapping/<run>/mappings.yaml), or run Steps 1-6 first."
    )
ALLOW_GAPS = _config("CFO_ALLOW_GAPS", "false").strip().lower() in ("true", "1", "yes")


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
            return f"  CAST(NULL AS STRING) AS {col_name} /* gap: no source column mapped */"
        # Resolve which alias this column comes from
        col_cat = col_map.get("source_catalog") or primary_catalog
        col_sch = col_map.get("source_schema") or primary_schema
        col_tab = col_map.get("source_table") or src_table
        alias = alias_lookup.get((col_cat, col_sch, col_tab))
        if not alias:
            # Source table isn't the primary and has no joins entry → can't resolve it.
            # Emit NULL rather than invalid SQL; the Step 7 gate flags it for a joins: block.
            return f"  CAST(NULL AS STRING) AS {col_name} /* unresolved: {col_cat}.{col_sch}.{col_tab} needs a joins: block */"
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

    # Never silently build a view from a mapping we're not sure about — a wrong number in the
    # app is worse than a blank one. A column is "unresolved" if: it has no source at all, it
    # still carries an unreviewed ⚠️ action line, or its source table isn't reachable from the
    # view's primary source + joins. The customer resolves by confirming/fixing the mapping and
    # deleting the action line (and adding a joins: block where needed).
    def _unresolved_reason(tmap: dict, cmap: dict) -> str | None:
        sc = (cmap.get("source_column") or "").strip()
        expr = (cmap.get("sql_expression") or "").strip()
        if not sc and not expr:
            return "no source"
        if (cmap.get("action") or "").strip():
            return "unreviewed (⚠️ action line still present)"
        if sc and not expr:
            prim = tmap.get("primary_source") or {}
            reachable = {(prim.get("catalog"), prim.get("schema"), prim.get("table"))}
            for j in (tmap.get("joins") or []):
                reachable.add((j.get("catalog"), j.get("schema"), j.get("table")))
            key = (cmap.get("source_catalog") or prim.get("catalog"),
                   cmap.get("source_schema") or prim.get("schema"),
                   cmap.get("source_table") or prim.get("table"))
            if key not in reachable:
                return "source table unreachable (needs a joins: block)"
        return None

    unresolved = {}
    for tname, tmap in (parsed.get("tables") or {}).items():
        for cname, cmap in (tmap.get("columns") or {}).items():
            reason = _unresolved_reason(tmap, cmap)
            if reason:
                unresolved[f"{tname}.{cname}"] = reason
    if unresolved and not ALLOW_GAPS:
        sample = [f"{k} ({v})" for k, v in list(unresolved.items())[:25]]
        raise ValueError(
            f"{len(unresolved)} load-bearing column(s) are not resolved — NOT building views. "
            f"Review each in mappings.yaml (search ⚠️): confirm or fix the recommendation, add a "
            f"`joins:` block where flagged, then DELETE the `action:` line to mark it reviewed. "
            f"To stand the app up now with these columns blank, set CFO_ALLOW_GAPS='true'. "
            f"Unresolved: {sample}"
            + (f" ...(+{len(unresolved) - 25} more)" if len(unresolved) > 25 else "")
        )
    if unresolved and ALLOW_GAPS:
        # Ship blanks, not guesses: strip the source so the DDL emits CAST(NULL ...) for each.
        for tname, tmap in (parsed.get("tables") or {}).items():
            for cname, cmap in (tmap.get("columns") or {}).items():
                if f"{tname}.{cname}" in unresolved:
                    cmap.pop("source_column", None)
                    cmap.pop("sql_expression", None)
        print(f"⚠️ CFO_ALLOW_GAPS=true — {len(unresolved)} unresolved column(s) will be NULL (blank in the app): "
              f"{list(unresolved)[:25]}")

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

print(f"\nDone. Your bronze_* views are built. From here:")
print(f"")
print(f"1) FIRST TIME — point the app + Genie at your data (one deploy, ~25-30 min):")
print(f"   Re-run deploy.sh against your target schema with both flags:")
print(f"     --skip-bronze-hydrate   silver/gold build on your bronze_* VIEWS, not regenerated")
print(f"                             synthetic data (REQUIRED — or your views get clobbered)")
print(f"     --refresh-data          runs cfo_data_pipeline at the end (silver/gold + chip caches)")
print(f"   Incremental deploy: repoints app.yml (CFO_SCHEMA) + the Genie space at your schema")
print(f"   and runs the pipeline. Does NOT rebuild infra from scratch.")
print(f"")
print(f"     ./deploy.sh \\")
print(f"       --profile <cli-profile> --target dev \\")
print(f"       --skip-bronze-hydrate --refresh-data \\")
print(f"       --catalog {TARGET_VIEW_CATALOG} --schema {TARGET_VIEW_SCHEMA} \\")
print(f"       --warehouse-id <warehouse-id> \\")
print(f"       --genie-space-id <existing-genie-space-id> \\")
print(f"       --genie-space-title <same-title-as-first-deploy>")
print(f"")
print(f"2) RECURRING — refresh data on a cadence (no redeploy):")
print(f"   App + Genie already point at your schema; only the data refreshes. Schedule")
print(f"   cfo_data_pipeline with skip_hydrate_bronze=true — add a `schedule` block to the job")
print(f"   in databricks.yml and re-deploy, or set it in the Jobs UI on the deployed job.")
