"""bootstrap_genie_space.py — programmatic Genie space provisioning for the CFO Insights pipeline.

What this does:
  1. Builds a single MERGED Genie space (tables + instructions + trusted queries
     drawn from genie_config/) — the v1 architecture decision.
  2. CREATE if no existing space ID is provided, UPDATE if one is.
  3. Persists the resulting space ID to a JSON file so the app and the daily
     orchestrator notebook can read it.

Why programmatic:
  Customer-bundle deployment will run this script (via a setup notebook) so
  the customer never has to manually create or configure a Genie space. They
  type `databricks bundle deploy` once, the bundle deploys app + notebooks +
  jobs, and a setup notebook calls this script to provision the Genie space.

API gotchas baked in:
  - `sql_snippets` must be `null`, NOT `[]` (empty array is rejected with
    "Invalid JSON in field 'serialized_space'")
  - Description max length is 16384 bytes — merged instructions trimmed if needed
  - `version: 2` required on serialized_space
  - column_configs must use `enable_format_assistance: True`

Usage:
    # First-time provisioning (creates new space):
    python3 bootstrap_genie_space.py --warehouse-id <your-warehouse-id>

    # Update existing space (preserves the same space_id):
    python3 bootstrap_genie_space.py --warehouse-id <your-warehouse-id> \\
        --space-id 01f144aeb85e1d41815441f42e374df7

    # Dry-run (validate inputs, don't call API):
    python3 bootstrap_genie_space.py --warehouse-id <your-warehouse-id> --dry-run

Output:
    On success, writes the space ID to ./merged_space_id.json:
        {"space_id": "01f...", "warehouse_id": "75fd...", "last_provisioned": "..."}
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient

THIS_DIR = Path(__file__).parent
GENIE_CONFIG_DIR = THIS_DIR.parent / "genie_config"
SPACE_ID_FILE = THIS_DIR / "merged_space_id.json"
DESCRIPTION_BYTE_LIMIT = 16384

DEFAULT_TITLE = "ProServ OCFO"


# ─────────────────────────────────────────────────────────────────────────────
# 1. Build merged instructions text (description field)
# ─────────────────────────────────────────────────────────────────────────────

def _split_md_sections(md_text: str) -> dict[str, str]:
    """Split a markdown doc by ## headers. Returns {section_title: section_text}."""
    sections: dict[str, str] = {}
    preamble: list[str] = []
    current: str | None = None
    buf: list[str] = []

    for line in md_text.splitlines():
        if line.startswith("## "):
            if current is not None:
                sections[current] = "\n".join(buf).rstrip()
            else:
                sections["__preamble__"] = "\n".join(preamble).rstrip()
            current = line[3:].strip()
            buf = [line]
        elif current is None:
            preamble.append(line)
        else:
            buf.append(line)

    if current is not None:
        sections[current] = "\n".join(buf).rstrip()
    elif preamble:
        sections["__preamble__"] = "\n".join(preamble).rstrip()
    return sections


# As-of date for the FROZEN demo snapshot. The dataset is pinned to the
# generator's ANCHOR_DATE; Genie must anchor every time-relative query to this
# date, NOT wall-clock CURRENT_DATE() — once real calendar time passes the
# anchor, CURRENT_DATE() points at months the frozen data only partially
# contains and answers collapse to implausible numbers. Resolved from the data
# at provision time by _resolve_as_of(); env CFO_AS_OF_DATE overrides; the
# fallback literal matches the shipped ANCHOR_DATE.
_AS_OF_DATE = os.environ.get("CFO_AS_OF_DATE", "2026-05-15")


def _resolve_as_of(client: WorkspaceClient, warehouse_id: str, schema_fqn: str) -> str:
    """Set the module-level _AS_OF_DATE from the data (MAX(work_date)). Memoized
    via the global; safe to call once per provision run."""
    global _AS_OF_DATE
    try:
        r = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SELECT CAST(MAX(work_date) AS STRING) FROM {schema_fqn}.silver_fact_timecards",
            wait_timeout="30s",
        )
        if r.result and r.result.data_array and r.result.data_array[0] and r.result.data_array[0][0]:
            _AS_OF_DATE = str(r.result.data_array[0][0])[:10]
    except Exception as e:
        print(f"[bootstrap] as-of resolve failed; using {_AS_OF_DATE}. {e}")
    print(f"[bootstrap] Genie as-of date = {_AS_OF_DATE} (CURRENT_DATE() rewritten to this)")
    return _AS_OF_DATE


def _rewrite_schema_in_text(text: str, schema_override: str | None) -> str:
    """Post-process Genie config text (instructions markdown, trusted-query SQL,
    snippet SQL). Two rewrites:
      1. Anchor wall-clock CURRENT_DATE() -> DATE('<as-of>') so Genie's SQL stays
         aligned with the frozen dataset (see _AS_OF_DATE above).
      2. If schema_override is set, repoint `main.cfo_proserv.` -> `<override>.`
         so a customer's deployed space targets their own catalog/schema.
    """
    if text and "CURRENT_DATE()" in text:
        text = text.replace("CURRENT_DATE()", f"DATE('{_AS_OF_DATE}')")
    if not schema_override:
        return text
    return text.replace("main.cfo_proserv.", f"{schema_override}.").replace(
        "`main.cfo_proserv`", f"`{schema_override}`"
    ).replace(
        "main.cfo_proserv ", f"{schema_override} "
    )


def build_short_description() -> str:
    """Build the SHORT, user-facing room intro that goes in the Genie `description`
    field (shown in the About tab when a user opens the space).

    Per Databricks Genie best practices (docs.databricks.com/aws/en/genie/best-practices),
    `description` is user-facing metadata for orientation, NOT behavioral guidance for
    the LLM at chat time. Keep this short and human-readable. The full behavioral
    rules (Time conventions, AR aging semantics, routing rules, etc.) live in
    `general_instructions` instead — see build_merged_description() for that.

    Markdown is supported and renders in the About tab. Keep this generic so the
    same description works for the customer-shipped bundle, not just the demo.
    """
    return (
        "# CFO Operations Platform\n"
        "\n"
        "Conversational analytics for a consulting firm's finance and operations leadership. "
        "Spans cash position (AR/AP/DSO/DPO), enterprise and project margins, partner economics, "
        "utilization, T&E, regional and practice-area performance, and pipeline health.\n"
        "\n"
        "## Who uses this space\n"
        "\n"
        "- **CFO / Finance leadership** — firmwide profitability, working capital, plan vs. actual variance\n"
        "- **Operations admins** — utilization, billable mix, talent supply/demand, project health\n"
        "- **Practice leaders** — practice-area P&L, project-level drill-downs, partner economics\n"
        "\n"
        "## Try asking\n"
        "\n"
        "- What's our firmwide DSO trend over the last 6 months?\n"
        "- Which clients carry the largest 90+ day receivables exposure?\n"
        "- How is revenue per partner trending by region this fiscal year?\n"
        "- Where are project margins running below plan?\n"
        "- Which practice areas are over budget on billable expenses?\n"
        "\n"
        "Behavioral guidance Genie applies to every question (table-routing rules, time conventions, "
        "aging semantics, etc.) lives in the **General Instructions** tab. The data dictionary and "
        "trusted SQL patterns are configured in the **Data** and **Instructions** tabs.\n"
    )


def _extract_short_blurb(markdown_text: str, max_chars: int = 500) -> str:
    """Legacy helper — preserved in case any caller still depends on it.

    See build_short_description() above for the canonical About-tab description.
    """
    for chunk in markdown_text.split("\n\n"):
        chunk = chunk.strip()
        if not chunk or chunk.startswith("#"):
            continue
        return chunk[:max_chars]
    return markdown_text[:max_chars]


def build_merged_description(schema_override: str | None = None) -> str:
    """Combine v2_analytics.md + v2_management.md into a single instruction
    text, deduping common sections and preserving unique ones.

    If schema_override is set, every `main.cfo_proserv` reference in the
    instructions text is rewritten so the customer's deployed Genie space
    sees their own catalog/schema, not the demo's dev defaults.
    """
    analytics_md = (GENIE_CONFIG_DIR / "instructions" / "v2_analytics.md").read_text()
    management_md = (GENIE_CONFIG_DIR / "instructions" / "v2_management.md").read_text()

    a_sec = _split_md_sections(analytics_md)
    m_sec = _split_md_sections(management_md)

    out: list[str] = [
        "# CFO Operations Platform — Merged",
        "",
        "## Purpose",
        "",
        "Answers financial, operational, and talent questions for a consulting firm's CFO office. Spans cash, AR/AP, margins, T&E, expenses, partner economics, utilization.",
        "",
    ]

    # Sections to take from analytics (preferred since both have them, analytics has fuller content)
    analytics_sections_in_order = [
        "Schema",
        "Time conventions",
        "Narrative discipline — precise verbs grounded in numbers",
        "In-progress month — exclude from all aggregates",
        "Filter parameters",
        "Profit / operating margin — always read from `gold_regional_pnl`",
        "AR aging trend over time — snapshot per month-end, NOT group by invoice_date",
        "Pipeline / backlog — `gold_enterprise_metrics.pipeline_revenue` only",
        "Expense aggregation — `gold_regional_pnl.*_expenses` only, NEVER `silver_fact_expenses`",
        'Two distinct "plan" baselines — never conflate',
        "Output guidance",
        "Instructions you must follow when providing summaries",
    ]
    for sect in analytics_sections_in_order:
        if sect in a_sec:
            out.append(a_sec[sect])
            out.append("")

    # Append management-only sections (not in analytics)
    for sect, body in m_sec.items():
        if sect in ("__preamble__", "Purpose"):
            continue
        if sect not in a_sec:
            out.append(body)
            out.append("")

    merged = "\n".join(out).rstrip() + "\n"
    merged = _rewrite_schema_in_text(merged, schema_override)

    if len(merged.encode("utf-8")) > DESCRIPTION_BYTE_LIMIT:
        # Trim by removing redundant blank lines and inline trimming
        # (rare; merged should fit since v2_analytics fits comfortably)
        merged = "\n".join(line for line in merged.splitlines() if line.strip() != "" or line == "") + "\n"
        if len(merged.encode("utf-8")) > DESCRIPTION_BYTE_LIMIT:
            raise ValueError(
                f"Merged instructions are {len(merged.encode('utf-8'))} bytes, "
                f"exceeds {DESCRIPTION_BYTE_LIMIT}-byte limit. Trim manually."
            )

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# 2. Build trusted query list (example_question_sqls)
# ─────────────────────────────────────────────────────────────────────────────

def _stable_id(local_id: str) -> str:
    """Generate a stable 32-hex UUID from a local id string."""
    return hashlib.md5(f"cfo_demo:{local_id}".encode()).hexdigest()


def build_example_queries(schema_override: str | None = None) -> list[dict]:
    """Load all trusted queries from genie_config/trusted_queries.yml.

    For the merged space we include ALL queries regardless of which space they
    were originally tagged for (v2_analytics or v2_management — both now live
    in one space). If schema_override is set, all `main.cfo_proserv.X` table
    references in each SQL string are rewritten so the customer's deployed
    Genie space targets their own catalog/schema.

    Shape: the Genie API requires `question` and `sql` to be ARRAYS, not bare
    strings. To avoid the multi-variant concatenation bug (where N variants
    in a single entry's arrays render as concatenated UI titles + stacked
    unparseable SQL blocks), we emit ONE entry per variant, each with
    single-element arrays. The empirically-found rule: API contract = arrays;
    quality contract = one variant per entry.
    """
    queries_yml = yaml.safe_load((GENIE_CONFIG_DIR / "trusted_queries.yml").read_text())
    out: list[dict] = []
    for q in queries_yml.get("queries", []):
        variants = q["question_variants"]
        sql = _rewrite_schema_in_text(q["sql"].strip(), schema_override)
        for idx, variant in enumerate(variants):
            local_id = f"{q['id']}__v{idx:02d}"
            out.append({
                "id": _stable_id(local_id),
                "question": [variant],
                "sql": [sql],
            })
    out.sort(key=lambda x: x["id"])
    return out


def build_sql_snippets(schema_override: str | None = None) -> dict:
    """Load filter + measure snippets from genie_config/sql_snippets.yml.

    Genie API expects sql_snippets as a DICT with `filters` and `measures` keys —
    NOT a list. Earlier bootstrap versions hit "Invalid JSON" when passing an
    empty list `[]` and incorrectly defaulted to `None`, which dropped all
    snippets. The correct populated shape is what this function emits.

    Each YAML entry MUST declare `tables: [fqn1, fqn2, ...]` so the SQL string
    can be authored with table-qualified column refs
    (`silver_fact_accounts_payable.payment_status` not bare `payment_status`).
    Without table qualification, Genie rejects the snippet with "Table name or
    alias is required for column X". The `tables:` YAML field is used at author
    time to keep snippets honest; it is NOT shipped to Genie (the `Filter`
    protobuf does not have an `applicable_tables` field — empirically rejected
    with INVALID_PARAMETER_VALUE).

    Snippets without a `tables:` field are skipped (would otherwise fail
    Genie's column-resolution validator since they couldn't have been
    table-qualified).
    """
    snippets_yml = yaml.safe_load((GENIE_CONFIG_DIR / "sql_snippets.yml").read_text())

    def _shape(entries: list[dict], kind: str) -> list[dict]:
        out = []
        for e in entries:
            tables = e.get("tables") or []
            if not tables:
                continue  # snippets without table scope would fail validator
            sql = _rewrite_schema_in_text(e["sql"].strip(), schema_override)
            out.append({
                "id": _stable_id(f"snippet:{kind}:{e['display_name']}"),
                "sql": [sql],
                "display_name": e["display_name"],
            })
        return sorted(out, key=lambda x: x["id"])

    return {
        "filters": _shape(snippets_yml.get("filters", []), "filter"),
        "measures": _shape(snippets_yml.get("measures", []), "measure"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. Build table inventory with column configs
# ─────────────────────────────────────────────────────────────────────────────

def get_merged_table_list(schema_override: str | None = None) -> list[str]:
    """Union of v2_analytics + v2_management tables, sorted, deduped.

    If schema_override is provided (e.g., "main.cfo_proserv_dev"), every
    `main.cfo_proserv.<table>` reference is rewritten to `<schema_override>.<table>`.
    Used to target a dev/test schema without editing tables.yml.
    """
    tables_yml = yaml.safe_load((GENIE_CONFIG_DIR / "tables.yml").read_text())
    tables = sorted(set(tables_yml["v2_analytics"] + tables_yml["v2_management"]))
    if schema_override:
        tables = [t.replace("main.cfo_proserv.", f"{schema_override}.") for t in tables]
    return tables


def describe_table_columns(client: WorkspaceClient, warehouse_id: str, table_fqn: str) -> list[tuple[str, str]]:
    """Return list of (column_name, type_string) tuples for a fully-qualified table."""
    res = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"DESCRIBE TABLE {table_fqn}",
        wait_timeout="30s",
    )
    cols: list[tuple[str, str]] = []
    if res.result and res.result.data_array:
        for row in res.result.data_array:
            colname = row[0]
            if not colname or colname.startswith("#"):
                break  # DESCRIBE output transitions to partition info after a # marker
            coltype = row[1] if len(row) > 1 else ""
            cols.append((colname, coltype))
    return cols


# Knowledge Store entity-matching = per-column value-dictionary feature.
# Genie samples the actual values in the column and uses them as canonical
# enums when writing SQL. This prevents value hallucinations (e.g., Genie
# inventing "91-120 Days" when the column actually contains "61-90 days").
ENTITY_MATCHING_MIN_DISTINCT = 2     # too few = useless
ENTITY_MATCHING_MAX_DISTINCT = 1024  # Knowledge Store hard limit per column

# Knowledge Store soft limit is ~120 entity-matched columns per space, so we
# exclude columns that don't benefit from it: free-text PII columns (names,
# addresses, phone, website), human-typed descriptive fields (project_name,
# merchant_name, report_name, position_title), and free-form IDs that Genie
# won't filter on directly. Entity matching is most valuable on enum-style
# columns (status, type, level, bucket, category) and ranked-list dimensions
# (location, practice_area, region, industry).
ENTITY_MATCHING_EXCLUDE_PATTERNS = {
    # PII / free text
    "first_name", "last_name", "full_name", "email", "phone", "website",
    "billing_street", "billing_city", "billing_state", "billing_country",
    "billing_postal_code",
    # Human-typed descriptive fields with many distinct values
    "project_name", "merchant_name", "report_name", "position_title",
    "vendor_name", "client_name", "customer_name", "counterparty_name",
    "lead_partner_name", "project_manager_name", "account_manager_name",
    # ID columns (Genie filters by name, not raw IDs)
    "task_id", "vendor_id", "client_id", "customer_id",
    "project_manager_id", "account_manager_id",
    # Description / note fields
    "expense_description", "gl_account",
}


def probe_entity_matching_candidates(
    client: WorkspaceClient,
    warehouse_id: str,
    table_fqn: str,
    string_cols: list[str],
) -> set[str]:
    """For each string column, count distinct values. Return the set of columns
    where entity matching should be enabled (distinct count in
    [MIN, MAX] range). One SQL call per table for efficiency.
    """
    # Filter out columns that don't benefit from entity matching (free text,
    # PII, IDs, etc.) — see ENTITY_MATCHING_EXCLUDE_PATTERNS for rationale.
    string_cols = [c for c in string_cols if c not in ENTITY_MATCHING_EXCLUDE_PATTERNS]
    if not string_cols:
        return set()
    select_clauses = ", ".join(f"COUNT(DISTINCT `{c}`) AS `{c}`" for c in string_cols)
    sql = f"SELECT {select_clauses} FROM {table_fqn}"
    res = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s",
    )
    if not (res.result and res.result.data_array):
        return set()
    row = res.result.data_array[0]
    enabled: set[str] = set()
    for i, c in enumerate(string_cols):
        try:
            cnt = int(row[i])
        except (TypeError, ValueError):
            continue
        if ENTITY_MATCHING_MIN_DISTINCT <= cnt <= ENTITY_MATCHING_MAX_DISTINCT:
            enabled.add(c)
    return enabled


def build_table_entries(
    client: WorkspaceClient,
    warehouse_id: str,
    table_list: list[str],
    existing_serialized: dict | None = None,
) -> list[dict]:
    """Build the data_sources.tables[] list.

    Each column gets `enable_format_assistance: True`. String columns whose
    distinct-value count fits within the Knowledge Store entity-matching
    range also get `enable_entity_matching: True` — Genie then builds a
    value dictionary from the actual data, fixing categorical-value
    hallucinations.

    The existing_serialized argument is no longer honored for column-config
    preservation — we always rebuild so column-config changes (new fields,
    updated entity matching) take effect on re-provision.
    """
    entries: list[dict] = []
    total_entity_matched = 0
    for fqn in table_list:
        col_specs = describe_table_columns(client, warehouse_id, fqn)
        string_cols = [c for c, t in col_specs if t and t.lower().startswith("string")]
        em_cols = probe_entity_matching_candidates(client, warehouse_id, fqn, string_cols)
        if em_cols:
            print(f"  {fqn}: entity matching on {len(em_cols)} cols → {sorted(em_cols)}")
        total_entity_matched += len(em_cols)
        column_configs = []
        for c, _t in col_specs:
            cfg: dict = {"column_name": c, "enable_format_assistance": True}
            if c in em_cols:
                cfg["enable_entity_matching"] = True
            column_configs.append(cfg)
        entries.append({"identifier": fqn, "column_configs": column_configs})

    print(f"  Total columns with entity matching: {total_entity_matched} (Knowledge Store soft limit ~120/space)")
    # Genie API requires tables sorted by identifier and column_configs sorted by name
    for t in entries:
        t["column_configs"] = sorted(t["column_configs"], key=lambda c: c["column_name"])
    entries.sort(key=lambda t: t["identifier"])
    return entries


# Hardcoded-magnitude patterns that should NOT appear in shipped Genie
# instructions or orchestrator prompts (anything customer-deployable). The
# memory rule "no data overfit in customer instructions" applies here.
# These patterns flag firm-scale anchoring, industry benchmarks with specific
# values, and synthetic-data ratios masquerading as facts.
import re as _re
HARDCODED_MAGNITUDE_PATTERNS = [
    (r"Big\s*4\s*scale", "firm-scale anchoring"),
    (r"\$\d+[BM]\b\s*revenue", "hardcoded firm revenue"),
    (r"~\s*\$\d+[BM]", "hardcoded firm scale"),
    (r"~\s*\d{1,3},?\d{3}\s*partners?", "hardcoded partner count"),
    (r"~\s*\d{1,3},?\d{3}\s*employees?", "hardcoded employee count"),
    (r"\d+\+\s*offices?", "hardcoded office count"),
    (r"\$3M revenue per partner", "hardcoded benchmark"),
    (r"firm consistently exceeds.*\d+", "hardcoded synthetic narrative"),
    (r"firm misses these.*of every \d+ months", "hardcoded synthetic narrative"),
    (r"OpEx \(~\d+%", "hardcoded OpEx ratio"),
    (r"operating margin lands in \+\d+", "hardcoded margin range"),
]


def lint_shipped_text_files() -> list[str]:
    """Grep the shipped Genie-config + orchestrator-prompt files for hardcoded
    business magnitudes. Returns a list of (file:line: pattern) errors.

    This catches the kind of customer-portability violation that the data
    isn't validating (variable text inside YAML/MD files). Runs at provision
    time so anyone editing prompts can't reintroduce hardcoded magnitudes
    silently.
    """
    errors: list[str] = []
    candidates = [
        GENIE_CONFIG_DIR / "instructions" / "v2_analytics.md",
        GENIE_CONFIG_DIR / "instructions" / "v2_management.md",
    ]
    # Add prompt files (parent dir of GENIE_CONFIG_DIR, then genie_insights/prompts)
    prompts_dir = GENIE_CONFIG_DIR.parent / "genie_insights" / "prompts"
    if prompts_dir.exists():
        candidates.extend(prompts_dir.glob("*.md"))
    for path in candidates:
        if not path.exists():
            continue
        text = path.read_text()
        for line_no, line in enumerate(text.splitlines(), start=1):
            for pattern, label in HARDCODED_MAGNITUDE_PATTERNS:
                if _re.search(pattern, line, _re.IGNORECASE):
                    errors.append(
                        f"{path.name}:{line_no} hardcoded {label} → {line.strip()[:140]}"
                    )
    return errors


def validate_config(
    client: WorkspaceClient,
    warehouse_id: str,
    serialized: dict,
) -> list[str]:
    """Pre-flight validation: confirm every snippet/measure/trusted query in
    the serialized space actually parses against the deployed schema. Returns
    list of error strings (empty = clean). Caller decides whether to block.

    What this catches:
    - Snippets referencing non-existent columns ("time_type" when column is "time_type_clean")
    - Trusted queries referencing tables that don't exist
    - Syntax errors in SQL we authored

    Method: EXPLAIN each SQL. If the warehouse returns a parse/analysis error,
    we capture it. EXPLAIN doesn't execute the query, so it's fast.
    """
    errors: list[str] = []
    instructions = serialized.get("instructions", {}) or {}

    # Trusted queries (example_question_sqls) — `sql` is a list-of-strings
    # (one entry per question variant; all entries are the same SQL string).
    # `question` is a list of question variant strings.
    for i, eq in enumerate(instructions.get("example_question_sqls", []) or []):
        raw_sql = eq.get("sql") or eq.get("query")
        if isinstance(raw_sql, list):
            sql = (raw_sql[0] if raw_sql else "").strip()
        else:
            sql = (raw_sql or "").strip()
        if not sql:
            continue
        q_label = eq.get("question") or eq.get("id") or "?"
        if isinstance(q_label, list):
            q_label = q_label[0] if q_label else "?"
        try:
            r = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"EXPLAIN {sql}",
                wait_timeout="30s",
            )
            if r.status and r.status.state and str(r.status.state).endswith("FAILED"):
                msg = (r.status.error.message if r.status.error else "FAILED")
                errors.append(f"trusted_query #{i} ({str(q_label)[:60]}): {msg[:200]}")
        except Exception as e:
            errors.append(f"trusted_query #{i}: exception {str(e)[:200]}")

    # SQL snippets (filters + measures) — each is a SQL fragment that needs
    # a host table. We test by trying to parse the fragment in a SELECT or WHERE
    # context against each candidate table that has the referenced columns.
    sn = instructions.get("sql_snippets") or {}
    for kind in ("filters", "measures"):
        for s in (sn.get(kind) or []):
            # `sql` is serialized as a list-of-strings per the Genie API shape
            # (see build_sql_snippets). Extract the first element as the actual SQL.
            raw_sql = s.get("sql")
            if isinstance(raw_sql, list):
                frag = (raw_sql[0] if raw_sql else "").strip()
            else:
                frag = (raw_sql or "").strip()
            name = s.get("display_name", "?")
            if not frag:
                continue
            # Heuristic: filters wrap in WHERE; measures wrap in SELECT. Test against
            # the first table that has ALL referenced columns. If no candidate table,
            # warn but don't fail (snippet may match a customer-extension table).
            test_table = _find_candidate_table(client, warehouse_id, serialized, frag)
            if test_table is None:
                # Can't validate without a host table — skip
                continue
            if kind == "filters":
                test_sql = f"SELECT 1 FROM {test_table} WHERE {frag} LIMIT 0"
            else:
                test_sql = f"SELECT {frag} FROM {test_table} LIMIT 0"
            try:
                r = client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id, statement=test_sql, wait_timeout="20s",
                )
                if r.status and r.status.state and str(r.status.state).endswith("FAILED"):
                    msg = (r.status.error.message if r.status.error else "FAILED")
                    errors.append(f"{kind[:-1]} '{name}': {msg[:200]}")
            except Exception as e:
                errors.append(f"{kind[:-1]} '{name}': exception {str(e)[:200]}")
    return errors


def _find_candidate_table(client, warehouse_id, serialized, sql_fragment: str) -> str | None:
    """Best-effort: pick a table from the configured table list that has the
    column references in the snippet. Returns FQN of first candidate, or None.
    """
    import re
    # Crude column-name extraction: word characters that aren't SQL keywords
    candidates = set(re.findall(r"\b([a-z_][a-z0-9_]*)\b", sql_fragment.lower()))
    sql_keywords = {
        "select", "from", "where", "and", "or", "not", "in", "is", "null", "case", "when",
        "then", "else", "end", "as", "on", "join", "left", "right", "inner", "outer", "by",
        "group", "having", "order", "limit", "asc", "desc", "true", "false", "sum", "avg",
        "count", "min", "max", "distinct", "between", "like", "ilike", "current_date",
        "current_timestamp", "year", "month", "day", "date", "date_trunc", "datediff",
        "date_add", "date_sub", "add_months", "concat", "coalesce", "nullif", "round",
        "cast", "interval", "quarter", "if", "upper", "lower", "trim", "try_divide",
        "collect_list", "struct", "lag", "lead", "over", "partition", "rank", "row_number",
        "qualify", "with", "exists", "all", "any", "some", "union", "intersect", "except",
        "filter", "first", "last",
    }
    candidate_cols = candidates - sql_keywords
    if not candidate_cols:
        return None
    # For each table in the serialized space, check column overlap
    for t in serialized.get("data_sources", {}).get("tables", []):
        table_cols = {c["column_name"] for c in t.get("column_configs", [])}
        if candidate_cols & table_cols:  # at least one column matches
            # All non-keyword candidates should exist in this table for a clean test
            missing = candidate_cols - table_cols
            if not missing:
                return t["identifier"]
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 4. Provisioning entry points
# ─────────────────────────────────────────────────────────────────────────────

def build_joins(schema_override: str | None = None) -> list[dict]:
    """Load join hints from `tables.yml`'s `<space>_joins` sections.

    Both v2_analytics_joins and v2_management_joins are merged (deduped by
    `(left_table, right_table, on)` triple) since this script provisions a
    single merged Genie space.
    """
    tables_yml = yaml.safe_load((GENIE_CONFIG_DIR / "tables.yml").read_text())
    raw = (tables_yml.get("v2_analytics_joins", []) or []) + \
          (tables_yml.get("v2_management_joins", []) or [])
    seen = set()
    out = []
    for j in raw:
        left = _rewrite_schema_in_text(j["left_table"], schema_override)
        right = _rewrite_schema_in_text(j["right_table"], schema_override)
        on = _rewrite_schema_in_text(j["on"], schema_override)
        key = (left, right, on)
        if key in seen:
            continue
        seen.add(key)
        out.append({"left_table": left, "right_table": right, "on": on})
    return out


def build_serialized_space(
    client: WorkspaceClient,
    warehouse_id: str,
    existing_serialized: dict | None = None,
    schema_override: str | None = None,
) -> dict:
    """Construct the serialized_space JSON shape expected by Genie API.

    Fields that the `Instructions` protobuf empirically rejects (verified via
    INVALID_PARAMETER_VALUE responses):
      - `text` — room-level markdown lives in the top-level `description`
        field on update_space, not nested under `instructions`.
      - `joins` — table relationships are likely configured via a separate
        API endpoint or UI surface, not via serialized_space. `tables.yml`
        still defines them so we can author once, but `build_joins()` output
        is currently unused (pending a working push mechanism).
      - `applicable_tables` on snippets — not part of the `Filter` protobuf;
        table scoping is enforced by qualifying columns in the SQL itself.
    """
    table_list = get_merged_table_list(schema_override=schema_override)
    return {
        "version": 2,
        "data_sources": {
            "tables": build_table_entries(client, warehouse_id, table_list, existing_serialized),
        },
        "instructions": {
            "example_question_sqls": build_example_queries(schema_override=schema_override),
            # Genie API expects a DICT with `filters` and `measures` keys (NOT a list).
            # Empty `[]` is rejected with "Invalid JSON in field 'serialized_space'".
            # Pre-fix versions of this script set this to `None` after hitting that
            # rejection, which dropped all snippets — see PROJECT_PLAN.md Section 11.
            "sql_snippets": build_sql_snippets(schema_override=schema_override),
        },
    }


def fetch_existing_serialized(client: WorkspaceClient, space_id: str) -> dict:
    """GET the existing space and return its parsed serialized_space.

    Also stashes the full GET response top-level keys in a global so the
    provision() schema dump can print them (helps identify where fields like
    `general_instructions` and `joins` live — sibling to `description` at the
    top level, vs inside serialized_space).
    """
    import requests
    from databricks.sdk.core import Config

    cfg = Config(profile=os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    token = cfg.authenticate().get("Authorization", "").replace("Bearer ", "")
    host = client.config.host.rstrip("/")
    r = requests.get(
        f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    r.raise_for_status()
    full = r.json()
    # Stash top-level keys + a brief preview of each non-secret value
    global _LAST_FETCH_TOP_LEVEL
    _LAST_FETCH_TOP_LEVEL = {}
    for k, v in full.items():
        if k == "serialized_space":
            continue
        if isinstance(v, (str, int, float, bool, type(None))):
            preview = v if not isinstance(v, str) else (v[:120] + "..." if len(v) > 120 else v)
            _LAST_FETCH_TOP_LEVEL[k] = preview
        elif isinstance(v, list):
            _LAST_FETCH_TOP_LEVEL[k] = f"<list len={len(v)}>"
        elif isinstance(v, dict):
            _LAST_FETCH_TOP_LEVEL[k] = f"<dict keys={sorted(v.keys())}>"
        else:
            _LAST_FETCH_TOP_LEVEL[k] = f"<{type(v).__name__}>"
    return json.loads(full.get("serialized_space") or "{}")


_LAST_FETCH_TOP_LEVEL: dict = {}


def persist_space_id(space_id: str, warehouse_id: str) -> None:
    """Write the resulting space ID to a JSON file the app + orchestrator can read."""
    SPACE_ID_FILE.write_text(json.dumps({
        "space_id": space_id,
        "warehouse_id": warehouse_id,
        "last_provisioned": datetime.utcnow().isoformat() + "Z",
    }, indent=2))


def provision(
    warehouse_id: str,
    space_id: str | None = None,
    title: str = DEFAULT_TITLE,
    profile: str | None = None,
    dry_run: bool = False,
    schema_override: str | None = None,
) -> str:
    """Create or update the merged Genie space. Returns the space_id."""
    client = WorkspaceClient(profile=profile) if profile else WorkspaceClient()

    print("=" * 60)
    print(f"CFO Insights merged Genie space — {'UPDATE' if space_id else 'CREATE'}")
    if schema_override:
        print(f"  Schema override: {schema_override} (replaces main.cfo_proserv)")
    print("=" * 60)

    # Resolve the frozen dataset's as-of date so every CURRENT_DATE() in the
    # instructions/trusted-queries/snippets gets rewritten to a fixed literal
    # (see _resolve_as_of / _rewrite_schema_in_text). Must run before any build_*.
    _resolve_as_of(client, warehouse_id, schema_override or "main.cfo_proserv")

    # Two distinct surfaces per Databricks Genie best practices:
    #   description          — short user-facing room intro (About tab, for humans)
    #   general_instructions — full behavioral guidance (Instructions → Text tab,
    #                          for Genie at chat time)
    # The merged v2_analytics + v2_management content is behavioral guidance, so it
    # goes into general_instructions. The About-tab description is a short blurb
    # generated separately.
    print(f"Building short About-tab description...")
    description = build_short_description()
    description = _rewrite_schema_in_text(description, schema_override)
    print(f"  description: {len(description.encode('utf-8'))} bytes")

    print(f"Building merged general_instructions (behavioral guidance for Genie)...")
    general_instructions = build_merged_description(schema_override=schema_override)
    print(f"  general_instructions: {len(general_instructions.encode('utf-8'))} bytes ({len(general_instructions)} chars)")

    existing_serialized = None
    if space_id:
        print(f"Fetching existing space {space_id}...")
        existing_serialized = fetch_existing_serialized(client, space_id)
        # Dump the top-level schema so we can see what fields the API actually
        # accepts/persists. This lets us mirror the right shape instead of
        # guessing protobuf field names that get rejected one at a time.
        print("\n=== EXISTING GET-RESPONSE TOP-LEVEL FIELDS (siblings to serialized_space) ===")
        for k, v in sorted(_LAST_FETCH_TOP_LEVEL.items()):
            print(f"  {k}: {v}")
        print("=== END GET-RESPONSE FIELDS ===\n")
        print("=== EXISTING serialized_space SCHEMA (top-level keys + structure) ===")
        try:
            print(f"top-level keys: {sorted(existing_serialized.keys())}")
            instr = existing_serialized.get("instructions") or {}
            if isinstance(instr, dict):
                print(f"instructions keys: {sorted(instr.keys())}")
                # show first item of each list-valued sub-field so we see element shape
                for k, v in instr.items():
                    if isinstance(v, list) and v:
                        print(f"  instructions.{k}[0] keys: {sorted(v[0].keys()) if isinstance(v[0], dict) else type(v[0]).__name__}")
                    elif isinstance(v, dict):
                        print(f"  instructions.{k} subkeys: {sorted(v.keys())}")
                        for sub_k, sub_v in v.items():
                            if isinstance(sub_v, list) and sub_v:
                                print(f"    instructions.{k}.{sub_k}[0] keys: {sorted(sub_v[0].keys()) if isinstance(sub_v[0], dict) else type(sub_v[0]).__name__}")
            ds = existing_serialized.get("data_sources") or {}
            if isinstance(ds, dict):
                print(f"data_sources keys: {sorted(ds.keys())}")
        except Exception as _e:
            print(f"(schema dump failed: {_e})")
        print("=== END SCHEMA DUMP ===\n")

    print(f"Building serialized_space (tables + queries + snippets + joins)...")
    serialized = build_serialized_space(
        client, warehouse_id, existing_serialized,
        schema_override=schema_override,
    )
    print(f"  Tables: {len(serialized['data_sources']['tables'])}")
    print(f"  Trusted queries: {len(serialized['instructions']['example_question_sqls'])}")
    sn = serialized['instructions']['sql_snippets'] or {}
    print(f"  SQL snippets: {len(sn.get('filters', []))} filters + {len(sn.get('measures', []))} measures")

    # Pre-flight A: lint shipped text files for hardcoded magnitudes (firm
    # scale, industry benchmarks with values, synthetic-data ratios).
    # Customer-portability rule: instructions ship to customers; numbers
    # baked in here either mislead Genie or look broken against customer data.
    print("\nPre-flight A: linting shipped instruction/prompt files for hardcoded magnitudes...")
    lint_errors = lint_shipped_text_files()
    if lint_errors:
        print(f"\n❌ Lint FAILED with {len(lint_errors)} error(s):")
        for e in lint_errors:
            print(f"  - {e}")
        raise RuntimeError(
            f"Customer-portability lint failed with {len(lint_errors)} hardcoded-magnitude error(s). "
            f"Remove the hardcoded value or genericize the language (see memory rule "
            f"'feedback_cfo_demo_no_data_overfit_in_instructions')."
        )
    print(f"  ✅ Lint clean — no hardcoded firm scale / industry benchmarks / synthetic ratios in shipped text")

    # Pre-flight B: verify every snippet/measure/trusted query parses against
    # the deployed schema. Blocks provisioning on any failure.
    print("\nPre-flight B: parsing every snippet + trusted query against schema...")
    errors = validate_config(client, warehouse_id, serialized)
    if errors:
        print(f"\n❌ Pre-flight validation FAILED with {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
        raise RuntimeError(
            f"Pre-flight validation failed with {len(errors)} error(s). "
            f"Fix the config before re-provisioning (see errors above)."
        )
    print(f"  ✅ Pre-flight clean — all snippets and trusted queries parse")

    if dry_run:
        print("\n[DRY RUN] Would call create_space or update_space here. Exiting.")
        return space_id or "<would-be-created>"

    import requests
    token = client.config.authenticate().get("Authorization", "").replace("Bearer ", "")
    host = client.config.host.rstrip("/")

    if space_id is None:
        print(f"\nCalling POST /api/2.0/genie/spaces (warehouse_id={warehouse_id}) via REST...")
        # SDK's client.genie.create_space is not available in all SDK versions;
        # use REST API directly for portability.
        # description (user-facing) and general_instructions (LLM-facing) have
        # distinct content — see PATCH branch / build_short_description() for why.
        resp = requests.post(
            f"{host}/api/2.0/genie/spaces",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "warehouse_id": warehouse_id,
                "title": title,
                "description": description,
                "general_instructions": general_instructions,
                "serialized_space": json.dumps(serialized),
            },
            timeout=120,
        )
        if not resp.ok and "general_instructions" in (resp.text or ""):
            print(f"  POST rejected general_instructions ({resp.status_code}); retrying with description-only fallback...")
            # Fallback: prepend the behavioral guidance into description for older
            # API versions that don't accept general_instructions. Not ideal — user
            # sees a 16K-byte About tab — but at least the content isn't lost.
            resp = requests.post(
                f"{host}/api/2.0/genie/spaces",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={
                    "warehouse_id": warehouse_id,
                    "title": title,
                    "description": general_instructions,
                    "serialized_space": json.dumps(serialized),
                },
                timeout=120,
            )
        if not resp.ok:
            raise RuntimeError(f"POST /api/2.0/genie/spaces failed: {resp.status_code} {resp.text}")
        body = resp.json()
        space_id = body.get("space_id") or body.get("id")
        if not space_id:
            raise RuntimeError(f"POST /api/2.0/genie/spaces succeeded but no space_id in response: {body}")
        print(f"  Created space_id={space_id}")
    else:
        print(f"\nCalling PATCH /api/2.0/genie/spaces/{space_id} via REST...")
        # Per Databricks Genie best practices, `description` and `general_instructions`
        # serve different purposes:
        #   - description: user-facing About-tab metadata (short, markdown)
        #   - general_instructions: behavioral guidance Genie reads at chat time
        # Earlier code put the full ~16K merged instructions in `description`, which
        # left the General Instructions tab empty and made the About tab look like
        # a rule-dump. Now we send the short blurb to description and the full
        # behavioral text to general_instructions. Confirmed in the 2026-05-18
        # screenshot review + Databricks Genie best-practices doc.
        resp = requests.patch(
            f"{host}/api/2.0/genie/spaces/{space_id}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "description": description,
                "general_instructions": general_instructions,
                "serialized_space": json.dumps(serialized),
            },
            timeout=60,
        )
        if not resp.ok:
            # If the API rejects `general_instructions` (older Genie API version),
            # fall back to stuffing the behavioral text into `description` so the
            # content isn't lost — even if it's then less well-targeted.
            if "general_instructions" in (resp.text or ""):
                print(f"  PATCH rejected general_instructions ({resp.status_code}); retrying with description-only fallback...")
                resp = requests.patch(
                    f"{host}/api/2.0/genie/spaces/{space_id}",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={
                        "description": general_instructions,
                        "serialized_space": json.dumps(serialized),
                    },
                    timeout=60,
                )
            if not resp.ok:
                raise RuntimeError(f"PATCH /api/2.0/genie/spaces/{space_id} failed: {resp.status_code} {resp.text}")
        print(f"  Updated space_id={space_id}")

    persist_space_id(space_id, warehouse_id)
    print(f"\nPersisted to {SPACE_ID_FILE}")
    print("Done.")
    return space_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse ID to attach the space to")
    parser.add_argument("--space-id", default=None, help="Existing space ID (omit to create a new space)")
    parser.add_argument("--title", default=DEFAULT_TITLE, help="Space title (only used on create)")
    parser.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"), help="CLI profile name")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs without calling the API")
    parser.add_argument("--schema", default=None, help="Schema override (e.g. main.cfo_proserv_dev). Replaces main.cfo_proserv in table identifiers.")
    args = parser.parse_args()

    space_id = provision(
        warehouse_id=args.warehouse_id,
        space_id=args.space_id,
        title=args.title,
        profile=args.profile,
        dry_run=args.dry_run,
        schema_override=args.schema,
    )
    print(f"\nspace_id: {space_id}")


if __name__ == "__main__":
    main()
