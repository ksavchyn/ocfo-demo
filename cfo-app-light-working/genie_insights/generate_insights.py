# Databricks notebook source
# DBTITLE 1,CFO Insights Orchestrator
# MAGIC %md
# MAGIC # CFO Insights Orchestrator
# MAGIC
# MAGIC Populates `gold_persona_insights` with firmwide insight tiles, action areas, and
# MAGIC bottom-chip questions for each persona (admin / finance / hr). The notebook is
# MAGIC invoked as the `generate_insights` task in the bundle's `cfo_data_pipeline` job.
# MAGIC
# MAGIC For each persona at firmwide scope:
# MAGIC   1. Runs canonical SQL queries directly against the warehouse to pull KPI values
# MAGIC   2. Builds a focused Haiku prompt with the pre-computed values
# MAGIC   3. Calls `databricks-claude-haiku-4-5` to compose 4 insights + 3 action areas +
# MAGIC      3 bottom chips as one JSON document
# MAGIC   4. For each chip: Opus decomposes into 3 sub-queries → fires them at Genie in
# MAGIC      parallel → caches the result as `cached_agent_payload`. Two depth-1 follow-up
# MAGIC      questions per chip are also pre-cached with the same depth.
# MAGIC   5. DELETE+INSERT the rows for that persona at `filter_axis='firmwide'`,
# MAGIC      `filter_value='all'`.
# MAGIC
# MAGIC Non-firmwide rows (region / location / practice_area / industry / customer slices)
# MAGIC are computed lazily by the app on first visit and written-through to the same
# MAGIC table — they're not pre-computed here.

# COMMAND ----------

# DBTITLE 1,Configuration
import os
import sys

# Widget declarations — REQUIRED so the bundle's notebook_task base_parameters flow
# through. Without these calls, dbutils.widgets.get raises and we fall through to
# env vars / source defaults, which broke customer-deploy portability.
# Defaults below are the dev-environment fallback for ad-hoc / interactive runs.
_WIDGET_DEFAULTS = {
    "CFO_CATALOG": "main",
    "CFO_SCHEMA_NAME": "cfo_proserv",
    "CFO_WAREHOUSE_ID": "",
    "CFO_GENIE_SPACE_ID": "",
    # Used by _resolve_genie_space_id() as fallback when the task-value
    # reference for CFO_GENIE_SPACE_ID fails to populate. Defaults to "CFO Demo"
    # matching the bundle's genie_space_title default.
    "CFO_GENIE_SPACE_TITLE": "CFO Demo",
    # Bundle resolves ${workspace.host}/serving-endpoints/databricks-claude-opus-4-7
    # at deploy time and passes it via base_parameters. Empty default forces the
    # bundle parameterization path so notebooks never accidentally hit Kateryna's host.
    "CFO_CLAUDE_GATEWAY_URL": "",
    "CFO_CLAUDE_MODEL": "databricks-claude-opus-4-7",
    "CFO_CLAUDE_MODEL_COMPOSE": "databricks-claude-haiku-4-5",
    "CFO_CLAUDE_MODEL_AGENT": "databricks-claude-opus-4-7",
    "CFO_PERSONAS": "",
    "CFO_PAGES": "",
    "CFO_GENIE_INTER_CALL_DELAY_SEC": "2.0",
    "CFO_GENIE_MAX_RETRIES": "3",
    "CFO_GENIE_POLL_TIMEOUT_SEC": "120",
    "CFO_DEPTH1_FOLLOWUP_COUNT": "2",
    "CFO_SKIP_PERSONAS": "false",
    "CFO_SKIP_PAGES": "false",
}
try:
    for _name, _default in _WIDGET_DEFAULTS.items():
        dbutils.widgets.text(_name, _default)  # noqa: F821
    _WIDGETS_AVAILABLE = True
except Exception:
    _WIDGETS_AVAILABLE = False  # local / non-notebook execution


def _config(name: str, default: str) -> str:
    """Read config in priority order: notebook widget → env var → default.

    The bundle passes config via `base_parameters` (widgets) on the notebook
    task — that's how customer environments override defaults at deploy time.
    Local / interactive runs typically use env vars. Hardcoded default is
    last-resort for pure-source execution.
    """
    if _WIDGETS_AVAILABLE:
        try:
            v = dbutils.widgets.get(name)  # noqa: F821
            if v:
                return v
        except Exception:
            pass
    return os.environ.get(name, default)


# Defaults target the dev environment. Production cutover flips these via the
# bundle's notebook_task.base_parameters (widget values).
CATALOG = _config("CFO_CATALOG", "main")
SCHEMA = _config("CFO_SCHEMA_NAME", "cfo_proserv")
SCHEMA_FQN = f"{CATALOG}.{SCHEMA}"
WAREHOUSE_ID = _config("CFO_WAREHOUSE_ID", "").strip()

# Push widget-resolved values into os.environ BEFORE any module that resolves
# schema at import time (insights_compose, insights_queries) gets imported.
# Without this, those modules read os.environ directly, find nothing, and
# fall back to the hardcoded `cfo_proserv` default — so test deploys silently
# read prod data while writing to the test schema's gold_persona_insights.
os.environ["CFO_CATALOG"] = CATALOG
os.environ["CFO_SCHEMA_NAME"] = SCHEMA
os.environ["CFO_SCHEMA"] = SCHEMA_FQN
if WAREHOUSE_ID:
    os.environ["CFO_WAREHOUSE_ID"] = WAREHOUSE_ID
GENIE_SPACE_ID = _config("CFO_GENIE_SPACE_ID", "").strip()
GENIE_SPACE_TITLE = _config("CFO_GENIE_SPACE_TITLE", "CFO Demo").strip()

# Defensive runtime title-lookup. The bundle wires
# CFO_GENIE_SPACE_ID via {{tasks.provision_genie_space.values.space_id}} so
# on first-time deploys generate_insights gets the just-provisioned space.
# But if someone runs this notebook manually outside the bundle, or that task
# value reference fails to resolve (older Databricks runtime / different
# orchestration), fall back to title lookup so chip pre-caching still works.
# Without this guard, the chips silently fall through the "skip pre-cache"
# branch and the customer's app loads chips in 30-60s instead of instantly.
# This is the same bug that hit the 2026-05-16 first-time prod cutover.
def _resolve_genie_space_id() -> str:
    global GENIE_SPACE_ID
    if GENIE_SPACE_ID:
        return GENIE_SPACE_ID
    if not GENIE_SPACE_TITLE:
        return ""
    print(
        f"  CFO_GENIE_SPACE_ID empty — looking up by title "
        f"{GENIE_SPACE_TITLE!r} as fallback..."
    )
    try:
        from databricks.sdk import WorkspaceClient as _WC2
        _w = _WC2()
        for s in _w.genie.list_spaces():
            if (s.title or "").strip() == GENIE_SPACE_TITLE.strip():
                print(f"  ✅ found existing space: {s.space_id}")
                GENIE_SPACE_ID = s.space_id
                return GENIE_SPACE_ID
        print(f"  ⚠️  no space matched title {GENIE_SPACE_TITLE!r}; chip pre-caching will be skipped")
    except Exception as e:
        print(f"  ⚠️  title-lookup fallback failed: {type(e).__name__}: {e}")
    return GENIE_SPACE_ID


GENIE_SPACE_ID = _resolve_genie_space_id()

# GENIE_SPACE_ID can be empty here — the orchestrator's firmwide insight + action
# composition uses direct SQL and doesn't need Genie. Chip pre-caching (the Opus
# decompose + parallel Genie sub-queries) skips itself if GENIE_SPACE_ID is empty,
# and clicks fall back to live agent generation in the app. This is the safe degraded
# mode if Genie provisioning hasn't happened yet at the time this notebook runs.

# Claude — AI Gateway URL pattern. Empty default forces explicit configuration
# from bundle's notebook_task.base_parameters (which resolves
# ${workspace.host}/ai-gateway/mlflow/v1 at deploy time per the customer's workspace).
CLAUDE_GATEWAY_URL = _config("CFO_CLAUDE_GATEWAY_URL", "").strip()
# Two failure modes to repair:
#   (1) Empty — the bundle didn't pass anything in. Auto-resolve via SDK.
#   (2) Relative path like "/ai-gateway/mlflow/v1" — the bundle's notebook_task
#       base_parameters silently stripped the leading ${workspace.host} prefix
#       (same Databricks bundle quirk that broke apps.config.env in app.yml).
#       Detect missing scheme and prepend the workspace host.
if not CLAUDE_GATEWAY_URL or not CLAUDE_GATEWAY_URL.startswith(("http://", "https://")):
    try:
        from databricks.sdk import WorkspaceClient as _WC
        _host = _WC().config.host.rstrip("/")
        if CLAUDE_GATEWAY_URL.startswith("/"):
            # Relative path → prepend host
            CLAUDE_GATEWAY_URL = f"{_host}{CLAUDE_GATEWAY_URL}"
        else:
            # Empty → assume workspace's auto-provisioned AI Gateway
            CLAUDE_GATEWAY_URL = f"{_host}/ai-gateway/mlflow/v1"
    except Exception as _e:
        raise ValueError(
            f"CFO_CLAUDE_GATEWAY_URL is invalid ({CLAUDE_GATEWAY_URL!r}) and could "
            "not auto-resolve via SDK. Bundle's notebook_task.base_parameters should "
            "populate this from ${var.claude_endpoint_url}."
        ) from _e
# Orchestrator composition uses the COMPOSE model. Lookup priority:
#   CFO_CLAUDE_MODEL_COMPOSE (preferred — explicitly the compose-tier model)
#   → CFO_CLAUDE_MODEL (fallback single-model var for simple deploys)
#   → hardcoded default databricks-claude-haiku-4-5
# Customers who only override CFO_CLAUDE_MODEL still get a working deploy;
# customers who want different models per task (compose vs agent) set
# CFO_CLAUDE_MODEL_COMPOSE explicitly.
_fallback_claude_model = _config("CFO_CLAUDE_MODEL", "").strip()
CLAUDE_MODEL = _config(
    "CFO_CLAUDE_MODEL_COMPOSE",
    _fallback_claude_model or "databricks-claude-haiku-4-5",
)
# Agent-tier model (Opus by default) for chip decomposition. Decomposition is a
# reasoning task (planning supporting sub-queries) — quality matters more than
# speed since it runs once per chip per orchestrator cycle.
CLAUDE_MODEL_AGENT = _config(
    "CFO_CLAUDE_MODEL_AGENT",
    _fallback_claude_model or "databricks-claude-opus-4-7",
)

# Politeness / rate-limit tuning for Genie calls (used during chip pre-caching).
GENIE_INTER_CALL_DELAY_SEC = float(os.environ.get("CFO_GENIE_INTER_CALL_DELAY_SEC", "2.0"))
GENIE_MAX_RETRIES = int(os.environ.get("CFO_GENIE_MAX_RETRIES", "3"))
GENIE_POLL_TIMEOUT_SEC = int(os.environ.get("CFO_GENIE_POLL_TIMEOUT_SEC", "120"))

# Personas to run this cycle, keyed by ROLE (not display name) so the table is
# customer-deploy-clean: every customer's environment has finance/admin/hr roles
# regardless of who fills them. Override via CFO_PERSONAS env var (comma-separated).
# Example: CFO_PERSONAS=finance → runs only finance role (dev / single-persona testing).
_personas_env = _config("CFO_PERSONAS", "").strip()
PERSONAS = [p.strip() for p in _personas_env.split(",") if p.strip()] or ["admin", "finance", "hr"]

# Pages with their own bottom chip sets (page-scoped, NOT persona-scoped).
_pages_env = _config("CFO_PAGES", "").strip()
PAGES = [p.strip() for p in _pages_env.split(",") if p.strip()] or ["finance", "admin"]
SHARED_PERSONA_KEY = "_shared"  # sentinel for page-scoped rows in gold_persona_insights

# Path to persona prompts — relative to notebook.
PROMPTS_DIR = "./prompts"

print(f"Widgets available:    {_WIDGETS_AVAILABLE}")
print(f"Target table:         {SCHEMA_FQN}.gold_persona_insights")
print(f"Warehouse ID:         {WAREHOUSE_ID or '(unset — falls back to spark.sql)'}")
print(f"Claude model:         {CLAUDE_MODEL}")
print(f"Claude URL:           {CLAUDE_GATEWAY_URL}")
print(f"Personas:             {PERSONAS}")

# COMMAND ----------

# DBTITLE 1,Imports & clients
import json
import time
import re
import sys as _sys
from datetime import datetime, date
from pathlib import Path

import requests
from databricks.sdk import WorkspaceClient

# Add the project root to sys.path so we can `import insights_compose` from
# either the notebook or local CLI execution. We try several strategies because
# Databricks notebook task contexts don't always populate `__file__` reliably,
# and the working directory is usually /databricks/driver, not the notebook's
# parent — so a naive Path(".").resolve() fallback would put the wrong dir on
# the path and the import below would fail with ModuleNotFoundError.
def _find_project_root() -> Path:
    # Strategy 1: __file__ if defined (most local CLI runs + newer DBRs)
    if "__file__" in globals():
        return Path(__file__).resolve().parent.parent  # noqa: F821

    # Strategy 2: Databricks notebook REPL context (DBR 13.3+)
    try:
        from dbruntime.databricks_repl_context import get_context  # type: ignore
        ctx = get_context()
        nb_path = getattr(ctx, "notebookPath", None) if ctx else None
        if nb_path:
            return Path("/Workspace" + nb_path).resolve().parent.parent
    except Exception:
        pass

    # Strategy 3: dbutils notebook-context API (older DBRs)
    try:
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # type: ignore  # noqa: F821
        if nb_path:
            return Path("/Workspace" + nb_path).resolve().parent.parent
    except Exception:
        pass

    # Strategy 4: walk up from CWD looking for the sibling insights_compose.py
    cwd = Path(".").resolve()
    for parent in [cwd] + list(cwd.parents):
        if (parent / "insights_compose.py").exists():
            return parent

    # Final fallback — CWD (will fail loudly on the import below if wrong)
    return Path(".").resolve()


_PROJECT_ROOT = _find_project_root()
print(f"[generate_insights] _PROJECT_ROOT resolved to: {_PROJECT_ROOT}")
if str(_PROJECT_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_PROJECT_ROOT))

# Shared SQL+Haiku composition module — pulled out so the live-filter Flask
# endpoint can call the SAME pull/prompt/Haiku pipeline. Notebook still owns
# the firmwide DELETE+INSERT writer.
from insights_compose import (  # noqa: E402
    pull_admin_data as _pull_admin_data,
    pull_finance_data as _pull_finance_data,
    pull_hr_data as _pull_hr_data,
    build_admin_prompt as _build_admin_prompt,
    build_finance_prompt as _build_finance_prompt,
    build_hr_prompt as _build_hr_prompt,
    call_haiku as _call_haiku,
    parse_json_with_fence_strip as _parse_json_with_fence_strip,
    COMMON_INSTRUCTIONS,  # re-exported for any downstream consumer
)

# Workspace auth — picks up DATABRICKS_HOST + token from environment / cluster context.
# Same client is used for SQL execution AND for Haiku serving-endpoint calls (OAuth M2M).
w = WorkspaceClient()
host = w.config.host.rstrip("/")

print(f"Workspace host: {host}")
print(f"Auth type:      {w.config.auth_type}")

# COMMAND ----------

# DBTITLE 1,Spark detection helper
def _spark_available() -> bool:
    """Used by ensure_table_exists / write_firmwide_rows below — those still
    require a Spark session because they DELETE+INSERT into Delta. The pull
    functions are now imported from insights_compose and run via the SDK
    Statement Execution API (or spark.sql if the executor passed is a Spark
    session — preserved for parity with the previous notebook behavior)."""
    try:
        spark  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# Bridge executor for the imported pull functions. When running inside a
# Databricks notebook, prefer spark.sql (no warehouse round-trip needed).
# Outside a notebook, the WorkspaceClient `w` + SQL_WAREHOUSE_ID path kicks
# in via insights_compose._run_sql.
if _spark_available():
    # insights_compose._is_spark() looks for .sql on the object; the global
    # `spark` matches. We pass it through directly.
    _EXECUTOR = spark  # type: ignore[name-defined]  # noqa: F821
    # Make sure CFO_WAREHOUSE_ID also goes into the env so any pull functions
    # that fall back to API path can find it.
    if WAREHOUSE_ID:
        os.environ.setdefault("SQL_WAREHOUSE_ID", WAREHOUSE_ID)
else:
    _EXECUTOR = w
    if not WAREHOUSE_ID:
        raise RuntimeError(
            "No spark session AND CFO_WAREHOUSE_ID unset — cannot execute SQL."
        )
    os.environ.setdefault("SQL_WAREHOUSE_ID", WAREHOUSE_ID)

# COMMAND ----------

# DBTITLE 1,Canonical SQL pulls — admin / finance / hr (firmwide scope)
# NOTE: The actual SQL pulls now live in `insights_compose.pull_*_data` so the
# live filter-compute Flask endpoint can share them. These thin wrappers
# preserve the old zero-arg signature (firmwide scope, spark/SDK executor
# auto-resolved at notebook import time).

def pull_admin_data() -> dict:
    """Admin (Senior Partner) firmwide KPIs + supporting drill-downs."""
    print("[admin] pulling canonical SQL...")
    return _pull_admin_data(_EXECUTOR, None)


def pull_finance_data() -> dict:
    """Finance (Finance Director) firmwide KPIs + supporting drill-downs."""
    print("[finance] pulling canonical SQL...")
    return _pull_finance_data(_EXECUTOR, None)


def pull_hr_data() -> dict:
    """HR (Talent leader) firmwide KPIs + supporting drill-downs."""
    print("[hr] pulling canonical SQL...")
    return _pull_hr_data(_EXECUTOR, None)


# COMMAND ----------

# DBTITLE 1,Prompt builders (per persona)
# The actual prompt-building bodies live in `insights_compose.build_*_prompt`.
# These shims preserve the single-arg signature for backward compatibility with
# any caller that didn't migrate to the new (d, filters) signature.

def build_admin_prompt(d: dict) -> str:
    return _build_admin_prompt(d, None)


def build_finance_prompt(d: dict) -> str:
    return _build_finance_prompt(d, None)


def build_hr_prompt(d: dict) -> str:
    return _build_hr_prompt(d, None)


# Persona → (puller, prompt builder) — used by run_firmwide_persona below.
PERSONA_PIPELINES = {
    "admin":   (pull_admin_data,   build_admin_prompt),
    "finance": (pull_finance_data, build_finance_prompt),
    "hr":      (pull_hr_data,      build_hr_prompt),
}


# ─── Backup follow-up question pool loader ──────────────────────────────────
# Used by the chip pre-cache loop. When an LLM-generated follow-up produces
# a hollow payload (0 rows / "no data was returned" prose), precache_chip_followups
# substitutes from this pool until one validates. Pool is keyed by:
#   {persona}_backup_followups       e.g. admin_backup_followups
#   {page}_page_backup_followups     e.g. admin_page_backup_followups
# Pool YAML lives next to the Genie config so it ships with the bundle and
# customers can edit it without touching Python.
_BACKUP_QUESTIONS_CACHE: dict[str, list[str]] | None = None


def _load_backup_followups(pool_key: str) -> list[str]:
    """Return the list of backup follow-up questions for a given pool key, or
    an empty list if the YAML is missing or the key isn't present. Cached on
    first read.

    Path resolution: uses `_PROJECT_ROOT` which is computed by
    `_find_project_root()` with 4 fallback strategies (covers local CLI,
    Databricks notebook REPL, dbutils notebook-context, and CWD-walk).
    DON'T add a `__file__`-based fallback candidate here — in notebook
    contexts `__file__` is undefined and Python eagerly evaluates the path
    when the candidates list is built, raising NameError before the loop
    runs. That was the source of the "name '__file__' is not defined"
    error that disabled the backup-followups salvage path on prod.
    """
    global _BACKUP_QUESTIONS_CACHE
    if _BACKUP_QUESTIONS_CACHE is None:
        _BACKUP_QUESTIONS_CACHE = {}
        try:
            import yaml  # type: ignore
            from pathlib import Path as _Path
            # Primary candidate: project root + genie_config/. Additional
            # candidates can be added as needed but MUST be built without
            # referencing `__file__` directly.
            candidates = [
                _Path(_PROJECT_ROOT) / "genie_config" / "backup_questions.yml",
            ]
            # Optional: only add a __file__-based candidate if __file__ is defined,
            # using globals() lookup so Python doesn't fail on the NameError.
            if "__file__" in globals():
                candidates.append(
                    _Path(globals()["__file__"]).resolve().parent.parent / "genie_config" / "backup_questions.yml"
                )
            for path in candidates:
                if path.exists():
                    with open(path, "r") as fh:
                        data = yaml.safe_load(fh) or {}
                    if isinstance(data, dict):
                        _BACKUP_QUESTIONS_CACHE = {
                            k: list(v) for k, v in data.items() if isinstance(v, list)
                        }
                    print(f"[backup_followups] loaded {sum(len(v) for v in _BACKUP_QUESTIONS_CACHE.values())} questions from {path.name}")
                    break
            else:
                print(f"[backup_followups] backup_questions.yml not found in {[str(c) for c in candidates]}; hollow follow-ups will be dropped without substitution")
        except Exception as e:
            print(f"[backup_followups] load failed ({type(e).__name__}: {e}); hollow follow-ups will be dropped")
    return list(_BACKUP_QUESTIONS_CACHE.get(pool_key, []))

# COMMAND ----------

# DBTITLE 1,Haiku 4.5 invocation + JSON parsing
# These are now imported from `insights_compose`. We provide a local wrapper
# `call_haiku(prompt)` that adapts the new (workspace_client, prompt) → str
# signature into the old (prompt) → (text, elapsed, raw) tuple shape so
# `run_firmwide_persona` doesn't need to change.

def call_haiku(prompt: str) -> tuple[str, float, dict]:
    """Single Haiku call. Returns (text, elapsed_seconds, full_response_json).

    Wraps insights_compose.call_haiku with a 3-tuple return shape so the notebook
    has access to elapsed-time + raw response for diagnostics.
    """
    t0 = time.time()
    text = _call_haiku(w, prompt)
    elapsed = time.time() - t0
    # The shared helper drops the raw response on the floor; for the notebook
    # we'd rather have it for diagnostics, so re-issue a thin POST. But the
    # extra round-trip is wasteful — fall back to a synthetic empty payload
    # since the only thing the writer uses from `raw` is the (optional)
    # token count for logging.
    raw = {"usage": {}}
    return text, elapsed, raw


def parse_json_with_fence_strip(text: str) -> dict:
    """Parse Haiku JSON output, defensively stripping ```json fences if present."""
    return _parse_json_with_fence_strip(text)


def validate_payload(persona: str, payload: dict) -> list[str]:
    """Return list of validation errors (empty = valid)."""
    errs: list[str] = []
    insights = payload.get("insights") or []
    actions = payload.get("action_areas") or []
    chips = payload.get("bottom_chips") or []
    if len(insights) < 4:
        errs.append(f"[{persona}] insights: expected ≥4, got {len(insights)}")
    if len(actions) < 3:
        errs.append(f"[{persona}] action_areas: expected ≥3, got {len(actions)}")
    if len(chips) < 3:
        errs.append(f"[{persona}] bottom_chips: expected ≥3, got {len(chips)}")
    for i, ins in enumerate(insights[:4]):
        if not isinstance(ins, dict):
            errs.append(f"[{persona}] insights[{i}] not a dict")
            continue
        for required in ("headline", "value", "status_color", "trend_direction"):
            if not ins.get(required):
                errs.append(f"[{persona}] insights[{i}].{required} missing")
    for i, a in enumerate(actions[:3]):
        if not isinstance(a, dict):
            errs.append(f"[{persona}] action_areas[{i}] not a dict")
            continue
        for required in ("headline", "narrative", "status_color"):
            if not a.get(required):
                errs.append(f"[{persona}] action_areas[{i}].{required} missing")
    return errs

# COMMAND ----------

# DBTITLE 1,Schema bootstrap — ensure target table exists
def ensure_table_exists() -> None:
    """Verify the gold_persona_insights table is present.

    Shell DDL lives in data_pipeline/02_build_silver_gold.py so the table exists
    before the first orchestrator run on a clean customer deploy. Here we just
    verify it exists and run an idempotent column-add for older shells deployed
    before trend_direction was introduced.
    """
    if not _spark_available():
        print("  spark unavailable — skipping table-exists check (assumed pre-provisioned)")
        return
    fqn = f"{SCHEMA_FQN}.gold_persona_insights"
    if not spark.catalog.tableExists(fqn):  # noqa: F821
        raise RuntimeError(
            f"{fqn} does not exist. Run data_pipeline/02_build_silver_gold.py first "
            f"(the bundle's build_silver_gold task creates the shell)."
        )
    try:
        spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS (trend_direction STRING)")  # noqa: F821
    except Exception:
        pass  # column already exists — Delta will throw, that's fine
    print(f"Verified {fqn} exists")


ensure_table_exists()

# COMMAND ----------

# DBTITLE 1,Firmwide refactor — pull → compose → write (insight + action_area only)
def run_firmwide_persona(persona: str) -> dict:
    """Pull canonical SQL → compose via Haiku 4.5 → INLINE PROBE + RETRY → return parsed JSON.

    Inline retry: after the first Haiku call, run the regex consistency probe
    on the composed prose. If it finds violations (sum-of-parts mismatches,
    subset > total, scale outliers), fire ONE more Haiku call appending the
    violation list with a "fix these errors" directive. Use the retry's
    payload if it's clean; otherwise use whichever has fewer violations.

    This prevents bad insights/action_areas from reaching `gold_persona_insights`
    in the first place, rather than relying on the downstream `validate_consistency`
    task to surface them after-the-fact.

    Returns dict with keys: insights, action_areas, bottom_chips (chips are
    intentionally discarded by the writer below — chip rows are not regenerated
    this run).
    """
    if persona not in PERSONA_PIPELINES:
        raise ValueError(f"unknown persona {persona!r}; expected one of {list(PERSONA_PIPELINES)}")
    puller, prompter = PERSONA_PIPELINES[persona]
    data = puller()
    prompt = prompter(data)
    print(f"  [{persona}] calling Haiku 4.5 ({CLAUDE_MODEL})…")
    text, elapsed, raw = call_haiku(prompt)
    out_tokens = (raw.get("usage") or {}).get("completion_tokens")
    extra = f" / {out_tokens} tok" if out_tokens else ""
    print(f"  [{persona}] Haiku returned in {elapsed:.2f}s{extra}")
    payload = parse_json_with_fence_strip(text)

    # ─── Stamp canonical KPI values from SQL onto the headline `value` fields.
    # Haiku occasionally drifts on big-number headlines (e.g. 2,466→2,483
    # partners) despite the prompt embedding exact integers. The value tile
    # users see must match the dashboard, so we lock it to SQL truth here.
    try:
        from insights_compose import stamp_canonical_kpi_values
        stamp_canonical_kpi_values(persona, data, payload)
    except Exception as e:
        print(f"  [{persona}] canonical-value stamp failed ({type(e).__name__}: {e}); using raw LLM values")

    # ─── Inline regex retry ───────────────────────────────────────────────
    # Load lazily so a missing validation_probe (e.g. customer env that hasn't
    # synced the file yet) doesn't break the orchestrator entirely.
    try:
        from validation_probe import find_violations_in_prose
        from insights_compose import extract_prose_from_payload
    except Exception as e:
        print(f"  [{persona}] inline probe unavailable ({type(e).__name__}: {e}); skipping retry")
        return payload

    prose = extract_prose_from_payload(payload)
    violations = find_violations_in_prose(prose)
    if not violations:
        return payload

    print(f"  [{persona}] inline probe caught {len(violations)} violation(s); retrying with corrections")
    for v in violations[:5]:
        print(f"    - [{v.get('type','?')}] {v.get('message','')}")

    correction_prompt = (
        prompt
        + "\n\n---\nYour previous draft contained these arithmetic / consistency errors:\n"
        + "\n".join(f"- {v.get('message','')}" for v in violations)
        + "\n\nProduce a corrected response in the SAME JSON structure. Fix the cited numbers so the math is internally consistent (e.g. sum of listed parts must equal the stated total; no subset can exceed the firmwide total). Do NOT preserve the same arithmetic errors."
    )
    try:
        text2, elapsed2, raw2 = call_haiku(correction_prompt)
        payload_retry = parse_json_with_fence_strip(text2)
        # Re-stamp canonical values on retry payload too — the retry can
        # introduce its own drift on the value fields even if the corrections
        # focus on narrative arithmetic.
        try:
            from insights_compose import stamp_canonical_kpi_values
            stamp_canonical_kpi_values(persona, data, payload_retry)
        except Exception:
            pass
        prose_retry = extract_prose_from_payload(payload_retry)
        residual = find_violations_in_prose(prose_retry)
        if not residual:
            print(f"  [{persona}] retry passed clean ({elapsed2:.2f}s)")
            return payload_retry
        print(f"  [{persona}] retry still has {len(residual)} violation(s) — using retry payload anyway (best-effort); validate_consistency will hard-fail if material")
        return payload_retry
    except Exception as e:
        print(f"  [{persona}] retry call failed ({type(e).__name__}: {e}); keeping first-pass payload")
        return payload


def write_firmwide_rows(persona: str, payload: dict) -> int:
    """DELETE+INSERT firmwide insight + action_area + bottom_chip rows for this persona.

    Touches ONLY rows where:
        persona = <persona>
        AND filter_axis = 'firmwide' AND filter_value = 'all'
        AND slot_type IN ('insight','action_area','bottom_chip')

    followup_l1 rows are LEFT IN PLACE — follow-up regeneration is owned by a
    separate path (Genie agent expansion).

    Non-firmwide rows (region/location/practice_area/industry) are NEVER touched.
    """
    if not _spark_available():
        raise RuntimeError(
            "spark unavailable — cannot write rows. The firmwide refactor must "
            "run inside a Databricks notebook context (or be invoked with a Spark "
            "session available)."
        )
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType

    fqn = f"{SCHEMA_FQN}.gold_persona_insights"
    now_ts = datetime.utcnow()

    # Same column order as the deployed table (filter_axis + filter_value appended
    # at the end via ALTER TABLE ADD COLUMN in 02_build_silver_gold.py).
    schema = StructType([
        StructField("persona", StringType(), False),
        StructField("slot_type", StringType(), False),
        StructField("slot_id", IntegerType(), False),
        StructField("parent_slot_id", IntegerType(), True),
        StructField("parent_slot_type", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("value", StringType(), True),
        StructField("comparison", StringType(), True),
        StructField("trend", StringType(), True),
        StructField("trend_direction", StringType(), True),
        StructField("status_color", StringType(), True),
        StructField("narrative", StringType(), True),
        StructField("question_text", StringType(), True),
        StructField("routed_subqueries", StringType(), True),
        StructField("cached_agent_payload", StringType(), True),
        StructField("target_entity_type", StringType(), True),
        StructField("target_entity_value", StringType(), True),
        StructField("last_refreshed", TimestampType(), False),
        StructField("fiscal_period_anchor", DateType(), True),
        StructField("filter_axis", StringType(), False),
        StructField("filter_value", StringType(), False),
    ])

    def _row(slot_type: str, slot_id: int, **kwargs) -> dict:
        base = {
            "persona": persona,
            "slot_type": slot_type,
            "slot_id": int(slot_id),
            "parent_slot_id": None,
            "parent_slot_type": None,
            "headline": None,
            "value": None,
            "comparison": None,
            "trend": None,
            "trend_direction": None,
            "status_color": None,
            "narrative": None,
            "question_text": None,
            "routed_subqueries": None,
            "cached_agent_payload": None,
            "target_entity_type": None,
            "target_entity_value": None,
            "last_refreshed": now_ts,
            "fiscal_period_anchor": None,
            "filter_axis": "firmwide",
            "filter_value": "all",
        }
        base.update(kwargs)
        return base

    rows: list[dict] = []

    # Insights — keep first 4 (POC validated this shape).
    for i, ins in enumerate((payload.get("insights") or [])[:4], start=1):
        rows.append(_row(
            "insight", i,
            headline=ins.get("headline"),
            value=ins.get("value"),
            comparison=ins.get("comparison"),
            trend=ins.get("trend"),
            trend_direction=ins.get("trend_direction"),
            status_color=ins.get("status_color"),
            narrative=ins.get("narrative"),
            target_entity_type=ins.get("target_entity_type"),
            target_entity_value=ins.get("target_entity_value"),
        ))

    # Action areas — keep first 3.
    for i, a in enumerate((payload.get("action_areas") or [])[:3], start=1):
        rows.append(_row(
            "action_area", i,
            parent_slot_id=a.get("linked_insight_slot_id"),
            parent_slot_type="insight" if a.get("linked_insight_slot_id") else None,
            headline=a.get("headline"),
            status_color=a.get("status_color"),
            narrative=a.get("narrative"),
            target_entity_type=a.get("target_entity_type"),
            target_entity_value=a.get("target_entity_value"),
        ))

    # Bottom chips — keep first 3. Each chip pre-caches a 2-3 sub-query supporting
    # Genie payload (Opus decomposes → parallel Genie firing → sub_question_results
    # serialized to cached_agent_payload). ALSO pre-caches 2 depth-1 follow-up
    # questions per chip (Haiku generates the follow-up text from the chip's
    # sub-results context → fires each at Genie → caches single-shot sub_result).
    # The follow-up question texts also get attached to the parent chip's
    # suggested_questions so the chat modal surfaces them after the click reply.
    from insights_compose import precache_chip_payload, precache_chip_followups, PAGE_TILES
    _persona_to_page = {"admin": "admin", "finance": "finance", "hr": "admin"}
    _persona_tiles = PAGE_TILES[_persona_to_page.get(persona, "admin")]

    # Load backup follow-up questions for this persona. Used by
    # precache_chip_followups to substitute when an LLM-generated follow-up
    # produces a hollow payload (0 rows / "no data was returned"). Without
    # backups, hollow follow-ups get dropped entirely; with backups, we
    # substitute and re-validate.
    _persona_backups = _load_backup_followups(f"{persona}_backup_followups")

    # Stash (chip_slot_id, followups) so we can write the follow-up rows AFTER
    # the parent chip rows are inserted.
    persona_chip_followups: list[tuple[int, list[dict]]] = []

    for i, c in enumerate((payload.get("bottom_chips") or [])[:3], start=1):
        if not isinstance(c, dict):
            continue
        chip_text = c.get("question_text")
        cached_payload_json = None
        if chip_text and GENIE_SPACE_ID:
            try:
                print(f"    Pre-caching chip {i} for {persona}: '{chip_text[:80]}...'")
                payload_dict = precache_chip_payload(
                    workspace_client=w,
                    genie_space_id=GENIE_SPACE_ID,
                    chip_text=chip_text,
                    page_context_tiles=_persona_tiles,
                    agent_model_endpoint_url=CLAUDE_GATEWAY_URL,
                    agent_model_name=CLAUDE_MODEL_AGENT,
                    n_subqueries=3,
                )
                n_sub = len(payload_dict.get("sub_question_results") or [])
                print(f"      → cached {n_sub} sub-results")

                # Depth-1 follow-ups: generate 2 follow-up question texts from
                # the just-cached sub-results context + fire each at Genie.
                # Updates payload_dict.suggested_questions in-place.
                try:
                    print(f"      → pre-caching 2 depth-1 follow-ups...")
                    fu_list = precache_chip_followups(
                        workspace_client=w,
                        genie_space_id=GENIE_SPACE_ID,
                        chip_text=chip_text,
                        main_payload=payload_dict,
                        page_context_tiles=_persona_tiles,
                        agent_model_endpoint_url=CLAUDE_GATEWAY_URL,
                        agent_model_name=CLAUDE_MODEL_AGENT,
                        n=2,
                        n_subqueries=3,
                        backup_followups=_persona_backups,
                    )
                    print(f"      → cached {len(fu_list)} follow-up(s)")
                    persona_chip_followups.append((i, fu_list))
                except Exception as fu_e:
                    print(f"      → FOLLOW-UP PRE-CACHE FAILED (non-fatal): {type(fu_e).__name__}: {fu_e}")

                import json as _json
                cached_payload_json = _json.dumps(payload_dict)
                print(f"      → parent chip payload total {len(cached_payload_json)} chars")
            except Exception as e:
                print(f"      → PRE-CACHE FAILED (non-fatal, will fall back to live click): {type(e).__name__}: {e}")
                cached_payload_json = None
        rows.append(_row(
            "bottom_chip", i,
            question_text=chip_text,
            cached_agent_payload=cached_payload_json,
        ))

    # `persona_chip_followups` is consumed below, after the parent INSERT.

    field_order = [fld.name for fld in schema.fields]
    coerced_tuples = [tuple(r.get(k) for k in field_order) for r in rows]
    df = spark.createDataFrame(coerced_tuples, schema=schema)  # noqa: F821
    tmp_view = f"_firmwide_refactor_{persona}_tmp"
    df.createOrReplaceTempView(tmp_view)

    # SCOPED DELETE — insight, action_area, AND bottom_chip rows for this persona
    # at firmwide/all. followup_l1 rows are LEFT IN PLACE (owned by a separate
    # agent-expansion path).
    spark.sql(  # noqa: F821
        f"""
        DELETE FROM {fqn}
        WHERE persona = '{persona}'
          AND filter_axis = 'firmwide'
          AND filter_value = 'all'
          AND slot_type IN ('insight','action_area','bottom_chip')
        """
    )
    spark.sql(f"INSERT INTO {fqn} SELECT * FROM {tmp_view}")  # noqa: F821

    n = spark.sql(  # noqa: F821
        f"""
        SELECT COUNT(*) AS n FROM {fqn}
        WHERE persona = '{persona}'
          AND filter_axis = 'firmwide'
          AND filter_value = 'all'
          AND slot_type IN ('insight','action_area','bottom_chip')
        """
    ).collect()[0]["n"]

    # Now that the parent chip rows are in place, write the depth-1 follow-up
    # rows for each chip. Each chip's follow-ups live as their own slot_type
    # ='followup_l1' rows linked back via parent_slot_id.
    for chip_slot_id, fu_list in persona_chip_followups:
        try:
            n_fu = write_followup_rows(
                persona=persona,
                parent_slot_type="bottom_chip",
                parent_slot_id=chip_slot_id,
                followups=fu_list,
            )
            if n_fu:
                print(f"    Wrote {n_fu} followup_l1 rows for {persona} chip {chip_slot_id}")
        except Exception as e:
            print(f"    Follow-up write FAILED (non-fatal) for chip {chip_slot_id}: {type(e).__name__}: {e}")

    return n


def write_followup_rows(persona: str, parent_slot_type: str, parent_slot_id: int,
                         followups: list[dict], slot_type_override: str = None) -> int:
    """DELETE+INSERT depth-1 follow-up rows for ONE parent chip.

    Each follow-up gets its own row:
        persona = same as parent (admin/finance/hr OR '_shared')
        slot_type = 'followup_l1' (persona chips) OR 'followup_l1_<page>' (page chips,
            passed via slot_type_override)
        slot_id = sequential 1..N within (persona, slot_type, parent_slot_id)
        parent_slot_id = the parent chip's slot_id
        parent_slot_type = the parent chip's slot_type (so we can link back)
        question_text = the follow-up text (what the user clicks)
        cached_agent_payload = JSON-serialized sub_question_results

    `followups` is the list returned by `precache_chip_followups` — each item has
    {question_text, cached_payload}.

    Returns count of rows written.
    """
    if not _spark_available():
        raise RuntimeError("spark unavailable — cannot write follow-up rows")
    if not followups:
        return 0
    import json as _json
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType

    fqn = f"{SCHEMA_FQN}.gold_persona_insights"
    slot_type = slot_type_override or "followup_l1"
    now_ts = datetime.utcnow()

    schema = StructType([
        StructField("persona", StringType(), False),
        StructField("slot_type", StringType(), False),
        StructField("slot_id", IntegerType(), False),
        StructField("parent_slot_id", IntegerType(), True),
        StructField("parent_slot_type", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("value", StringType(), True),
        StructField("comparison", StringType(), True),
        StructField("trend", StringType(), True),
        StructField("trend_direction", StringType(), True),
        StructField("status_color", StringType(), True),
        StructField("narrative", StringType(), True),
        StructField("question_text", StringType(), True),
        StructField("routed_subqueries", StringType(), True),
        StructField("cached_agent_payload", StringType(), True),
        StructField("target_entity_type", StringType(), True),
        StructField("target_entity_value", StringType(), True),
        StructField("last_refreshed", TimestampType(), False),
        StructField("fiscal_period_anchor", DateType(), True),
        StructField("filter_axis", StringType(), False),
        StructField("filter_value", StringType(), False),
    ])

    rows = []
    for i, fu in enumerate(followups, start=1):
        if not isinstance(fu, dict):
            continue
        cached_json = None
        if fu.get("cached_payload"):
            try:
                cached_json = _json.dumps(fu["cached_payload"])
            except (TypeError, ValueError):
                cached_json = None
        rows.append((
            persona, slot_type, int(i),
            int(parent_slot_id), parent_slot_type,
            None, None, None, None, None, None,         # headline/value/comparison/trend/trend_direction/status_color
            None,                                        # narrative
            fu.get("question_text"),                    # question_text
            None, cached_json,                           # routed_subqueries, cached_agent_payload
            None, None,                                  # target_entity_type, target_entity_value
            now_ts, None,                                # last_refreshed, fiscal_period_anchor
            "firmwide", "all",
        ))

    if not rows:
        return 0

    df = spark.createDataFrame(rows, schema=schema)  # noqa: F821
    tmp_view = f"_followups_{persona}_{parent_slot_type}_{parent_slot_id}_tmp"
    df.createOrReplaceTempView(tmp_view)

    # SCOPED DELETE — only this parent's follow-ups. Other follow-ups stay put.
    spark.sql(  # noqa: F821
        f"""
        DELETE FROM {fqn}
        WHERE persona = '{persona}'
          AND slot_type = '{slot_type}'
          AND parent_slot_id = {int(parent_slot_id)}
          AND parent_slot_type = '{parent_slot_type}'
          AND filter_axis = 'firmwide'
          AND filter_value = 'all'
        """
    )
    spark.sql(f"INSERT INTO {fqn} SELECT * FROM {tmp_view}")  # noqa: F821
    return len(rows)


def write_page_chip_rows(page: str, chips: list[dict]) -> int:
    """DELETE+INSERT 3 chip rows for a deepdive page under persona='_shared'.

    Rows are scoped to:
        persona = '_shared'
        AND slot_type = 'bottom_chip_<page>'    # bottom_chip_admin or bottom_chip_finance
        AND filter_axis = 'firmwide' AND filter_value = 'all'

    Used by the AI Assistant modal on /admin-deepdive and /finance-deepdive
    (see app.py:/api/get-page-chips → persona_insights_reader.get_bottom_chips_for_page).

    Returns count of rows actually present after the write.
    """
    if not _spark_available():
        raise RuntimeError("spark unavailable — cannot write page-chip rows")
    if page not in ("admin", "finance"):
        raise ValueError(f"Unknown page {page!r}; expected 'admin' or 'finance'")

    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType

    fqn = f"{SCHEMA_FQN}.gold_persona_insights"
    slot_type = f"bottom_chip_{page}"
    now_ts = datetime.utcnow()

    schema = StructType([
        StructField("persona", StringType(), False),
        StructField("slot_type", StringType(), False),
        StructField("slot_id", IntegerType(), False),
        StructField("parent_slot_id", IntegerType(), True),
        StructField("parent_slot_type", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("value", StringType(), True),
        StructField("comparison", StringType(), True),
        StructField("trend", StringType(), True),
        StructField("trend_direction", StringType(), True),
        StructField("status_color", StringType(), True),
        StructField("narrative", StringType(), True),
        StructField("question_text", StringType(), True),
        StructField("routed_subqueries", StringType(), True),
        StructField("cached_agent_payload", StringType(), True),
        StructField("target_entity_type", StringType(), True),
        StructField("target_entity_value", StringType(), True),
        StructField("last_refreshed", TimestampType(), False),
        StructField("fiscal_period_anchor", DateType(), True),
        StructField("filter_axis", StringType(), False),
        StructField("filter_value", StringType(), False),
    ])

    # Pre-cache each chip's 2-3 supporting Genie sub-queries + 2 depth-1
    # follow-ups (same pattern as persona chips). Follow-up rows are written
    # after the parent chip rows are inserted.
    from insights_compose import precache_chip_payload, precache_chip_followups, PAGE_TILES
    page_tiles = PAGE_TILES[page]

    # Load page-scoped backup follow-ups (admin_page_backup_followups /
    # finance_page_backup_followups). Substituted into hollow LLM-generated
    # follow-ups by precache_chip_followups.
    _page_backups = _load_backup_followups(f"{page}_page_backup_followups")

    page_chip_followups: list[tuple[int, list[dict]]] = []

    rows = []
    for i, c in enumerate(chips[:3], start=1):
        if not isinstance(c, dict):
            continue
        chip_text = c.get("question_text")
        cached_payload_json = None
        if chip_text and GENIE_SPACE_ID:
            try:
                print(f"    Pre-caching {slot_type} chip {i}: '{chip_text[:80]}...'")
                payload_dict = precache_chip_payload(
                    workspace_client=w,
                    genie_space_id=GENIE_SPACE_ID,
                    chip_text=chip_text,
                    page_context_tiles=page_tiles,
                    agent_model_endpoint_url=CLAUDE_GATEWAY_URL,
                    agent_model_name=CLAUDE_MODEL_AGENT,
                    n_subqueries=3,
                )
                n_sub = len(payload_dict.get("sub_question_results") or [])
                print(f"      → cached {n_sub} sub-results")

                # Depth-1 follow-ups (same as persona chip flow)
                try:
                    print(f"      → pre-caching 2 depth-1 follow-ups...")
                    fu_list = precache_chip_followups(
                        workspace_client=w,
                        genie_space_id=GENIE_SPACE_ID,
                        chip_text=chip_text,
                        main_payload=payload_dict,
                        page_context_tiles=page_tiles,
                        agent_model_endpoint_url=CLAUDE_GATEWAY_URL,
                        agent_model_name=CLAUDE_MODEL_AGENT,
                        n=2,
                        n_subqueries=3,
                        backup_followups=_page_backups,
                    )
                    print(f"      → cached {len(fu_list)} follow-up(s)")
                    page_chip_followups.append((i, fu_list))
                except Exception as fu_e:
                    print(f"      → FOLLOW-UP PRE-CACHE FAILED (non-fatal): {type(fu_e).__name__}: {fu_e}")

                import json as _json
                cached_payload_json = _json.dumps(payload_dict)
            except Exception as e:
                print(f"      → PRE-CACHE FAILED (non-fatal): {type(e).__name__}: {e}")
                cached_payload_json = None
        rows.append((
            SHARED_PERSONA_KEY, slot_type, int(i),
            None, None,                             # parent_slot_id, parent_slot_type
            None, None, None, None, None, None,     # headline, value, comparison, trend, trend_direction, status_color
            None,                                   # narrative
            chip_text,                              # question_text — the chip text itself
            None, cached_payload_json,              # routed_subqueries, cached_agent_payload
            None, None,                             # target_entity_type, target_entity_value
            now_ts, None,                           # last_refreshed, fiscal_period_anchor
            "firmwide", "all",                      # filter_axis, filter_value
        ))

    df = spark.createDataFrame(rows, schema=schema)  # noqa: F821
    tmp_view = f"_page_chips_{page}_tmp"
    df.createOrReplaceTempView(tmp_view)

    # SCOPED DELETE — only this page's _shared chip rows. Other _shared rows
    # (e.g. followup_l1_<page>) are untouched.
    spark.sql(  # noqa: F821
        f"""
        DELETE FROM {fqn}
        WHERE persona = '{SHARED_PERSONA_KEY}'
          AND slot_type = '{slot_type}'
          AND filter_axis = 'firmwide'
          AND filter_value = 'all'
        """
    )
    spark.sql(f"INSERT INTO {fqn} SELECT * FROM {tmp_view}")  # noqa: F821

    n = spark.sql(  # noqa: F821
        f"""
        SELECT COUNT(*) AS n FROM {fqn}
        WHERE persona = '{SHARED_PERSONA_KEY}'
          AND slot_type = '{slot_type}'
          AND filter_axis = 'firmwide'
          AND filter_value = 'all'
        """
    ).collect()[0]["n"]

    # Write depth-1 follow-up rows for each page chip. slot_type is suffixed
    # with the page (e.g. 'followup_l1_admin') so the table read path can
    # find them per page.
    fu_slot_type = f"followup_l1_{page}"
    for chip_slot_id, fu_list in page_chip_followups:
        try:
            n_fu = write_followup_rows(
                persona=SHARED_PERSONA_KEY,
                parent_slot_type=slot_type,
                parent_slot_id=chip_slot_id,
                followups=fu_list,
                slot_type_override=fu_slot_type,
            )
            if n_fu:
                print(f"    Wrote {n_fu} '{fu_slot_type}' rows for {page} chip {chip_slot_id}")
        except Exception as e:
            print(f"    Follow-up write FAILED (non-fatal) for {page} chip {chip_slot_id}: {type(e).__name__}: {e}")

    return n

# COMMAND ----------

# DBTITLE 1,Main run loop — firmwide insight + action_area refresh
if True:
    print("\n" + "=" * 60)
    print(f"Firmwide insight refresh — personas: {PERSONAS}")
    print("Refreshing firmwide insight/action_area/bottom_chip rows and wiping the")
    print("compound filter-cache (write-through cache populated lazily on app visits).")
    print("=" * 60)

    # Wipe the write-through filter cache before refreshing firmwide. This
    # guarantees filter narratives never reference a stale firmwide snapshot —
    # they all recompute lazily on the next visit, against the same data state
    # this orchestrator run produced. Single DML, runs once per orchestrator
    # invocation regardless of how many personas are processed.
    fqn_for_compound_wipe = f"{SCHEMA_FQN}.gold_persona_insights"
    if _spark_available():
        print(f"Wiping compound filter-cache rows from {fqn_for_compound_wipe}...")
        spark.sql(  # noqa: F821
            f"DELETE FROM {fqn_for_compound_wipe} WHERE filter_axis = 'compound'"
        )
        print("  Compound cache wiped — filter combos will recompute on next visit.")

    for persona in PERSONAS:
        if persona not in PERSONA_PIPELINES:
            print(f"  [{persona}] unknown persona, skipping")
            continue
        print(f"\n=== {persona.upper()} | FIRMWIDE ===")
        try:
            payload = run_firmwide_persona(persona)
        except Exception as e:
            print(f"  [{persona}] FAILED during pull/compose: {type(e).__name__}: {e}")
            print(f"  [{persona}] previous cycle's rows preserved as fallback")
            continue

        errs = validate_payload(persona, payload)
        if errs:
            print(f"  [{persona}] validation errors ({len(errs)}): {errs[:5]}")
            print(f"  [{persona}] previous cycle's rows preserved as fallback")
            continue

        insights = payload.get("insights") or []
        actions = payload.get("action_areas") or []
        chips = payload.get("bottom_chips") or []
        print(f"  [{persona}] composed {len(insights)} insights / {len(actions)} actions / "
              f"{len(chips)} chips")

        # Echo the headline values so the run log shows what was written.
        for i, ins in enumerate(insights[:4], start=1):
            print(f"    insight {i}: {ins.get('headline','')} = {ins.get('value','')}  ({ins.get('trend','')})")

        try:
            n = write_firmwide_rows(persona, payload)
        except Exception as e:
            print(f"  [{persona}] FAILED during write: {type(e).__name__}: {e}")
            continue

        print(f"Wrote {n} firmwide rows for {persona}")

    # ─── PAGE-SCOPED CHIP PHASE ─────────────────────────────────────────────
    # The Admin Overview and Finance Overview deepdive pages each show their
    # own AI Assistant modal with 3 suggested questions ("chips"). Those chips
    # are stored under persona='_shared' and slot_type='bottom_chip_<page>'.
    # Generate fresh chips here using the new clean prompt that bans compound
    # multi-part questions and forecasting tails — see build_page_chips_prompt
    # in insights_compose.py.
    print("\n" + "=" * 60)
    print("Page-scoped chip phase — Admin Overview + Finance Overview deepdive pages")
    print("Writing rows: persona='_shared', slot_type IN ('bottom_chip_admin','bottom_chip_finance')")
    print("=" * 60)

    for page in ("admin", "finance"):
        print(f"\n=== {page.upper()} DEEPDIVE PAGE | CHIPS ===")
        try:
            from insights_compose import compose_page_chips
            chips = compose_page_chips(
                page=page,
                warehouse_id=WAREHOUSE_ID,
                sdk_workspace_client=w,
            )
            if not chips:
                print(f"  [{page}] composer returned no chips — skipping")
                continue
            print(f"  [{page}] composed {len(chips)} chips")
            for i, c in enumerate(chips, start=1):
                print(f"    chip {i}: {c.get('question_text','')[:100]}")
            n = write_page_chip_rows(page, chips)
            print(f"  Wrote {n} '_shared/bottom_chip_{page}' rows")
        except Exception as e:
            print(f"  [{page}] FAILED: {type(e).__name__}: {e}")
            continue

# COMMAND ----------

# DBTITLE 1,Summary of what is currently in gold_persona_insights
fqn = f"{SCHEMA_FQN}.gold_persona_insights"
if _spark_available():
    spark.sql(  # noqa: F821
        f"""
        SELECT persona,
               slot_type,
               COUNT(*) AS rows,
               MAX(last_refreshed) AS most_recent
        FROM {fqn}
        GROUP BY persona, slot_type
        ORDER BY persona, slot_type
        """
    ).show(truncate=False)
else:
    print(f"  (spark unavailable — skipping summary SELECT on {fqn})")
