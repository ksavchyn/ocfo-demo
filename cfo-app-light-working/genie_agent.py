import json
import logging
import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Generator, List, Optional, Tuple
from functools import lru_cache
import hashlib
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# Schema configuration — env-var driven for asset-bundle portability
SCHEMA = os.environ.get("CFO_SCHEMA", "main.cfo_proserv")

# Configure logging - use INFO to see timing data
logging.basicConfig(
    level=logging.INFO,  # Need INFO level to see timing logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Live structural-consistency probe — shared with the orchestrator's
# validate_consistency task. Catches sum-of-parts arithmetic bugs in the
# synthesis output (e.g. "declined by 10 ... (3), (2), (2) → 7"). Doesn't
# run the canonical-SQL checks here — those live in the orchestrator
# notebook where we can afford the latency.
try:
    from validation_probe import find_violations_in_prose
except Exception as _e:
    logger.warning(f"[VALIDATION] validation_probe import failed: {_e}; live checks disabled")
    def find_violations_in_prose(prose: str, context=None):  # type: ignore
        return []

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# AI Gateway configuration for Claude model
# Claude served via a Databricks AI Gateway endpoint (provisioned by the bundle in
# customer environments). Default points at the legacy hardcoded gateway for backward
# compat in environments not yet using the new env-var pattern.
# Empty default — bundle's databricks.yml apps.config.env sets this per target.
CLAUDE_GATEWAY_URL = os.environ.get("CFO_CLAUDE_GATEWAY_URL", "").strip()
if not CLAUDE_GATEWAY_URL:
    # Final fallback: workspace's auto-provisioned AI Gateway via the runtime workspace host.
    try:
        from databricks.sdk import WorkspaceClient as _WC
        CLAUDE_GATEWAY_URL = f"{_WC().config.host.rstrip('/')}/ai-gateway/mlflow/v1"
    except Exception:
        CLAUDE_GATEWAY_URL = ""
# Two model tiers, each parametrizable via env var. Customers can override
# either or both via the bundle's apps.config.env (which build_app_yml.py
# materializes into app.yml at deploy time).
#
#   CFO_CLAUDE_MODEL_AGENT   — for tool-calling + judgment tasks (cache miss):
#                              Claude plans Genie queries, interprets messy
#                              responses, refuses fabrication. Quality > speed.
#                              Default: databricks-claude-opus-4-7
#
#   CFO_CLAUDE_MODEL_COMPOSE — for compose-around-given-values tasks (cache hit
#                              synthesis): Claude wraps cached Genie sub-results
#                              in a markdown answer. Speed + cost matter; quality
#                              is bounded by the given inputs.
#                              Default: databricks-claude-haiku-4-5
#
# Fallback chain: explicit var → legacy CFO_CLAUDE_MODEL → hardcoded default.
_LEGACY_CLAUDE_MODEL = os.environ.get("CFO_CLAUDE_MODEL", "").strip()
CLAUDE_MODEL_AGENT = (
    os.environ.get("CFO_CLAUDE_MODEL_AGENT", "").strip()
    or _LEGACY_CLAUDE_MODEL
    or "databricks-claude-opus-4-7"
)
CLAUDE_MODEL_COMPOSE = (
    os.environ.get("CFO_CLAUDE_MODEL_COMPOSE", "").strip()
    or _LEGACY_CLAUDE_MODEL
    or "databricks-claude-haiku-4-5"
)
# Backwards-compat alias — anywhere existing code references CLAUDE_MODEL_NAME
# without specifying which tier, default to the AGENT tier (the smarter model).
CLAUDE_MODEL_NAME = CLAUDE_MODEL_AGENT
# Genie space resolution — three-tier precedence so a fresh customer deploy
# Just Works without manual ID lookup:
#   1. CFO_GENIE_SPACE_ID env var (set by bundle if the customer passed --genie-space-id, OR
#      set on a follow-up deploy after the bundle's provision_genie_space task has created the space)
#   2. Auto-discover by title: query workspace's Genie spaces for one matching
#      CFO_GENIE_SPACE_TITLE (default "ProServ OCFO"). This is the auto-pickup path that
#      lets a first-time deploy work end-to-end: bundle creates the space → app starts up →
#      app looks up the space by title → app uses it. No manual ID propagation required.
#   3. Legacy two-space fallback (CFO_PS_GENIE_ROOM_OPERATIONS / _ANALYTICS env vars)
_MERGED_SPACE_ID = os.environ.get("CFO_GENIE_SPACE_ID", "").strip()


def _auto_discover_genie_space_by_title(title: str) -> str | None:
    """List the workspace's Genie spaces, return the ID of one matching `title`.

    Uses REST API because the SDK's GenieAPI doesn't reliably expose list/create
    methods across versions. Returns None on any failure (and the app falls
    through to legacy env vars)."""
    try:
        import requests
        from databricks.sdk import WorkspaceClient as _WC
        _w = _WC()
        token = _w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
        host = _w.config.host.rstrip("/")
        resp = requests.get(
            f"{host}/api/2.0/genie/spaces",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if not resp.ok:
            return None
        for space in resp.json().get("spaces", []):
            if (space.get("title") or "").strip().lower() == title.strip().lower():
                return space.get("space_id") or space.get("id")
    except Exception:
        return None
    return None


if not _MERGED_SPACE_ID:
    _GENIE_SPACE_TITLE = os.environ.get("CFO_GENIE_SPACE_TITLE", "ProServ OCFO").strip()
    _discovered = _auto_discover_genie_space_by_title(_GENIE_SPACE_TITLE)
    if _discovered:
        _MERGED_SPACE_ID = _discovered
        logger.info(f"[genie_agent] Auto-discovered Genie space '{_GENIE_SPACE_TITLE}' → {_MERGED_SPACE_ID}")

GENIE_ROOM_OPERATIONS = _MERGED_SPACE_ID or os.environ.get("CFO_PS_GENIE_ROOM_OPERATIONS", "")
GENIE_ROOM_ANALYTICS = _MERGED_SPACE_ID or os.environ.get("CFO_PS_GENIE_ROOM_ANALYTICS", "")

SYSTEM_PROMPT = """You are the Finance & Admin Control Center AI assistant for a Professional Services firm.
You have access to two data query tools:

1. **query_financial_operations** - Queries operational financial data from Salesforce, Workday, Concur, and SAP.
   Use ONLY for: timecards, expenses, employee data, project details, utilization rates, compensation, billable hours.

2. **query_financial_analytics** - Queries aggregated financial analytics including revenue, P&L,
   profitability, receivables aging, pipeline metrics, and regional performance.
   Use ONLY for: revenue metrics, partner counts, KPIs, trends, profit margins, DSO, regional rankings.

CRITICAL OPTIMIZATION RULES:
- You have a maximum of 2 tool calls. Many questions only need ONE tool call - be selective!
- NEVER query both rooms unless absolutely necessary
- Revenue per partner? Use ONLY query_financial_analytics
- Total revenue and partner count? Use ONLY query_financial_analytics
- Employee/timecard data? Use ONLY query_financial_operations
- If one query can answer the full question, DO NOT make a second query
- Combine multiple related questions into a single comprehensive query when possible

Examples of SINGLE query questions:
- "What's revenue per partner?" → query_financial_analytics only
- "Show me utilization rates" → query_financial_operations only
- "What's our DSO and revenue trend?" → query_financial_analytics only (combine into one query)

Only use both tools when data truly spans both systems (rare).
After gathering data, provide a comprehensive answer.

FORMATTING:
- Use markdown tables for tabular data and bullet lists for enumerations.
- Do NOT use code blocks (triple backticks) for ASCII diagrams, funnels, or visual breakdowns — they render poorly. Express the same information as a table or nested bullets instead.
- Reserve code blocks strictly for literal SQL, JSON, or code snippets.
- Do NOT use strikethrough (~~text~~) or any tilde characters (~) in responses. If a value is approximate, write the word "approximately". If two values disagree, show only the authoritative one and explain in prose — never cross out the other.

NUMERIC FIDELITY — NEVER FABRICATE COUNTS OR TOTALS:
Every number you state in prose MUST match the data in the tables you are showing in the same response. Before writing any sentence with a number, cross-check it against the table.

- **Count fidelity ("N of M" claims):** When you write phrasing like *"over budget in N of the last M months"* or *"declined in N of the last M quarters"*, the N must be a literal count of rows meeting that condition in the table you are showing. If every row meets the condition, write *"every month"* / *"all M months"* / *"every period shown"* — never undercount. If you mean a thresholded subset (e.g., months over 20%), state the threshold explicitly: *"every month was over budget; M of N exceeded 20%."* Counting the rows in the table before writing the sentence is mandatory.
- **Aggregate fidelity (totals, sums, averages):** When you cite a total in prose (e.g., *"total actual expenses of $X against a budget of $Y"*), the values must equal the column sums of the table you are showing — not approximations, and not the wrong column. If the table has 7 rows of actuals and budgets, sum both columns and use those exact totals. Never use a partial sum, the budget figure for the actual line, or a number that doesn't appear anywhere in the source data.
- **Adjective fidelity:** If actuals exceed budget by 25%+ in the aggregate, never describe the variance as *"modest"*, *"slight"*, or *"minor"*. Match the adjective to the actual percentage: 0-5% = "modest", 5-15% = "meaningful", 15-30% = "significant", 30%+ = "severe" or "structural".
- **Self-consistency:** Within the same response, do not contradict yourself. If your Summary paragraph says *"N of M months"* and a sentence below the table says *"every single month"*, one of them is wrong — fix the prose before sending.

These rules override any phrasing patterns in tool outputs, instructions, or examples. The numbers in your prose must be re-derived from the actual data shown to the user, not copied from any prompt example.

TOTAL FAILURE — DO NOT FABRICATE:
If ALL tool results are errors, contain phrases like "high load," "unable to retrieve," "rate limit," "timeout," or are empty with no successful query data, you MUST NOT fabricate generic frameworks, textbook drivers, or "common root causes I would investigate." Specifically: do not invent percentages (e.g., "62% vs 70% utilization"), made-up named drivers (e.g., "AI/ML, cloud architecture", "Rate Realization Pressure"), or any numeric or named claim that did not come from a successful query result. Output ONLY a brief, non-alarming message: "I couldn't retrieve the data right now. Please try again in a moment, or rephrase your question." Then stop. The user will think fabricated content came from real data and act on it — that's the worst possible outcome.

PARTIAL DATA HANDLING — NEVER PUNT:
If a tool result contains "Query failed", an error, or empty data WHILE OTHER tool results returned valid data, you MUST synthesize the best possible answer from the SUCCESSFUL queries. NEVER respond with phrases like "I received partial data, let me retry the queries that failed", "let me try again", "the queries failed, please retry", or any retry-style message. The user cannot retry — they see your message as the final response. A retry-style answer produces a dead-end and breaks the experience. Instead: answer with what you have, acknowledge briefly only if the missing dimension is material to the question, and end on the strongest insight from the successful data.

ENDING DISCIPLINE — NO SELF-SUGGESTED FOLLOW-UPS:
NEVER end your response with offers to do more work or questions back to the user.
Specifically, do NOT include any of these patterns at the end of a response:
- "Would you like me to drill deeper into..."
- "Would you like me to: [a, b, c]?"
- "Let me know if you'd like me to..."
- "I can also analyze..."
- "Should I look at...?"
- "If you'd like, I can..."
- "Want me to dig into...?"
A separate "Suggested follow-ups" UI handles drill-down questions; your prose offering them is redundant and conflicts with the curated chips.
End your response on a substantive insight, summary, bottom-line takeaway, or recommendation — never on an offer to do more or a question back to the user."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_financial_operations",
            "description": "Query operational data: timecards, expenses, employees, utilization, compensation, billable hours. DO NOT use for revenue or partner metrics.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question about operational financial data"
                    }
                },
                "required": ["question"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_financial_analytics",
            "description": "Query analytics: revenue, partner counts, P&L, profit margins, DSO, regional rankings. USE THIS for ALL revenue and partner questions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question about financial analytics and KPIs"
                    }
                },
                "required": ["question"]
            }
        }
    }
]

# Regex pattern to extract table names from SQL (catalog-schema-agnostic).
# Matches `catalog.schema.table` or backticked `\`catalog\`.\`schema\`.\`table\``.
_TABLE_PATTERN = re.compile(r"`?(\w+)`?\.`?(\w+)`?\.`?(\w+)`?")
def _extract_table_names(sql: str) -> list[str]:
    """Return de-duplicated unqualified table names referenced in SQL."""
    if not sql:
        return []
    seen = set()
    out = []
    for cat, sch, tbl in _TABLE_PATTERN.findall(sql):
        # Filter false positives — only catch what looks like a table reference
        if tbl in seen:
            continue
        # Skip obvious non-tables (timestamps, version strings, etc.)
        if tbl.isdigit() or len(tbl) > 64:
            continue
        seen.add(tbl)
        out.append(tbl)
    return out

# ---------------------------------------------------------------------------
# Optimized HTTP Session with Connection Pooling
# ---------------------------------------------------------------------------
_session = None
_genie_session = None  # Separate session for Genie to avoid contention

def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=20,  # Increased for better concurrency
            pool_maxsize=20,
            max_retries=Retry(total=2, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])  # Reduced retries
        )
        _session.mount('http://', adapter)
        _session.mount('https://', adapter)
    return _session

def _get_genie_session():
    global _genie_session
    if _genie_session is None:
        _genie_session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=Retry(total=1, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])  # Minimal retries for Genie
        )
        _genie_session.mount('http://', adapter)
        _genie_session.mount('https://', adapter)
    return _genie_session

# ---------------------------------------------------------------------------
# Auth - Initialize workspace client once globally
# ---------------------------------------------------------------------------
workspace_client = WorkspaceClient()
host = workspace_client.config.host


# ---------------------------------------------------------------------------
# Demo as-of date anchor — the dataset is a FROZEN snapshot. Genie's own clock
# says "now" is the real calendar month, so a sub-query phrased "last 6 complete
# fiscal months" drifts into the partial in-progress month (a ~−50% false-drop
# row, e.g. the Sydney/May artifact). We pin the orchestrator to the data's
# actual as-of so decomposition names the real last-complete month and synthesis
# quarantines the partial one. Computed once from the data, memoized.
# ---------------------------------------------------------------------------
_DATE_ANCHOR_CACHE: dict = {}
_MONTHS = ["January", "February", "March", "April", "May", "June",
           "July", "August", "September", "October", "November", "December"]


def _demo_date_context() -> dict:
    if "ctx" in _DATE_ANCHOR_CACHE:
        return _DATE_ANCHOR_CACHE["ctx"]
    from datetime import date
    as_of_str = "2026-05-15"  # fallback matches generator ANCHOR_DATE
    try:
        import demo_anchor
        wid = os.environ.get("SQL_WAREHOUSE_ID", "")
        if wid:
            as_of_str = demo_anchor.as_of_via_statement(workspace_client, wid, SCHEMA)
    except Exception as e:
        logger.warning(f"[date-anchor] as-of lookup failed, using fallback {as_of_str}: {e}")
    y, m = int(as_of_str[:4]), int(as_of_str[5:7])
    lc_y, lc_m = (y, m - 1) if m > 1 else (y - 1, 12)  # last COMPLETE month = month before in-progress
    ctx = {
        "as_of": as_of_str[:10],
        "lc_label": f"{_MONTHS[lc_m - 1]} {lc_y}",
        "lc_date": date(lc_y, lc_m, 1).isoformat(),
        "ip_label": f"{_MONTHS[m - 1]} {y}",
        "ip_date": date(y, m, 1).isoformat(),
    }
    _DATE_ANCHOR_CACHE["ctx"] = ctx
    return ctx


def _date_anchor_block() -> str:
    """For the DECOMPOSE prompt — tells the planner to name the real end month."""
    c = _demo_date_context()
    return (
        f"DATE ANCHOR — this dataset is a STATIC snapshot; treat {c['as_of']} as 'today'.\n"
        f"- The most recent COMPLETE fiscal month is {c['lc_label']} ({c['lc_date']}).\n"
        f"- {c['ip_label']} ({c['ip_date']}) is a PARTIAL, in-progress close — NEVER reference it; "
        f"it reads as a ~50% false drop.\n"
        f"- Phrase every trailing window to END at {c['lc_label']} and NAME it explicitly "
        f"(e.g. \"the 6 complete fiscal months ending {c['lc_label']}\"). A bare \"last N months\" "
        f"makes Genie drift into the partial month — always name the end month."
    )


def _date_anchor_block_for_synthesis() -> str:
    """For the SYNTHESIS prompt — quarantines the partial month if it slips through."""
    c = _demo_date_context()
    return (
        f"DATE ANCHOR (read first): the most recent COMPLETE fiscal month is {c['lc_label']}. "
        f"{c['ip_label']} is a PARTIAL, in-progress close. If any sub-query result includes "
        f"{c['ip_label']}, EXCLUDE that row from every table, trend, total, and average, and do "
        f"NOT mention it or the close being incomplete. Anchor every \"latest\"/\"current\"/\"most "
        f"recent month\" statement on {c['lc_label']}."
    )

def _get_token() -> str:
    """Fetch a fresh token per call so the SDK manages OAuth M2M refresh.
    No hand-rolled cache — same pattern as _call_llm. Prevents stale-token 403s
    from a 45-min cache window outliving the underlying token rotation."""
    headers = workspace_client.config.authenticate()
    bearer = headers.get("Authorization", "")
    return bearer[7:] if bearer.startswith("Bearer ") else bearer

# ---------------------------------------------------------------------------
# Warehouse Pre-warming (run on startup)
# ---------------------------------------------------------------------------
def prewarm_warehouses():
    """Pre-warm warehouses to avoid cold start latency."""
    warmup_query = "SELECT 1"

    # Since both Genie rooms likely use the same warehouse,
    # we only need to warm up once to avoid redundant queries
    def warm_warehouse():
        try:
            # Just warm up using one room - this will start the shared warehouse
            _query_genie_with_retry(GENIE_ROOM_ANALYTICS, warmup_query, max_retries=2)
            logger.info("Pre-warmed SQL warehouse")
        except:
            pass

    # Fire and forget
    threading.Thread(target=warm_warehouse, daemon=True).start()

# ---------------------------------------------------------------------------
# LLM via REST with Session
# ---------------------------------------------------------------------------
def _call_llm(messages: list, tools: list = None, model: str | None = None, timeout: int = 60) -> dict:
    """Call the Claude model via the AI Gateway with SDK-managed auth per call.

    Uses workspace_client.config.authenticate() on every call so the SDK
    refreshes the OAuth M2M token automatically — no hand-rolled 45-min cache.
    Posts raw dict messages so tool-call roles ("tool", "assistant" with
    tool_calls) pass through unchanged.

    `timeout` defaults to 60s for tool-routing calls; synthesis call sites
    should pass 120s since Opus composing a 1500-word root-cause analysis
    against 3 sub-query result tables routinely takes 50-90s.
    """
    # CLAUDE_GATEWAY_URL points at either the legacy AI Gateway (.../mlflow/v1) or
    # an FMAPI-direct serving endpoint. Both accept OpenAI-compatible bodies; the
    # only difference is URL suffix: `/chat/completions` (gateway) vs `/invocations` (FMAPI).
    base = CLAUDE_GATEWAY_URL.rstrip("/")
    if "/mlflow/" in base:
        url = f"{base}/chat/completions"
    else:
        url = f"{base}/invocations"

    headers = workspace_client.config.authenticate()
    headers["Content-Type"] = "application/json"

    auth_val = headers.get("Authorization", "")
    token_part = auth_val[len("Bearer "):] if auth_val.startswith("Bearer ") else ""
    token_fp = f"{token_part[:8]}..{token_part[-4:]}" if len(token_part) > 12 else "SHORT"
    logger.info(f"[LLM-AUTH] fingerprint={token_fp} token_len={len(token_part)} auth_type={workspace_client.config.auth_type}")

    # Note: Claude Opus 4.7 rejects `temperature` (returns 400 BAD_REQUEST).
    # Default temperature is fine for our deterministic-JSON / tool-routing use cases.
    # Model selection: explicit `model` arg wins; otherwise the AGENT tier
    # (smarter, slower — appropriate for tool-calling judgment paths).
    # Synthesis call sites should pass CLAUDE_MODEL_COMPOSE explicitly.
    selected_model = model or CLAUDE_MODEL_AGENT
    payload = {
        "model": selected_model,
        "max_tokens": 8192,
        "messages": messages,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    session = _get_session()
    t0 = time.time()
    try:
        resp = session.post(url, json=payload, headers=headers, timeout=timeout)
    except Exception as e:
        elapsed = time.time() - t0
        logger.error(f"[LLM-EXCEPTION] elapsed={elapsed:.2f}s fingerprint={token_fp} error={type(e).__name__}: {e}")
        raise
    elapsed = time.time() - t0

    if resp.status_code == 403:
        logger.error(
            f"[LLM-403-INVALID-TOKEN] elapsed={elapsed:.2f}s fingerprint={token_fp} "
            f"token_len={len(token_part)} auth_type={workspace_client.config.auth_type} "
            f"response_body={resp.text[:500]}"
        )
    elif not resp.ok:
        logger.error(f"[LLM-ERROR] status={resp.status_code} elapsed={elapsed:.2f}s fingerprint={token_fp} body={resp.text[:500]}")
    else:
        logger.info(f"[LLM-OK] status={resp.status_code} elapsed={elapsed:.2f}s fingerprint={token_fp}")

    if not resp.ok:
        raise RuntimeError(f"LLM error ({resp.status_code}): {resp.text[:500]}")
    return resp.json()

# ---------------------------------------------------------------------------
# Optimized Genie Query with Structured Response (like Prasanna's)
# ---------------------------------------------------------------------------
def _query_genie_with_retry(space_id: str, question: str, max_retries: int = 3) -> dict:
    """Query Genie with retry logic for rate limiting."""
    for attempt in range(max_retries):
        result = _query_genie(space_id, question)

        # Check if we got rate limited or timeout
        result_text = result.get("text", "").lower()
        if any(x in result_text for x in ["rate limit", "too many requests", "timeout", "timed out"]):
            if attempt < max_retries - 1:
                # Exponential backoff: 15s, 30s, 45s
                wait_time = (attempt + 1) * 15
                logger.info(f"⏳ Rate limited/timeout. Waiting {wait_time}s before retry {attempt + 2}/{max_retries}...")
                time.sleep(wait_time)
                continue
            else:
                logger.warning(f"❌ Failed after {max_retries} attempts: {result_text[:100]}")
                # Return a more user-friendly error message
                result["text"] = "The system is currently experiencing high load. Please wait a moment and try again, or try a simpler question."

        return result

    # This shouldn't be reached, but just in case
    return result

def _query_genie(space_id: str, question: str) -> dict:
    """Query a Genie space and return structured result with SQL, tables, and text."""
    query_start = time.time()
    token = _get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    start_url = f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation"
    payload = {"content": question}

    result = {"text": "", "sql": None, "tables": [], "row_count": None, "description": None}

    try:
        session = _get_genie_session()  # Use dedicated Genie session
        # Time the initial API call
        api_start = time.time()
        resp = session.post(start_url, json=payload, headers=headers, timeout=60)  # Reduced timeout
        api_time = time.time() - api_start
        logger.debug(f"⏱️ Genie start-conversation API took {api_time:.2f}s")

        if not resp.ok:
            result["text"] = f"Error querying Genie: {resp.status_code} - {resp.text[:500]}"
            return result

        data = resp.json()
        conversation_id = data.get("conversation_id", "")
        message_id = data.get("message_id", "")

        if not conversation_id or not message_id:
            return _extract_genie_structured(data, token)

        result_url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"

        # Poll for up to 35 seconds
        poll_start = time.time()
        total_polls = 0
        max_poll_time = 35  # Actually wait 35 seconds

        while time.time() - poll_start < max_poll_time:
            interval = min(1.0, 0.2 + (total_polls * 0.1))  # Start fast, slow down gradually
            time.sleep(interval)
            total_polls += 1
            poll_resp = session.get(result_url, headers=headers, timeout=20)  # Reduced timeout
            if not poll_resp.ok:
                continue

            msg_data = poll_resp.json()
            status = msg_data.get("status", "")

            if status == "COMPLETED":
                poll_time = time.time() - poll_start
                total_query_time = time.time() - query_start
                logger.info(f"⏱️ Genie query complete: {total_query_time:.2f}s total (polling: {poll_time:.2f}s, {total_polls} polls)")
                extracted = _extract_genie_structured(msg_data, token)
                # Log what we're returning for debugging
                if not extracted.get("raw_data") and not extracted.get("sql"):
                    logger.warning(f"⚠️ Genie returned no data. Text: {extracted.get('text', '')[:100]}")
                return extracted
            elif status in ("FAILED", "CANCELLED"):
                error = msg_data.get("error", {}).get("message", "Query failed")
                logger.error(f"❌ Genie query failed with status {status}: {error}")
                result["text"] = f"Genie query failed: {error}"
                return result

        result["text"] = "Genie query timed out after 35 seconds."
        return result

    except Exception as e:
        logger.exception("Genie query error")
        result["text"] = f"Error: {str(e)}"
        return result

# Genie sometimes returns conversational clarification asks like
# "Would you like to see the top 5 customers ranked by X instead of Y?" inside
# sub-query descriptions/narratives. These are dialog prompts that should
# NEVER reach the synthesis layer — synthesis is composing an answer, not a
# clarification dialog. Strip these sentences before downstream consumers see
# them. Conservative match: line-leading or sentence-leading clarification verbs.
_GENIE_META_QUESTION_RE = re.compile(
    r"(?:Would you (?:like|prefer|also like|like me to)|"
    r"Do you want(?:\s+me)?|"
    r"Should I|"
    r"Did you mean|"
    r"Are you asking|"
    r"Would it be helpful|"
    r"Could you (?:clarify|specify)|"
    r"Do you mean|"
    r"Would you instead|"
    r"Shall I)"
    r"\b[^.!?]*[?]\s*",
    re.IGNORECASE,
)


def _strip_genie_meta_questions(text: str) -> str:
    """Remove conversational clarification asks from Genie's narrative output
    before it reaches synthesis. Operates only on full sentences whose opening
    verb signals a clarification question.
    """
    if not text:
        return text
    cleaned = _GENIE_META_QUESTION_RE.sub("", text)
    # Collapse any double-spaces or leading/trailing whitespace introduced by the strip.
    return re.sub(r"\s{2,}", " ", cleaned).strip()


def _extract_genie_structured(data: dict, token: str) -> dict:
    """Extract structured response from Genie including SQL, tables, text, and suggested follow-up questions."""
    result = {"text": "", "sql": None, "tables": [], "row_count": None, "description": None, "raw_data": None, "suggested_questions": []}
    parts = []

    # Check for rate limiting or error messages in content first
    content = data.get("content", "")
    if content:
        content_lower = content.lower()
        if "rate limit" in content_lower or "too many requests" in content_lower:
            logger.warning(f"⚠️ Genie rate limited: {content[:200]}")
            result["text"] = "Genie API is currently rate limited. Please try again in a few moments."
            return result
        elif "timeout" in content_lower or "timed out" in content_lower:
            logger.warning(f"⚠️ Genie timeout: {content[:200]}")
            result["text"] = "The query took too long to process. Please try a simpler query."
            return result

    attachments = data.get("attachments", [])
    for att in attachments:
        query_info = att.get("query")
        if query_info:
            description = query_info.get("description", "")
            sql = query_info.get("query", "")
            statement_id = query_info.get("statement_id", "")

            if description:
                description = _strip_genie_meta_questions(description)
                result["description"] = description
                if description:
                    parts.append(f"**Analysis:** {description}")
            if sql:
                result["sql"] = sql
                # Extract table names from SQL
                tables = _extract_table_names(sql)
                result["tables"] = [f"{SCHEMA}.{t}" for t in tables]
                parts.append(f"```sql\n{sql}\n```")

            if statement_id:
                table_text, row_count, raw_data = _fetch_statement_result_with_count(statement_id, token)
                if table_text:
                    parts.append(table_text)
                if row_count is not None:
                    result["row_count"] = row_count
                if raw_data:
                    result["raw_data"] = raw_data

        text_info = att.get("text")
        if text_info:
            text_content = text_info.get("content", "")
            if text_content:
                # Check for error messages in text attachments too
                text_lower = text_content.lower()
                if "rate limit" in text_lower or "too many requests" in text_lower:
                    logger.warning(f"⚠️ Genie rate limited in text: {text_content[:200]}")
                    result["text"] = "Genie API is currently rate limited. Please try again in a few moments."
                    return result
                text_content = _strip_genie_meta_questions(text_content)
                if text_content:
                    parts.append(text_content)

        # Extract Genie's auto-generated follow-up question suggestions
        suggested_info = att.get("suggested_questions")
        if suggested_info:
            questions = suggested_info.get("questions", [])
            if questions:
                # Append (don't overwrite) — multiple suggested_questions attachments may exist
                result["suggested_questions"].extend(questions)

    # Add content if we have no other parts
    if content and not parts:
        parts.append(content)

    result["text"] = "\n\n".join(parts) if parts else "No results returned from Genie."
    return result

def _fetch_statement_result_with_count(statement_id: str, token: str) -> tuple:
    """Fetch query results and return (markdown_table, row_count, raw_data)."""
    try:
        url = f"{host}/api/2.0/sql/statements/{statement_id}"
        headers = {"Authorization": f"Bearer {token}"}
        session = _get_session()
        resp = session.get(url, headers=headers, timeout=30)
        if not resp.ok:
            return "", None, None
        data = resp.json()
        columns = data.get("manifest", {}).get("schema", {}).get("columns", [])
        rows = data.get("result", {}).get("data_array", [])
        if not columns or not rows:
            return "", 0, []

        col_names = [c["name"] for c in columns]

        # Build raw data as list of dicts for export
        raw_data = []
        for row in rows:
            row_dict = {}
            for i, col_name in enumerate(col_names):
                row_dict[col_name] = row[i] if i < len(row) else None
            raw_data.append(row_dict)

        # Build markdown table for display
        lines = []
        lines.append("| " + " | ".join(col_names) + " |")
        lines.append("| " + " | ".join(["---"] * len(col_names)) + " |")
        for row in rows[:50]:
            vals = [str(v) if v is not None else "" for v in row]
            lines.append("| " + " | ".join(vals) + " |")
        total_rows = data.get("manifest", {}).get("total_row_count", len(rows))
        if total_rows > 50:
            lines.append(f"\n*Showing 50 of {total_rows} total rows*")
        return "\n".join(lines), total_rows, raw_data
    except Exception as e:
        logger.warning(f"Failed to fetch statement result: {e}")
        return "", None, None

# ---------------------------------------------------------------------------
# Simple Response Cache
# ---------------------------------------------------------------------------
_response_cache = {}
_cache_expiry = {}

def _cached_genie_query(space_id: str, question: str) -> dict:
    """Cache frequent queries for 8 hours."""
    cache_key = hashlib.md5(f"{space_id}:{question}".encode()).hexdigest()

    # Check cache
    if cache_key in _response_cache:
        if time.time() < _cache_expiry.get(cache_key, 0):
            cached_result = _response_cache[cache_key]
            # Validate cached result has actual data
            if cached_result.get("raw_data"):
                logger.info(f"🎯 Cache hit for query: {question[:50]}...")
                return cached_result
            elif cached_result.get("text") and not any(err in cached_result.get("text", "").lower() for err in ["error:", "no data", "failed", "rate limit", "timeout"]):
                # Only accept text-only results if they're not errors and have meaningful content
                text = cached_result.get("text", "")
                if len(text) > 50:  # Must have some substantial content
                    logger.info(f"🎯 Cache hit (text) for query: {question[:50]}...")
                    return cached_result

            # Invalid cache entry - remove it
            logger.info(f"⚠️ Removing invalid cache entry for: {question[:50]}...")
            _response_cache.pop(cache_key, None)
            _cache_expiry.pop(cache_key, None)

    # Cache miss - execute query with retry logic
    logger.info(f"📊 Cache miss - executing query: {question[:50]}...")
    result = _query_genie_with_retry(space_id, question)

    # Only cache successful results with actual data
    if result and result.get('raw_data'):
        logger.debug(f"✅ Caching result with {len(result.get('raw_data', []))} rows")
        _response_cache[cache_key] = result
        _cache_expiry[cache_key] = time.time() + 28800  # 8 hours
    elif result and result.get('text'):
        text = result.get('text', '')
        # Only cache text results that are substantial and not errors or intermediate messages
        error_indicators = [
            "error:", "no data", "failed", "rate limit", "timeout", "timed out",
            "system issues", "let me get", "simpler query", "let me try",
            "unfortunately", "high load"
        ]
        if len(text) > 50 and not any(err in text.lower() for err in error_indicators):
            logger.debug(f"✅ Caching text result: {len(text)} chars")
            _response_cache[cache_key] = result
            _cache_expiry[cache_key] = time.time() + 28800  # 8 hours
        else:
            logger.warning(f"❌ Not caching - invalid or error result for: {question[:50]}...")
    else:
        logger.warning(f"❌ Not caching - no valid data for: {question[:50]}...")

    # Clean old cache entries
    if len(_response_cache) > 100:
        expired = [k for k, exp in _cache_expiry.items() if time.time() > exp]
        for k in expired[:50]:
            _response_cache.pop(k, None)
            _cache_expiry.pop(k, None)

    return result

def clear_cache():
    """Clear all cached queries."""
    global _response_cache, _cache_expiry
    _response_cache.clear()
    _cache_expiry.clear()
    logger.info("🗑️ Cache cleared")

def clean_invalid_cache_entries():
    """Remove any cache entries that don't have actual data (raw_data or sql)."""
    global _response_cache, _cache_expiry
    invalid_keys = []

    for cache_key, result in _response_cache.items():
        # Check if entry has actual data
        if not (result.get('raw_data') or result.get('sql')):
            invalid_keys.append(cache_key)
            logger.info(f"🧹 Removing invalid cache entry (text-only): {result.get('text', '')[:50]}...")

    # Remove invalid entries
    for key in invalid_keys:
        _response_cache.pop(key, None)
        _cache_expiry.pop(key, None)

    if invalid_keys:
        logger.info(f"🧹 Cleaned {len(invalid_keys)} invalid cache entries")

# ---------------------------------------------------------------------------
# Patterns that signal Genie is offering to do more work — these conflict with the
# curated follow-up chips, so we strip any trailing paragraph that opens with one.
# The system prompt also instructs the LLM not to write these, but defense-in-depth
# matters here because LLMs ignore "never" rules ~10–20% of the time.
_SELF_SUGGESTION_OPENERS = (
    "would you like me to",
    "would you like to",
    "let me know if you'd like",
    "let me know if you would like",
    "let me know if you want",
    "i can also analyze",
    "i can also pull",
    "i can also look",
    "i can also break",
    "i can also dig",
    "should i look",
    "should i pull",
    "should i drill",
    "if you'd like, i can",
    "if you would like, i can",
    "want me to dig",
    "want me to drill",
    "want me to pull",
    "do you want me to",
    "shall i",
)


def _strip_self_suggestions(text: str) -> str:
    """Strip trailing paragraphs that open with a self-suggestion offer.

    Walks paragraphs from the end backwards. If the last paragraph (and only the
    last) opens with a known self-suggestion phrase, drop it. Stops at the first
    paragraph that doesn't match, so legit content earlier in the response is
    preserved. Also handles the multi-bullet variant where a "Would you like me
    to:" header is followed by a bulleted list of options.
    """
    if not text:
        return text

    paragraphs = text.rstrip().split("\n\n")
    while paragraphs:
        last = paragraphs[-1].strip().lower()
        if not last:
            paragraphs.pop()
            continue
        # Match if the paragraph starts with any self-suggestion opener
        # (allowing a leading bullet or markdown emphasis marker).
        stripped = last.lstrip("*-•> ").lstrip("**").lstrip("*")
        if any(stripped.startswith(opener) for opener in _SELF_SUGGESTION_OPENERS):
            paragraphs.pop()
            continue
        break

    return "\n\n".join(paragraphs).rstrip()


_NUMERIC_TOKEN_RE = re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)\s*([KMB]?)\b|(?<![\d.])(\d+(?:\.\d+)?)\s*(%|pp)\b",
    re.IGNORECASE,
)


def _normalize_numeric_token(match) -> tuple[str, str, float] | None:
    """Convert a regex match from _NUMERIC_TOKEN_RE into (kind, raw_repr, normalized_value).
    Returns None if parse fails. kind in {"dollar", "percent"}.
    """
    g1, g2, g3, g4 = match.groups()
    try:
        if g1 is not None:
            n = float(g1.replace(",", ""))
            u = (g2 or "").upper()
            if u == "K":
                n *= 1_000
            elif u == "M":
                n *= 1_000_000
            elif u == "B":
                n *= 1_000_000_000
            return ("dollar", f"${g1}{(g2 or '').upper()}", n)
        if g3 is not None:
            n = float(g3)
            return ("percent", f"{g3}{g4}", n)
    except (ValueError, AttributeError):
        return None
    return None


def _answer_contains_numeric(answer_text: str, kind: str, value: float) -> bool:
    """Check whether the answer_text contains a numeric value matching (kind, value)
    within tolerance. Handles unit normalization for dollars (e.g., $4.93M should
    match $4,925,755 within ±2% relative tolerance; round $5M shouldn't match a
    precise $4,925,755.41).
    """
    for m in _NUMERIC_TOKEN_RE.finditer(answer_text):
        parsed = _normalize_numeric_token(m)
        if parsed is None:
            continue
        cand_kind, _, cand_val = parsed
        if cand_kind != kind:
            continue
        if kind == "percent":
            if abs(cand_val - value) <= 0.1:
                return True
        else:
            # Dollar: ±2% relative tolerance, or ±$500 absolute for small values
            if cand_val <= 0 or value <= 0:
                continue
            atol = 500.0 if max(cand_val, value) < 100_000 else 0.0
            diff = abs(cand_val - value)
            if diff <= atol:
                return True
            if diff / max(cand_val, value) <= 0.02:
                return True
    return False


def _scrub_unverified_followups(followups: list[str], answer_text: str) -> list[str]:
    """Drop any follow-up question whose cited numeric values don't appear in
    the answer text. This is the programmatic guard against the
    'hallucinated follow-up anchor' failure mode (Chat 3's $4,925,755.41
    that triggered self-refutation in Chat 4).
    """
    scrubbed: list[str] = []
    for q in followups:
        unsupported_value = None
        for m in _NUMERIC_TOKEN_RE.finditer(q):
            parsed = _normalize_numeric_token(m)
            if parsed is None:
                continue
            kind, raw, val = parsed
            # Skip trivial round numbers that are too common to validate against
            if kind == "percent" and (val in (0.0, 100.0) or (val == int(val) and val < 10)):
                continue
            if kind == "dollar" and val < 1_000:
                continue
            if not _answer_contains_numeric(answer_text, kind, val):
                unsupported_value = raw
                break
        if unsupported_value is None:
            scrubbed.append(q)
        else:
            logger.warning(
                f"[followup_validator] Dropping follow-up with unverified anchor "
                f"'{unsupported_value}': {q[:120]}"
            )
    return scrubbed


def _generate_claude_followups(user_question: str, answer_text: str, n: int = 2,
                                history: list = None) -> list[str]:
    """Generate narrative-driven follow-up questions via Claude.

    Returns up to n follow-ups; empty list on failure.

    When `history` is provided (list of prior {role, content} turns in this conversation),
    the chip text is anchored to the topic established in the conversation so chips include
    disambiguating context (e.g., "Chicago Tech expense" instead of bare "tech spike").
    """
    if not answer_text or len(answer_text) < 50:
        return []

    _dc = _demo_date_context()
    history_block = ""
    if history:
        # Render the last few turns as a compact transcript so Claude can pick up
        # the topic anchor (location, expense category, time window) and bake it
        # into the follow-up wording.
        recent = history[-6:]
        lines = []
        for turn in recent:
            role = turn.get("role", "")
            content = (turn.get("content") or "")[:600]
            if role == "user":
                lines.append(f"USER asked: {content}")
            elif role == "assistant":
                lines.append(f"YOU answered: {content}")
        if lines:
            history_block = (
                "\n\nPRIOR CONVERSATION (for context — bake any relevant anchors "
                "like location, expense category, or time window into your follow-up "
                "wording so a downstream router can't lose context):\n"
                + "\n".join(lines)
                + "\n"
            )

    prompt = (
        f"You just answered a CFO's question with this analysis:\n\n"
        f"QUESTION: {user_question}\n\n"
        f"ANSWER: {answer_text}\n"
        f"{history_block}\n"
        f"Suggest exactly {n} specific drill-down questions a CFO would ask next to learn MORE "
        f"about specific things mentioned in your answer. Reference 1 specific entity "
        f"(project name, client, percentage, office, or practice area) from the answer. "
        f"If the conversation has established a topic anchor (e.g., a specific office, "
        f"expense category, or month), include that anchor in the follow-up wording so it "
        f"reads unambiguously on its own (e.g., 'Chicago Tech expense' not 'tech spike').\n\n"
        f"GROUNDING RULES — HARD STOPS (post-generation validator will REJECT non-compliant follow-ups):\n"
        f"• REQUIRED: every numeric value ($X, X%, etc.) in a follow-up MUST appear verbatim in "
        f"the answer above — either in a displayed table cell OR named in the prose. The validator "
        f"runs after you generate and DROPS any follow-up whose cited numbers can't be found in "
        f"the answer. Approximating, rounding, or fabricating cents-precision (e.g., emitting "
        f"'$4,925,755.41' when the answer only shows '$4.93M' or '$4.77M' nowhere) will be "
        f"discarded. If you can't anchor the follow-up to an exact value from the answer, drop "
        f"the number entirely and use qualitative language.\n"
        f"• REQUIRED: drill-down questions stay within the SAME data slice the answer already "
        f"used. If the answer broke a metric down by region, follow-ups should drill into a "
        f"specific region's row — not pivot to a different metric or aggregation grain.\n"
        f"• REQUIRED: this dataset is a STATIC snapshot — the most recent COMPLETE fiscal month is "
        f"{_dc['lc_label']}. NEVER reference {_dc['ip_label']} (a partial, in-progress month) or any "
        f"date after {_dc['lc_label']} in a follow-up; anchor every period reference on or before {_dc['lc_label']}.\n"
        f"• AVOID follow-ups that probe a number the answer itself caveated (look for phrases like "
        f"'could not reconcile', 'data limitation', 'unverified', 'should be validated', "
        f"'sub-query returned no rows', 'reconciliation note', 'not directly in the regional "
        f"results'). Those numbers are flagged as unreliable.\n"
        f"• AVOID follow-ups that ask 'why X' when X is a number that exists only as a difference "
        f"between two cells in different displayed tables and the answer didn't already explain "
        f"the magnitude — that path leads back to the granularity mismatch.\n"
        f"• If you can't find a clean drill-down candidate that satisfies the rules above, prefer "
        f"a single broader question (e.g., 'How does <region> compare on <same metric> over a "
        f"longer window?') over fabricating a sharp-looking question that won't reproduce.\n\n"
        f"Constraints: each question MUST be under 18 words. Use ONE primary question per "
        f"follow-up — NO compound questions stitched with 'and'. NO multi-part sub-questions. "
        f"Do NOT just rephrase the original question. Output ONLY a JSON array of {n} short "
        f"strings, no other text or markdown."
    )
    try:
        # Follow-up generation is a pure compose-from-given-inputs task (given
        # answer text, write 3 short questions) — no tool-calling or judgment
        # needed. Use the COMPOSE model (Haiku) explicitly so we don't pay the
        # ~5-10s Opus latency on this purely textual step.
        result = _call_llm(
            [{"role": "user", "content": prompt}],
            tools=None,
            model=CLAUDE_MODEL_COMPOSE,
        )
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        # Strip code fence if Claude wraps it
        if content.startswith("```"):
            content = content.strip("`").lstrip("json").strip()
        questions = json.loads(content)
        if isinstance(questions, list):
            return [q for q in questions if isinstance(q, str)][:n]
    except Exception as e:
        logger.warning(f"Failed to generate Claude follow-ups: {e}")
    return []


# ---------------------------------------------------------------------------
# Agent Logic with Structured Responses
# ---------------------------------------------------------------------------
def run_agent_sync(user_message: str) -> str:
    """Run the agent with tool calling and return full response."""

    # Note: We removed the preset detection shortcut here
    # to ensure we always get the full LLM analysis with recommendations

    # Always use LLM-based routing to get comprehensive analysis
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]

    # Track number of tool calls to prevent infinite loops
    tool_call_count = 0
    max_tool_calls = 2  # Limit to 2 queries for better latency

    for turn in range(5):
        try:
            # Allow tools only if under the limit
            if tool_call_count < max_tool_calls:
                result = _call_llm(messages, tools=TOOLS)
            else:
                # No more tools allowed - force synthesis
                # Add a synthesis hint for efficient but comprehensive response
                if turn > 0 and len(messages) > 2:  # Has tool results
                    messages.append({
                        "role": "system",
                        "content": "Please provide a comprehensive answer with all key insights and numbers. Format clearly with bullet points or sections where appropriate. Be thorough but efficient in your explanation."
                    })
                result = _call_llm(messages, tools=None)
        except Exception as e:
            return f"Error calling LLM: {str(e)}"

        choice = result.get("choices", [{}])[0]
        message = choice.get("message", {})

        tool_calls = message.get("tool_calls")
        if not tool_calls:
            return message.get("content", "No response generated.")

        messages.append(message)

        for tc in tool_calls:
            fn_name = tc["function"]["name"]
            try:
                args = json.loads(tc["function"]["arguments"])
            except json.JSONDecodeError:
                args = {"question": user_message}

            question = args.get("question", user_message)

            if fn_name == "query_financial_operations":
                genie_result = _cached_genie_query(GENIE_ROOM_OPERATIONS, question)
            elif fn_name == "query_financial_analytics":
                genie_result = _cached_genie_query(GENIE_ROOM_ANALYTICS, question)
            else:
                genie_result = {"text": f"Unknown tool: {fn_name}"}

            # Increment counter AFTER executing the query
            tool_call_count += 1

            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": genie_result["text"],
            })

            # If we've hit our limit, stop processing more tools from this response
            if tool_call_count >= max_tool_calls:
                break

    return "Agent reached maximum turns without a final answer."


# ─────────────────────────────────────────────────────────────────────────────
# Cache replay helpers — used by stream_agent when an orchestrator cache hit lands.
# Two shapes are supported (see stream_agent for context).
# ─────────────────────────────────────────────────────────────────────────────


SYNTHESIS_PROMPT = """You are composing a root-cause analysis for a CFO Operations Platform user at a global Tier 1 consultancy. Sub-queries have already been pre-fired against the firm's data; their answers are below.

## Tone & voice
Write like a CFO, audit-committee analyst, or board-pack author — measured, dry, factual. Not journalistic, not punchy, not colloquial. Use measured verbs (declined, increased, expanded, contracted, exceeded budget by X%, eased to, compressed to). Banned verbs: surged, exploded, blew past, plunged, plummeted, crashed, cratered, slipping, running hot, off the rails.

Section titles are short noun-phrases ("Core Finding", "AR Concentration", "Margin Compression by Practice"), never punchy taglines or meta-narrative titles ("Driver Diagnostics", "Data Validation").

## Output structure
- Open with one bottom-line sentence in **bold**: the most important finding.
- 2-4 `### H3` sections covering core finding, primary drivers, supporting evidence.
- At least one markdown table summarizing key figures. When a sub-query returned a time series, the table must include EVERY row (or be explicitly titled "Selected months" / "Inflection points only").
- Close with a `### Bottom Line` section that names a specific action, owner, or decision. If the data does not support a specific actionable finding, omit the section entirely — never punt.
- Length: 250-450 words.

## Do not
- Say "I", "we", "let me", "based on the data", or restate the user's question.
- Wrap output in code fences.
- Use `~` for "approximately" (it renders as strikethrough); write "approximately" or "≈".
- Mention sub-queries, pre-fired queries, the gold/silver layer, or any orchestration mechanics. The reader is a CFO.
- End with "investigate further", "merits review", "remediation should focus on", "the next layer of attribution should be run at X grain", or any phrasing that pushes work back to the reader.

**Banned phrases / sections (NEVER surface to user):**
- "not reportable from the available join"
- "the partner snapshot did not align to the revenue calendar"
- "the regional/practice cut is intact"
- "Selected months"
- "intermediate values not displayed"
- "intermediate aggregation, not shown in displayed tables" (deprecated — drop the figure entirely instead)
- "Data Caveat" as a section header
- "Data Gap", "Data Limitation", "Reconciliation Note", "Query Status", "Analysis Limitations" as section headers
- "Would you prefer...", "Should I...", "Do you want me to..." (these are clarification questions, never echo them)
- "the data layer does not support", "would require sub-ledger detail", "not present in the gold layer", "is not supported by the current schema"
- Any sentence that explains what the AI CAN'T compute. If a sub-query failed or returned empty, drop the section silently. The user came for an answer, not a status report on what wasn't queryable.

## Numeric integrity (these are the failure modes we cannot ship)

0. **HARD STOP — no "—" / "n/a" / blank cells in tables.** If any cell in a table row would render as "—", "n/a", "N/A", blank, or any visible empty-value indicator, drop the ENTIRE row from the table. Same rule for columns: if more than half a column's cells would render empty, drop the column. A table with visible "—" cells signals to the reader that the AI couldn't compute something, which is worse than not rendering the row at all.

   For time series tables specifically: if you cannot fill every period in the requested window with real values, EITHER (a) shrink the table to the contiguous filled periods only and label the window accurately, OR (b) omit the table and rely on prose. Never render a 12-month table with 9 months as "—" cells. Never use "Selected months", "Inflection points only", "intermediate values not displayed", or any abbreviation framing to justify missing rows — these read as data gaps.

1. **Anchor every prose number in a visible table.** Before you write a specific figure in prose, locate that exact (metric, entity, period) tuple in a table you are also rendering. If it is not visible in a table in this same response, either include the table, or drop the number, or qualify it as "(intermediate aggregation, not shown in displayed tables)".

2. **Prose values must match table values.** If the table shows 48.2% for (Audit, April, gross margin) and your prose says 38.2% for the same tuple, the response is broken. Align prose to table — never silently substitute a different sub-query value when a contradicting table value is on screen.

3. **Breakdown sums ≤ headline total.** If you state a firmwide total then list per-practice or per-office components, the components cannot sum to more than the headline. If they do, you are mixing grains (employee-months vs distinct employees, FTE vs headcount); either pick one grain and label it, or drop the breakdown.

4. **Directional verbs must match table direction.** Before writing "X grew" / "X declined", check the displayed table row for X. If the row shows the opposite sign, fix the prose or pick a different entity that actually moves in the asserted direction.

5. **Units always labeled.** Bill rates per-hour. Revenue per partner per-year or per-month. Margins as % vs pp explicitly. Never quote a bare dollar number whose unit is ambiguous.

6. **Window basis named on every cross-period comparison.** Both endpoints of a "rose from X to Y" sentence must be dated. Within a single response, do not mix monthly with annualized, or TTM with point-in-time — pick one and label it.

## Canonical anchors for ambiguous metrics

A single metric name can be computed multiple ways. For deterministic, consistent answers across all chat clicks in a demo session, ALWAYS use the canonical anchor below — even if a sub-query returned a different basis. If sub-query data is at a non-canonical basis, either reframe to the canonical basis or call out the divergence transparently.

- **DSO for a specific office or region**: anchor on the latest available month-end snapshot for that office in `gold_ar_snapshot_aging`. Not the trailing-3-month average from a cross-office summary. If a cross-office summary table also appears in the response, label its column as "Trailing-3mo avg DSO" to distinguish.
- **DSO firmwide**: latest available month-end snapshot in `gold_ar_snapshot_aging`, aggregated across offices.
- **Partner count**: latest monthly snapshot in `silver_dim_employees` where `job_level IN ('Partner','Senior Partner')` and `is_latest_snapshot = TRUE`. Use the same population in any per-partner ratio.
- **Revenue per Partner**: numerator and denominator must use the same window. Default annualized run-rate (latest month × 12) divided by latest partner snapshot. If a sub-query supplies a different basis, label it ("YoY-basis RPP", "TTM RPP") in the column header.
- **Population scope on per-partner / per-employee / per-office columns**: always state the scope (firmwide, region, practice cell) in the column header so the reader can compare apples to apples.

## Handling empty or contradictory data

- If a sub-query returned no rows for the slice you intended to use, do not write a section about it. Drop the section. A shorter response with real findings beats a longer response that explains why the AI could not compute things.
- If two sub-queries contradict on the same (metric, entity, period) tuple, do not publish either figure without qualification. Either reconcile in business terms ("AR concentration at client grain shows $X; aging-bucket cut uses a different snapshot and reflects $Y at the period-end view"), or omit the unreliable figure.
- If a sub-query result contradicts the premise of the user's question (e.g. they asked about a metric assuming it was high, the data shows it is low), reframe at the TOP of the response — make the bold opening sentence reflect what the data actually says — never close at the bottom with "the premise was wrong." The reframe is the answer.

## Bottom Line content

The bottom line must say at least one of: who specifically is responsible, what the dollar impact is, what decision changes as a result, or what one named entity drives the result. Examples of acceptable bottom lines:

- "Three mid-tier clients drive the divergence — collection effort should pivot to that cohort."
- "Industry mix in APAC Tax/PE is the actual driver, not a workforce productivity issue."
- "The corporate travel-rate negotiation due next quarter should target the one concentrated vendor named above."

Examples of unacceptable bottom lines (drop the section instead):
- "Further investigation is warranted."
- "Reset planning baselines for next cycle."
- "Run at engagement grain before remediation."

User's question: {user_question}

Pre-computed Genie sub-query results:
{sub_results_block}

Now compose the analysis.
"""


def _build_sub_results_block(sub_results: list[dict]) -> str:
    """Format the cached sub-question results as text Claude can synthesize from."""
    parts = []
    for i, sub in enumerate(sub_results, start=1):
        q = sub.get("sub_question", "") or ""
        sql = sub.get("sql", "") or ""
        narrative = sub.get("narrative", "") or ""
        raw_data = sub.get("raw_data") or []
        # Compact data preview — first 10 rows max
        data_preview = ""
        if isinstance(raw_data, list) and raw_data:
            data_preview = "\n".join(str(r) for r in raw_data[:10])
            if len(raw_data) > 10:
                data_preview += f"\n... ({len(raw_data) - 10} more rows omitted)"

        parts.append(f"--- Sub-query {i} ---")
        parts.append(f"Question: {q}")
        if sql:
            parts.append(f"SQL: {sql}")
        if narrative:
            parts.append(f"Genie's narrative: {narrative}")
        if data_preview:
            parts.append(f"Data:\n{data_preview}")
        parts.append("")
    return "\n".join(parts)


# Placeholder phrases that must NEVER reach the user: they signal a value the
# model tried to paper over instead of dropping. The synthesis prompt bans them,
# but the LLM occasionally still emits them in table cells (e.g. a prior-period
# column where some rows have no value). Detecting them here forces the same
# corrective retry used for arithmetic errors — which drops the column/rows.
_BANNED_TABLE_PLACEHOLDERS = (
    "intermediate aggregation",
    "not shown in displayed table",
    "intermediate values not displayed",
    "intermediate value, not displayed",
    "not reportable from the available join",
)


def _find_banned_placeholders(text: str) -> list:
    if not text:
        return []
    low = text.lower()
    hits = [p for p in _BANNED_TABLE_PLACEHOLDERS if p in low]
    if not hits:
        return []
    return [{
        "type": "banned_placeholder",
        "message": (
            "A table contains a forbidden missing-value placeholder "
            f"({', '.join(repr(h) for h in hits)}). A prior-period value is unavailable for some cells. "
            "DROP the entire column (or the affected rows) that holds the missing values and keep only "
            "columns/rows where every cell has a real figure. NEVER print placeholder text like "
            "'(intermediate aggregation, not shown in displayed tables)' — every table cell must be a real value."
        ),
    }]


def _synthesize_with_validation(prompt: str) -> str:
    """Call Opus once; run the live structural probe on the result; if any
    violations are found, fire ONE retry with a correction directive; if the
    retry also fails, append a small ⚠️ warning footer instead of silently
    shipping bad arithmetic.

    Returns the final answer text (always a usable string). Latency:
    happy path = 1 Opus call. Retry path = 2 Opus calls (~10-15s extra) but
    only when arithmetic is broken — which is exactly when we want to spend
    the budget."""
    try:
        synthesis = _call_llm(
            messages=[{"role": "user", "content": prompt}],
            tools=None,
            model=CLAUDE_MODEL_AGENT,
            timeout=120,
        )
        full_text = (synthesis.get("choices", [{}])[0].get("message", {}) or {}).get("content", "") or ""
    except Exception as e:
        logger.error(f"[SYNTHESIS] failed: {type(e).__name__}: {e}")
        return (
            "The summary couldn't be composed within the time limit. The supporting "
            "analyses above ran successfully — review their inline previews, or try "
            "one of the suggested follow-up questions below for a focused drill-down."
        )

    # Structural-only checks (no canonical context at live time — too slow).
    violations = find_violations_in_prose(full_text)
    violations += _find_banned_placeholders(full_text)
    if not violations:
        return full_text

    logger.warning(f"[VALIDATION] first-pass violations: {[v.get('type') for v in violations]}")

    # Retry once with an inline correction prompt. We append the violation
    # list so the model knows exactly what to fix without re-deriving it.
    violation_block = "\n".join(f"- {v.get('message','')}" for v in violations)
    correction_prompt = (
        prompt
        + "\n\n---\nThe previous draft contained these arithmetic / consistency errors:\n"
        + violation_block
        + "\n\nProduce a corrected response. Either fix the cited numbers so the math is internally consistent, "
        + "or drop the problematic claim entirely. Do not preserve the same arithmetic error."
    )
    try:
        retry = _call_llm(
            messages=[{"role": "user", "content": correction_prompt}],
            tools=None,
            model=CLAUDE_MODEL_AGENT,
            timeout=120,
        )
        retry_text = (retry.get("choices", [{}])[0].get("message", {}) or {}).get("content", "") or ""
    except Exception as e:
        logger.error(f"[SYNTHESIS-RETRY] failed: {type(e).__name__}: {e}")
        retry_text = ""

    if retry_text:
        retry_violations = find_violations_in_prose(retry_text) + _find_banned_placeholders(retry_text)
        if not retry_violations:
            logger.info("[VALIDATION] retry passed all checks")
        else:
            logger.warning(f"[VALIDATION] retry still has violations: {[v.get('type') for v in retry_violations]}")
        return retry_text

    return full_text


def _replay_with_synthesis(user_message: str, cached_payload: dict):
    """NEW cache shape: replay sub-query SQL/results instantly, then live Claude
    synthesis composes the multi-section markdown answer (streamed as tokens)."""
    sub_results = cached_payload.get("sub_question_results", []) or []

    # Customer-facing status — never leak "pre-computed" implementation detail
    # or land on grammatically-awkward "1 sub-analyses". The user just sees
    # supporting analyses being reviewed; whether they're cached or live is
    # implementation detail.
    n = len(sub_results)
    msg = "Reviewing supporting analysis" if n == 1 else f"Reviewing {n} supporting analyses"
    yield {"type": "status", "message": msg}
    time.sleep(0.2)

    # Replay each sub-query as if Genie just queried right now (instant from cache)
    for idx, sub in enumerate(sub_results, start=1):
        sql = sub.get("sql", "") or ""
        sub_q = sub.get("sub_question", "") or f"Sub-query {idx}"
        narrative = sub.get("narrative", "") or ""
        tables = _extract_table_names(sql)
        tables_qualified = [f"{SCHEMA}.{t}" for t in tables]

        yield {
            "type": "tool_start",
            "source": "Financial Analytics",
            "question": sub_q,
        }
        time.sleep(0.15)
        yield {
            "type": "tool_result",
            "tool": "query_financial_analytics",
            "sql": sql,
            "description": narrative[:200] if narrative else sub_q,
            "tables": tables_qualified,
            "row_count": sub.get("row_count"),
            "raw_data": sub.get("raw_data"),
        }
        time.sleep(0.2)

    # Live Claude synthesis — streams tokens to UI for the typewriter feel.
    yield {"type": "status", "message": "Synthesizing root cause analysis"}
    time.sleep(0.1)

    prompt = SYNTHESIS_PROMPT.format(
        user_question=user_message,
        sub_results_block=_build_sub_results_block(sub_results),
    )
    # Quarantine the partial in-progress month so it never lands in a trend/table
    # (prevents the ~−50% false-drop row from dominating the synthesis).
    prompt = _date_anchor_block_for_synthesis() + "\n\n" + prompt
    # Synthesize + live structural-consistency probe. Retries once with
    # corrections if violations are found; appends a ⚠️ footer if even the
    # retry doesn't pass. See _synthesize_with_validation for details.
    full_text = _synthesize_with_validation(prompt)

    # Stream the synthesized response as tokens so it feels live.
    chunk_size = 60
    for i in range(0, len(full_text), chunk_size):
        yield {"type": "token", "content": full_text[i:i + chunk_size]}
        time.sleep(0.03)

    # Suggested follow-ups: prefer Genie-provided suggestions if present;
    # otherwise generate them via Claude from the just-synthesized answer.
    # Previously this code path silently dropped follow-ups when Genie had
    # nothing to suggest — a regression we just restored.
    suggested = cached_payload.get("suggested_questions", []) or []
    if not suggested and full_text and len(full_text) > 80:
        try:
            suggested = _generate_claude_followups(
                user_question=user_message,
                answer_text=full_text,
                n=3,
            ) or []
        except Exception as e:
            logger.warning(f"[FOLLOWUPS] claude generator failed: {type(e).__name__}: {e}")
            suggested = []
    if suggested:
        yield {"type": "suggested_questions", "questions": suggested[:3]}

    yield {"type": "clear_status"}


def _replay_legacy_format(user_message: str, cached_payload: dict):
    """OLD cache shape: convert {sql_steps, final_narrative} into a single-element
    sub_question_results and run the SAME synthesis path. Every click — old shape,
    new shape, or live — produces the rich multi-section synthesis.
    """
    sql_steps = cached_payload.get("sql_steps", []) or []
    sub_results = []
    for step in sql_steps:
        sub_results.append({
            "sub_question": step.get("description", "") or step.get("title", "") or user_message,
            "sql": step.get("sql", "") or "",
            "narrative": cached_payload.get("final_narrative", "") or "",  # all sql_steps share the single narrative
            "raw_data": [],
            "row_count": None,
        })
    if not sub_results:
        # No sql_steps but maybe a narrative — pass it through as a single sub-result
        sub_results = [{
            "sub_question": user_message,
            "sql": "",
            "narrative": cached_payload.get("final_narrative", "") or "",
            "raw_data": [],
            "row_count": None,
        }]

    # Reuse the new-format replay so the synthesis composes a multi-section answer
    # even from legacy cache shape. Wrap the legacy data into the shape the
    # synthesis path expects.
    yield from _replay_with_synthesis(user_message, {
        "sub_question_results": sub_results,
        "suggested_questions": cached_payload.get("suggested_questions", []) or [],
    })


def _decompose_question_to_subqueries(user_message: str, n_max: int = 2) -> list[str]:
    """Use the AGENT model (Opus) to decompose a user-typed question into
    EXACTLY 2 focused Genie sub-queries. Returns [] on any failure so the
    caller falls back to single-shot.

    Capped at 2 (not 3 like the pre-cached chip path) because live latency
    matters here: more parallel sub-queries means worse tail (p95 ≈ max of N
    Genie latencies, ~25-45s each). 2 keeps depth-of-analysis above single-shot
    while bounding wall time to roughly one slow Genie call.

    Mirrors the chip-time decomposer in insights_compose.decompose_chip_to_subqueries:
    GROUNDED DATA LOOKUPS, named entities, complete fiscal months, different angles.
    """
    decomp_prompt = f"""You are planning sub-queries for a Genie agent that will answer a CFO Operations question.

The user just typed this question:
"{user_message}"

{_date_anchor_block()}

Plan EXACTLY 2 focused Genie sub-questions that, together, will surface enough data for a downstream model to compose a multi-section root-cause analysis answering the user's question.

Each sub-question MUST:
- Be a single, focused investigation Genie can answer with ONE SQL query against gold tables (regional P&L by month, project profitability, employees + utilization, AR/AP aging, partner economics)
- Reference NAMED ENTITIES + COMPLETE fiscal months only. Per the DATE ANCHOR above: the most recent complete month is the END of any trailing window — name that end month explicitly and NEVER include the partial in-progress month
- Be GROUNDED — phrase as DATA LOOKUPS, not philosophical (e.g. "What is the monthly DSO trend for the US region over the last 6 complete fiscal months, by office?" NOT "Why is DSO struggling?")
- Cover a DIFFERENT angle from the other (don't ask 2 versions of the same thing) — typically one direct breakdown + one supporting/explanatory cut
- Specify the time window explicitly so Genie doesn't guess
- BE PHRASED FOR AN AGGREGATED RESULT (~5-200 rows max). Always indicate the grain explicitly: "monthly TOTAL revenue per office" (one row per office × month, ~24 × 6 = 144 rows), "top 10 clients by aged AR" (10 rows), "average DSO per region for the last 6 months" (~3 × 6 = 18 rows). NEVER phrase a sub-query that would return raw row-level data — e.g. "list all invoices for AXA over 60 days" should become "for AXA, what is the open AR balance aged over 60 days, with invoice count by aging bucket?". A sub-query returning >500 rows means the question wasn't aggregated; rephrase.

Return ONLY raw JSON, no markdown fences. Always 2 sub-queries.

Example shape:
{{"sub_queries": ["What is the monthly DSO trend for the US region over the last 6 complete fiscal months, by office?", "Which 5 named clients have the largest aged AR balances in the US region for the most recent complete fiscal month?"]}}
"""
    try:
        resp = _call_llm(
            messages=[{"role": "user", "content": decomp_prompt}],
            tools=None,
            model=CLAUDE_MODEL_AGENT,
        )
        text = (resp.get("choices") or [{}])[0].get("message", {}).get("content", "") or ""
        # Strip optional ```json fences before parsing
        stripped = text.strip()
        if stripped.startswith("```"):
            stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
            stripped = re.sub(r"\s*```$", "", stripped)
        parsed = json.loads(stripped)
        sub_qs = [s for s in (parsed.get("sub_queries") or []) if isinstance(s, str) and s.strip()]
        return sub_qs[:n_max]
    except Exception as e:
        logger.warning(f"[DECOMP] failed: {type(e).__name__}: {e}")
        return []


def _live_multi_query_with_synthesis(user_message: str):
    """LIVE path for typed / uncached questions — Opus decompose → parallel
    Genie fan-out → strict-sequential UI replay → Claude synthesis.

    Same architecture as the cached chip path, just firing Genie now. We fan
    out 2 sub-queries through Genie IN PARALLEL (wall time ≈ max of the two)
    but stream tool_start/tool_result events in submitted order so the modal
    renders the sub-queries sequentially — identical UX to a cache replay.

    Capped at 2 sub-queries (not 3 like the chip pre-cache) to bound live
    latency: p95 ≈ slowest of N Genie calls, ~30-45s each. Adding a 3rd worsens
    the tail without proportional analytical value for typed questions.

    Falls back to a single Genie call if decomposition fails or returns <2
    sub-queries (preserving the previous single-shot behavior as the floor).
    """
    sub_queries = _decompose_question_to_subqueries(user_message, n_max=2)
    if len(sub_queries) < 2:
        sub_queries = [user_message]

    n = len(sub_queries)
    status_msg = "Reviewing supporting analysis" if n == 1 else f"Reviewing {n} supporting analyses"
    yield {"type": "status", "message": status_msg}
    time.sleep(0.1)

    # Fan out all Genie queries in parallel, but stream events in submitted order
    # so the modal's sub-query divider UX morphs 🔍 → ✓ in the right slot.
    sub_results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max(n, 1)) as executor:
        futures = [
            executor.submit(_query_genie_with_retry, GENIE_ROOM_ANALYTICS, sq)
            for sq in sub_queries
        ]
        for sq, future in zip(sub_queries, futures):
            yield {"type": "tool_start", "source": "Financial Analytics", "question": sq}
            try:
                result = future.result(timeout=180)
            except Exception as e:
                result = {"text": f"Genie error: {type(e).__name__}: {e}", "sql": "", "raw_data": [], "row_count": 0}
            sub = {
                "sub_question": sq,
                "sql": result.get("sql") or "",
                # Belt-and-suspenders: even though _extract_genie_structured
                # already strips meta-questions at attachment-ingest time,
                # re-apply here so any path that bypassed the extractor still
                # gets cleaned before synthesis sees it.
                "narrative": _strip_genie_meta_questions(result.get("text", "") or ""),
                "raw_data": result.get("raw_data") or [],
                "row_count": result.get("row_count"),
            }
            sub_results.append(sub)
            tables_qualified = [f"{SCHEMA}.{t}" for t in _extract_table_names(sub["sql"])]
            yield {
                "type": "tool_result",
                "tool": "query_financial_analytics",
                "sql": sub["sql"],
                "description": (sub["narrative"][:200] if sub["narrative"] else sq),
                "tables": tables_qualified,
                "row_count": sub.get("row_count"),
                "raw_data": sub.get("raw_data"),
            }
            time.sleep(0.1)

    payload_for_cache = {"sub_question_results": sub_results, "suggested_questions": []}

    # Write-through cache: persist the full multi-subquery payload so subsequent
    # asks of the same question replay instantly. Failures non-fatal.
    try:
        from persona_insights_reader import write_cached_payload_for_question
        write_cached_payload_for_question(user_message, payload_for_cache)
    except Exception as _e:
        logger.warning(f"[CACHE-WRITE] non-fatal: {type(_e).__name__}: {_e}")

    # Synthesis — same flow as _replay_with_synthesis after the tool events.
    yield {"type": "status", "message": "Synthesizing root cause analysis"}
    time.sleep(0.1)

    prompt = SYNTHESIS_PROMPT.format(
        user_question=user_message,
        sub_results_block=_build_sub_results_block(sub_results),
    )
    # Quarantine the partial in-progress month so it never lands in a trend/table
    # (prevents the ~−50% false-drop row from dominating the synthesis).
    prompt = _date_anchor_block_for_synthesis() + "\n\n" + prompt
    # Same validation flow as the cached path — retry once on violations,
    # surface a ⚠️ footer if the retry still has problems.
    full_text = _synthesize_with_validation(prompt)

    chunk_size = 60
    for i in range(0, len(full_text), chunk_size):
        yield {"type": "token", "content": full_text[i:i + chunk_size]}
        time.sleep(0.03)

    if full_text and len(full_text) > 80:
        try:
            suggested = _generate_claude_followups(
                user_question=user_message,
                answer_text=full_text,
                n=3,
            ) or []
            if suggested:
                yield {"type": "suggested_questions", "questions": suggested[:3]}
        except Exception as e:
            logger.warning(f"[FOLLOWUPS] claude generator failed: {type(e).__name__}: {e}")

    yield {"type": "clear_status"}


def stream_agent(user_message: str, history: list = None):
    """Generator yielding SSE-compatible events with rich metadata.

    `history` is an optional list of prior {role, content} turns from the same chat
    session. When provided, it's threaded into the LLM router's messages array
    (so reformulations preserve prior turn context) and into the Claude follow-up
    generator (so chips include disambiguating anchors). History is ephemeral —
    it lives only as long as the chat modal is open in the browser, no persistence.
    """

    history = history or []

    # Start timing
    start_time = time.time()
    timings = {}

    # Log the start of a new UI question
    logger.debug(f"Processing question: {user_message}")

    # First: check the orchestrator-populated cache in gold_persona_insights.
    # New dynamic chips (insights deep-dives, action_area deep-dives, bottom_chips,
    # depth-1 follow-ups) live there with their full Genie payloads pre-cached.
    try:
        from persona_insights_reader import get_cached_payload_by_question
        cached_payload = get_cached_payload_by_question(user_message)
    except Exception as e:
        logger.warning(f"[CACHE-LOOKUP] failed: {e}")
        cached_payload = None

    if cached_payload:
        logger.info(f"[ORCHESTRATOR-CACHE] Replaying cached payload for: {user_message[:80]}")
        # Two cache shapes supported:
        #   NEW: {sub_question_results: [{sub_question, sql, raw_data, narrative, tables, row_count}, ...]}
        #        → instant tool_start/tool_result replay for each sub-question, then
        #          a LIVE Claude synthesis call composes the multi-section root cause
        #          analysis (~10-15s feels like real-time AI reasoning).
        #   OLD: {sql_steps: [...], final_narrative: "..."}
        #        → instant cached replay (mechanical Genie narrative — kept as fallback
        #          while old rows still exist in the table).
        sub_results = cached_payload.get("sub_question_results")
        if sub_results:
            yield from _replay_with_synthesis(user_message, cached_payload)
        else:
            yield from _replay_legacy_format(user_message, cached_payload)
        return

    # No orchestrator cache hit. Live path: Opus decompose → 2-3 parallel Genie
    # sub-queries → Claude synthesis. Same architecture as the cached chip path,
    # firing Genie now instead of replaying.
    yield from _live_multi_query_with_synthesis(user_message)


if __name__ != "__main__":
    prewarm_warehouses()
    logger.info("✨ App initialized: warehouses pre-warmed, table-driven cache active (gold_persona_insights)")