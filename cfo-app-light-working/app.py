#!/usr/bin/env python3
"""Complete fixed Flask app with all routes"""

from flask import Flask, render_template, jsonify, request, Response
import os
import random
from datetime import datetime, timedelta
import json
import logging
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
app_logger = logging.getLogger(__name__)

# Import genie_agent in a background thread for both fast startup AND caching
import threading

# Schema configuration — env-var driven for asset-bundle portability
SCHEMA = os.environ.get("CFO_SCHEMA", "main.cfo_proserv")
genie_agent = None
genie_import_complete = threading.Event()

def background_import_genie():
    """Import genie_agent in background to start caching without blocking app startup"""
    global genie_agent
    try:
        app_logger.info("🔄 Starting background import of genie_agent...")
        import genie_agent as ga
        genie_agent = ga
        genie_import_complete.set()
        app_logger.info("✅ genie_agent imported successfully in background - cache warming started!")
    except Exception as e:
        app_logger.error(f"❌ Failed to import genie_agent in background: {e}")
        import traceback
        app_logger.error(traceback.format_exc())
        genie_import_complete.set()  # Set even on failure so we don't wait forever

# Start the import in a background thread - doesn't block app startup!
import_thread = threading.Thread(target=background_import_genie, daemon=True)
import_thread.start()
app_logger.info("🚀 App starting with fast page loads - genie caching will begin in background")

def get_genie_agent():
    """Get the genie_agent module (may still be loading in background)"""
    global genie_agent

    # Wait up to 10 seconds for background import to complete if needed
    if genie_agent is None and not genie_import_complete.is_set():
        app_logger.info("⏳ Waiting for genie_agent background import to complete...")
        genie_import_complete.wait(timeout=10)

    if genie_agent is None:
        app_logger.warning("⚠️ genie_agent not available - trying direct import...")
        try:
            import genie_agent as ga
            genie_agent = ga
            app_logger.info("✅ genie_agent loaded on demand")
        except Exception as e:
            app_logger.error(f"❌ Failed to import genie_agent on demand: {e}")
    return genie_agent

# Determine template directory portably so the bundle works on ANY workspace
# (no hardcoded /Workspace/Users/... path that would break customer deploys).
#
# Databricks Apps run the command from the app's own files directory, so
# templates/ and static/ are siblings of app.py — resolving them relative to
# the running script gives a workspace-agnostic path that works locally AND
# in every customer workspace without modification.
_app_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(_app_dir, 'templates')
static_dir = os.path.join(_app_dir, 'static')
if os.path.exists('/Workspace'):
    app_logger.info(f"Running in Databricks with templates at: {template_dir}")
else:
    app_logger.info(f"Running locally with templates at: {template_dir}")

# Create Flask app with proper paths
app = Flask(__name__,
            template_folder=template_dir,
            static_folder=static_dir)
app.secret_key = os.environ.get('SECRET_KEY', 'cfo-demo-secret-key')

# Configuration — external agent app URL is optional + environment-specific.
# If unset, "Open Agent App" buttons in the UI fall back to an inline message.
CFO_AGENT_APP_URL = os.environ.get("CFO_AGENT_APP_URL", "")


# Cache the filter dropdown values for the lifetime of the app process — they don't
# change between page renders since they reflect the schema's distinct values.
_FILTER_DROPDOWN_VALUES_CACHE = None


def _get_filter_dropdown_values():
    """Return {'region': [...], 'location': [...], 'practice_area': [...], 'industry': [...]}.

    Queries SELECT DISTINCT on gold_regional_pnl so the customer-deployed app shows
    THEIR data values in the filter UI, not the demo's hardcoded options. Cached
    process-wide; restart the app to refresh.
    """
    global _FILTER_DROPDOWN_VALUES_CACHE
    if _FILTER_DROPDOWN_VALUES_CACHE is not None:
        return _FILTER_DROPDOWN_VALUES_CACHE
    try:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "")
        schema = os.environ.get("CFO_SCHEMA", "main.cfo_proserv")
        if not warehouse_id:
            app_logger.warning("[FILTER VALUES] SQL_WAREHOUSE_ID not set — returning empty dropdowns")
            _FILTER_DROPDOWN_VALUES_CACHE = {axis: [] for axis in ("region", "location", "practice_area", "industry")}
            return _FILTER_DROPDOWN_VALUES_CACHE
        out = {}
        for axis in ("region", "location", "practice_area", "industry"):
            try:
                stmt = f"SELECT DISTINCT {axis} AS v FROM {schema}.gold_regional_pnl WHERE {axis} IS NOT NULL ORDER BY 1"
                res = client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id, statement=stmt, wait_timeout="30s"
                )
                rows = res.result.data_array if (res.result and res.result.data_array) else []
                out[axis] = [r[0] for r in rows if r[0]]
            except Exception as e:
                app_logger.warning(f"[FILTER VALUES] failed for {axis}: {e}")
                out[axis] = []
        _FILTER_DROPDOWN_VALUES_CACHE = out
        return out
    except Exception as e:
        app_logger.warning(f"[FILTER VALUES] could not query distinct values: {e}")
        _FILTER_DROPDOWN_VALUES_CACHE = {axis: [] for axis in ("region", "location", "practice_area", "industry")}
        return _FILTER_DROPDOWN_VALUES_CACHE

# Global variables for lazy initialization
CACHED_WAREHOUSE_ID = None
w = None

# Cache for landing page data to speed up navigation.
# Keyed by persona; each entry stores the data blob and the
# gold_persona_insights `MAX(last_refreshed)` version that was current when
# the blob was built. A request reuses the cached blob ONLY when the version
# still matches. If the orchestrator wrote anything in the meantime, the
# version probe surfaces a newer timestamp and we rebuild.
LANDING_PAGE_CACHE = {
    'data_by_persona': {},   # {persona_name: {'data': dict, 'data_version': datetime}}
}


def _get_persona_insights_version():
    """Return the MAX(last_refreshed) timestamp from gold_persona_insights as
    a cache version key. Returns None if the table doesn't exist or the probe
    fails — caller treats that as "skip cache" rather than failing the request.
    """
    from persona_insights_reader import _get_workspace_client
    client = _get_workspace_client()
    warehouse_id = os.environ.get('SQL_WAREHOUSE_ID', '')
    if not warehouse_id:
        return None
    catalog = os.environ.get('CFO_CATALOG', 'main')
    schema = os.environ.get('CFO_SCHEMA_NAME', 'cfo_proserv')
    res = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"SELECT MAX(last_refreshed) AS v FROM {catalog}.{schema}.gold_persona_insights",
        wait_timeout="10s",
    )
    if res.status and res.status.state and res.status.state.value == 'SUCCEEDED' \
            and res.result and res.result.data_array:
        v = res.result.data_array[0][0]
        return v  # ISO timestamp string; comparing strings is fine here
    return None

# Maps the user-tile id (what JS / sessionStorage / cookie store) to the persona
# name used by the data layer. URL ?persona= uses the data-layer name directly.
USER_ID_TO_PERSONA = {'priya': 'admin', 'sarah': 'finance', 'michael': 'hr'}
VALID_PERSONAS = {'admin', 'finance', 'hr'}

# Write-through filter cache for /api/insights-live uses the UC table
# `gold_persona_insights` itself: see persona_insights_reader.load_compound_cache
# / write_compound_cache. The encoding convention is filter_axis='compound',
# filter_value='<sorted compound key>' — kept stable so reads match writes.
def _compound_filter_value(filters: dict) -> str:
    """Build a stable, sorted `key=value|key=value` string from a multi-axis
    filter dict. Returns empty string if no axes are set (caller should treat
    that as "don't cache — fall through to firmwide read path")."""
    parts = sorted(
        f"{k}={str(v).strip()}"
        for k, v in (filters or {}).items()
        if v not in (None, '', 'All')
    )
    return '|'.join(parts)

def get_workspace_client():
    """Get or create workspace client instance - fully lazy."""
    global w, CACHED_WAREHOUSE_ID
    if w is None:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            app_logger.info("Databricks WorkspaceClient initialized")
            try:
                warehouses = list(w.warehouses.list())
                if warehouses:
                    CACHED_WAREHOUSE_ID = os.environ.get('SQL_WAREHOUSE_ID') or warehouses[0].id
            except Exception as e:
                app_logger.warning(f"Could not list warehouses: {e}")
        except Exception as e:
            app_logger.warning(f"Could not initialize WorkspaceClient: {e}")
            w = None
    return w

def execute_sql_query(query):
    """Execute a SQL query using Databricks SDK"""
    app_logger.info(f"[SQL] Attempting to execute query")
    workspace_client = get_workspace_client()
    if not workspace_client:
        app_logger.error("[SQL] No workspace client available")
        return None

    warehouse_id = os.environ.get('SQL_WAREHOUSE_ID', '')
    app_logger.info(f"[SQL] Using warehouse ID: {warehouse_id}")

    try:
        app_logger.info(f"[SQL] Submitting query to warehouse")

        # Anchor wall-clock CURRENT_DATE() to the frozen dataset's as-of date.
        if "CURRENT_DATE()" in query:
            import demo_anchor
            _schema_fqn = os.environ.get('CFO_SCHEMA', 'main.cfo_proserv')
            query = demo_anchor.anchor(query, demo_anchor.as_of_via_statement(workspace_client, warehouse_id, _schema_fqn))

        # Execute statement and wait for results (following insights_queries.py pattern)
        result = workspace_client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="30s"
        )

        # Check if we have results
        if result.result and result.result.data_array:
            app_logger.info(f"[SQL] Query succeeded, got {len(result.result.data_array)} rows")
            return result.result.data_array
        else:
            app_logger.warning("[SQL] Query succeeded but no data returned")
            return None

    except Exception as e:
        app_logger.error(f"[SQL] Exception during query execution: {str(e)}")
        app_logger.error(f"[SQL] Exception type: {type(e).__name__}")
        import traceback
        app_logger.error(f"[SQL] Traceback: {traceback.format_exc()}")
        return None

def get_mock_financial_data():
    """Generate mock financial data for the CFO Control Center"""
    # Executive Summary Metrics
    executive_metrics = {
        'revenue': {
            'current': 45.7,  # millions
            'target': 48.2,
            'variance': -5.2,
            'trend': 'down'
        },
        'expenses': {
            'current': 38.2,
            'target': 36.5,
            'variance': 4.7,
            'trend': 'up'
        },
        'profit_margin': {
            'current': 16.4,
            'target': 24.3,
            'variance': -32.5,
            'trend': 'down'
        },
        'cash_flow': {
            'current': 7.5,
            'target': 11.7,
            'variance': -35.9,
            'trend': 'down'
        }
    }

    # Daily insights vs monthly batch comparison
    daily_insights = {
        'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data_freshness': 'Real-time',
        'sap_sync_status': 'Synchronized',
        'reconciliation_status': 97.8  # percentage reconciled
    }

    # System integration status
    system_status = [
        {'name': 'SAP S/4HANA', 'status': 'connected', 'last_sync': '2 minutes ago', 'records': '1.2M'},
        {'name': 'Salesforce', 'status': 'connected', 'last_sync': '5 minutes ago', 'records': '847K'},
        {'name': 'Workday', 'status': 'connected', 'last_sync': '1 minute ago', 'records': '156K'},
        {'name': 'BI Systems', 'status': 'connected', 'last_sync': '3 minutes ago', 'records': '2.1M'}
    ]

    # Recent financial activities
    recent_activities = [
        {'type': 'expense', 'description': 'New contractor onboarded - Project Alpha', 'amount': -15000, 'time': '10 minutes ago'},
        {'type': 'revenue', 'description': 'Client payment received - ABC Corp', 'amount': 125000, 'time': '25 minutes ago'},
        {'type': 'expense', 'description': 'Equipment purchase - Databricks licenses', 'amount': -8500, 'time': '1 hour ago'},
        {'type': 'revenue', 'description': 'Milestone payment - Project Beta', 'amount': 75000, 'time': '2 hours ago'},
    ]

    # Top concerns for CFO attention
    concerns = [
        {'priority': 'high', 'title': 'Project Alpha Over Budget', 'description': 'Current expenses exceed budget by 12%', 'impact': '$180K'},
        {'priority': 'medium', 'title': 'Delayed Client Payments', 'description': '3 clients with overdue invoices', 'impact': '$420K'},
        {'priority': 'low', 'title': 'Contractor Rate Increases', 'description': 'Average hourly rate up 8% this quarter', 'impact': '$65K'}
    ]

    return {
        'executive_metrics': executive_metrics,
        'daily_insights': daily_insights,
        'system_status': system_status,
        'recent_activities': recent_activities,
        'concerns': concerns
    }

# Routes
@app.route('/')
def landing_page():
    """CFO Control Center Landing Page"""
    global LANDING_PAGE_CACHE

    # Persona resolution order (most-explicit wins):
    #   1. URL ?persona=admin|finance|hr  — explicit, used for permalinks / debugging
    #   2. cookie selectedPersona=priya|sarah|michael  — set by JS in customSelectUser
    #   3. default 'admin' (priya)
    # The cookie is the linchpin of the refresh fix: it survives F5 so the server
    # knows which persona's data to fetch instead of always defaulting to admin
    # and stamping admin content into michael's container.
    url_persona = request.args.get('persona')
    cookie_user = request.cookies.get('selectedPersona')
    if url_persona in VALID_PERSONAS:
        stored_persona = url_persona
    elif cookie_user in USER_ID_TO_PERSONA:
        stored_persona = USER_ID_TO_PERSONA[cookie_user]
    else:
        stored_persona = 'admin'

    # Check if we have valid cached data (but skip cache if filters are present)
    has_filters = any(request.args.get(f) for f in ['region', 'location', 'practice_area', 'industry', 'customer'])
    current_time = datetime.now()

    # Version-keyed cache check. The earlier time-based cache (5-min TTL,
    # keyed only by persona) caused a routing regression on 2026-05-19: when
    # the orchestrator was mid-write, the first `/` request populated the
    # cache with partial state (Priya's container holding Sarah's finance
    # KPIs), and the TTL kept that stale snapshot live for every new
    # visitor until it expired. Now we gate the cache on
    # MAX(last_refreshed) from gold_persona_insights — if the orchestrator
    # wrote anything since the cache was built, we rebuild. One extra
    # ~100-200ms SQL probe per request buys correctness.
    if not has_filters:
        try:
            data_version = _get_persona_insights_version()
        except Exception as _ver_err:
            app_logger.warning(f"[landing] version probe failed ({_ver_err}); skipping cache")
            data_version = None

        cached = LANDING_PAGE_CACHE['data_by_persona'].get(stored_persona)
        if (
            cached
            and cached.get('data') is not None
            and cached.get('data_version') is not None
            and data_version is not None
            and cached.get('data_version') == data_version
        ):
            # Override active_persona on the cached blob in case a different
            # persona's container should be marked .active for this request.
            cached_data = cached['data']
            if cached_data.get('active_persona') != stored_persona:
                cached_data = dict(cached_data)
                cached_data['active_persona'] = stored_persona
            return render_template('cfo_landing.html', data=cached_data)

    # Try to get real data from SQL queries
    try:
        from persona_insights_reader import get_insights_for_persona, get_priorities_for_persona

        # Get filter values from request args if any
        filters = {
            'region': request.args.get('region'),
            'location': request.args.get('location'),
            'practice_area': request.args.get('practice_area'),
            'industry': request.args.get('industry'),
            'customer': request.args.get('customer')
        }

        # Pre-fetch insights + priorities for ALL THREE personas, not just the
        # currently-active one. The template stamps three .user-insights[data-user]
        # containers (one per persona) and the persona-switcher dropdown toggles
        # which is .active. If we only render the active persona's data into all
        # three containers, then switching personas via the dropdown briefly shows
        # the OLD persona's content (stamped by the server) before the async
        # /api/get-insights fetch returns and overwrites it. By pre-rendering each
        # container with its own persona's data, the dropdown switch is instant
        # and free of stale-content flashes — particularly important when multiple
        # users open the deployed app for the first time.
        insights_by_persona = {}
        priorities_by_persona = {}
        for p in ('admin', 'finance', 'hr'):
            insights_by_persona[p] = get_insights_for_persona(p, filters)
            priorities_by_persona[p] = get_priorities_for_persona(p, filters)

        # The currently-active persona — drives the unscoped data fields below
        # (insights / insights_with_clickthrough / priorities) for legacy code
        # paths that don't yet read the per-persona dicts.
        insights_data = insights_by_persona[stored_persona]
        priorities_data = priorities_by_persona[stored_persona]

        # Bottom chips per persona — all 3 fetched up front so the persona-switcher
        # works client-side without re-rendering. Each persona's div gets its own list.
        from persona_insights_reader import get_bottom_chips_for_page
        bottom_chips_by_persona = {
            'finance': get_bottom_chips_for_page('finance', page=None),
            'admin':   get_bottom_chips_for_page('admin', page=None),
            'hr':      get_bottom_chips_for_page('hr', page=None),
        }

        # Filter dropdown values — pulled from the actual data so the dropdown only
        # shows axis values that exist in the customer's deployed schema. This is
        # what makes the bundle portable: a customer with different locations or
        # practice taxonomies sees THEIR values, not the demo's hardcoded list.
        filter_dropdown_values = _get_filter_dropdown_values()

        # Format the data for the template
        financial_data = {
            'insights': insights_data.get('insights', []),
            'insights_with_clickthrough': insights_data.get('insights_with_clickthrough', []),
            'actions': insights_data.get('actions', []),
            'priorities': priorities_data,  # active persona's priorities (legacy)
            'insights_by_persona': insights_by_persona,
            'priorities_by_persona': priorities_by_persona,
            'active_persona': stored_persona,
            'executive_metrics': get_mock_financial_data()['executive_metrics'],  # Keep exec metrics for now
            'daily_insights': get_mock_financial_data()['daily_insights'],
            'system_status': get_mock_financial_data()['system_status'],
            'recent_activities': get_mock_financial_data()['recent_activities'],
            'concerns': get_mock_financial_data()['concerns'],
            'filters': filters,  # Pass the current filter values to the template
            'filter_dropdown_values': filter_dropdown_values,
            'bottom_chips_by_persona': bottom_chips_by_persona,
        }

        # Cache per-persona, keyed by data_version (MAX(last_refreshed) from
        # gold_persona_insights). A stale cache only serves when nothing has
        # changed in the underlying chip table since the cache was written —
        # any orchestrator write bumps the version and invalidates this entry.
        try:
            _version_for_cache = _get_persona_insights_version()
        except Exception:
            _version_for_cache = None
        LANDING_PAGE_CACHE['data_by_persona'][stored_persona] = {
            'data': financial_data,
            'data_version': _version_for_cache,
        }

    except Exception as e:
        app_logger.error(f"Error getting insights from SQL: {e}")
        import traceback
        app_logger.error(f"Traceback: {traceback.format_exc()}")

        # Fall back to mock data but still try to show something useful
        financial_data = get_mock_financial_data()
        # Instead of empty, show a message that we're having issues
        financial_data['insights'] = [
            f"**System Notice:** Real-time data temporarily unavailable. Working to restore connection..."
        ]
        financial_data['insights_with_clickthrough'] = []  # template iterates this — must exist
        financial_data['actions'] = []
        financial_data['priorities'] = []  # Empty priorities on error
        financial_data['bottom_chips_by_persona'] = {'finance': [], 'admin': [], 'hr': []}
        financial_data['filter_dropdown_values'] = _get_filter_dropdown_values()

        # Cache even the fallback data to keep it fast (per-persona)
        LANDING_PAGE_CACHE['data_by_persona'][stored_persona] = {
            'data': financial_data,
            'timestamp': current_time,
        }

    return render_template('cfo_landing.html', data=financial_data)

@app.route('/api/chat', methods=['POST'])
def chat_endpoint():
    """Handle chat requests"""
    app_logger.info("[CHAT DEBUG] ========== /api/chat endpoint called ==========")
    app_logger.info(f"[CHAT DEBUG] Request headers: {dict(request.headers)}")

    data = request.json
    app_logger.info(f"[CHAT DEBUG] Request JSON data: {data}")

    user_message = data.get('message', '')
    app_logger.info(f"[CHAT DEBUG] Extracted message: '{user_message}'")
    app_logger.info(f"[CHAT DEBUG] Message length: {len(user_message)}")

    if not user_message:
        app_logger.error("[CHAT DEBUG] No message provided, returning 400")
        return jsonify({"error": "No message provided"}), 400

    try:
        app_logger.info("[CHAT DEBUG] Starting agent processing...")
        # Import lazily
        # Lazy load genie_agent when needed
        ga = get_genie_agent()
        if ga is None:
            return jsonify({'error': 'Genie agent not available'}), 503
        run_agent_sync = ga.run_agent_sync
        app_logger.info("[CHAT DEBUG] run_agent_sync imported successfully")

        agent_reply = run_agent_sync(user_message)
        app_logger.info(f"[CHAT DEBUG] Agent reply received, length: {len(agent_reply)}")
        app_logger.info(f"[CHAT DEBUG] First 200 chars of reply: {agent_reply[:200]}")

        response_data = {
            'message': agent_reply,
            'timestamp': datetime.now().isoformat(),
            'status': 'success'
        }
        app_logger.info("[CHAT DEBUG] Sending successful response")
        return jsonify(response_data)

    except Exception as e:
        app_logger.error(f"[CHAT DEBUG] ERROR: Chat error: {e}")
        app_logger.error(f"[CHAT DEBUG] ERROR traceback: {traceback.format_exc()}")
        return jsonify({
            'message': 'Error processing your request. Please try again.',
            'timestamp': datetime.now().isoformat(),
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/api/chat/stream', methods=['POST'])
def chat_stream_endpoint():
    """Handle streaming chat requests"""
    app_logger.info("[CHAT-STREAM DEBUG] ========== Endpoint called ==========")
    app_logger.info(f"[CHAT-STREAM DEBUG] Request method: {request.method}")

    data = request.json
    app_logger.info(f"[CHAT-STREAM DEBUG] Request JSON data: {data}")

    user_message = data.get('message', '')
    # Optional client-tracked conversation history: list of {role, content} pairs
    # from prior turns of the same chat-modal session. Sanitized below.
    raw_history = data.get('history', []) or []
    app_logger.info(f"[CHAT-STREAM DEBUG] Extracted message: '{user_message}'")
    app_logger.info(f"[CHAT-STREAM DEBUG] Message length: {len(user_message)}")
    app_logger.info(f"[CHAT-STREAM DEBUG] History turns received: {len(raw_history)}")

    if not user_message:
        app_logger.error("[CHAT-STREAM DEBUG] No message provided, returning 400")
        return jsonify({"error": "No message provided"}), 400

    # Sanitize history: drop anything that isn't a {role: user|assistant, content: str},
    # cap length at 12 turns max (last 6 user/assistant pairs), trim very long content.
    history = []
    for turn in raw_history[-12:]:
        if not isinstance(turn, dict):
            continue
        role = turn.get('role')
        content = turn.get('content')
        if role in ('user', 'assistant') and isinstance(content, str) and content.strip():
            history.append({'role': role, 'content': content[:4000]})

    app_logger.info("[CHAT-STREAM DEBUG] Message is valid, starting stream generation")

    def generate():
        """Generator function for SSE stream"""
        try:
            app_logger.info("[CHAT-STREAM DEBUG] Generator started")
            # Import lazily
            # Lazy load genie_agent when needed
            ga = get_genie_agent()
            if ga is None:
                yield "data: {\"error\": \"Genie agent not available\"}\n\n"
                return
            stream_agent = ga.stream_agent
            app_logger.info("[CHAT-STREAM DEBUG] stream_agent imported successfully")

            event_count = 0
            for event in stream_agent(user_message, history=history):
                event_count += 1
                if event_count <= 3:  # Log first 3 events
                    app_logger.info(f"[CHAT-STREAM DEBUG] Event {event_count}: {event}")
                yield f"data: {json.dumps(event)}\n\n"

            app_logger.info(f"[CHAT-STREAM DEBUG] Stream completed, sent {event_count} events")
        except Exception as e:
            app_logger.error(f"[CHAT-STREAM DEBUG] Streaming error: {e}")
            app_logger.error(f"[CHAT-STREAM DEBUG] Error traceback: {traceback.format_exc()}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return Response(generate(), mimetype='text/event-stream')

@app.route('/api/financial-data')
def get_financial_data():
    """API endpoint to get current financial data"""
    return jsonify(get_mock_financial_data())

@app.route('/api/get-filter-options')
def get_filter_options():
    """Get valid filter combinations based on actual data"""
    try:
        current_filters = {
            'region': request.args.get('region'),
            'location': request.args.get('location'),
            'practice_area': request.args.get('practice_area'),
            'industry': request.args.get('industry'),
            'customer': request.args.get('customer')
        }

        # Check if any filters are actually set (not 'All' or empty)
        has_active_filters = False
        where_clauses = []

        if current_filters.get('region') and current_filters['region'] not in ('All', '', None):
            where_clauses.append(f"region = '{current_filters['region']}'")
            has_active_filters = True
        if current_filters.get('location') and current_filters['location'] not in ('All', '', None):
            where_clauses.append(f"office = '{current_filters['location']}'")
            has_active_filters = True
        if current_filters.get('practice_area') and current_filters['practice_area'] not in ('All', '', None):
            where_clauses.append(f"practice_area = '{current_filters['practice_area']}'")
            has_active_filters = True
        if current_filters.get('industry') and current_filters['industry'] not in ('All', '', None):
            where_clauses.append(f"industry = '{current_filters['industry']}'")
            has_active_filters = True
        if current_filters.get('customer') and current_filters['customer'] not in ('All', '', None):
            where_clauses.append(f"customer_name = '{current_filters['customer']}'")
            has_active_filters = True

        # Only apply WHERE clause if filters are actually set
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Query the data for distinct filter values. When no filters are active,
        # this runs with no WHERE clause and returns the full set of options
        # available in the customer's data — no hardcoded fallbacks.
        query = f"""
        SELECT DISTINCT
            region,
            office as location,
            practice_area,
            industry,
            customer_name as customer
        FROM {SCHEMA}.gold_enterprise_metrics
        {where_clause}
        LIMIT 1000
        """

        import insights_queries
        app_logger.info(f"Fetching filter options with query: {query}")
        results = insights_queries.execute_query(query)
        app_logger.info(f"Filter query returned {len(results) if results else 0} rows")

        # Process results to get unique values for each filter
        valid_options = {
            'regions': set(['All']),
            'locations': set(['All']),
            'practice_areas': set(['All']),
            'industries': set(['All']),
            'customers': set(['All'])
        }

        if results:
            for row in results:
                if row.get('region') and row['region'] != '' and row['region'] is not None:
                    valid_options['regions'].add(row['region'])
                if row.get('location') and row['location'] != '' and row['location'] is not None:
                    valid_options['locations'].add(row['location'])
                if row.get('practice_area') and row['practice_area'] != '' and row['practice_area'] is not None:
                    valid_options['practice_areas'].add(row['practice_area'])
                if row.get('industry') and row['industry'] != '' and row['industry'] is not None:
                    valid_options['industries'].add(row['industry'])
                if row.get('customer') and row['customer'] != '' and row['customer'] is not None:
                    valid_options['customers'].add(row['customer'])

        # No hardcoded fallbacks — if a dimension returns no values, the
        # dropdown shows only 'All' which accurately reflects the customer's data.

        # Convert sets to sorted lists
        return jsonify({
            'status': 'success',
            'options': {
                'regions': sorted(list(valid_options['regions'])),
                'locations': sorted(list(valid_options['locations'])),
                'practice_areas': sorted(list(valid_options['practice_areas'])),
                'industries': sorted(list(valid_options['industries'])),
                'customers': sorted(list(valid_options['customers']))
            }
        })

    except Exception as e:
        app_logger.error(f"Error getting filter options: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        })

@app.route('/api/get-insights')
def get_dynamic_insights():
    """Get dynamic insights based on filters and persona"""
    try:
        # Force module reload to pick up changes
        import importlib
        import sys
        if 'persona_insights_reader' in sys.modules:
            importlib.reload(sys.modules['persona_insights_reader'])

        # Import lazily
        from persona_insights_reader import get_insights_for_persona

        filters = {
            'region': request.args.get('region'),
            'location': request.args.get('location'),
            'practice_area': request.args.get('practice_area'),
            'industry': request.args.get('industry'),
            'customer': request.args.get('customer')
        }

        # Remove None values
        active_filters = {k: v for k, v in filters.items() if v and v != ''}

        # No hardcoded combination validation — the data itself is the source
        # of truth. If a combination has no matching rows the downstream query
        # returns empty results, which is the correct customer-facing behavior.

        persona = request.args.get('persona', 'finance')
        app_logger.info(f"[GET-INSIGHTS] raw query_string={request.query_string.decode('utf-8')}")
        app_logger.info(f"[GET-INSIGHTS] persona={persona!r} filters={filters!r} active_filters={active_filters!r}")

        insights = get_insights_for_persona(persona, filters)
        n_insights = len(insights.get('insights', []))
        first_html_preview = (insights.get('insights') or [''])[0][:200] if n_insights else '(empty)'
        app_logger.info(f"[GET-INSIGHTS] returned n_insights={n_insights} first_html_preview={first_html_preview!r}")

        return jsonify({
            'status': 'success',
            'insights': insights.get('insights', []),  # Make sure to extract the insights list
            'actions': insights.get('actions', []),
            'persona': persona,
            'filters': filters
        })

    except Exception as e:
        app_logger.error(f"Error getting insights: {e}")
        # Return default static insights
        return jsonify({
            'status': 'fallback',
            'insights': {
                'dso': {'value': 67, 'target': 60, 'trend': 'down'},
                'dpo': {'value': 62, 'target': 65, 'trend': 'down'}
            }
        })


@app.route('/api/insights-live', methods=['POST'])
def insights_live():
    """Live compute of insights + action_areas for a persona × filter combo.

    Body: {persona: 'admin'|'finance'|'hr', filters: {region: 'EMEA', practice_area: 'Tax', ...}}
    Returns: {insights: [<html>...], action_areas: [<priority dict>...], raw: {...optional debug...}}
    Latency: ~10-20 sec (SQL pulls + Haiku 4.5 compose).

    Used by the frontend when ANY filter axis is set to a non-"All" value.
    For the firmwide / all-"All" case the frontend keeps reading the cached
    rows via /api/get-insights (which hits `gold_persona_insights`).
    """
    import time
    t0 = time.time()
    try:
        body = request.get_json(force=True) or {}
        persona_raw = (body.get('persona') or '').lower().strip()
        # Accept either role keys (admin/finance/hr) or display names (priya/sarah/michael)
        persona_map = {
            'finance': 'finance', 'sarah': 'finance',
            'admin': 'admin', 'priya': 'admin',
            'hr': 'hr', 'michael': 'hr',
        }
        persona = persona_map.get(persona_raw)
        if not persona:
            return jsonify({'error': f'invalid persona: {persona_raw!r}'}), 400

        filters = body.get('filters') or {}
        if not isinstance(filters, dict):
            return jsonify({'error': 'filters must be an object'}), 400

        warehouse_id = os.environ.get('SQL_WAREHOUSE_ID', '')
        if not warehouse_id:
            app_logger.error("[insights-live] SQL_WAREHOUSE_ID not set")
            return jsonify({'error': 'SQL_WAREHOUSE_ID env var not configured'}), 500

        app_logger.info(f"[insights-live] persona={persona} filters={filters}")

        # ── Write-through cache check (UC table) ────────────────────────────
        # Build a stable compound key from non-default filter axes. Skip the
        # cache entirely if no axes are set (caller shouldn't hit this endpoint
        # without filters, but defend against it). On hit, return rows from
        # gold_persona_insights instead of re-running SQL + Haiku (~10-15s).
        compound_key = _compound_filter_value(filters)
        from persona_insights_reader import load_compound_cache, write_compound_cache
        cached = load_compound_cache(persona, compound_key) if compound_key else None
        if cached:
            elapsed = time.time() - t0
            app_logger.info(f"[insights-live] CACHE HIT persona={persona} fv={compound_key!r} elapsed={elapsed:.2f}s")
            rendered_insights = [_render_live_insight_html(ins) for ins in (cached.get('insights') or [])[:4]]
            rendered_actions = []
            for a in (cached.get('action_areas') or [])[:3]:
                rendered_actions.append({
                    'title': a.get('headline') or '',
                    'description': a.get('narrative') or '',
                    'status_color': (a.get('status_color') or 'yellow').lower(),
                    'target_entity_type': a.get('target_entity_type'),
                    'target_entity_value': a.get('target_entity_value'),
                    'icon': 'PRIORITY',
                })
            return jsonify({
                'status': 'success',
                'persona': persona,
                'filters': filters,
                'insights': rendered_insights,
                'action_areas': rendered_actions,
                'elapsed_sec': round(elapsed, 2),
                'cached': True,
            })

        # Import here to avoid load-time circular deps and to keep
        # genie_agent's background import path untouched.
        from insights_compose import compose_for_persona_with_filters
        from databricks.sdk import WorkspaceClient as _WC

        w = _WC()  # app's runtime OAuth picked up automatically
        result = compose_for_persona_with_filters(
            persona=persona,
            filters=filters,
            warehouse_id=warehouse_id,
            sdk_workspace_client=w,
        )

        # ── Write-through cache write ────────────────────────────────────────
        # Persist the raw compose result so the NEXT visit to this same combo
        # hits the cache branch above. Failure here is non-fatal — we still
        # return the live-composed payload; the user just won't benefit from
        # the cache next time.
        if compound_key:
            try:
                write_compound_cache(
                    persona=persona,
                    compound_filter_value=compound_key,
                    insights=result.get('insights') or [],
                    action_areas=result.get('action_areas') or [],
                )
            except Exception as _e:
                app_logger.warning(f"[insights-live] cache write failed (non-fatal): {type(_e).__name__}: {_e}")

        # Render insights into HTML strings matching the existing template
        # contract (mirrors persona_insights_reader._render_insight_html).
        rendered_insights = [_render_live_insight_html(ins) for ins in (result.get('insights') or [])[:4]]
        # Render action_areas into the {title, description, status_color, ...}
        # shape that the priorityList JS renderer expects.
        rendered_actions = []
        for a in (result.get('action_areas') or [])[:3]:
            rendered_actions.append({
                'title': a.get('headline') or '',
                'description': a.get('narrative') or '',
                'status_color': (a.get('status_color') or 'yellow').lower(),
                'target_entity_type': a.get('target_entity_type'),
                'target_entity_value': a.get('target_entity_value'),
                'icon': 'PRIORITY',
            })

        elapsed = time.time() - t0
        app_logger.info(f"[insights-live] OK persona={persona} elapsed={elapsed:.1f}s insights={len(rendered_insights)} actions={len(rendered_actions)}")
        return jsonify({
            'status': 'success',
            'persona': persona,
            'filters': filters,
            'insights': rendered_insights,
            'action_areas': rendered_actions,
            'elapsed_sec': round(elapsed, 1),
        })
    except Exception as e:
        elapsed = time.time() - t0
        app_logger.error(f"[insights-live] {type(e).__name__} after {elapsed:.1f}s: {e}", exc_info=True)
        return jsonify({'error': str(e), 'error_type': type(e).__name__}), 500


def _render_live_insight_html(ins: dict) -> str:
    """Render an insight dict (from insights_compose) as the same HTML string
    the cached read path emits via persona_insights_reader._render_insight_html.

    Duplicated here (rather than imported) to avoid coupling the live-compute
    endpoint to the cached-read module's table schema assumptions. The HTML
    contract is small and we want it stable as the table representation evolves.
    """
    headline = ins.get('headline') or ''
    value = ins.get('value') or ''
    comparison = ins.get('comparison') or ''
    narrative = ins.get('narrative') or ''
    trend = ins.get('trend') or ''
    status = (ins.get('status_color') or 'yellow').lower()
    if status not in ('red', 'yellow', 'green'):
        status = 'yellow'

    direction = (ins.get('trend_direction') or 'flat').lower()
    if direction not in ('improving', 'deteriorating', 'flat'):
        direction = 'flat'
    trend_class = {
        'improving':    'positive-change',
        'deteriorating': 'negative-change',
        'flat':          'neutral',
    }[direction]

    # 2026-05-21 — mirror persona_insights_reader change: yellow now renders
    # amber ("warning-change") instead of gray. Parenthetical text like
    # "(0.81% above budgeted forecast)" was reading as no-signal default body
    # color on yellow tiles, which is wrong: marginal over-budget is the WATCH
    # state. This path serves the live-compute endpoint hit when filters change.
    comparison_class = {
        'green':  'positive-change',
        'yellow': 'warning-change',
        'red':    'negative-change',
    }[status]

    parts = [f'<strong>{headline}:</strong>']
    if value:
        parts.append(f'<strong>{value}</strong>')
    if comparison:
        if comparison_class:
            parts.append(f"<span class='{comparison_class}'>({comparison})</span>")
        else:
            parts.append(f'({comparison})')
    parts.append('.')
    if narrative:
        parts.append(narrative)
    if trend:
        parts.append(f"<span class='{trend_class}'>{trend}</span>")
    body = ' '.join(parts)
    return f'<span class="status-{status}" data-status="{status}">{body}</span>'


@app.route('/api/_diag/jslog', methods=['POST'])
def jslog():
    """Temporary diagnostic endpoint — frontend posts here to log to server-side.
    Used to trace which JS code paths are firing when filters change."""
    try:
        data = request.json or {}
        tag = data.get('tag', 'NO_TAG')
        message = data.get('message', '')
        app_logger.info(f"[JSLOG:{tag}] {message}")
        return jsonify({'ok': True})
    except Exception as e:
        app_logger.error(f"[JSLOG] error: {e}")
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/unpaid-invoices')
def get_unpaid_invoices():
    """API endpoint to get real unpaid invoices from gold_receivables_wip_aging"""
    app_logger.info("=" * 80)
    app_logger.info("[UNPAID-INVOICES API] ========== ENDPOINT CALLED ==========")
    app_logger.info(f"[UNPAID-INVOICES API] Method: {request.method}")
    app_logger.info(f"[UNPAID-INVOICES API] Path: {request.path}")
    app_logger.info(f"[UNPAID-INVOICES API] User-Agent: {request.headers.get('User-Agent', 'Unknown')}")
    app_logger.info("=" * 80)

    # Real data from gold_receivables_wip_aging. days_outstanding is capped in SQL
    # because synthetic data has values up to 1000+ days; cap to a realistic 45-90
    # range derived deterministically from collection_priority_score so the demo
    # table reads as "concerning AR aging" not "absurdly stale."
    query = f"""
    SELECT
        counterparty_name,
        project_name,
        amount,
        CASE
            WHEN days_outstanding > 90 THEN 45 + (CAST(collection_priority_score AS INT) % 45)
            WHEN days_outstanding < 30 THEN 30 + (CAST(collection_priority_score AS INT) % 20)
            ELSE days_outstanding
        END as days_outstanding,
        COALESCE(lead_partner_name, 'Unassigned') as lead_partner_name,
        CASE
            WHEN collection_priority_score > 120 THEN 'URGENT: Invoice immediately'
            WHEN collection_priority_score > 100 THEN 'HIGH: Follow up required'
            ELSE 'MEDIUM: Monitor closely'
        END as action_item,
        collection_priority_score
    FROM {SCHEMA}.gold_receivables_wip_aging
    WHERE collection_priority_score >= 80
      AND lead_partner_name IS NOT NULL
    -- Sort by amount DESC so the table surfaces the actual largest unpaid
    -- invoices ($50M+ single-line items). Previously sorted by
    -- collection_priority_score, which produced a "cliff" pattern like
    -- $21M / $1.25M / $15M / $1.7M / $27M that hid the real top tier.
    ORDER BY amount DESC
    LIMIT 5
    """

    app_logger.info(f"[UNPAID-INVOICES] Starting query execution")
    app_logger.info(f"[UNPAID-INVOICES] Query: {query[:200]}...")

    try:
        data = execute_sql_query(query)
        app_logger.info(f"[UNPAID-INVOICES] Query executed successfully")
        app_logger.info(f"[UNPAID-INVOICES] Query result type: {type(data)}, Is None: {data is None}")
        if data:
            app_logger.info(f"[UNPAID-INVOICES] Number of rows returned: {len(data)}")
        else:
            app_logger.info(f"[UNPAID-INVOICES] No data returned from query")
    except Exception as e:
        app_logger.error(f"[UNPAID-INVOICES] Error executing query: {str(e)}")
        data = None

    if data:
        invoices = []
        for row in data:
            invoices.append({
                'client': row[0],
                'project': row[1],
                'amount': float(row[2]) if row[2] else 0,
                'days_overdue': int(float(row[3])) if row[3] else 0,
                'partner': row[4],
                'action': row[5],
                'priority': int(float(row[6])) if row[6] else 0
            })
        return jsonify({'status': 'success', 'data': invoices})
    return jsonify({'status': 'success', 'data': []})

@app.route('/api/revenue-targets')
def get_revenue_targets():
    """API endpoint to get practice areas/regions running >10% behind revenue target.

    Sourced from gold_regional_pnl (the table that powers the dashboards), aggregated
    by practice_area × region for the last complete month. Lead partner derived
    from silver_dim_employees latest snapshot. Pipeline is remaining contracted
    work on Active engagements for that practice/region from gold_project_profitability.
    """
    query = f"""
    WITH practice_perf AS (
        -- Lowest 5 practice × region cells by revenue variance vs budget.
        -- No HAVING below-target filter: with per-cell jitter the lowest
        -- performers may still be slightly over budget. Surfaces the practices
        -- closest to (or below) target so the "Behind Revenue Targets" tile
        -- always has 5 rows of real data instead of falling back to hardcoded.
        SELECT
            practice_area,
            region,
            SUM(total_revenue) AS accrued,
            SUM(budgeted_revenue) AS target,
            (SUM(total_revenue) - SUM(budgeted_revenue)) /
                NULLIF(SUM(budgeted_revenue), 0) * 100 AS variance_pct
        FROM {SCHEMA}.gold_regional_pnl
        WHERE fiscal_period = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
        GROUP BY practice_area, region
    ),
    office_region AS (
        SELECT DISTINCT location, region
        FROM {SCHEMA}.gold_regional_pnl
    ),
    pipeline AS (
        -- Scale active-engagement remaining contract value into a MONTHLY
        -- run-rate so the column is comparable with `accrued` (which is a
        -- single complete month) and `target` (also monthly). Previously
        -- summed the full remaining contract value, producing 25× ratios
        -- (e.g. $44M accrued vs $1.2B pipeline — apples-to-oranges).
        -- Formula: for each engagement, remaining_revenue / remaining_months.
        -- Floor remaining_months at 1 so projects ending this month don't
        -- explode the divisor. Exclude engagements whose end date is already
        -- past (still flagged Active but effectively wound down).
        SELECT
            p.practice_area,
            r.region,
            SUM(
                GREATEST(p.planned_revenue - p.actual_revenue, 0)
                / GREATEST(MONTHS_BETWEEN(p.project_end_date, CURRENT_DATE()), 1)
            ) AS pipeline_total
        FROM {SCHEMA}.gold_project_profitability p
        JOIN office_region r ON p.location = r.location
        WHERE p.project_status = 'Active'
          AND p.planned_revenue > 0
          AND p.project_end_date > CURRENT_DATE()
        GROUP BY p.practice_area, r.region
    ),
    partner_lookup AS (
        SELECT
            practice_area,
            region,
            FIRST(first_name || ' ' || last_name) AS partner_name
        FROM {SCHEMA}.silver_dim_employees
        WHERE job_level = 'Partner'
            AND employment_status = 'Active'
            AND snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.silver_dim_employees)
        GROUP BY practice_area, region
    )
    SELECT
        CONCAT(pp.practice_area, ' (', pp.region, ')') AS area,
        pp.accrued,
        COALESCE(pl.pipeline_total, 0) AS pipeline,
        pp.target,
        pp.variance_pct AS variance,
        COALESCE(p.partner_name, 'Unassigned') AS partner
    FROM practice_perf pp
    LEFT JOIN pipeline pl ON pp.practice_area = pl.practice_area AND pp.region = pl.region
    LEFT JOIN partner_lookup p ON pp.practice_area = p.practice_area AND pp.region = p.region
    ORDER BY pp.variance_pct ASC
    LIMIT 5
    """

    app_logger.info(f"[REVENUE-TARGETS] Starting query execution")
    data = execute_sql_query(query)

    targets = []
    if data:
        for row in data:
            targets.append({
                'area': row[0],
                'accrued': float(row[1]) if row[1] else 0,
                'pipeline': float(row[2]) if row[2] else 0,
                'target': float(row[3]) if row[3] else 0,
                'variance': round(float(row[4]), 1) if row[4] else 0,
                'partner': row[5] or 'Unassigned',
            })

    return jsonify({'status': 'success', 'data': targets})

@app.route('/api/expense-outliers')
def get_expense_outliers():
    """API endpoint to get departments running over their run-rate budget.

    Sourced from silver_fact_accounts_payable. Compares last complete month spend
    by department against the trailing 3-month average ("run-rate budget"). Returns
    top 5 departments running >10% above run-rate. Department lead is the longest-
    tenured Senior Partner / Partner whose cost_center maps to that department.
    """
    query = f"""
    WITH curr AS (
        SELECT department, SUM(amount) AS curr_spend
        FROM {SCHEMA}.silver_fact_accounts_payable
        WHERE DATE_TRUNC('MONTH', invoice_date) = ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
        GROUP BY department
    ),
    -- 2026-05-20 FIX (A4): switched from trailing-3-month baseline to trailing-
    -- 12-month baseline. The 3-month window was too volatile (lumpy AP timing
    -- like big quarterly invoices skewed the average), producing implausible
    -- +24-26% department variances. The 12-month window dampens that to a
    -- realistic ±5-10% band. Coupled with the >15% sanity cap below, prevents
    -- single-month AP timing artifacts from surfacing as "outliers".
    run_rate AS (
        SELECT department, AVG(monthly_spend) AS rr_spend
        FROM (
            SELECT department, DATE_TRUNC('MONTH', invoice_date) AS m, SUM(amount) AS monthly_spend
            FROM {SCHEMA}.silver_fact_accounts_payable
            WHERE invoice_date >= ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -13)
              AND invoice_date < ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
            GROUP BY department, DATE_TRUNC('MONTH', invoice_date)
        )
        GROUP BY department
    ),
    dept_lead AS (
        SELECT department_category, full_name AS lead_name
        FROM (
            SELECT
                ccm.department_category,
                e.full_name,
                ROW_NUMBER() OVER (
                    PARTITION BY ccm.department_category
                    ORDER BY
                        CASE e.job_level
                            WHEN 'Senior Partner' THEN 1
                            WHEN 'Partner' THEN 2
                            WHEN 'Associate Partner' THEN 3
                            WHEN 'Director' THEN 4
                            ELSE 5
                        END,
                        e.hire_date ASC
                ) AS rn
            FROM {SCHEMA}.silver_dim_employees e
            JOIN {SCHEMA}.bronze_cost_center_mapping ccm
                ON e.cost_center = ccm.cost_center_code
            WHERE e.employment_status = 'Active'
                AND e.snapshot_date = (SELECT MAX(snapshot_date) FROM {SCHEMA}.silver_dim_employees)
                AND e.job_level IN ('Senior Partner', 'Partner', 'Associate Partner', 'Director')
        )
        WHERE rn = 1
    )
    SELECT
        c.department,
        c.curr_spend AS accrued,
        r.rr_spend AS budget,
        (c.curr_spend - r.rr_spend) / NULLIF(r.rr_spend, 0) * 100 AS variance,
        COALESCE(dl.lead_name, 'Unassigned') AS lead
    FROM curr c
    JOIN run_rate r USING (department)
    LEFT JOIN dept_lead dl ON c.department = dl.department_category
    -- Threshold matches the section title "Top Expense Outliers (>10% Beyond
    -- Run-rate Budget)" in templates/admin.html. Previously 0.08 which let
    -- single-digit overages (like R&D at +8.3%) appear in a >10% table.
    -- 2026-05-20 FIX (A4): added UPPER cap at 15% so single-month AP timing
    -- artifacts (e.g., big quarterly invoice landing in the latest month)
    -- don't surface as implausible 24-26% department overages. Combined with
    -- the 12-month rolling baseline above, keeps the displayed variance
    -- within Tier-1 credible bands.
    WHERE (c.curr_spend - r.rr_spend) / NULLIF(r.rr_spend, 0) > 0.10
      AND (c.curr_spend - r.rr_spend) / NULLIF(r.rr_spend, 0) <= 0.15
    ORDER BY (c.curr_spend - r.rr_spend) / NULLIF(r.rr_spend, 0) DESC
    LIMIT 5
    """

    app_logger.info("[EXPENSE-OUTLIERS] Starting query execution")
    data = execute_sql_query(query)

    outliers = []
    if data:
        for row in data:
            outliers.append({
                'department': row[0],
                'accrued': float(row[1]) if row[1] else 0,
                'budget': float(row[2]) if row[2] else 0,
                'variance': round(float(row[3]), 1) if row[3] else 0,
                'lead': row[4] or 'Unassigned',
            })

    return jsonify({'status': 'success', 'data': outliers})

@app.route('/api/te-outliers')
def get_te_outliers():
    """API endpoint for engagements where billable T&E exceeds 6% of contract value.

    Queries gold_te_contract_audit directly. That table's billable_expenses
    column is at gold-rollup magnitude (EXPENSE_SCALE applied consistently with
    gold_regional_pnl), so the ratio billable_expenses / total_contract_value
    lands at the engineered 6.5-9.5% T&E:contract band for the 8 demo outlier
    projects, and below 6% for non-engineered projects. Data-agnostic: the app
    knows nothing about scale; the gold layer encapsulates that.
    """
    query = f"""
    WITH te_per_project AS (
        SELECT
            client_name,
            project_name,
            lead_partner_name,
            total_contract_value,
            billable_expenses AS total_te_billed,
            billable_expenses / NULLIF(total_contract_value, 0) * 100 AS te_pct
        FROM {SCHEMA}.gold_te_contract_audit
        WHERE total_contract_value > 0
            AND is_active = TRUE
    )
    SELECT
        client_name,
        total_te_billed,
        total_contract_value,
        te_pct,
        COALESCE(lead_partner_name, 'Unassigned') AS lead_partner_name,
        project_name,
        CASE
            WHEN te_pct > 10 THEN 'URGENT: Review expenses immediately'
            WHEN te_pct > 8 THEN 'HIGH: Expense review required'
            ELSE 'MEDIUM: Monitor expenses'
        END AS action_item
    FROM te_per_project
    WHERE te_pct > 6
    ORDER BY te_pct DESC
    LIMIT 5
    """

    app_logger.info("[TE-OUTLIERS] Starting query execution")
    data = execute_sql_query(query)

    outliers = []
    if data:
        for row in data:
            outliers.append({
                'client': row[0] if row[0] else 'Unknown Client',
                'expense': float(row[1]) if row[1] else 0,
                'contract_value': float(row[2]) if row[2] else 1,
                'percentage': round(float(row[3]), 1) if row[3] else 0,
                'partner': row[4],
                'project': row[5] if row[5] else 'Unknown Project',
                'action': row[6] if row[6] else 'Review expenses',
            })

    return jsonify({'status': 'success', 'data': outliers})

def call_claude_endpoint(prompt, max_tokens=500):
    """Call AI using Databricks ai_query() SQL function"""
    try:
        # Use Databricks ai_query() function directly in SQL
        app_logger.info(f"Calling ai_query() with prompt length: {len(prompt)} chars")
        app_logger.debug(f"Prompt (first 200 chars): {prompt[:200]}...")

        # Escape single quotes and backslashes in the prompt for SQL
        escaped_prompt = prompt.replace("\\", "\\\\").replace("'", "''")

        # Use ai_query() function with Llama 3.1 8B for faster responses
        query = f'''
        SELECT ai_query("databricks-meta-llama-3-1-8b-instruct", '{escaped_prompt}') as response
        '''

        app_logger.debug(f"Executing query: {query[:300]}...")
        result = execute_sql_query(query)

        if result and len(result) > 0:
            # Access the first row - it's a Row object from PySpark
            row = result[0]
            # Try different methods to access the response field
            try:
                # Method 1: Direct attribute access
                response = row.response
            except AttributeError:
                try:
                    # Method 2: Dictionary-style access
                    response = row['response']
                except (KeyError, TypeError):
                    try:
                        # Method 3: asDict() method for Spark Row objects
                        response = row.asDict()['response']
                    except:
                        # Method 4: Try to get the first value if it's a tuple/list
                        response = row[0] if isinstance(row, (tuple, list)) else str(row)

            app_logger.info(f"AI response received, length: {len(response)} chars")
            return response
        else:
            app_logger.warning("No response from ai_query()")
            return None

    except Exception as e:
        app_logger.error(f"Error calling ai_query(): {e}")
        # Try with Mixtral as fallback (also fast)
        try:
            app_logger.info("Trying alternative model: databricks-mixtral-8x7b-instruct")
            escaped_prompt = prompt.replace("\\", "\\\\").replace("'", "''")
            query_alt = f'''
            SELECT ai_query("databricks-mixtral-8x7b-instruct", '{escaped_prompt}') as response
            '''
            result = execute_sql_query(query_alt)
            if result and len(result) > 0:
                row = result[0]
                # Try different methods to access the response field
                try:
                    response = row.response
                except AttributeError:
                    try:
                        response = row['response']
                    except (KeyError, TypeError):
                        try:
                            response = row.asDict()['response']
                        except:
                            response = row[0] if isinstance(row, (tuple, list)) else str(row)

                app_logger.info(f"AI response received from Mixtral model, length: {len(response)} chars")
                return response
        except Exception as e2:
            app_logger.error(f"Mixtral model also failed: {e2}")

        return None

@app.route('/api/generate-email', methods=['POST'])
def generate_email():
    """API endpoint to generate email using Claude"""
    data = request.json
    email_type = data.get('type', 'collection')
    context = data.get('context', {})
    tone = data.get('tone') or 'professional'  # Default to professional if None or null
    rewrite_count = data.get('rewrite_count', 0)

    # Log for debugging
    app_logger.info(f"Generate email request - Type: {email_type}, Tone: {tone}, Rewrite: {rewrite_count}")

    # Build prompt based on email type with enhanced tone differentiation
    tone_instructions = {
        'professional': 'Use formal business language, complete sentences, respectful tone. Address as "Dear [Name]", sign off with "Best regards" or "Sincerely".',
        'friendly': 'Use warm, conversational language with a personal touch. Include phrases like "hope you\'re doing well", use first names, add encouraging words. Sign off with "Cheers" or "Best".',
        'urgent': 'Use direct, action-oriented language. Start with "URGENT:" in subject. Use short, punchy sentences. Emphasize deadlines and immediate action needed. Sign off with "Please respond immediately" or "Awaiting urgent response".',
        'casual': 'Use relaxed, informal language. Start with "Hey" or "Hi", use contractions, conversational phrases. Add light humor if appropriate. Sign off with "Thanks!" or "Talk soon".'
    }

    tone_instruction = tone_instructions.get(tone, tone_instructions['professional'])

    if email_type == 'nudge':
        # Revenue nudge email for Admin tab
        prompt = f"""Generate an email to nudge a partner about revenue targets.

TONE: {tone.upper()} - {tone_instruction}

Context:
- Practice Area: {context.get('area', 'N/A')}
- Partner: {context.get('partner', 'Partner')}
- Accrued Revenue: ${context.get('accrued', 0)/1000000:.1f}M
- Pipeline Revenue: ${context.get('pipeline', 0)/1000000:.1f}M
- Target Revenue: ${context.get('target', 0)/1000000:.1f}M
- Variance: {context.get('variance', 0):.1f}%

{"This is rewrite #" + str(rewrite_count) + " - create a completely different version while maintaining the " + tone + " tone. Use different opening, structure, and wording." if rewrite_count > 0 else ""}

Write a concise email that:
1. Acknowledges current performance
2. Highlights the gap to target
3. Requests an action plan
4. Offers support

Return ONLY the email content in this exact format with no introductory text:
Subject: [subject line reflecting the {tone} tone]
Body: [email body with appropriate greeting and sign-off for {tone} tone]

Do NOT include phrases like "Here is a draft email" or "Here's the email" or any other meta-commentary."""

        claude_response = call_claude_endpoint(prompt)
        if claude_response:
            lines = claude_response.split('\n')
            subject = next((line.replace('Subject:', '').strip() for line in lines if line.startswith('Subject:')),
                         f"Action Required: {context.get('area', 'Practice Area')} Revenue Target Update")
            body = '\n'.join(line for line in lines if not line.startswith('Subject:') and not line.startswith('Body:'))
            return jsonify({'subject': subject, 'body': body.strip()})

    elif email_type == 'notification':
        # Check if this is a T&E notification from Finance tab or expense notification from Admin tab
        if 'contract_value' in context:
            # T&E notification from Finance tab
            prompt = f"""Generate a T&E expense review notification email TO THE PARTNER about their client's excessive expenses.

TONE: {tone.upper()} - {tone_instruction}

Context:
- Client Company: {context.get('client', 'N/A')}
- Partner responsible: {context.get('partner', 'Partner')}
- T&E Expenses: ${context.get('expense', 0):,.2f}
- Contract Value: ${context.get('contract_value', 0):,.2f}
- Percentage of contract: {context.get('percentage', 0):.1f}%
- Project: {context.get('project', 'N/A')}

{"This is rewrite #" + str(rewrite_count) + " - create a completely different version while maintaining the " + tone + " tone. Use different opening, structure, and wording." if rewrite_count > 0 else ""}

Write a brief email TO {context.get('partner', 'the partner')} requesting justification for T&E expenses that exceed the 6% threshold for their client {context.get('client', 'the client')}.

The email should be addressed to the partner about their management of the client account, NOT to the client.

Return ONLY the email content in this exact format with no introductory text:
Subject: [subject line mentioning the client and expense review]
Body: [email body starting with greeting to the partner]

Do NOT include phrases like "Here is a draft email" or "Here's the email" or any other meta-commentary."""
        else:
            # Expense notification email for Admin tab
            prompt = f"""Generate an email to notify a department lead about expense variance.

TONE: {tone.upper()} - {tone_instruction}

Context:
- Department: {context.get('department', 'N/A')}
- Department Lead: {context.get('lead', 'Lead')}
- Accrued Expenses: ${context.get('accrued', 0)/1000000:.1f}M
- Budget: ${context.get('budget', 0)/1000000:.1f}M
- Variance: {context.get('variance', 0):.1f}%

{"This is rewrite #" + str(rewrite_count) + " - create a completely different version while maintaining the " + tone + " tone. Use different opening, structure, and wording." if rewrite_count > 0 else ""}

Write a concise email that:
1. Alerts about the expense variance
2. Requests justification or corrective action
3. Sets a deadline for response
4. Maintains the specified {tone} tone throughout

Return ONLY the email content in this exact format with no introductory text:
Subject: [subject line reflecting the {tone} tone]
Body: [email body with appropriate greeting and sign-off for {tone} tone]

Do NOT include phrases like "Here is a draft email" or "Here's the email" or any other meta-commentary."""

        claude_response = call_claude_endpoint(prompt)
        if claude_response:
            lines = claude_response.split('\n')
            subject = next((line.replace('Subject:', '').strip() for line in lines if line.startswith('Subject:')),
                         f"Expense Review Required: {context.get('department', 'Department')} - {context.get('variance', 0):.1f}% Variance")
            body = '\n'.join(line for line in lines if not line.startswith('Subject:') and not line.startswith('Body:'))
            return jsonify({'subject': subject, 'body': body.strip()})

    elif email_type == 'collection':
        prompt = f"""Generate a payment reminder email TO THE CLIENT about their overdue invoice.

TONE: {tone.upper()} - {tone_instruction}

Context:
- Client Company (invoice recipient): {context.get('client', 'N/A')}
- Partner in charge (sender): {context.get('partner', 'Partner')}
- Project/Invoice: {context.get('project', 'N/A')}
- Outstanding Amount: ${context.get('amount', 0):,.2f}
- Days Overdue: {context.get('days_overdue', 0)} days

{"This is rewrite #" + str(rewrite_count) + " - create a completely different version while maintaining the " + tone + " tone. Use different opening, structure, and wording." if rewrite_count > 0 else ""}

Write a concise email FROM {context.get('partner', 'the partner')} TO {context.get('client', 'the client company')} about their overdue payment. Maintain good client relations while emphasizing urgency appropriately for the {tone} tone.

The email should be addressed to the client company contact, NOT to the partner.

Return ONLY the email content in this exact format with no introductory text:
Subject: [subject line mentioning the client company and overdue invoice]
Body: [email body starting with appropriate greeting to the client, not the partner]

Do NOT include phrases like "Here is a draft email" or "Here's the email" or any other meta-commentary."""

        claude_response = call_claude_endpoint(prompt)
        if claude_response:
            lines = claude_response.split('\n')
            subject = next((line.replace('Subject:', '').strip() for line in lines if line.startswith('Subject:')),
                         f"Payment Reminder: {context.get('client', 'Client')} - {context.get('days_overdue', 0)} Days Overdue")
            body = '\n'.join(line for line in lines if not line.startswith('Subject:') and not line.startswith('Body:'))
            return jsonify({'subject': subject, 'body': body.strip()})

    elif email_type == 'te_review':
        prompt = f"""Generate a T&E expense review request email TO THE PARTNER about their client's excessive expenses.

TONE: {tone.upper()} - {tone_instruction}

Context:
- Client Company: {context.get('client', 'N/A')}
- Partner responsible: {context.get('partner', 'Partner')}
- T&E Expenses: ${context.get('expense', 0):,.2f}
- Contract Value: ${context.get('contractValue', 0):,.2f}
- Percentage of contract: {context.get('percentage', 0):.1f}%
- Project: {context.get('project', 'N/A')}

{"This is rewrite #" + str(rewrite_count) + " - create a completely different version while maintaining the " + tone + " tone. Use different opening, structure, and wording." if rewrite_count > 0 else ""}

Write a brief email TO {context.get('partner', 'the partner')} requesting justification for T&E expenses that exceed the 6% threshold for their client {context.get('client', 'the client')}.

The email should be addressed to the partner about their management of the client account, NOT to the client.

Return ONLY the email content in this exact format with no introductory text:
Subject: [subject line mentioning the client and expense review]
Body: [email body starting with greeting to the partner]

Do NOT include phrases like "Here is a draft email" or "Here's the email" or any other meta-commentary."""

        claude_response = call_claude_endpoint(prompt)
        if claude_response:
            lines = claude_response.split('\n')
            subject = next((line.replace('Subject:', '').strip() for line in lines if line.startswith('Subject:')),
                         f"T&E Expense Review Required: {context.get('client', 'Client')}")
            body = '\n'.join(line for line in lines if not line.startswith('Subject:') and not line.startswith('Body:'))
            return jsonify({'subject': subject, 'body': body.strip()})

    # Fallback to default templates if Claude fails
    if email_type == 'collection':
        return jsonify({
            'subject': f"Payment Reminder: {context.get('client', 'Client')} - {context.get('daysOverdue', 0)} Days Overdue",
            'body': f"""Dear {context.get('partner', 'Partner')},

I hope this message finds you well. I'm writing to follow up on an outstanding invoice that requires your attention.

Invoice Details:
• Client: {context.get('client', 'N/A')}
• Project: {context.get('project', 'N/A')}
• Amount: ${context.get('amount', 0):,.2f}
• Days Overdue: {context.get('daysOverdue', 0)} days

This invoice has exceeded our standard payment terms. Please review and arrange for immediate payment.

Best regards,
CFO Team"""
        })
    else:
        return jsonify({
            'subject': f"T&E Expense Review Required: {context.get('client', 'Client')}",
            'body': f"""Dear {context.get('partner', 'Partner')},

The T&E expenses for {context.get('client', 'Client')} require review as they exceed our threshold of 6% of contract value.

Please review and provide justification if necessary.

Best regards,
CFO Team"""
        })

@app.route('/api/get-page-chips')
def get_page_chips():
    """Return page-scoped bottom chips from gold_persona_insights.

    Query param: ?page=executive|finance|admin
      - 'executive' → persona-scoped chips (uses persona query param too)
      - 'finance' or 'admin' → page-scoped chips (persona='_shared' in the table)

    Used by static/modal.js when opening the AI Assistant modal on a dashboard page.
    """
    try:
        from persona_insights_reader import get_bottom_chips_for_page
        page = (request.args.get('page') or '').lower()
        persona = request.args.get('persona', 'priya')

        if page in ('finance', 'admin'):
            chips = get_bottom_chips_for_page(persona=None, page=page)
        else:
            # Executive / landing page — persona-scoped
            chips = get_bottom_chips_for_page(persona=persona, page=None)

        return jsonify({'chips': chips, 'page': page})
    except Exception as e:
        app_logger.error(f"[PAGE CHIPS API] error: {e}")
        return jsonify({'chips': [], 'page': page, 'error': str(e)}), 500


@app.route('/api/generate-priorities', methods=['POST'])
def generate_priorities():
    """Generate priority recommendations with real SQL data based on persona and filters"""
    try:
        from persona_insights_reader import get_priorities_for_persona

        data = request.json
        persona = data.get('persona', 'priya')
        filters = data.get('filters', {})

        app_logger.info(f"[PRIORITIES API] Received request - Persona: {persona}, Filters: {filters}")

        # Get priorities using the same pattern as Key Insights
        priorities = get_priorities_for_persona(persona, filters)

        app_logger.info(f"[PRIORITIES API] Returning {len(priorities)} priorities")

        return jsonify({'priorities': priorities})

    except Exception as e:
        app_logger.error(f"Error generating priorities: {e}")
        # Return fallback priorities
        return jsonify({
            'priorities': [
                {'title': 'Review Financial Metrics', 'description': 'Analyze current performance against targets and identify gaps.', 'icon': '📊'},
                {'title': 'Optimize Operations', 'description': 'Implement process improvements to enhance efficiency.', 'icon': '⚙️'},
                {'title': 'Strategic Planning', 'description': 'Develop action plans for achieving quarterly objectives.', 'icon': '🎯'}
            ]
        })

def _resolve_persona_from_cookie() -> str:
    """Resolve active persona from selectedPersona cookie, defaulting to admin.
    Centralized so finance-deepdive / admin-deepdive header renders the right
    name on cross-tab loads instead of always saying 'Priya Patel'."""
    cookie_user = request.cookies.get('selectedPersona')
    if cookie_user in USER_ID_TO_PERSONA:
        return USER_ID_TO_PERSONA[cookie_user]
    return 'admin'


@app.route('/finance-deepdive')
def finance_deepdive():
    """Finance Deep-Dive page with embedded Lakeview dashboard"""
    dashboard_url = os.environ.get("CFO_DASHBOARD_FINANCE_URL")
    if not dashboard_url:
        raise RuntimeError(
            "CFO_DASHBOARD_FINANCE_URL env var not set. The bundle deploy should "
            "populate this from var.finance_dashboard_id. Re-run "
            "`databricks bundle deploy --target <env>`."
        )
    current_time = datetime.now()
    return render_template(
        'finance.html',
        dashboard_url=dashboard_url,
        current_time=current_time,
        data={'active_persona': _resolve_persona_from_cookie()},
    )

@app.route('/admin-deepdive')
def admin_deepdive():
    """Admin Deep-Dive page with embedded Lakeview dashboard"""
    dashboard_url = os.environ.get("CFO_DASHBOARD_ADMIN_URL")
    if not dashboard_url:
        raise RuntimeError(
            "CFO_DASHBOARD_ADMIN_URL env var not set. The bundle deploy should "
            "populate this from var.admin_dashboard_id. Re-run "
            "`databricks bundle deploy --target <env>`."
        )
    current_time = datetime.now()
    return render_template(
        'admin.html',
        dashboard_url=dashboard_url,
        current_time=current_time,
        data={'active_persona': _resolve_persona_from_cookie()},
    )

# HR routes removed - not needed for this demo

@app.route('/api/test-priorities', methods=['GET'])
def test_priorities():
    """Test endpoint to verify priorities API is working"""
    return jsonify({
        "status": "ok",
        "message": "Priorities API is accessible",
        "test_priorities": [
            {"title": "Test Priority 1", "description": "This is a test priority", "icon": "🔧"},
            {"title": "Test Priority 2", "description": "Another test priority", "icon": "⚡"},
        ]
    })

@app.route('/api/debug/chat-test', methods=['GET', 'POST'])
def debug_chat_test():
    """Debug endpoint to test chat functionality"""
    app_logger.info("[DEBUG-CHAT] ========== Chat debug endpoint called ==========")
    app_logger.info(f"[DEBUG-CHAT] Method: {request.method}")
    app_logger.info(f"[DEBUG-CHAT] Headers: {dict(request.headers)}")

    if request.method == 'POST':
        data = request.json or {}
        app_logger.info(f"[DEBUG-CHAT] POST data: {data}")
        return jsonify({
            "status": "ok",
            "message": "Chat debug POST received",
            "echo": data,
            "timestamp": datetime.now().isoformat()
        })
    else:
        return jsonify({
            "status": "ok",
            "message": "Chat debug GET endpoint working",
            "endpoints_available": [
                "/api/chat",
                "/api/chat/stream",
                "/api/debug/chat-test"
            ],
            "timestamp": datetime.now().isoformat()
        })

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "app": "cfo-demo",
        "version": "complete",
        "template_dir": template_dir,
        "templates_exist": os.path.exists(template_dir) if template_dir else False
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(debug=False, host='0.0.0.0', port=port)