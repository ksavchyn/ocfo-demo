#!/usr/bin/env bash
# CFO Operations Platform — single-mode smart-detection deploy.
#
# One command handles BOTH first-time deploys and incremental code refreshes.
# The bundle has ONE target (default) — workspace selection is via --profile
# (which carries both host + auth), and everything else is parameterized via
# flags below.
#
# Usage:
#     ./deploy.sh --profile <name> \
#                 --catalog <name> \
#                 --schema <name> \
#                 --warehouse-id <id> \
#                 [--app-name <name>] \
#                 [--workspace-root-path <path>] \
#                 [--genie-space-id <id>] \
#                 [--genie-space-title <title>]
#
# What the script does:
#   1. Builds the dashboard .lvdash.json files for the target catalog + schema
#   2. Looks up the Genie space by title (skips this if --genie-space-id given)
#   3. Bundle deploy #1 — creates job + app + dashboards (so resource IDs exist)
#      If no Genie space exists yet: triggers the data-pipeline job and waits
#      for the provision_genie_space task to finish (~25-30 min), then captures
#      the new Genie space ID.
#   4. Generates app.yml from the bundle's resolved env values (workaround for
#      Databricks Apps not propagating resource-level env to the runtime)
#   5. Bundle deploy #2 — syncs the regenerated app.yml to the workspace
#   6. Apps deploy — restarts the app so it picks up the new env

set -euo pipefail

PROFILE=""
CATALOG=""
SCHEMA=""
WAREHOUSE_ID=""
APP_NAME="cfo-demo"
WORKSPACE_ROOT_PATH=""
GENIE_SPACE_ID=""
# Default to the bundle var's default. Override per-deployment with
# --genie-space-title so dev/staging/prod each get distinct titles and the
# Genie API doesn't collide-and-rename them (which would break title lookup
# on re-deploy).
GENIE_SPACE_TITLE="CFO Demo"
TARGET=""
REFRESH_DATA=""
FORCE_LOCK=""

usage() {
  cat <<EOF
Usage: $0 --profile <name> --catalog <name> --schema <name> --warehouse-id <id> [options]

Required:
  --profile               Databricks CLI profile (sets workspace host + auth)
  --catalog               Unity Catalog where demo data lives
  --schema                Schema name under {catalog}
  --warehouse-id          SQL warehouse ID for app + dashboards

Optional:
  --target                Bundle target name from databricks.yml (default:
                          whichever target has \`default: true\`). Each target
                          gets isolated terraform state at
                          .databricks/bundle/<target>/, so you can deploy the
                          same bundle to multiple workspaces from one clone.
  --app-name              Databricks App name (default: "cfo-demo")
  --workspace-root-path   Bundle sync root (default: /Workspace/Users/<current-user>/cfo-app)
  --genie-space-id        Skip title lookup; use this Genie space ID directly.
  --genie-space-title     Title to look up (default: "CFO Demo").
  --refresh-data          Trigger the cfo_data_pipeline job at the end of the
                          deploy and block until it finishes (~25-30 min).
                          Re-generates bronze + silver + gold + Genie space +
                          insight/chip caches. Use after editing the data
                          generator or chip-text prompts, or after
                          re-pointing at a new schema via the customer-mapping
                          notebook. Skip for fast app-only re-deploys.
  --skip-bronze-hydrate   Customer-data path. When set, the hydrate_bronze task
                          no-ops and the pipeline rebuilds silver+gold on top
                          of whatever bronze_* views already exist in the
                          target schema. Use AFTER running
                          genie_insights/customer_mapping.py, which emits
                          bronze_* views over the customer's real data.
                          Combine with --refresh-data so the rest of the
                          pipeline reruns. Default off → synthetic demo data.

Examples:
  # Customer-ship default (uses bundle's default target)
  ./deploy.sh \\
      --profile <your-cli-profile> \\
      --catalog main \\
      --schema cfo_proserv \\
      --warehouse-id <warehouse-id> \\
      --workspace-root-path /Workspace/Users/<your-username>/cfo-app \\
      --genie-space-title "CFO Demo"

  # Customer deploys same bundle to a second workspace (e.g. staging)
  ./deploy.sh \\
      --profile <your-staging-profile> \\
      --target staging \\
      --catalog main \\
      --schema cfo_proserv \\
      --warehouse-id <warehouse-id> \\
      --workspace-root-path /Workspace/Users/<your-username>/cfo-app-staging \\
      --genie-space-title "CFO Demo — Staging"

  # Dev iteration (separate target, separate Genie room, separate app name)
  ./deploy.sh \\
      --profile <your-cli-profile> \\
      --target dev \\
      --catalog main \\
      --schema proserv_cfo \\
      --warehouse-id <warehouse-id> \\
      --app-name cfo-demo-test \\
      --workspace-root-path /Workspace/Users/<your-username>/cfo-app-test \\
      --genie-space-title "CFO Demo — Dev"

  # Production deploy
  ./deploy.sh \\
      --profile <your-cli-profile> \\
      --target prod \\
      --catalog main \\
      --schema cfo_proserv \\
      --warehouse-id <warehouse-id> \\
      --app-name cfo-demo-wip \\
      --workspace-root-path /Workspace/Users/<your-username>/cfo-app \\
      --genie-space-title "<your prod Genie space title>"
EOF
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="${2:-}"; [[ -z "$PROFILE" ]] && { echo "ERROR: --profile requires a value"; usage; }; shift 2 ;;
    --catalog) CATALOG="${2:-}"; [[ -z "$CATALOG" ]] && { echo "ERROR: --catalog requires a value"; usage; }; shift 2 ;;
    --schema) SCHEMA="${2:-}"; [[ -z "$SCHEMA" ]] && { echo "ERROR: --schema requires a value"; usage; }; shift 2 ;;
    --warehouse-id) WAREHOUSE_ID="${2:-}"; [[ -z "$WAREHOUSE_ID" ]] && { echo "ERROR: --warehouse-id requires a value"; usage; }; shift 2 ;;
    --target) TARGET="${2:-}"; shift 2 ;;
    --app-name) APP_NAME="${2:-}"; shift 2 ;;
    --workspace-root-path) WORKSPACE_ROOT_PATH="${2:-}"; shift 2 ;;
    --genie-space-id) GENIE_SPACE_ID="${2:-}"; shift 2 ;;
    --genie-space-title) GENIE_SPACE_TITLE="${2:-}"; shift 2 ;;
    --refresh-data) REFRESH_DATA="1"; shift 1 ;;
    --skip-bronze-hydrate) SKIP_BRONZE_HYDRATE="1"; shift 1 ;;
    --force-lock) FORCE_LOCK="1"; shift 1 ;;
    -h|--help) usage ;;
    *) echo "Unknown arg: $1"; usage ;;
  esac
done

[[ -z "$PROFILE" || -z "$CATALOG" || -z "$SCHEMA" || -z "$WAREHOUSE_ID" ]] && usage

# Build --target arg array (empty if no target specified — bundle uses its
# default target in that case). Splat this into every bundle CLI call.
TARGET_ARGS=()
if [[ -n "$TARGET" ]]; then
  TARGET_ARGS=(--target "$TARGET")
fi

echo "═══════════════════════════════════════════════════════════"
echo " CFO Demo Bundle Deploy"
echo "─────────────────────────────────────────────────────────"
echo "  Profile:           $PROFILE"
echo "  Target:            ${TARGET:-(bundle default)}"
echo "  Catalog:           $CATALOG"
echo "  Schema:            $SCHEMA"
echo "  Warehouse ID:      $WAREHOUSE_ID"
echo "  App name:          $APP_NAME"
echo "  Workspace path:    ${WORKSPACE_ROOT_PATH:-(bundle default: /Workspace/Users/<you>/cfo-app)}"
echo "  Genie space ID:    ${GENIE_SPACE_ID:-(auto-detect by title)}"
echo "  Genie space title: $GENIE_SPACE_TITLE"
echo "  Refresh data:      ${REFRESH_DATA:+yes (will run cfo_data_pipeline at end, blocks ~25-30 min)}"
echo "  Skip bronze:       ${SKIP_BRONZE_HYDRATE:+yes (hydrate_bronze task no-ops; uses existing bronze_* views)}"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Common --var args for every bundle command.
build_common_vars() {
  local extra_id="${1:-}"
  COMMON_VARS=(
    --var "app_name=$APP_NAME"
    --var "catalog=$CATALOG"
    --var "schema_name=$SCHEMA"
    --var "warehouse_id=$WAREHOUSE_ID"
    --var "genie_space_title=$GENIE_SPACE_TITLE"
  )
  # Only set skip_hydrate_bronze=true when the flag was passed; otherwise the
  # bundle's default ("false") kicks in. Passing --var skip_hydrate_bronze=""
  # would override the default with an empty string and break the truthiness
  # check in 01_generate_bronze_data.py.
  if [[ -n "${SKIP_BRONZE_HYDRATE:-}" ]]; then
    COMMON_VARS+=(--var "skip_hydrate_bronze=true")
  fi
  if [[ -n "$WORKSPACE_ROOT_PATH" ]]; then
    COMMON_VARS+=(--var "workspace_root_path=$WORKSPACE_ROOT_PATH")
  fi
  if [[ -n "$extra_id" ]]; then
    COMMON_VARS+=(--var "genie_space_id=$extra_id")
  fi
}

# Look up a Genie space ID by title via the Genie REST API.
# Outputs the ID to stdout (empty if not found). All errors swallowed.
lookup_genie_space() {
  local title="$1"
  GENIE_SPACE_TITLE="$title" PROFILE="$PROFILE" python3 - <<'PY'
import json, os, subprocess, sys

title = (os.environ.get("GENIE_SPACE_TITLE") or "").strip()
profile = os.environ.get("PROFILE") or ""

try:
    r = subprocess.run(
        ["databricks", "api", "get", "/api/2.0/genie/spaces", "--profile", profile],
        capture_output=True, text=True, check=True,
    )
    payload = json.loads(r.stdout or "{}")
except Exception:
    sys.exit(0)

spaces = list(payload.get("spaces") or [])
next_token = payload.get("next_page_token") or ""
while next_token:
    try:
        r = subprocess.run(
            ["databricks", "api", "get",
             f"/api/2.0/genie/spaces?page_token={next_token}",
             "--profile", profile],
            capture_output=True, text=True, check=True,
        )
        payload = json.loads(r.stdout or "{}")
    except Exception:
        break
    spaces.extend(payload.get("spaces") or [])
    next_token = payload.get("next_page_token") or ""

for s in spaces:
    if (s.get("title") or "").strip() == title:
        print(s.get("space_id") or "", end="")
        break
PY
}

# 1. Build dashboards for the target catalog + schema.
echo "[1/6] Building dashboards for $CATALOG.$SCHEMA..."
python3 scripts/build_dashboards.py --catalog "$CATALOG" --schema "$SCHEMA"
echo ""

# 2. Resolve Genie space ID via title lookup (unless caller provided one).
if [[ -z "$GENIE_SPACE_ID" ]]; then
  echo "[2/6] Looking up Genie space '$GENIE_SPACE_TITLE'..."
  GENIE_SPACE_ID=$(lookup_genie_space "$GENIE_SPACE_TITLE")
  if [[ -n "$GENIE_SPACE_ID" ]]; then
    echo "    Found existing Genie space: $GENIE_SPACE_ID"
  else
    echo "    No Genie space with title '$GENIE_SPACE_TITLE' yet (first-time deploy)."
  fi
  echo ""
else
  echo "[2/6] Using caller-supplied Genie space ID: $GENIE_SPACE_ID"
  echo ""
fi

# 3. Bundle deploy #1 — creates job + app + dashboards. On the fast path this
#    deploy carries the resolved Genie ID. On first-time deploy it has no
#    Genie ID; the branch below triggers the job to create one.
echo "[3/6] Bundle deploy #1 (creates/refreshes resources)..."
build_common_vars "$GENIE_SPACE_ID"
databricks bundle deploy --profile "$PROFILE" ${TARGET_ARGS[@]+"${TARGET_ARGS[@]}"} "${COMMON_VARS[@]}" ${FORCE_LOCK:+--force-lock}
echo ""

# First-time-only branch: trigger the data-pipeline job, wait for the Genie
# space to be provisioned, then re-resolve the ID.
if [[ -z "$GENIE_SPACE_ID" ]]; then
  echo "═══════════════════════════════════════════════════════════"
  echo " First-time deploy detected."
  echo " Triggering the data-pipeline job. Waiting for"
  echo " 'provision_genie_space' task to finish (~25-30 min)."
  echo " (The 'generate_insights' task runs afterward and typically"
  echo "  takes ~30-45 min; we don't block on it — the app is"
  echo "  fully usable once Genie is provisioned.)"
  echo "═══════════════════════════════════════════════════════════"
  echo ""

  FIRSTTIME_SUMMARY=$(databricks bundle summary --profile "$PROFILE" ${TARGET_ARGS[@]+"${TARGET_ARGS[@]}"} "${COMMON_VARS[@]}" -o json)
  JOB_ID=$(printf '%s' "$FIRSTTIME_SUMMARY" | python3 -c '
import json, sys
s = json.load(sys.stdin)
jobs = (s.get("resources") or {}).get("jobs") or {}
job = jobs.get("cfo_data_pipeline") or {}
print(job.get("id") or "", end="")
')
  FIRSTTIME_HOST=$(printf '%s' "$FIRSTTIME_SUMMARY" | python3 -c '
import json, sys
from urllib.parse import urlparse
s = json.load(sys.stdin)
host = ((s.get("workspace") or {}).get("host") or "").rstrip("/")
if not host:
    for kind in ("jobs", "dashboards"):
        for v in ((s.get("resources") or {}).get(kind) or {}).values():
            u = (v.get("url") or "")
            if u.startswith("http"):
                p = urlparse(u)
                host = f"{p.scheme}://{p.netloc}"
                break
        if host:
            break
print(host, end="")
')
  if [[ -z "$JOB_ID" ]]; then
    echo "ERROR: could not find cfo_data_pipeline job in bundle summary." >&2
    exit 1
  fi
  echo "    Job ID: $JOB_ID"

  # --no-wait returns the run_id immediately without waiting for RUNNING state.
  # The CLI's default 20-minute wait can time out before all tasks start, which
  # surfaces as an empty stdout that breaks the downstream JSON parse. Our own
  # poll loop below handles the wait properly.
  RUN_ID=$(databricks jobs run-now "$JOB_ID" --profile "$PROFILE" --no-wait -o json \
    | python3 -c 'import json, sys; print(json.load(sys.stdin).get("run_id") or "", end="")')
  if [[ -z "$RUN_ID" ]]; then
    echo "ERROR: jobs run-now did not return a run_id." >&2
    exit 1
  fi
  echo "    Run ID: $RUN_ID"
  if [[ -n "$FIRSTTIME_HOST" ]]; then
    echo "    Run URL: $FIRSTTIME_HOST/jobs/$JOB_ID/runs/$RUN_ID"
  fi
  echo ""
  echo "    Polling every 5 min. Cancel with Ctrl-C if you want to stop."
  echo ""

  ELAPSED=0
  while true; do
    STATUS=$(databricks jobs get-run "$RUN_ID" --profile "$PROFILE" -o json \
      | python3 -c '
import json, sys
data = json.load(sys.stdin)
hit = None
for t in (data.get("tasks") or []):
    if t.get("task_key") == "provision_genie_space":
        hit = t
        break
if not hit:
    print("MISSING|")
else:
    state = hit.get("state") or {}
    lc = state.get("life_cycle_state", "")
    rs = state.get("result_state", "")
    print(f"{lc}|{rs}")
')
    LIFECYCLE="${STATUS%%|*}"
    RESULT="${STATUS##*|}"
    printf "    [%04ds] provision_genie_space: %s / %s\n" "$ELAPSED" "${LIFECYCLE:-?}" "${RESULT:-?}"
    if [[ "$LIFECYCLE" == "TERMINATED" && "$RESULT" == "SUCCESS" ]]; then
      echo "    provision_genie_space succeeded."
      break
    fi
    if [[ "$LIFECYCLE" == "TERMINATED" || "$LIFECYCLE" == "INTERNAL_ERROR" || "$LIFECYCLE" == "SKIPPED" ]]; then
      echo "ERROR: provision_genie_space ended with lifecycle=$LIFECYCLE result=$RESULT." >&2
      if [[ "$RESULT" == "UPSTREAM_FAILED" ]]; then
        echo "       An upstream task failed before provision_genie_space could run." >&2
        echo "       Common cause: missing Unity Catalog permissions (CREATE SCHEMA on the target catalog)." >&2
      fi
      echo "Inspect the run in the UI or via:" >&2
      echo "  databricks jobs get-run $RUN_ID --profile $PROFILE" >&2
      exit 1
    fi
    sleep 300
    ELAPSED=$((ELAPSED + 300))
  done
  echo ""

  echo "    Re-resolving Genie space ID..."
  GENIE_SPACE_ID=$(lookup_genie_space "$GENIE_SPACE_TITLE")
  if [[ -z "$GENIE_SPACE_ID" ]]; then
    echo "ERROR: provision_genie_space succeeded but the Genie space with title" >&2
    echo "       '$GENIE_SPACE_TITLE' wasn't found. Check the workspace and re-run." >&2
    exit 1
  fi
  echo "    Genie space: $GENIE_SPACE_ID"
  echo ""

  # Bundle summary will now have the right Genie ID, but the app.yml step needs
  # to read it via vars too. Update COMMON_VARS.
  build_common_vars "$GENIE_SPACE_ID"
fi

# 4. Generate app.yml from the bundle's resolved env values. This step is the
#    fix for the env-propagation gap: the Databricks Apps runtime reads env
#    from app.yml in source, not from the bundle's resource config.
echo "[4/6] Generating app.yml from bundle's resolved env..."
python3 scripts/build_app_yml.py \
  --profile "$PROFILE" \
  --target "$TARGET" \
  --catalog "$CATALOG" \
  --schema "$SCHEMA" \
  --warehouse-id "$WAREHOUSE_ID" \
  --app-name "$APP_NAME" \
  --workspace-root-path "${WORKSPACE_ROOT_PATH:-/Workspace/Users/$(databricks current-user me --profile "$PROFILE" -o json | python3 -c 'import json,sys; print(json.load(sys.stdin).get("userName",""), end="")')/cfo-app}" \
  --genie-space-id "$GENIE_SPACE_ID" \
  --app-yml-path app.yml
echo ""

# 5. Bundle deploy #2 — syncs the regenerated app.yml to the workspace so the
#    next apps deploy will pick it up.
echo "[5/6] Bundle deploy #2 (syncs regenerated app.yml)..."
databricks bundle deploy --profile "$PROFILE" ${TARGET_ARGS[@]+"${TARGET_ARGS[@]}"} "${COMMON_VARS[@]}" ${FORCE_LOCK:+--force-lock}
echo ""

# 6. Apps deploy — restarts the app from source. Reads the freshly-synced
#    app.yml, so the app process now has every env var it needs.
#    Before deploying we ensure the app's compute is RUNNING — `apps deploy`
#    refuses to push to a stopped app, so self-heal by starting it if needed.
echo "[6/6] Restarting the app (databricks apps deploy)..."
APP_SOURCE_PATH=$(databricks bundle summary --profile "$PROFILE" ${TARGET_ARGS[@]+"${TARGET_ARGS[@]}"} "${COMMON_VARS[@]}" -o json \
  | python3 -c '
import json, sys
s = json.load(sys.stdin)
apps = (s.get("resources") or {}).get("apps") or {}
if not apps:
    print("", end="")
else:
    first = next(iter(apps.values()))
    print(first.get("source_code_path") or "", end="")
')
if [[ -z "$APP_SOURCE_PATH" ]]; then
  echo "WARNING: could not resolve app source_code_path from bundle summary."
  echo "         Restart the app manually in the Apps UI."
else
  echo "    App: $APP_NAME"
  echo "    Source: $APP_SOURCE_PATH"

  # Self-heal: start the app if it's not already RUNNING. `apps deploy` will
  # error otherwise. `apps start` blocks until the compute is up.
  APP_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    d = {}
print((d.get("compute_status") or {}).get("state") or "", end="")
')
  if [[ -n "$APP_STATE" && "$APP_STATE" != "RUNNING" && "$APP_STATE" != "ACTIVE" ]]; then
    echo "    App compute is '$APP_STATE'; starting before deploy..."
    databricks apps start "$APP_NAME" --profile "$PROFILE" || true
  fi

  databricks apps deploy "$APP_NAME" --source-code-path "$APP_SOURCE_PATH" --profile "$PROFILE"
fi
echo ""

# ───────────────────────────────────────────────────────────
# Grant the app's service principal the Unity Catalog + warehouse permissions
# it needs to query the demo schema. Databricks Apps each run as their own
# auto-created service principal; that SP only gets default access to the
# workspace's built-in catalog (e.g. `main`). If the customer deploys to any
# other catalog (e.g. `users.<name>` or a custom catalog), the SP gets
# PERMISSION_DENIED on the first query and the app silently shows empty tiles.
# Idempotent — re-running these statements is safe.
# ───────────────────────────────────────────────────────────
echo "Granting app's service principal access to $CATALOG.$SCHEMA + warehouse..."
APP_SP_CLIENT_ID=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
  | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
    print(d.get("service_principal_client_id") or "", end="")
except Exception:
    pass
')

if [[ -z "$APP_SP_CLIENT_ID" ]]; then
  echo "  WARNING: could not resolve app service principal — skipping permission grants." >&2
  echo "  The app may show empty tiles until you manually grant it USE CATALOG/SCHEMA + SELECT" >&2
  echo "  on $CATALOG.$SCHEMA and CAN_USE on warehouse $WAREHOUSE_ID." >&2
else
  echo "  App SP: $APP_SP_CLIENT_ID"

  # UC: catalog + schema + table-level SELECT (one schema-level grant covers all current and future tables in the schema)
  databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "{
    \"statement\": \"GRANT USE CATALOG ON CATALOG \`$CATALOG\` TO \`$APP_SP_CLIENT_ID\`\",
    \"warehouse_id\": \"$WAREHOUSE_ID\",
    \"wait_timeout\": \"30s\"
  }" > /dev/null && echo "  ✓ USE CATALOG on $CATALOG"

  databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "{
    \"statement\": \"GRANT USE SCHEMA, SELECT ON SCHEMA \`$CATALOG\`.\`$SCHEMA\` TO \`$APP_SP_CLIENT_ID\`\",
    \"warehouse_id\": \"$WAREHOUSE_ID\",
    \"wait_timeout\": \"30s\"
  }" > /dev/null && echo "  ✓ USE SCHEMA + SELECT on $CATALOG.$SCHEMA"

  # Warehouse CAN_USE — also required for the app's SP to execute queries.
  databricks api patch "/api/2.0/permissions/warehouses/$WAREHOUSE_ID" --profile "$PROFILE" --json "{
    \"access_control_list\":[{\"service_principal_name\":\"$APP_SP_CLIENT_ID\",\"permission_level\":\"CAN_USE\"}]
  }" > /dev/null && echo "  ✓ CAN_USE on warehouse $WAREHOUSE_ID"

  # Genie space CAN_RUN — chat queries fail with 403 PERMISSION_DENIED without this.
  if [[ -n "$GENIE_SPACE_ID" ]]; then
    databricks api patch "/api/2.0/permissions/genie/$GENIE_SPACE_ID" --profile "$PROFILE" --json "{
      \"access_control_list\":[{\"service_principal_name\":\"$APP_SP_CLIENT_ID\",\"permission_level\":\"CAN_RUN\"}]
    }" > /dev/null && echo "  ✓ CAN_RUN on Genie space $GENIE_SPACE_ID"
  else
    echo "  WARNING: GENIE_SPACE_ID not resolved — skipping Genie space grant." >&2
  fi
fi
echo ""

# ───────────────────────────────────────────────────────────
# Ensure Lakeview dashboards have a daily refresh schedule. Idempotent: if the
# customer already configured a schedule in the UI we leave it alone. DABs
# don't support `schedule:` on the dashboards resource directly, so this runs
# post-deploy via the Lakeview API. Non-fatal — a failure here doesn't break
# anything else; dashboards just stay without auto-refresh.
# ───────────────────────────────────────────────────────────
echo "Ensuring dashboard refresh schedules..."
FIN_DASH_ID=$(grep -A1 "CFO_DASHBOARD_FINANCE_URL" app.yml | tail -1 \
  | sed -nE 's|.*/embed/dashboardsv3/([^?"]+).*|\1|p')
ADM_DASH_ID=$(grep -A1 "CFO_DASHBOARD_ADMIN_URL" app.yml | tail -1 \
  | sed -nE 's|.*/embed/dashboardsv3/([^?"]+).*|\1|p')
if [[ -n "$FIN_DASH_ID" && -n "$ADM_DASH_ID" ]]; then
  python3 scripts/setup_dashboard_schedules.py \
    --profile "$PROFILE" \
    --finance-dashboard-id "$FIN_DASH_ID" \
    --admin-dashboard-id "$ADM_DASH_ID" || true
else
  echo "  Could not resolve dashboard IDs from app.yml — skipping schedule setup."
fi
echo ""

# ───────────────────────────────────────────────────────────
# Optional step [7/7]: re-run the data pipeline + insights orchestrator.
# Triggered by --refresh-data. Use when:
#   - You've edited the data generator (e.g. office overage profiles) and need
#     the schema repopulated.
#   - You've edited the chip-text / synthesis prompts and need the chip cache
#     regenerated.
#   - You've re-pointed the demo at a new schema (customer-mapping step) and
#     need the views' insight cache built.
# Skip for fast app-only re-deploys where you only want to push new app code.
#
# Logic:
#   - If the first-time-deploy branch already triggered the job (RUN_ID is set),
#     reuse that run and just keep polling until full completion.
#   - Otherwise, trigger a fresh run now.
#   - Either way, block until the run is TERMINATED + SUCCESS (~25-30 min).
# ───────────────────────────────────────────────────────────
if [[ -n "$REFRESH_DATA" ]]; then
  echo "[7/7] --refresh-data set → running cfo_data_pipeline + waiting for full completion..."

  # Resolve the job ID + workspace host once (used both for run-now and for the
  # clickable URL print below). One bundle summary call covers both.
  REFRESH_SUMMARY=$(databricks bundle summary --profile "$PROFILE" ${TARGET_ARGS[@]+"${TARGET_ARGS[@]}"} "${COMMON_VARS[@]}" -o json)
  REFRESH_JOB_ID=$(printf '%s' "$REFRESH_SUMMARY" | python3 -c '
import json, sys
s = json.load(sys.stdin)
jobs = (s.get("resources") or {}).get("jobs") or {}
job = jobs.get("cfo_data_pipeline") or {}
print(job.get("id") or "", end="")
')
  REFRESH_HOST=$(printf '%s' "$REFRESH_SUMMARY" | python3 -c '
import json, sys
from urllib.parse import urlparse
s = json.load(sys.stdin)
host = ((s.get("workspace") or {}).get("host") or "").rstrip("/")
# Fallback: derive scheme+netloc from any resource URL (jobs/dashboards) in case
# bundle summary did not populate workspace.host directly.
if not host:
    for kind in ("jobs", "dashboards"):
        for v in ((s.get("resources") or {}).get(kind) or {}).values():
            u = (v.get("url") or "")
            if u.startswith("http"):
                p = urlparse(u)
                host = f"{p.scheme}://{p.netloc}"
                break
        if host:
            break
print(host, end="")
')

  if [[ -z "${RUN_ID:-}" ]]; then
    if [[ -z "$REFRESH_JOB_ID" ]]; then
      echo "ERROR: could not resolve cfo_data_pipeline job ID for --refresh-data." >&2
      exit 1
    fi
    # --no-wait returns the run_id immediately without waiting for RUNNING state.
    # Our own poll loop below handles the wait. Avoids the CLI's default 20-min
    # wait-for-RUNNING timeout which surfaces as an empty stdout that breaks
    # the downstream JSON parse.
    RUN_ID=$(databricks jobs run-now "$REFRESH_JOB_ID" --profile "$PROFILE" --no-wait -o json \
      | python3 -c 'import json, sys; print(json.load(sys.stdin).get("run_id") or "", end="")')
    if [[ -z "$RUN_ID" ]]; then
      echo "ERROR: databricks jobs run-now did not return a run_id." >&2
      exit 1
    fi
    echo "    Run ID: $RUN_ID (new run triggered)"
  else
    echo "    Run ID: $RUN_ID (resuming the run already triggered by the first-time-deploy branch)"
  fi
  if [[ -n "$REFRESH_HOST" && -n "$REFRESH_JOB_ID" ]]; then
    echo "    Run URL: $REFRESH_HOST/jobs/$REFRESH_JOB_ID/runs/$RUN_ID"
    echo "    (open this URL in your browser to watch progress while deploy.sh polls)"
  fi
  echo "    Polling every 5 min. Output is quiet — one line per task completion, plus a heartbeat every poll."
  echo ""

  REFRESH_ELAPSED=0
  HEARTBEAT_INTERVAL=300   # 5 min
  LAST_HEARTBEAT=0
  ANNOUNCED_TASKS=""       # space-separated list of task_keys we've already printed
  while true; do
    RUN_JSON=$(databricks jobs get-run "$RUN_ID" --profile "$PROFILE" -o json)
    RUN_STATUS=$(printf '%s' "$RUN_JSON" | python3 -c '
import json, sys
data = json.load(sys.stdin)
state = data.get("state") or {}
lc = state.get("life_cycle_state", "")
rs = state.get("result_state", "")
print(f"{lc}|{rs}")
')
    RUN_LIFECYCLE="${RUN_STATUS%%|*}"
    RUN_RESULT="${RUN_STATUS##*|}"

    # Parse two things per poll:
    #   1. NEWLY_TERMINATED — pipe-separated "task_key:result_state" entries for
    #      tasks that JUST entered TERMINATED state (i.e. not in ANNOUNCED_TASKS).
    #   2. RUNNING_TASKS    — comma-separated names of tasks currently RUNNING,
    #      for use in the periodic heartbeat line.
    PARSED=$(printf '%s' "$RUN_JSON" | ANNOUNCED="$ANNOUNCED_TASKS" python3 -c '
import json, os, sys
data = json.load(sys.stdin)
announced = set((os.environ.get("ANNOUNCED") or "").split())
newly_done = []
running = []
n_total = 0
n_done = 0
for t in (data.get("tasks") or []):
    n_total += 1
    s = t.get("state") or {}
    lc = s.get("life_cycle_state", "")
    rs = s.get("result_state", "")
    tk = t.get("task_key", "")
    if lc == "TERMINATED":
        n_done += 1
        if tk and tk not in announced:
            newly_done.append(f"{tk}:{rs}")
    elif lc == "RUNNING":
        running.append(tk)
# Emit three lines: newly_done (pipe-joined), running (comma-joined), counts.
print("|".join(newly_done))
print(",".join(running))
print(f"{n_done}/{n_total}")
')
    NEWLY_DONE=$(printf '%s' "$PARSED" | sed -n '1p')
    RUNNING_TASKS=$(printf '%s' "$PARSED" | sed -n '2p')
    PROGRESS=$(printf '%s' "$PARSED" | sed -n '3p')

    # Announce each newly-completed task on its own line.
    if [[ -n "$NEWLY_DONE" ]]; then
      IFS='|' read -ra DONE_ARR <<< "$NEWLY_DONE"
      for entry in "${DONE_ARR[@]}"; do
        tk="${entry%%:*}"
        rs="${entry##*:}"
        # Color cue: green tick for SUCCESS, red x for anything else.
        if [[ "$rs" == "SUCCESS" ]]; then
          printf "    [%04ds] ✓ %-24s SUCCESS\n" "$REFRESH_ELAPSED" "$tk"
        else
          printf "    [%04ds] ✗ %-24s %s\n" "$REFRESH_ELAPSED" "$tk" "$rs"
        fi
        ANNOUNCED_TASKS="$ANNOUNCED_TASKS $tk"
      done
    fi

    # Periodic heartbeat — proves we're alive without dumping the full state.
    if (( REFRESH_ELAPSED - LAST_HEARTBEAT >= HEARTBEAT_INTERVAL )); then
      if [[ -n "$RUNNING_TASKS" ]]; then
        printf "    [%04ds] still running: %s (%s tasks done)\n" "$REFRESH_ELAPSED" "$RUNNING_TASKS" "$PROGRESS"
      else
        printf "    [%04ds] waiting on next task (%s tasks done)\n" "$REFRESH_ELAPSED" "$PROGRESS"
      fi
      LAST_HEARTBEAT=$REFRESH_ELAPSED
    fi

    if [[ "$RUN_LIFECYCLE" == "TERMINATED" && "$RUN_RESULT" == "SUCCESS" ]]; then
      echo ""
      echo "    cfo_data_pipeline finished successfully ($PROGRESS tasks, ${REFRESH_ELAPSED}s)."
      break
    fi
    if [[ "$RUN_LIFECYCLE" == "TERMINATED" || "$RUN_LIFECYCLE" == "INTERNAL_ERROR" ]]; then
      echo ""
      echo "ERROR: cfo_data_pipeline ended with result=$RUN_RESULT." >&2
      echo "Inspect via: databricks jobs get-run $RUN_ID --profile $PROFILE" >&2
      exit 1
    fi
    sleep 300
    REFRESH_ELAPSED=$((REFRESH_ELAPSED + 300))
  done
  echo ""
fi

echo "═══════════════════════════════════════════════════════════"
echo " Deploy complete."
echo "═══════════════════════════════════════════════════════════"

# Print clickable URLs.
GENIE_SPACE_ID="$GENIE_SPACE_ID" GENIE_SPACE_TITLE="$GENIE_SPACE_TITLE" \
PROFILE="$PROFILE" APP_NAME="$APP_NAME" \
CATALOG="$CATALOG" SCHEMA="$SCHEMA" WAREHOUSE_ID="$WAREHOUSE_ID" \
WORKSPACE_ROOT_PATH="$WORKSPACE_ROOT_PATH" TARGET="$TARGET" \
python3 - <<'PYEOF' || true
import json, os, subprocess, sys

vars_ = [
    "--profile", os.environ["PROFILE"],
    "-o", "json",
    "--var", f"app_name={os.environ['APP_NAME']}",
    "--var", f"catalog={os.environ['CATALOG']}",
    "--var", f"schema_name={os.environ['SCHEMA']}",
    "--var", f"warehouse_id={os.environ['WAREHOUSE_ID']}",
]
target = (os.environ.get("TARGET") or "").strip()
if target:
    vars_ += ["--target", target]
wrp = (os.environ.get("WORKSPACE_ROOT_PATH") or "").strip()
if wrp:
    vars_ += ["--var", f"workspace_root_path={wrp}"]
gid = (os.environ.get("GENIE_SPACE_ID") or "").strip()
if gid:
    vars_ += ["--var", f"genie_space_id={gid}"]

try:
    r = subprocess.run(["databricks", "bundle", "summary"] + vars_,
                       capture_output=True, text=True, check=True)
    s = json.loads(r.stdout)
except Exception as e:
    print(f"\n(could not pull bundle summary: {e})")
    sys.exit(0)

ws = s.get("workspace", {}) or {}
host = (ws.get("host") or "").rstrip("/")

res = s.get("resources", {}) or {}
ws_id = ""
for kind in ("jobs", "dashboards"):
    for v in (res.get(kind) or {}).values():
        u = v.get("url") or ""
        if "?o=" in u:
            ws_id = u.split("?o=")[-1].split("&")[0]
            break
    if ws_id:
        break

# Fallback: bundle summary doesn't always populate workspace.host. If empty,
# derive scheme+netloc from any resource URL we already have (jobs/dashboards).
# Without this, the App URL falls back to "(app: <name>)" and the Genie space
# URL line is silently dropped.
if not host:
    from urllib.parse import urlparse
    for kind in ("jobs", "dashboards"):
        for v in (res.get(kind) or {}).values():
            u = v.get("url") or ""
            if u.startswith("http"):
                p = urlparse(u)
                host = f"{p.scheme}://{p.netloc}"
                break
        if host:
            break

cloud = ""
if "azuredatabricks.net" in host:
    cloud = "azure"
elif "gcp.databricks.com" in host:
    cloud = "gcp"
elif host:
    cloud = "aws"

print()
print("Deployed resources:")
for kind, label in [("jobs", "Job"), ("dashboards", "Dashboard"), ("apps", "App")]:
    items = res.get(kind) or {}
    for k, v in items.items():
        if kind == "apps":
            name = v.get("name") or v.get("id") or k
            if ws_id and cloud:
                url = f"https://{name}-{ws_id}.{cloud}.databricksapps.com"
            else:
                url = f"{host}/apps/{name}" if host else f"(app: {name})"
            print(f"  {label} [{name}]: {url}")
        else:
            url = v.get("url") or "(no url)"
            print(f"  {label} [{k}]: {url}")

# Always print SOMETHING about the Genie space. If gid wasn't resolved earlier
# (title-lookup silently failed), try once more here, then fall back to a
# helpful line telling the user where to look.
if not gid and host:
    title = os.environ.get("GENIE_SPACE_TITLE") or "ProServ OCFO"
    try:
        rr = subprocess.run(
            ["databricks", "api", "get", "/api/2.0/genie/spaces", "--profile", os.environ["PROFILE"]],
            capture_output=True, text=True, check=False,
        )
        if rr.returncode == 0 and rr.stdout:
            payload = json.loads(rr.stdout)
            for sp in (payload.get("spaces") or []):
                if (sp.get("title") or "").strip() == title.strip():
                    gid = sp.get("space_id") or sp.get("id") or ""
                    break
    except Exception:
        pass

if gid and host:
    print(f"  Genie space [{gid}]: {host}/genie/rooms/{gid}")
elif host:
    title = os.environ.get("GENIE_SPACE_TITLE") or "ProServ OCFO"
    print(f"  Genie space: (auto-lookup by title '{title}' failed)")
    print(f"               Browse {host}/genie or open the provision_genie_space")
    print(f"               task output in the cfo_data_pipeline job for the URL.")
print()
PYEOF

echo "Notes:"
echo "  • First-time deploys: the 'generate_insights' task continues running in the"
echo "    background after this script exits — typically ~30-45 min depending on your"
echo "    Genie QPM tier. The app is fully usable once Genie is provisioned;"
echo "    Executive Summary insight tiles populate as that task progresses."
echo "  • Customer-specific: open and run notebooks/customer_mapping.py to swap"
echo "    the synthetic data for views over your real source tables."
