#!/usr/bin/env bash
# CFO Operations Platform — teardown / decommission.
#
# The inverse of deploy.sh. Removes everything the demo creates, with no
# orphans, and is safe to re-run (every step is idempotent — a second run just
# reports "nothing to remove"). After teardown you can run deploy.sh again to
# rebuild the demo from zero.
#
# What it removes:
#   1. DAB-managed resources (Databricks App, cfo_data_pipeline job, both
#      Lakeview dashboards, synced workspace files) via `databricks bundle destroy`.
#   2. The Genie space — created at runtime by the pipeline, so it has NO DAB
#      resource type and bundle destroy can't touch it. Removed via
#      scripts/delete_genie_space.py (deletes by title, matching deploy.sh's
#      --genie-space-title lookup).
#   3. The Unity Catalog schema and every table/view/metric-view setup created
#      in it, via `DROP SCHEMA ... CASCADE`.
#
# Usage:
#     ./teardown.sh --profile <name> \
#                   --schema <name> \
#                   --warehouse-id <id> \
#                   [--catalog <name>] \
#                   [--target <name>] \
#                   [--genie-space-title <title>] \
#                   [--genie-space-id <id>] \
#                   [--workspace-root-path <path>] \
#                   [--app-name <name>] \
#                   [--keep-schema] \
#                   [--yes] [--force-lock]

set -euo pipefail

PROFILE=""
CATALOG="main"
SCHEMA=""
WAREHOUSE_ID=""
TARGET=""
APP_NAME="cfo-demo"
WORKSPACE_ROOT_PATH=""
GENIE_SPACE_ID=""
GENIE_SPACE_TITLE="CFO Demo"
KEEP_SCHEMA=""
ASSUME_YES=""
FORCE_LOCK=""

# Directory this script lives in — so scripts/ helpers resolve regardless of cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<EOF
Usage: $0 --profile <name> --schema <name> --warehouse-id <id> [options]

Required:
  --profile               Databricks CLI profile (sets workspace host + auth)
  --schema                Schema name that setup created (under {catalog})
  --warehouse-id          SQL warehouse ID used to run DROP SCHEMA

Optional:
  --catalog               Unity Catalog (default: "main")
  --target                Bundle target from databricks.yml (default: bundle default)
  --app-name              Databricks App name (default: "cfo-demo") — must match
                          how you deployed, so bundle destroy targets the right app
  --workspace-root-path   Bundle sync root used at deploy time (default: bundle default)
  --genie-space-title     Genie space title to delete (default: "CFO Demo")
  --genie-space-id        Explicit Genie space ID (skips title lookup)
  --keep-schema           Do NOT drop the UC schema (keep the demo data/tables)
  --yes                   Skip the interactive confirmation prompt
  --force-lock            Pass --force-lock to bundle destroy (steal a stale deploy lock)

Examples:
  # Full teardown (matches a default deploy.sh deploy)
  ./teardown.sh --profile <your-profile> --schema cfo_proserv --warehouse-id <id>

  # Teardown a named target / app, non-interactive
  ./teardown.sh --profile <your-profile> --target dev --app-name cfo-demo-test \\
                --schema proserv_cfo --warehouse-id <id> \\
                --genie-space-title "CFO Demo — Dev" --yes
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
    --keep-schema) KEEP_SCHEMA="1"; shift 1 ;;
    --yes|-y) ASSUME_YES="1"; shift 1 ;;
    --force-lock) FORCE_LOCK="1"; shift 1 ;;
    -h|--help) usage ;;
    *) echo "Unknown arg: $1"; usage ;;
  esac
done

# warehouse-id is only needed when we're dropping the schema.
[[ -z "$PROFILE" || -z "$SCHEMA" ]] && usage
if [[ -z "$KEEP_SCHEMA" && -z "$WAREHOUSE_ID" ]]; then
  echo "ERROR: --warehouse-id is required to drop the schema (or pass --keep-schema)."
  usage
fi

# Build --target arg array (empty if no target — bundle uses its default).
TARGET_ARGS=()
if [[ -n "$TARGET" ]]; then
  TARGET_ARGS=(--target "$TARGET")
fi

# --var args so `bundle destroy` resolves the same bundle config deploy used.
# (The bundle's default workspace_root_path calls ${workspace.current_user...},
# which needs auth; passing the vars keeps destroy consistent with the deploy.)
DESTROY_VARS=(
  --var "app_name=$APP_NAME"
  --var "catalog=$CATALOG"
  --var "schema_name=$SCHEMA"
  --var "warehouse_id=${WAREHOUSE_ID:-unused}"
  --var "genie_space_title=$GENIE_SPACE_TITLE"
)
if [[ -n "$WORKSPACE_ROOT_PATH" ]]; then
  DESTROY_VARS+=(--var "workspace_root_path=$WORKSPACE_ROOT_PATH")
fi

echo "═══════════════════════════════════════════════════════════"
echo " CFO Demo Teardown"
echo "─────────────────────────────────────────────────────────"
echo "  Profile:           $PROFILE"
echo "  Target:            ${TARGET:-(bundle default)}"
echo "  App name:          $APP_NAME"
echo "  Catalog.Schema:    $CATALOG.$SCHEMA"
echo "  Warehouse ID:      ${WAREHOUSE_ID:-(n/a — schema kept)}"
echo "  Genie space:       ${GENIE_SPACE_ID:-title \"$GENIE_SPACE_TITLE\"}"
echo "  Drop schema:       ${KEEP_SCHEMA:+NO (--keep-schema)}${KEEP_SCHEMA:-YES (DROP SCHEMA $CATALOG.$SCHEMA CASCADE)}"
echo "═══════════════════════════════════════════════════════════"
echo ""

if [[ -z "$ASSUME_YES" ]]; then
  echo "This will permanently delete the demo's app, job, dashboards, Genie space,"
  [[ -z "$KEEP_SCHEMA" ]] && echo "and the ENTIRE schema $CATALOG.$SCHEMA (all tables/views) via DROP SCHEMA CASCADE."
  read -r -p "Type 'yes' to proceed: " CONFIRM
  if [[ "$CONFIRM" != "yes" ]]; then
    echo "Aborted."
    exit 1
  fi
  echo ""
fi

# ───────────────────────────────────────────────────────────
# 1. Destroy DAB-managed resources (app, job, dashboards, synced files).
#    Idempotent: destroying an already-destroyed / never-deployed target is a
#    no-op that exits 0.
# ───────────────────────────────────────────────────────────
echo "[1/3] databricks bundle destroy (app + job + dashboards + files)..."
databricks bundle destroy \
  --profile "$PROFILE" \
  ${TARGET_ARGS[@]+"${TARGET_ARGS[@]}"} \
  "${DESTROY_VARS[@]}" \
  --auto-approve \
  ${FORCE_LOCK:+--force-lock} || {
    echo "  WARNING: bundle destroy returned non-zero (may already be destroyed)." >&2
  }
echo ""

# ───────────────────────────────────────────────────────────
# 2. Delete the Genie space (no DAB resource type — bundle destroy can't).
#    Idempotent: no matching space => no-op.
# ───────────────────────────────────────────────────────────
echo "[2/3] Deleting Genie space..."
GENIE_DELETE_ARGS=(--profile "$PROFILE")
if [[ -n "$GENIE_SPACE_ID" ]]; then
  GENIE_DELETE_ARGS+=(--space-id "$GENIE_SPACE_ID")
else
  GENIE_DELETE_ARGS+=(--title "$GENIE_SPACE_TITLE")
fi
python3 "$SCRIPT_DIR/scripts/delete_genie_space.py" "${GENIE_DELETE_ARGS[@]}" || {
  echo "  WARNING: Genie space deletion returned non-zero — check the Genie UI." >&2
}
echo ""

# ───────────────────────────────────────────────────────────
# 3. Drop the Unity Catalog schema + everything setup created in it.
#    DROP SCHEMA ... CASCADE removes all tables, views, and metric views.
#    IF EXISTS makes it idempotent. Skipped with --keep-schema.
# ───────────────────────────────────────────────────────────
if [[ -n "$KEEP_SCHEMA" ]]; then
  echo "[3/3] --keep-schema set → leaving $CATALOG.$SCHEMA in place."
else
  echo "[3/3] Dropping schema $CATALOG.$SCHEMA (CASCADE)..."
  databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "{
    \"statement\": \"DROP SCHEMA IF EXISTS \`$CATALOG\`.\`$SCHEMA\` CASCADE\",
    \"warehouse_id\": \"$WAREHOUSE_ID\",
    \"wait_timeout\": \"50s\"
  }" > /dev/null && echo "  ✓ Dropped $CATALOG.$SCHEMA" || {
    echo "  WARNING: DROP SCHEMA returned non-zero — it may already be gone, or you" >&2
    echo "           may lack DROP on $CATALOG.$SCHEMA. Verify in Unity Catalog." >&2
  }
fi
echo ""

echo "═══════════════════════════════════════════════════════════"
echo " Teardown complete."
echo " Rebuild any time with:  ./deploy.sh --profile $PROFILE \\"
echo "     --catalog $CATALOG --schema $SCHEMA --warehouse-id ${WAREHOUSE_ID:-<id>}"
echo "═══════════════════════════════════════════════════════════"
