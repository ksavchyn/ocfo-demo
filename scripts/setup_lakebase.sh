#!/usr/bin/env bash
# One-time Lakebase setup for the CFO app's operational serving layer.
# Run AFTER the data pipeline job has built gold_ai_cost_by_cost_center.
#
#   ./scripts/setup_lakebase.sh <profile> <uc_catalog> <uc_schema>
#   e.g. ./scripts/setup_lakebase.sh kat-personal kat_personal_catalog cfo_febar
#
# Creates:
#   1. Lakebase Autoscaling project "cfo-ops" (production branch + primary endpoint, scale-to-zero)
#   2. Synced table <uc_catalog>.<uc_schema>.ai_budget_serving -> Postgres <uc_schema>.ai_budget_serving
#      (snapshot sync from <uc_catalog>.<uc_schema>.gold_ai_cost_by_cost_center, PK cost_center).
#      Created in the existing UC catalog, so no CREATE CATALOG privilege is needed.
set -euo pipefail
PROFILE=${1:?profile}; CAT=${2:?uc catalog}; SCH=${3:?uc schema}
PROJECT=cfo-ops; BRANCH=projects/$PROJECT/branches/production

echo "== 1. Lakebase project"
databricks postgres create-project $PROJECT --json '{"spec": {"display_name": "CFO Ops (AI Cost & Governance)"}}' --profile $PROFILE -o json \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('name'), d.get('status',{}).get('pg_version',''))"
databricks postgres list-endpoints $BRANCH --profile $PROFILE -o json \
  | python3 -c "import json,sys; [print(e['name'], e['status']['hosts']['host'], e['status'].get('current_state','')) for e in json.load(sys.stdin)]"

echo "== 2. Synced table (UC gold -> Lakebase Postgres <uc_schema>.ai_budget_serving)"
databricks postgres create-synced-table $CAT.$SCH.ai_budget_serving --json "{
  \"spec\": {
    \"source_table_full_name\": \"$CAT.$SCH.gold_ai_cost_by_cost_center\",
    \"primary_key_columns\": [\"cost_center\"],
    \"scheduling_policy\": \"SNAPSHOT\",
    \"branch\": \"$BRANCH\",
    \"postgres_database\": \"databricks_postgres\",
    \"create_database_objects_if_missing\": true,
    \"new_pipeline_spec\": {\"storage_catalog\": \"$CAT\", \"storage_schema\": \"$SCH\"}
  }}" --profile $PROFILE -o json | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('name'), d.get('status',{}).get('detailed_state',''))"
echo "Done. Endpoint for the app: $BRANCH/endpoints/primary"
