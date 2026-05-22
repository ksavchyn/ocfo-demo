# Databricks notebook source
# DBTITLE 1,Provision (Create or Update) the merged Genie space
# MAGIC %md
# MAGIC # Genie Space Provisioner
# MAGIC
# MAGIC Notebook-task wrapper around `bootstrap_genie_space.provision()`.
# MAGIC
# MAGIC Runs as the FIRST step of the bundle's data pipeline (before the data layer
# MAGIC notebooks fire) so the dev/prod Genie space is created or kept in sync with
# MAGIC the curated `genie_config/` content (instructions, trusted queries, SQL
# MAGIC snippets, table inventory).
# MAGIC
# MAGIC **Idempotent**: if a Genie space with the configured title already exists,
# MAGIC the notebook UPDATES it. Otherwise it CREATES a new one. The resulting
# MAGIC space ID is printed prominently and written to a workspace file so the
# MAGIC orchestrator (`generate_insights.py`) can read it.

# COMMAND ----------

# DBTITLE 1,Configuration via widgets
import os
import sys
import json
from pathlib import Path

# Bundle's notebook_task.base_parameters flow through here.
try:
    dbutils.widgets.text("CFO_WAREHOUSE_ID", "")  # noqa: F821 — bundle populates from ${var.warehouse_id}
    dbutils.widgets.text("CFO_CATALOG", "main")  # noqa: F821
    dbutils.widgets.text("CFO_SCHEMA_NAME", "cfo_proserv")  # noqa: F821 — bundle populates from ${var.schema_name}
    dbutils.widgets.text("CFO_GENIE_SPACE_TITLE", "ProServ OCFO")  # noqa: F821
    dbutils.widgets.text("CFO_GENIE_SPACE_ID", "")  # noqa: F821 — empty = create new or look up by title
    dbutils.widgets.text("CFO_DASHBOARD_REFRESH_CRON", "0 0 0/12 * * ?")  # noqa: F821 — every 12h UTC, customer can override
    dbutils.widgets.text("CFO_FINANCE_DASHBOARD_ID", "")  # noqa: F821 — passed by bundle from ${resources.dashboards.cfo_finance_dashboard.id}
    dbutils.widgets.text("CFO_ADMIN_DASHBOARD_ID", "")  # noqa: F821 — passed by bundle from ${resources.dashboards.admin_dashboard.id}
    _WIDGETS = True
except Exception:
    _WIDGETS = False


def _config(name: str, default: str) -> str:
    if _WIDGETS:
        try:
            v = dbutils.widgets.get(name)  # noqa: F821
            if v:
                return v
        except Exception:
            pass
    return os.environ.get(name, default)


WAREHOUSE_ID = _config("CFO_WAREHOUSE_ID", "")
if not WAREHOUSE_ID:
    raise ValueError("CFO_WAREHOUSE_ID not set — bundle's notebook_task.base_parameters should populate this from ${var.warehouse_id}.")
CATALOG = _config("CFO_CATALOG", "main")
SCHEMA_NAME = _config("CFO_SCHEMA_NAME", "cfo_proserv")
TITLE = _config("CFO_GENIE_SPACE_TITLE", "ProServ OCFO")
EXISTING_SPACE_ID = _config("CFO_GENIE_SPACE_ID", "").strip()
SCHEMA_OVERRIDE = f"{CATALOG}.{SCHEMA_NAME}"

print(f"Warehouse:        {WAREHOUSE_ID}")
print(f"Schema override:  {SCHEMA_OVERRIDE}")
print(f"Space title:      {TITLE}")
print(f"Existing ID hint: {EXISTING_SPACE_ID or '(none — will look up by title or create)'}")

# COMMAND ----------

# DBTITLE 1,Locate existing space by title (idempotency)
# If CFO_GENIE_SPACE_ID is set, use it directly. Otherwise list spaces by title
# to find an existing one before creating a new one. Avoids producing duplicate
# spaces when the bundle re-runs.
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def find_space_by_title(title: str) -> str | None:
    """Return space_id of a Genie space whose title matches, or None.

    SDK's list_spaces returns paginated results; for our small workspace context,
    a single page is enough.
    """
    try:
        for s in w.genie.list_spaces():
            if (s.title or "").strip() == title.strip():
                return s.space_id
    except Exception as e:
        print(f"  list_spaces failed (continuing with create): {type(e).__name__}: {e}")
    return None


resolved_space_id = EXISTING_SPACE_ID or find_space_by_title(TITLE)
print(f"Resolved space_id: {resolved_space_id or '(creating new)'}")

# COMMAND ----------

# DBTITLE 1,Provision (create or update)
# Make the bootstrap script importable. Notebook runs from genie_insights/ so the
# script is right next to it.
sys.path.insert(0, str(Path("genie_insights").resolve()))
sys.path.insert(0, ".")

from bootstrap_genie_space import provision  # noqa: E402

new_space_id = provision(
    warehouse_id=WAREHOUSE_ID,
    space_id=resolved_space_id,
    title=TITLE,
    schema_override=SCHEMA_OVERRIDE,
)

print()
print("=" * 60)
print(f"GENIE SPACE READY")
print("=" * 60)
print(f"  Space ID: {new_space_id}")
print(f"  URL:      {w.config.host.rstrip('/')}/genie/rooms/{new_space_id}")
print()
print("ADD THIS TO YOUR BUNDLE VARIABLES:")
print(f'  --var genie_space_id={new_space_id}')
print()

# CRITICAL — propagate the just-resolved space_id to the downstream
# `generate_insights` task via Databricks task values. The bundle's job
# resolves task base_parameters at JOB SUBMIT time, so on a first-time deploy
# `${var.genie_space_id}` is still empty for generate_insights. Task values
# are resolved at TASK-START time, so this delivers the real ID. Without
# this, generate_insights skips chip pre-caching (see git history for the
# 2026-05-16 incident where this manifested).
try:
    dbutils.jobs.taskValues.set(key="space_id", value=new_space_id)  # noqa: F821
    print(f"Task value set: tasks.provision_genie_space.values.space_id = {new_space_id}")
except Exception as e:
    print(f"Could not set task value (running outside a job?): {e}")

# COMMAND ----------

# DBTITLE 1,Persist space_id for downstream notebooks
# Write the resolved space_id back to merged_space_id.json (in the working dir)
# so the orchestrator notebook can pick it up via _config fallback if widgets
# don't propagate. Customer can also pass --var genie_space_id at deploy time.
try:
    out_path = Path("genie_insights/merged_space_id.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "space_id": new_space_id,
        "warehouse_id": WAREHOUSE_ID,
    }, indent=2))
    print(f"Wrote {out_path}")
except Exception as e:
    print(f"Could not persist space ID file (non-fatal): {e}")

# COMMAND ----------

# DBTITLE 1,Dashboard setup — publish + CAN_READ + 12h refresh schedule
# Three things the customer-deployed dashboards need that bundle deploy alone
# doesn't do:
#   1. PUBLISH — bundle deploy creates the dashboard in DRAFT state; users can't
#      view published embeds until the publish API is called.
#   2. PERMISSIONS — by default only the deploying user can view; we grant
#      CAN_READ to the 'users' group so the app + workspace users can view.
#   3. AUTO-REFRESH SCHEDULE — embedded dashboards show stale data without a
#      schedule. Default: every 12h UTC. Override via CFO_DASHBOARD_REFRESH_CRON
#      widget at the top of this notebook (any Quartz cron expression).
# All three are idempotent — re-running this notebook on customer deploys is safe.

DASHBOARD_REFRESH_CRON = _config("CFO_DASHBOARD_REFRESH_CRON", "0 0 0/12 * * ?")  # every 12h

def _get_bundle_dashboard_ids() -> dict:
    """Return {label: dashboard_id} for the bundle's Admin + Finance dashboards.

    Bundle passes the IDs as task parameters (CFO_FINANCE_DASHBOARD_ID and
    CFO_ADMIN_DASHBOARD_ID, resolved from ${resources.dashboards.*.id} in
    databricks.yml). No workspace-wide dashboard scan needed. If either ID is
    empty (e.g., customer's bundle omits dashboard resources), it's skipped."""
    found = {}
    finance_id = _config("CFO_FINANCE_DASHBOARD_ID", "").strip()
    admin_id = _config("CFO_ADMIN_DASHBOARD_ID", "").strip()
    if finance_id:
        found["finance"] = finance_id
    if admin_id:
        found["admin"] = admin_id
    return found

def _setup_dashboard(dashboard_id: str, label: str) -> None:
    """Publish + permission + schedule a single dashboard. Idempotent."""
    from databricks.sdk.service.dashboards import Schedule, CronSchedule
    from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

    print(f"  [{label}] dashboard_id={dashboard_id}")

    # 1. Publish with embed credentials so the app's iframe doesn't require re-auth.
    try:
        w.lakeview.publish(dashboard_id=dashboard_id, embed_credentials=True, warehouse_id=WAREHOUSE_ID)
        print(f"    ✅ Published (embed_credentials=True)")
    except Exception as e:
        msg = str(e)
        if "already" in msg.lower() or "ALREADY_EXISTS" in msg.upper():
            print(f"    ⏭️  Already published")
        else:
            print(f"    ⚠️  Publish: {msg[:200]}")

    # 2. Grant CAN_READ to all workspace users (covers app's service principal too).
    try:
        w.permissions.update(
            request_object_type="dashboards",
            request_object_id=dashboard_id,
            access_control_list=[
                AccessControlRequest(group_name="users", permission_level=PermissionLevel.CAN_READ),
            ],
        )
        print(f"    ✅ CAN_READ granted to 'users' group")
    except Exception as e:
        print(f"    ⚠️  Permissions: {str(e)[:200]}")

    # 3. Create a refresh schedule if none exists. Default every 12h UTC.
    try:
        existing = list(w.lakeview.list_schedules(dashboard_id=dashboard_id))
        if existing:
            print(f"    ⏭️  Schedule already exists ({len(existing)}); leaving unchanged")
        else:
            sched = w.lakeview.create_schedule(
                dashboard_id=dashboard_id,
                schedule=Schedule(
                    display_name="Auto-refresh (CFO bundle default)",
                    cron_schedule=CronSchedule(
                        quartz_cron_expression=DASHBOARD_REFRESH_CRON,
                        timezone_id="UTC",
                    ),
                ),
            )
            print(f"    ✅ Schedule created ({DASHBOARD_REFRESH_CRON}): {sched.schedule_id}")
    except Exception as e:
        print(f"    ⚠️  Schedule: {str(e)[:200]}")


print()
print("=" * 60)
print("DASHBOARD SETUP — publish + permissions + auto-refresh")
print("=" * 60)
dash_ids = _get_bundle_dashboard_ids()
if not dash_ids:
    print("  No bundle-provisioned dashboards found — skipping dashboard setup.")
else:
    for label, did in dash_ids.items():
        _setup_dashboard(did, label)
print()
