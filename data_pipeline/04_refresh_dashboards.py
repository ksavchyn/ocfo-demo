# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Dashboards
# MAGIC
# MAGIC Final orchestrator task. After `validate_consistency` passes (so we know
# MAGIC the chip cache + KPIs are clean), trigger an immediate refresh on the two
# MAGIC Lakeview dashboards so the embedded iframes in the app reflect the
# MAGIC just-regenerated data without anyone having to click "Refresh" in the UI.
# MAGIC
# MAGIC Mechanism: for each dashboard, find its first schedule (set up at deploy
# MAGIC time by `scripts/setup_dashboard_schedules.py`) and trigger a one-shot
# MAGIC run. This bypasses Lakeview's cached query results for the next render.
# MAGIC If no schedule exists yet (e.g. first-time deploy where the schedule
# MAGIC setup raced ahead of this task), this is a no-op — the schedule will
# MAGIC fire on its next scheduled time and customers see fresh data then.
# MAGIC
# MAGIC Non-fatal by design: any API hiccup just leaves dashboards on their
# MAGIC normal Lakeview cache TTL. Doesn't fail the orchestrator job.

# COMMAND ----------

# DBTITLE 1,Configuration
import os
import sys

# Bundle's base_parameters pass these in.
try:
    dbutils.widgets.text("CFO_FINANCE_DASHBOARD_ID", "")  # noqa: F821
    dbutils.widgets.text("CFO_ADMIN_DASHBOARD_ID", "")    # noqa: F821
    FIN_ID = dbutils.widgets.get("CFO_FINANCE_DASHBOARD_ID")  # noqa: F821
    ADM_ID = dbutils.widgets.get("CFO_ADMIN_DASHBOARD_ID")    # noqa: F821
except Exception:
    FIN_ID = os.environ.get("CFO_FINANCE_DASHBOARD_ID", "")
    ADM_ID = os.environ.get("CFO_ADMIN_DASHBOARD_ID", "")

print(f"[refresh_dashboards] finance_id={FIN_ID[:20]}… admin_id={ADM_ID[:20]}…")

# COMMAND ----------

# DBTITLE 1,Trigger an immediate run for each dashboard's first schedule
from databricks.sdk import WorkspaceClient

wc = WorkspaceClient()


def _refresh_dashboard(dashboard_id: str, label: str) -> None:
    """Force the Lakeview dashboard to re-execute its tile queries.

    Mechanism: re-publish the current draft. Lakeview's published-snapshot
    version is part of the query cache key, so re-publishing invalidates every
    tile's cached result. The next embed/view triggers fresh SQL execution
    against the same warehouse that was previously published with.

    Falls back to a pause/unpause schedule bounce if publish is not available
    (older SDK) and finally to a true no-op if neither path works — this task
    is non-fatal by design.
    """
    if not dashboard_id:
        print(f"  [{label}] no dashboard_id provided; skipping")
        return

    # Primary path: re-publish to invalidate the published-snapshot cache key.
    try:
        wc.lakeview.publish(dashboard_id=dashboard_id, embed_credentials=True)
        print(f"  [{label}] re-published dashboard — tile queries will execute fresh on next view")
        return
    except Exception as e:
        print(f"  [{label}] publish failed ({type(e).__name__}: {e}); falling back to schedule bounce")

    # Fallback: pause + unpause the first schedule (actually mutates state,
    # unlike the prior no-op update_schedule(schedule=sched) call).
    try:
        from databricks.sdk.service.dashboards import SchedulePauseStatus

        schedules = list(wc.lakeview.list_schedules(dashboard_id=dashboard_id))
        if not schedules:
            print(f"  [{label}] no schedule to bounce; dashboard refresh deferred to next scheduled run")
            return

        sched = schedules[0]
        sched_id = sched.schedule_id

        wc.lakeview.update_schedule(
            dashboard_id=dashboard_id,
            schedule_id=sched_id,
            cron_schedule=sched.cron_schedule,
            display_name=sched.display_name,
            pause_status=SchedulePauseStatus.PAUSED,
        )
        wc.lakeview.update_schedule(
            dashboard_id=dashboard_id,
            schedule_id=sched_id,
            cron_schedule=sched.cron_schedule,
            display_name=sched.display_name,
            pause_status=SchedulePauseStatus.UNPAUSED,
        )
        print(f"  [{label}] bounced schedule '{sched.display_name}' — refresh on next view")
    except Exception as e:
        print(f"  [{label}] schedule bounce also failed ({type(e).__name__}: {e}); refresh deferred to next scheduled run")


_refresh_dashboard(FIN_ID, "cfo_finance_dashboard")
_refresh_dashboard(ADM_ID, "admin_dashboard")

print("\n[refresh_dashboards] complete.")
