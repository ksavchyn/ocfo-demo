#!/usr/bin/env python3
"""Ensure each Lakeview dashboard has a daily refresh schedule.

Idempotent. Run from deploy.sh after the bundle deploy completes (so dashboards
exist + their IDs are resolvable from bundle summary).

For each dashboard:
  - If at least one schedule already exists → leave alone (don't clobber a
    customer-customized schedule).
  - Otherwise → create a daily 4 AM schedule in the user's local timezone so
    dashboard queries re-execute against fresh data overnight, without anyone
    having to click Refresh in the UI.

DABs don't currently support `schedule:` as a first-class field on the
`lakeview.dashboards` resource, so we wire schedules via API after deploy.
Customer's workspace can edit them in the UI afterward without us overwriting.
"""
from __future__ import annotations

import argparse
import sys

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.dashboards import Schedule, CronSchedule, SchedulePauseStatus
except Exception as e:
    sys.stderr.write(f"Failed to import Databricks SDK: {e}\n")
    sys.exit(0)  # Non-fatal — deploy continues without scheduling


DEFAULT_CRON = "0 0 4 * * ?"  # 4:00 AM every day
DEFAULT_TIMEZONE = "America/New_York"
DEFAULT_DISPLAY_NAME = "Daily refresh (auto-provisioned)"


def _ensure_schedule(wc: WorkspaceClient, dashboard_id: str, dashboard_label: str,
                     cron: str, timezone_id: str) -> None:
    """Create a daily schedule on `dashboard_id` if it has none. No-op if any
    schedule already exists (don't clobber user-set schedules)."""
    try:
        existing = list(wc.lakeview.list_schedules(dashboard_id=dashboard_id))
    except Exception as e:
        print(f"  [{dashboard_label}] could not list schedules ({type(e).__name__}: {e}); skipping")
        return

    if existing:
        names = ", ".join((s.display_name or "<unnamed>") for s in existing)
        print(f"  [{dashboard_label}] already has {len(existing)} schedule(s) — leaving alone: {names}")
        return

    sched = Schedule(
        display_name=DEFAULT_DISPLAY_NAME,
        cron_schedule=CronSchedule(quartz_cron_expression=cron, timezone_id=timezone_id),
        pause_status=SchedulePauseStatus.UNPAUSED,
    )
    try:
        created = wc.lakeview.create_schedule(dashboard_id=dashboard_id, schedule=sched)
        print(f"  [{dashboard_label}] created schedule '{created.display_name}' ({cron} {timezone_id})")
    except Exception as e:
        print(f"  [{dashboard_label}] could not create schedule ({type(e).__name__}: {e}); skipping")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profile", required=True, help="Databricks CLI profile")
    p.add_argument("--finance-dashboard-id", required=True)
    p.add_argument("--admin-dashboard-id", required=True)
    p.add_argument("--cron", default=DEFAULT_CRON, help=f"Quartz cron (default: '{DEFAULT_CRON}' = 4 AM daily)")
    p.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    args = p.parse_args()

    import os
    os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile
    wc = WorkspaceClient(profile=args.profile)

    print(f"Ensuring daily refresh schedule for both dashboards (cron='{args.cron}' tz='{args.timezone}')…")
    _ensure_schedule(wc, args.finance_dashboard_id, "cfo_finance_dashboard", args.cron, args.timezone)
    _ensure_schedule(wc, args.admin_dashboard_id,  "admin_dashboard",        args.cron, args.timezone)


if __name__ == "__main__":
    main()
