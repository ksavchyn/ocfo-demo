#!/usr/bin/env python3
"""Create / refresh Unity Catalog metric views from local YAML definitions.

Reads `genie_config/metric_views/*.yml` and applies each one as a
`CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML` DDL statement against
the configured SQL warehouse.

Why a separate script from `02_build_silver_gold.py`:
  - Metric views depend on silver/gold tables existing, but don't need data
    regeneration every time we tune a KPI definition.
  - Decoupled iteration: change a measure, rerun this, refresh the Genie space.

Usage:
  python apply_metric_views.py --view firm_kpis_mv --dry-run
  python apply_metric_views.py --view firm_kpis_mv
  python apply_metric_views.py --view all
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

DEFAULT_PROFILE = "DEFAULT"
DEFAULT_CATALOG = "main"
DEFAULT_SCHEMA = "cfo_proserv"
CONFIG_DIR = Path(__file__).parent / "metric_views"


def list_view_names() -> list[str]:
    return sorted(p.stem for p in CONFIG_DIR.glob("*.yml"))


SOURCE_FQN_PLACEHOLDER = "main.cfo_proserv"


def build_ddl(view_name: str, yaml_body: str, catalog: str, schema: str) -> str:
    """Compose the CREATE OR REPLACE VIEW DDL for a metric view.

    Databricks metric view DDL embeds the YAML definition inside a
    `LANGUAGE YAML AS $$ ... $$` block. The $$ delimiter avoids needing to
    escape internal quotes. The YAML ships with `main.cfo_proserv` as the
    placeholder source FQN; substitute it with the deploy-time catalog.schema
    so source/join tables resolve in the customer's deploy.
    """
    fqn = f"{catalog}.{schema}.{view_name}"
    yaml_body = yaml_body.replace(SOURCE_FQN_PLACEHOLDER, f"{catalog}.{schema}")
    return f"""\
CREATE OR REPLACE VIEW {fqn}
WITH METRICS
LANGUAGE YAML
AS $$
{yaml_body.rstrip()}
$$
"""


def apply_view(client: WorkspaceClient, warehouse_id: str, view_name: str,
               catalog: str, schema: str, dry_run: bool) -> None:
    yaml_path = CONFIG_DIR / f"{view_name}.yml"
    if not yaml_path.exists():
        raise FileNotFoundError(f"No metric view YAML at {yaml_path}")
    yaml_body = yaml_path.read_text()
    ddl = build_ddl(view_name, yaml_body, catalog, schema)

    print(f"=== {catalog}.{schema}.{view_name} ===")
    print(f"  Source: {yaml_path}")
    print(f"  YAML size: {len(yaml_body)} chars")

    if dry_run:
        print("  DRY RUN — printing DDL only:")
        print("  " + "\n  ".join(ddl.splitlines()))
        return

    print("  Applying via statement_execution...")
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=ddl,
        wait_timeout="50s",
    )
    if resp.status and resp.status.state and str(resp.status.state) != "StatementState.SUCCEEDED":
        err = (resp.status.error.message if resp.status.error else "unknown")
        raise RuntimeError(f"DDL failed: state={resp.status.state} error={err}")
    print("  ✓ Applied.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--view", default="all",
                        help="Metric view stem (e.g., firm_kpis_mv) or 'all'.")
    parser.add_argument("--catalog", default=os.environ.get("CFO_CATALOG", DEFAULT_CATALOG))
    parser.add_argument("--schema", default=os.environ.get("CFO_SCHEMA", DEFAULT_SCHEMA))
    parser.add_argument("--warehouse-id", default=os.environ.get("CFO_WAREHOUSE_ID", ""))
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.warehouse_id and not args.dry_run:
        sys.exit("CFO_WAREHOUSE_ID env var (or --warehouse-id) is required for non-dry-run.")

    client = WorkspaceClient(profile=args.profile) if not args.dry_run else None
    views = list_view_names() if args.view == "all" else [args.view]
    for v in views:
        try:
            apply_view(client, args.warehouse_id, v, args.catalog, args.schema, args.dry_run)
        except Exception as e:
            print(f"  ✗ FAILED on {v}: {e}", file=sys.stderr)
            raise


if __name__ == "__main__":
    main()
