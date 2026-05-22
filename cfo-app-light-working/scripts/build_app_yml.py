#!/usr/bin/env python3
"""Materialize the bundle's resolved apps.config.env into the app's runtime
app.yml file.

Why this exists:
    `databricks bundle deploy` sets env vars on the Databricks App *resource*
    (visible in Apps UI > Environment tab). But the running app process reads
    its runtime env from app.yml in the source code, not from the resource
    config. So we read the resolved env block out of the bundle summary and
    write it to app.yml, where the runtime actually looks.

When this runs:
    deploy.sh calls this script AFTER `databricks bundle deploy` has run once
    (so dashboard IDs are assigned and references like
    ${resources.dashboards.cfo_finance_dashboard.id} resolve to real IDs).
    deploy.sh then runs `databricks bundle deploy` a second time to sync the
    regenerated app.yml to the workspace, followed by `databricks apps deploy`
    which restarts the app.

Usage:
    python3 scripts/build_app_yml.py \\
        --profile <name> \\
        --catalog <name> \\
        --schema <name> \\
        --warehouse-id <id> \\
        --app-name <name> \\
        --workspace-root-path <path> \\
        [--genie-space-id <id>] \\
        [--app-yml-path app.yml]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def _bundle_summary(args: argparse.Namespace) -> dict:
    cmd = [
        "databricks", "bundle", "summary",
        "--profile", args.profile,
        "-o", "json",
        "--var", f"app_name={args.app_name}",
        "--var", f"workspace_root_path={args.workspace_root_path}",
        "--var", f"catalog={args.catalog}",
        "--var", f"schema_name={args.schema}",
        "--var", f"warehouse_id={args.warehouse_id}",
    ]
    # CRITICAL: must summarize the SAME target deploy.sh just deployed to.
    # Without --target, `bundle summary` falls back to the bundle's default
    # target — which can have stale (or entirely different) dashboard IDs in
    # its terraform state, producing an app.yml that points at the wrong
    # workspace's dashboards.
    if args.target:
        cmd += ["--target", args.target]
    if args.genie_space_id:
        cmd += ["--var", f"genie_space_id={args.genie_space_id}"]

    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
        sys.exit(f"`databricks bundle summary` failed with exit code {r.returncode}.")
    return json.loads(r.stdout)


def _find_app_config(summary: dict, app_name: str) -> dict:
    """Pull the resolved config block (command + env) for our app."""
    apps = (summary.get("resources") or {}).get("apps") or {}
    if not apps:
        sys.exit("No apps found in bundle summary.")

    # Try to match by resolved app name; fall back to first if there's only one.
    for entry in apps.values():
        if (entry.get("name") or "").strip() == app_name.strip():
            return entry.get("config") or {}

    if len(apps) == 1:
        only = next(iter(apps.values()))
        return only.get("config") or {}

    sys.exit(f"No app named '{app_name}' in bundle summary (found: {list(apps.keys())}).")


def _yaml_escape(value: str) -> str:
    """Escape a value for inclusion inside double quotes in YAML."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profile", required=True)
    p.add_argument("--target", default="", help="Bundle target (dev/prod/etc.) — must match deploy.sh's --target so summary reads the right terraform state.")
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--warehouse-id", required=True)
    p.add_argument("--app-name", required=True)
    p.add_argument("--workspace-root-path", required=True)
    p.add_argument("--genie-space-id", default="")
    p.add_argument("--app-yml-path", default="app.yml")
    args = p.parse_args()

    summary = _bundle_summary(args)
    config = _find_app_config(summary, args.app_name)

    command = config.get("command") or ["python", "app.py"]
    env = config.get("env") or []
    if not env:
        sys.exit(
            "Bundle summary returned an empty env list for the app. "
            "Check databricks.yml apps.<key>.config.env."
        )

    # `databricks bundle summary` resolves plain ${var.X} references in the
    # apps.config.env block, but NOT ${workspace.host} or
    # ${resources.dashboards.X.id} references. Resolve those ourselves by
    # reading them from elsewhere in the summary and substituting.
    dashboards = (summary.get("resources") or {}).get("dashboards") or {}
    dashboard_id_by_key: dict[str, str] = {}
    workspace_host = ""
    workspace_org_id = ""
    for key, entry in dashboards.items():
        dash_id = entry.get("id") or ""
        if dash_id:
            dashboard_id_by_key[key] = dash_id
        # workspace.host isn't directly in the summary, but every resource URL
        # contains the full https://<host>/... — pluck it from a dashboard URL.
        url = entry.get("url") or ""
        if not workspace_host and url.startswith("https://"):
            workspace_host = "https://" + url[len("https://"):].split("/", 1)[0]
        # Extract workspace org_id from the ?o=<id> query param. The embed
        # endpoint REQUIRES this — without it, the iframe falls back to an
        # "embedding not available in this workspace" error message even when
        # workspace-level embedding is allowed.
        if not workspace_org_id and "?o=" in url:
            workspace_org_id = url.split("?o=", 1)[1].split("&", 1)[0].strip()

    # Fallback: ask the workspace itself if no dashboard URL was available.
    if not workspace_host:
        try:
            r = subprocess.run(
                ["databricks", "auth", "describe", "--profile", args.profile, "-o", "json"],
                capture_output=True, text=True, check=True,
            )
            workspace_host = (json.loads(r.stdout or "{}").get("host") or "").rstrip("/")
        except Exception:
            pass

    workspace_host = workspace_host.rstrip("/")

    # `databricks bundle summary` silently strips ${workspace.host} (the literal
    # token is gone from the value entirely) rather than substituting it. So
    # we can't just str.replace it — we have to detect values that lost their
    # host prefix and re-prepend the host.
    #
    # Heuristic: env values starting with one of these workspace API path
    # prefixes used to have ${workspace.host} in front of them. Anything else
    # that starts with `/` (e.g. `/Workspace/...` Workspace file paths) is left
    # alone.
    WORKSPACE_URL_PREFIXES = (
        "/ai-gateway/",
        "/embed/",
        "/dashboardsv3/",
        "/serving-endpoints/",
        "/api/",
    )

    def _resolve_late_refs(value: str) -> str:
        out = value
        if workspace_host:
            out = out.replace("${workspace.host}", workspace_host)
        for key, dash_id in dashboard_id_by_key.items():
            out = out.replace(f"${{resources.dashboards.{key}.id}}", dash_id)
        if workspace_host and out.startswith(WORKSPACE_URL_PREFIXES):
            out = workspace_host + out
        # Append ?o=<org_id> to dashboard embed URLs. The embed endpoint won't
        # render in an iframe without it (workspace-routing fallback fails).
        if workspace_org_id and "/embed/dashboardsv3/" in out and "?o=" not in out:
            out = f"{out}?o={workspace_org_id}"
        return out

    # Apply the late-substitution pass to every env value.
    for item in env:
        if item.get("value") is not None:
            item["value"] = _resolve_late_refs(str(item["value"]))

    # Sanity check — flag any env value that still has an unresolved ${...}.
    unresolved = [
        (item.get("name"), item.get("value"))
        for item in env
        if "${" in str(item.get("value") or "")
    ]
    if unresolved:
        sys.stderr.write(
            "WARNING: the following env values still contain unresolved ${...} references "
            "after substitution:\n"
        )
        for name, value in unresolved:
            sys.stderr.write(f"  {name} = {value}\n")
        sys.stderr.write(
            "These will reach the app process verbatim, which will probably break the "
            "feature they back. Investigate before relying on this deploy.\n"
        )

    lines: list[str] = []
    lines.append("# GENERATED by scripts/build_app_yml.py at deploy time.")
    lines.append("# Source of truth for these env vars is databricks.yml")
    lines.append("# (apps.cfo_app.config.env). DO NOT hand-edit this file —")
    lines.append("# every ./deploy.sh run overwrites it.")
    lines.append("#")
    lines.append("# Why this is generated: the Databricks Apps runtime reads")
    lines.append("# env from app.yml in the source code, not from the")
    lines.append("# resource-level config the bundle sets. So the bundle's")
    lines.append("# resolved env values are materialized here.")
    lines.append("")
    lines.append("command:")
    for token in command:
        lines.append(f'  - "{_yaml_escape(str(token))}"')
    lines.append("")
    lines.append("env:")
    for item in env:
        name = item.get("name") or ""
        value = "" if item.get("value") is None else str(item.get("value"))
        lines.append(f'  - name: "{_yaml_escape(name)}"')
        lines.append(f'    value: "{_yaml_escape(value)}"')

    out = Path(args.app_yml_path)
    out.write_text("\n".join(lines) + "\n")
    print(f"Wrote {out} with {len(env)} env vars for app '{args.app_name}'.")


if __name__ == "__main__":
    main()
