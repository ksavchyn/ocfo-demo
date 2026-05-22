#!/usr/bin/env python3
"""Build dashboard .lvdash.json files for a specific target catalog + schema.

The source dashboard files have `main.cfo_proserv_dev` hardcoded in their SQL.
Lakeview doesn't support variable substitution at deploy time, and Databricks
bundles treat .lvdash.json as opaque resources. So we substitute the FQN at
build time and emit the result to dashboards/_build/.

The bundle's `file_path` references the _build/ outputs. Customer's deploy
becomes: pass catalog + schema → build_dashboards.py runs → bundle deploys
dashboards bound to customer's schema → everything works.

Usage:
    python scripts/build_dashboards.py --catalog main --schema cfo_proserv_customer

Idempotent. Safe to re-run.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


SOURCE_DIR = Path(__file__).resolve().parent.parent / "dashboards"
BUILD_DIR = SOURCE_DIR / "_build"

# Source files we templatize. Anything else in dashboards/ is left alone.
SOURCE_FILES = [
    "cfo_finance_dashboard_v2_dq_dev.lvdash.json",
    "admin_dashboard_v2_dq_dev.lvdash.json",
]

# Placeholder we expect to find in source files (the dev-environment FQN).
SOURCE_FQN_LITERAL = "main.cfo_proserv_dev"


def build_one(src: Path, out: Path, target_catalog: str, target_schema: str) -> int:
    """Substitute the dev FQN with target FQN. Returns count of replacements."""
    text = src.read_text()
    target_fqn = f"{target_catalog}.{target_schema}"
    new_text, n = re.subn(re.escape(SOURCE_FQN_LITERAL), target_fqn, text)

    # Sanity: result should still parse as valid JSON
    try:
        json.loads(new_text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"templating {src.name} produced invalid JSON: {e}")

    out.write_text(new_text)
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--catalog", required=True, help="Target Unity Catalog name (e.g., 'main')")
    ap.add_argument("--schema", required=True, help="Target schema name (e.g., 'cfo_proserv_customer')")
    args = ap.parse_args()

    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    built = 0
    total = 0
    for name in SOURCE_FILES:
        src = SOURCE_DIR / name
        if not src.exists():
            print(f"  WARNING: source not found — {name}", file=sys.stderr)
            continue
        out = BUILD_DIR / name
        n = build_one(src, out, args.catalog, args.schema)
        total += n
        built += 1

    if total == 0:
        print(
            f"WARNING: dashboards were not retargeted. Sources may already point at "
            f"{args.catalog}.{args.schema}, or the source FQN literal "
            f"'{SOURCE_FQN_LITERAL}' is no longer in the files.",
            file=sys.stderr,
        )
        return 1

    print(f"  Built {built} dashboard(s) targeting {args.catalog}.{args.schema}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
