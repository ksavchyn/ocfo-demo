#!/usr/bin/env python3
"""Delete the demo's Genie space — the teardown counterpart to
genie_insights/provision_genie_space.py.

Why this exists:
    The Genie space is the one piece of the demo that has no first-class DAB
    resource type, so `databricks bundle destroy` can't remove it — it would be
    orphaned. teardown.sh calls this script to delete it symmetrically with the
    way provision_genie_space.py creates it (look up by title, or use an
    explicit ID).

Idempotent:
    - If a matching space is found, it's deleted.
    - If no matching space exists (already deleted, or never created), the
      script prints a note and exits 0 — safe to re-run.

Usage:
    # Delete by title (matches deploy.sh's --genie-space-title lookup)
    python3 scripts/delete_genie_space.py --profile <name> --title "CFO Demo"

    # Delete by explicit ID
    python3 scripts/delete_genie_space.py --profile <name> --space-id <id>

    # Preview without deleting
    python3 scripts/delete_genie_space.py --profile <name> --title "CFO Demo" --dry-run
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys


def _api(method: str, path: str, profile: str) -> tuple[int, str]:
    """Call `databricks api <method> <path>`; return (returncode, stdout)."""
    cmd = ["databricks", "api", method, path, "--profile", profile]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
    return r.returncode, r.stdout


def find_space_id_by_title(title: str, profile: str) -> str | None:
    """Return the space_id of the first Genie space whose title matches, or None.

    Mirrors the paginated lookup in deploy.sh's lookup_genie_space().
    """
    title = (title or "").strip()
    path = "/api/2.0/genie/spaces"
    while True:
        rc, out = _api("get", path, profile)
        if rc != 0:
            # Lookup failure — treat as "not found" so teardown stays best-effort.
            return None
        try:
            payload = json.loads(out or "{}")
        except json.JSONDecodeError:
            return None
        for s in payload.get("spaces") or []:
            if (s.get("title") or "").strip() == title:
                return s.get("space_id") or None
        token = payload.get("next_page_token") or ""
        if not token:
            return None
        path = f"/api/2.0/genie/spaces?page_token={token}"


def delete_space(space_id: str, profile: str) -> bool:
    """Delete a Genie space by ID. Returns True on success (or already-gone)."""
    rc, _ = _api("delete", f"/api/2.0/genie/spaces/{space_id}", profile)
    return rc == 0


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profile", required=True, help="Databricks CLI profile.")
    p.add_argument("--title", default="", help="Genie space title to look up and delete.")
    p.add_argument("--space-id", default="", help="Explicit space ID to delete (skips title lookup).")
    p.add_argument("--dry-run", action="store_true", help="Print what would be deleted without deleting.")
    args = p.parse_args()

    if not args.space_id and not args.title:
        p.error("provide --space-id or --title")

    space_id = args.space_id.strip()
    if not space_id:
        print(f"Looking up Genie space by title '{args.title}'...")
        space_id = find_space_id_by_title(args.title, args.profile) or ""

    if not space_id:
        print("No matching Genie space found — nothing to delete (idempotent no-op).")
        return

    if args.dry_run:
        print(f"[DRY RUN] Would delete Genie space {space_id}.")
        return

    print(f"Deleting Genie space {space_id}...")
    if delete_space(space_id, args.profile):
        print(f"  Deleted Genie space {space_id}.")
    else:
        # Non-fatal: the space may have already been removed, or the caller
        # lacks permission. teardown.sh continues either way.
        sys.stderr.write(
            f"  WARNING: could not delete Genie space {space_id}. It may already "
            "be gone, or you may lack permission. Check the Genie UI.\n"
        )


if __name__ == "__main__":
    main()
