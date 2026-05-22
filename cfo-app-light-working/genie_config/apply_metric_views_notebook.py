# Databricks notebook source
# DBTITLE 1,Create / refresh Unity Catalog metric views
# MAGIC %md
# MAGIC # Metric Views Provisioner
# MAGIC
# MAGIC Runs as the bundle task `create_metric_views`, between `build_silver_gold`
# MAGIC (which creates the source tables) and `provision_genie_space` (which
# MAGIC registers the metric view in the Genie space's table list).
# MAGIC
# MAGIC Applies every YAML file under `genie_config/metric_views/` as a
# MAGIC `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML` statement against
# MAGIC the configured warehouse, so the metric view's measure definitions
# MAGIC (RPP, DSO, utilization, margin, etc.) are the single source of truth
# MAGIC for KPIs.

# COMMAND ----------

# DBTITLE 1,Configuration via widgets
import os
import sys
from pathlib import Path

try:
    dbutils.widgets.text("CFO_CATALOG", "main")  # noqa: F821
    dbutils.widgets.text("CFO_SCHEMA_NAME", "cfo_proserv")  # noqa: F821
    dbutils.widgets.text("CFO_WAREHOUSE_ID", "")  # noqa: F821
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


CATALOG = _config("CFO_CATALOG", "main")
SCHEMA = _config("CFO_SCHEMA_NAME", "cfo_proserv")
WAREHOUSE_ID = _config("CFO_WAREHOUSE_ID", "")

# COMMAND ----------

# DBTITLE 1,Locate metric_views directory
# Notebook may run from a different CWD than the bundle root; resolve relative
# to this notebook's path.
NOTEBOOK_DIR = Path(__file__).parent if "__file__" in globals() else Path.cwd()
MV_DIR = NOTEBOOK_DIR / "metric_views"
if not MV_DIR.exists():
    # Fall back to bundle workspace layout if notebook is sync'd as files
    candidates = [
        Path.cwd() / "genie_config" / "metric_views",
        Path("/Workspace") / "Repos" / "cfo-app" / "genie_config" / "metric_views",
    ]
    for c in candidates:
        if c.exists():
            MV_DIR = c
            break

print(f"Metric view source dir: {MV_DIR}")
view_yamls = sorted(MV_DIR.glob("*.yml"))
print(f"Found {len(view_yamls)} metric view YAML(s): {[p.stem for p in view_yamls]}")

# COMMAND ----------

# DBTITLE 1,Apply each metric view via CREATE OR REPLACE VIEW
def build_ddl(fqn: str, yaml_body: str) -> str:
    """Compose CREATE OR REPLACE VIEW DDL embedding the YAML definition.

    Databricks metric view DDL accepts the YAML body inside a
    `LANGUAGE YAML AS $$ ... $$` block; the $$ delimiter avoids quote escaping.
    """
    return f"""\
CREATE OR REPLACE VIEW {fqn}
WITH METRICS
LANGUAGE YAML
AS $$
{yaml_body.rstrip()}
$$
"""


# Metric view YAMLs ship with `main.cfo_proserv` as the literal source FQN
# placeholder. Substitute the runtime catalog + schema so the metric view's
# joins point at the right tables in the customer's deploy.
SOURCE_FQN_PLACEHOLDER = "main.cfo_proserv"
TARGET_FQN = f"{CATALOG}.{SCHEMA}"

if not view_yamls:
    print("No metric view YAMLs found — nothing to apply.")
else:
    for path in view_yamls:
        view_name = path.stem
        yaml_body = path.read_text().replace(SOURCE_FQN_PLACEHOLDER, TARGET_FQN)
        fqn = f"{CATALOG}.{SCHEMA}.{view_name}"
        ddl = build_ddl(fqn, yaml_body)
        print(f"\nApplying {fqn} ({len(yaml_body)} chars YAML)...")
        spark.sql(ddl)  # noqa: F821 — spark provided by Databricks runtime
        print(f"  ✓ {fqn}")

print("\nAll metric views applied.")
