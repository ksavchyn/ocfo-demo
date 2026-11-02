"""Exercise the app's Lakebase read + write paths end to end (lakebase_store.py).

Usage (local, OAuth via your CLI profile):
  DATABRICKS_CONFIG_PROFILE=kat-personal \
  CFO_LAKEBASE_ENDPOINT=projects/cfo-ops/branches/production/endpoints/primary \
  CFO_LAKEBASE_HOST=<endpoint host> \
  python3 scripts/lakebase_smoke_test.py
"""
import os, sys, json, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lakebase_store as lb

print("Lakebase configured:", lb.is_configured())
t0 = time.time(); lb.ensure_ops_schema(); print(f"ensure_ops_schema: ok ({time.time()-t0:.2f}s)")

t0 = time.time(); rows = lb.read_cost_centers(limit=5)
print(f"\nREAD  cfo_serving.gold_ai_cost_by_cost_center (synced from UC) — top 5 by utilization ({(time.time()-t0)*1000:.0f} ms)")
print(f"{'cost_center':<12}{'department':<12}{'ttm_ai_cost':>14}{'annual_budget':>15}{'util_%':>9}  status")
for r in rows:
    print(f"{r[0]:<12}{str(r[1]):<12}{float(r[3]):>14,.0f}{float(r[4]):>15,.0f}{float(r[5]):>9.1f}  {r[6]}")

top = rows[0]
t0 = time.time()
aid = lb.log_action("nudge_dept_head", "cost_center", top[0], amount_usd=float(top[3]),
                    actor="smoke-test", note=f"AI budget at {float(top[5]):.0f}% of annual policy")
print(f"\nWRITE cfo_ops.ai_actions — inserted action {aid} ({(time.time()-t0)*1000:.0f} ms)")

print("\nREAD  cfo_ops.ai_actions — most recent")
for a in lb.recent_actions(limit=3):
    print(json.dumps(a, default=str))
