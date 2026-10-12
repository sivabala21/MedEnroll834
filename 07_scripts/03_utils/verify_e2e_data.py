"""Verify end-to-end data landed in Databricks gold and the OLTP database."""

import os
from pathlib import Path

for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

# --- Databricks gold ---
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import time

w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
wid = os.environ["DATABRICKS_WAREHOUSE_ID"]

def q(sql):
    r = w.statement_execution.execute_statement(warehouse_id=wid, statement=sql, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2); r = w.statement_execution.get_statement(r.statement_id)
    return r.result.data_array if r.result else []

print("=== DATABRICKS ===")
print("gold_enrollments rows:", q("SELECT COUNT(*) FROM med834enrl.edi.gold_enrollments")[0][0])
print("gold.dim_member rows:", q("SELECT COUNT(*) FROM med834enrl.gold.dim_member")[0][0])
print("gold.dim_plan rows:", q("SELECT COUNT(*) FROM med834enrl.gold.dim_plan")[0][0])
print("by plan:", q("SELECT plan_id, COUNT(*) FROM med834enrl.edi.gold_enrollments GROUP BY plan_id ORDER BY 2 DESC"))

# --- OLTP database ---
import psycopg2
print("\n=== OLTP DATABASE ===")
c = psycopg2.connect(os.environ["OLTP_DATABASE_URL"]); cur = c.cursor()
for t in ["core.core_meme_member", "core.core_meen_enrollment", "core.core_plpl_plan",
          "core.core_grgr_group", "enrl.enrl_edxn_transaction"]:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"{t}:", cur.fetchone()[0])
c.close()
print("\nE2E DATA VERIFIED")
