"""Verify the full OLAP star schema is populated, including FACT_ENROLLMENT."""
import os, time
from pathlib import Path
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip())
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
wid = os.environ["DATABRICKS_WAREHOUSE_ID"]
def q(sql):
    r = w.statement_execution.execute_statement(warehouse_id=wid, statement=sql, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2); r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        return "ERR: " + (r.status.error.message if r.status.error else "?")
    return r.result.data_array if r.result else []

print("=== FULL STAR SCHEMA ===")
print("DIM_MEMBER:", q("SELECT COUNT(*) FROM med834enrl.gold.dim_member"))
print("DIM_PLAN:", q("SELECT COUNT(*) FROM med834enrl.gold.dim_plan"))
print("DIM_GROUP:", q("SELECT COUNT(*) FROM med834enrl.gold.dim_group"))
print("DIM_DATE:", q("SELECT COUNT(*) FROM med834enrl.gold.dim_date"))
print("DIM_TRANSACTION_SOURCE:", q("SELECT COUNT(*) FROM med834enrl.gold.dim_transaction_source"))
print("FACT_ENROLLMENT:", q("SELECT COUNT(*) FROM med834enrl.gold.fact_enrollment"))
print()
print("Sample FACT_ENROLLMENT joined to all dims:")
print(q("""
    SELECT p.PLAN_CODE, gr.GROUP_NUMBER, m.FIRST_NAME, m.LAST_NAME,
           d1.FULL_DATE AS coverage_start, f.ENROLLMENT_STATUS
    FROM med834enrl.gold.fact_enrollment f
    JOIN med834enrl.gold.dim_plan p ON f.PLPL_KEY = p.PLPL_KEY
    JOIN med834enrl.gold.dim_group gr ON f.GRGR_KEY = gr.GRGR_KEY
    JOIN med834enrl.gold.dim_member m ON f.MEME_KEY = m.MEME_KEY
    JOIN med834enrl.gold.dim_date d1 ON f.COVERAGE_START_DATE_KEY = d1.DATE_KEY
    LIMIT 5
"""))
