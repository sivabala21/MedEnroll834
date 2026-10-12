"""Full inventory of every schema/table/row-count in the med834enrl catalog."""
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
        return None, (r.status.error.message if r.status.error else "?")
    return (r.result.data_array if r.result else []), None

schemas, err = q("SHOW SCHEMAS IN med834enrl")
print("SCHEMAS:", schemas, err)

for sch_row in (schemas or []):
    sch = sch_row[0]
    if sch in ("information_schema",):
        continue
    tables, err = q(f"SHOW TABLES IN med834enrl.{sch}")
    print(f"\n--- SCHEMA: {sch} ---")
    if err:
        print("  ERROR:", err)
        continue
    if not tables:
        print("  (no tables)")
        continue
    for t in tables:
        tname = t[1]
        cnt, cerr = q(f"SELECT COUNT(*) FROM med834enrl.{sch}.{tname}")
        print(f"  {tname}: {cnt if not cerr else 'ERR: '+cerr}")
