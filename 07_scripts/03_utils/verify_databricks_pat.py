"""Verify the Databricks PAT in .env authenticates and can reach the job/warehouse."""
import os
import sys
from pathlib import Path

# load .env
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

os.environ["DATABRICKS_HOST"] = os.environ["DATABRICKS_HOST"]
os.environ["DATABRICKS_TOKEN"] = os.environ["DATABRICKS_TOKEN"]

from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
me = w.current_user.me()
print("AUTH OK as:", me.user_name)

job_id = int(os.environ["DATABRICKS_JOB_ID"])
job = w.jobs.get(job_id=job_id)
print("Job found:", job.settings.name)

wid = os.environ["DATABRICKS_WAREHOUSE_ID"]
wh = w.warehouses.get(id=wid)
print("Warehouse found:", wh.name, "| state:", getattr(wh.state, "value", wh.state))
print("PAT_CHECK_PASSED")
