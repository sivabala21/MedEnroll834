"""Get the notebook output/logs from the latest medallion run."""
import os
from pathlib import Path
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip())
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
job_id = int(os.environ["DATABRICKS_JOB_ID"])
run = list(w.jobs.list_runs(job_id=job_id, limit=1))[0]
print("parent run:", run.run_id, run.state.result_state)
full = w.jobs.get_run(run_id=run.run_id)
for t in full.tasks or []:
    print("task:", t.task_key, "task_run_id:", t.run_id)
    try:
        out = w.jobs.get_run_output(run_id=t.run_id)
        if out.error:
            print("  ERROR:", out.error[:1500])
        if out.error_trace:
            print("  TRACE:", out.error_trace[-1500:])
        if out.logs:
            print("  LOGS tail:")
            for ln in out.logs.splitlines()[-40:]:
                print("   ", ln)
    except Exception as e:
        print("  (no output):", e)
