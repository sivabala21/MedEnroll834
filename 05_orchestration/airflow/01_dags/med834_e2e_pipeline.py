"""
med834_e2e_pipeline.py - End-to-end MedEnroll834 orchestration (Airflow 3.x).

Implements two architecture capabilities (see 08_docs/01_architecture_overview/
ARCHITECTURE.md): (1) Distributed Processing - generate -> land -> Databricks
medallion Bronze/Silver/Gold, and (2) Analytical Platform - Gold -> OLAP star
schema -> two independent analytical consumers (HTML report and Genie AI),
plus the separate OLTP load into the PostgreSQL operational database.

Flow: generate 834 -> blob storage -> Databricks UC Volume -> medallion job
-> OLTP database -> HTML report (blob) + Genie (two analytical consumer paths,
not pipeline transformation steps) -> archive in blob storage.

Serverless Databricks Free Edition cannot read external buckets, so files are
staged into the Unity Catalog Volume /Volumes/med834enrl/edi/landing/834.
This is a POC storage choice; the enterprise target substitutes AWS S3 at
this layer only (src/medenroll834/s3/s3_utils.py already implements the
equivalent interface) while the Bronze-Silver-Gold, OLTP, OLAP, BI, and AI
layers below are unchanged.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/project")

DEFAULT_ARGS = {"owner": "med834enrl", "retries": 1, "retry_delay": timedelta(minutes=3)}
UC_VOLUME_PATH = "/Volumes/med834enrl/edi/landing/834"


def _cfg(key: str, default: str | None = None) -> str | None:
    try:
        from airflow.models import Variable
        val = Variable.get(key, default_var=None)
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(key, default)


@dag(
    dag_id="med834_e2e_pipeline",
    description="MedEnroll834 e2e: 834 -> blob storage -> UC Volume -> Databricks -> OLTP DB -> Report/Genie",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["med834enrl", "e2e", "medallion", "blob-storage"],
)
def med834_e2e():

    @task
    def generate_834() -> str:
        from generator.synthetic_834_generator import generate_batch
        out_dir = "/opt/airflow/project/10_data/raw"
        files = generate_batch(output_dir=out_dir, num_files=1,
                               subscribers_per_file=50, dependents_per_sub=2)
        print(f"generated {len(files)} file(s)")
        return out_dir

    @task
    def upload_to_blob(local_dir: str) -> list[str]:
        from pathlib import Path
        from src.medenroll834.storage import blob_storage
        uris = [blob_storage.upload_834(str(f), s3_prefix="landing/834/")
                for f in Path(local_dir).glob("*.834")]
        print(f"uploaded {len(uris)} file(s) to blob storage landing/834/")
        return uris

    @task
    def push_to_uc_volume(_uploaded: list[str]) -> int:
        from databricks.sdk import WorkspaceClient
        from src.medenroll834.storage import blob_storage
        w = WorkspaceClient()
        try:
            w.files.create_directory(UC_VOLUME_PATH)
        except Exception:
            pass
        count = 0
        for key in blob_storage.list_landing_files("landing/834/"):
            name = key.split("/")[-1]
            local = f"/tmp/{name}"
            blob_storage.download_file(key, local)
            with open(local, "rb") as fh:
                w.files.upload(f"{UC_VOLUME_PATH}/{name}", fh, overwrite=True)
            count += 1
        print(f"pushed {count} file(s) to {UC_VOLUME_PATH}")
        return count

    @task
    def run_databricks_job(_count: int) -> int:
        from databricks.sdk import WorkspaceClient
        job_id = int(_cfg("DATABRICKS_JOB_ID"))
        run = WorkspaceClient().jobs.run_now_and_wait(job_id=job_id)
        state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
        print(f"Databricks job {job_id} finished: {state}")
        if state != "SUCCESS":
            raise RuntimeError(f"Databricks job failed: {state}")
        return run.run_id

    @task
    def load_oltp_database(_run_id: int) -> dict:
        from src.medenroll834.oltp.oltp_loader import load_gold_to_oltp
        summary = load_gold_to_oltp(oltp_db_url=_cfg("OLTP_DATABASE_URL"),
                                    warehouse_id=_cfg("DATABRICKS_WAREHOUSE_ID"))
        print(f"OLTP load summary: {summary}")
        return summary

    @task
    def generate_report(_summary: dict) -> str:
        from src.medenroll834.reporting.report_generator import generate_report as gen
        from src.medenroll834.storage import blob_storage
        html = gen(warehouse_id=_cfg("DATABRICKS_WAREHOUSE_ID"))
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        uri = blob_storage.put_text(f"audit/reports/enrollment_report_{stamp}.html",
                                    html, content_type="text/html")
        print(f"report written to {uri}")
        return uri

    @task
    def ask_genie(_report_uri: str) -> dict:
        space_id = _cfg("GENIE_SPACE_ID")
        if not space_id:
            print("GENIE_SPACE_ID not set - skipping Genie step")
            return {"skipped": True}
        from src.medenroll834.reporting.report_generator import ask_genie as genie
        qs = ["How many members are enrolled on each plan?",
              "What is the subscriber to dependent ratio?"]
        answers = [genie(space_id, q) for q in qs]
        print(f"Genie answered {len(answers)} question(s)")
        return {"answers": answers}

    @task(trigger_rule="all_done")
    def archive_blob(_genie: dict) -> int:
        from src.medenroll834.storage import blob_storage
        moved = 0
        for key in blob_storage.list_landing_files("landing/834/"):
            blob_storage.archive_834(key)
            moved += 1
        print(f"archived {moved} file(s)")
        return moved

    local = generate_834()
    uploaded = upload_to_blob(local)
    pushed = push_to_uc_volume(uploaded)
    run_id = run_databricks_job(pushed)
    summary = load_oltp_database(run_id)
    report = generate_report(summary)
    genie = ask_genie(report)
    archive_blob(genie)


med834_e2e()
