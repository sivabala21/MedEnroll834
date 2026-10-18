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

Archive safety: `archive_blob` only moves the exact files this run uploaded
(tracked by key through XCom from upload_to_blob -> push_to_uc_volume), and
only runs if the core processing chain (push_to_uc_volume -> run_databricks_job
-> load_oltp_database) succeeded -- it depends directly on that chain's
outputs with the default all_success trigger rule, so a Databricks or OLTP
failure causes Airflow to skip archiving automatically rather than risk
archiving a file that was never actually processed. generate_report/ask_genie
are an independent, optional branch off load_oltp_database and do not gate
archiving either way. If the core chain fails, quarantine_failed_files
(trigger_rule="one_failed") moves this run's files to failed/834/ instead of
leaving them ambiguously in landing/834/ or silently archiving them.

Serverless Databricks Free Edition cannot read external buckets, so files are
staged into the Unity Catalog Volume /Volumes/med834enrl/edi/landing/834.
This is a POC storage choice; the enterprise target substitutes AWS S3 at
this layer only (src/medenroll834/s3/s3_utils.py already implements the
equivalent interface) while the Bronze-Silver-Gold, OLTP, OLAP, BI, and AI
layers below are unchanged.

Logging: every task binds the Airflow dag_run's run_id into
src.medenroll834.common.logging's run_context() so log lines emitted by this
DAG *and* by every pipeline module it calls (blob_storage, oltp_loader,
report_generator, ...) carry the same run=<id> tag -- Airflow's own task log
capture means these show up directly in the task log view with no extra
wiring. The same run_id is also forwarded to the Databricks job as a job
parameter (airflow_run_id) so the notebook's own print/result output can be
correlated back to the triggering Airflow DAG run.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task

try:
    from airflow.sdk import get_current_context  # Airflow 3.x
except ImportError:  # pragma: no cover - Airflow 2.x fallback
    from airflow.operators.python import get_current_context

sys.path.insert(0, "/opt/airflow/project")

from src.medenroll834.common.logging import (  # noqa: E402
    log_pipeline_end,
    log_pipeline_error,
    log_pipeline_start,
    run_context,
    setup_logging,
)

DEFAULT_ARGS = {"owner": "med834enrl", "retries": 1, "retry_delay": timedelta(minutes=3)}
UC_VOLUME_PATH = "/Volumes/med834enrl/edi/landing/834"


def _cfg(key: str, default: str | None = None) -> str | None:
    """Airflow Variable override, falling back to env var.

    The bare `except Exception: pass` here is a config-lookup fallback, not
    a task-dependency/failure-handling concern: if the Variable backend is
    unreachable or the Variable simply isn't set, we fall through to the
    environment variable rather than failing the task outright. A debug log
    line still distinguishes "Variable lookup raised" from "Variable unset"
    for anyone grepping task logs for a misconfigured Variable backend.
    """
    try:
        from airflow.models import Variable
        val = Variable.get(key, default_var=None)
        if val:
            return val
    except Exception as exc:
        logging.getLogger("airflow_dag").debug(
            "CONFIG | Variable lookup for %s raised %s; falling back to env var", key, exc
        )
    return os.environ.get(key, default)


def _dag_run_id() -> str:
    """Best-effort Airflow dag_run.run_id for log/job correlation.

    Falls back to a placeholder rather than raising -- losing the
    correlation id must never fail the task itself.
    """
    try:
        return get_current_context()["run_id"]
    except Exception:
        return "unknown"



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
    logger = setup_logging("airflow_dag")

    @task
    def generate_834() -> str:
        with run_context(run_id=_dag_run_id()):
            start_time = log_pipeline_start(logger, "GENERATE", "generating synthetic 834 file(s)")
            try:
                from generator.synthetic_834_generator import generate_batch
                out_dir = "/opt/airflow/project/10_data/raw"
                files = generate_batch(output_dir=out_dir, num_files=1,
                                       subscribers_per_file=50, dependents_per_sub=2)
            except Exception:
                log_pipeline_error(logger, "GENERATE", "generating synthetic 834 file(s)", start_time)
                raise
            log_pipeline_end(logger, "GENERATE", f"files={len(files)} out_dir={out_dir}", start_time)
            return out_dir

    @task
    def upload_to_blob(local_dir: str) -> list[str]:
        """Returns the exact blob keys uploaded this run (e.g.
        'landing/834/enrollment_20261014_001.834'), not just a count/URI --
        downstream tasks (push_to_uc_volume, archive_blob,
        quarantine_failed_files) all operate on this exact list so they never
        act on a file some other run uploaded or one not yet processed."""
        with run_context(run_id=_dag_run_id()):
            start_time = log_pipeline_start(logger, "UPLOAD_BLOB", f"local_dir={local_dir}")
            try:
                from pathlib import Path
                from src.medenroll834.storage import blob_storage
                keys = [blob_storage.upload_834(str(f), s3_prefix="landing/834/").split("/", 3)[-1]
                        for f in Path(local_dir).glob("*.834")]
                # upload_834()/upload_file() return "blob://bucket/landing/834/<name>";
                # strip the "blob://bucket/" prefix so callers hold plain keys
                # ("landing/834/<name>") -- the form every other blob_storage
                # function (download_file/archive_834/list_objects) expects.
            except Exception:
                log_pipeline_error(logger, "UPLOAD_BLOB", f"local_dir={local_dir}", start_time)
                raise
            log_pipeline_end(logger, "UPLOAD_BLOB", f"uploaded={len(keys)}", start_time)
            return keys

    @task
    def push_to_uc_volume(keys: list[str]) -> list[str]:
        """Pushes only the exact keys this run uploaded (received as an
        argument, never a fresh blob_storage.list_landing_files() call) and
        passes that same key list through unchanged, so every downstream task
        (Databricks job trigger, archive, quarantine-on-failure) stays scoped
        to this run's files and never touches a file some other run owns."""
        with run_context(run_id=_dag_run_id()):
            start_time = log_pipeline_start(
                logger, "PUSH_UC_VOLUME", f"target={UC_VOLUME_PATH} files={len(keys)}"
            )
            try:
                from databricks.sdk import WorkspaceClient
                from src.medenroll834.storage import blob_storage
                w = WorkspaceClient()
                try:
                    w.files.create_directory(UC_VOLUME_PATH)
                except Exception:
                    logger.debug("PUSH_UC_VOLUME | directory already exists: %s", UC_VOLUME_PATH)
                for key in keys:
                    name = key.split("/")[-1]
                    local = f"/tmp/{name}"
                    blob_storage.download_file(key, local)
                    with open(local, "rb") as fh:
                        w.files.upload(f"{UC_VOLUME_PATH}/{name}", fh, overwrite=True)
            except Exception:
                log_pipeline_error(logger, "PUSH_UC_VOLUME", f"target={UC_VOLUME_PATH}", start_time)
                raise
            log_pipeline_end(logger, "PUSH_UC_VOLUME", f"pushed={len(keys)}", start_time)
            return keys

    @task
    def run_databricks_job(_pushed_keys: list[str]) -> int:
        with run_context(run_id=_dag_run_id()):
            job_id = int(_cfg("DATABRICKS_JOB_ID"))
            start_time = log_pipeline_start(logger, "DATABRICKS_JOB", f"job_id={job_id}")
            try:
                from databricks.sdk import WorkspaceClient
                # job_parameters surfaces the Airflow run_id as a Databricks
                # job parameter (readable via dbutils.widgets.get in the
                # notebook) so a human debugging a Databricks run can trace it
                # back to the Airflow DAG run that triggered it.
                run = WorkspaceClient().jobs.run_now_and_wait(
                    job_id=job_id, job_parameters={"airflow_run_id": _dag_run_id()}
                )
                state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
                if state != "SUCCESS":
                    raise RuntimeError(f"Databricks job failed: {state}")
            except Exception:
                log_pipeline_error(logger, "DATABRICKS_JOB", f"job_id={job_id}", start_time)
                raise
            log_pipeline_end(
                logger, "DATABRICKS_JOB", f"job_id={job_id} state={state} run_id={run.run_id}", start_time
            )
            return run.run_id

    @task
    def load_oltp_database(_run_id: int) -> dict:
        with run_context(run_id=_dag_run_id()):
            start_time = log_pipeline_start(logger, "LOAD_OLTP", "loading gold enrollments into OLTP")
            try:
                from src.medenroll834.oltp.oltp_loader import load_gold_to_oltp
                summary = load_gold_to_oltp(oltp_db_url=_cfg("OLTP_DATABASE_URL"),
                                            warehouse_id=_cfg("DATABRICKS_WAREHOUSE_ID"))
            except Exception:
                log_pipeline_error(logger, "LOAD_OLTP", "loading gold enrollments into OLTP", start_time)
                raise
            log_pipeline_end(logger, "LOAD_OLTP", str(summary), start_time)
            return summary

    @task
    def generate_report(_summary: dict) -> str:
        with run_context(run_id=_dag_run_id()):
            start_time = log_pipeline_start(logger, "GENERATE_REPORT", "rendering HTML analytics report")
            try:
                from src.medenroll834.reporting.report_generator import generate_report as gen
                from src.medenroll834.storage import blob_storage
                html = gen(warehouse_id=_cfg("DATABRICKS_WAREHOUSE_ID"))
                stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                uri = blob_storage.put_text(f"audit/reports/enrollment_report_{stamp}.html",
                                            html, content_type="text/html")
            except Exception:
                log_pipeline_error(logger, "GENERATE_REPORT", "rendering HTML analytics report", start_time)
                raise
            log_pipeline_end(logger, "GENERATE_REPORT", f"uri={uri}", start_time)
            return uri

    @task
    def ask_genie(_report_uri: str) -> dict:
        with run_context(run_id=_dag_run_id()):
            space_id = _cfg("GENIE_SPACE_ID")
            if not space_id:
                logger.info("ASK_GENIE | GENIE_SPACE_ID not set - skipping Genie step")
                return {"skipped": True}
            start_time = log_pipeline_start(logger, "ASK_GENIE", f"space_id={space_id}")
            try:
                from src.medenroll834.reporting.report_generator import ask_genie as genie
                qs = ["How many members are enrolled on each plan?",
                      "What is the subscriber to dependent ratio?"]
                answers = [genie(space_id, q) for q in qs]
            except Exception:
                log_pipeline_error(logger, "ASK_GENIE", f"space_id={space_id}", start_time)
                raise
            log_pipeline_end(logger, "ASK_GENIE", f"answers={len(answers)}", start_time)
            return {"answers": answers}

    @task
    def archive_blob(keys: list[str]) -> int:
        """Archives only the exact files this run pushed to the UC Volume.

        No trigger_rule override: the default all_success means Airflow
        skips this task automatically if push_to_uc_volume, run_databricks_job,
        or load_oltp_database failed -- it is wired directly off those tasks'
        outputs (see the dependency graph below), not off the optional
        generate_report/ask_genie branch, so a flaky analytics query can
        never block archiving and a real processing failure can never
        trigger it."""
        with run_context(run_id=_dag_run_id()):
            start_time = log_pipeline_start(
                logger, "ARCHIVE_BLOB", f"archiving {len(keys)} processed file(s)"
            )
            try:
                from src.medenroll834.storage import blob_storage
                for key in keys:
                    blob_storage.archive_834(key)
            except Exception:
                log_pipeline_error(logger, "ARCHIVE_BLOB", f"archiving {len(keys)} file(s)", start_time)
                raise
            log_pipeline_end(logger, "ARCHIVE_BLOB", f"archived={len(keys)}", start_time)
            return len(keys)

    @task(trigger_rule="one_failed")
    def quarantine_failed_files(keys: list[str] | None) -> int:
        """Runs only when a direct/transitive upstream in the core chain
        (push_to_uc_volume -> run_databricks_job -> load_oltp_database)
        failed or was itself UPSTREAM_FAILED (Airflow's ONE_FAILED trigger
        rule fires on either state, so a failure anywhere in that chain
        reaches this task). Moves this run's exact files from landing/834/
        to failed/834/ so they are not left ambiguous in landing and are
        never archived as if they had been processed successfully.

        keys may be None/empty if upload_to_blob itself is what failed (no
        keys were ever produced this run) -- that is a no-op here, not an
        error, since there is nothing to move."""
        with run_context(run_id=_dag_run_id()):
            keys = keys or []
            start_time = log_pipeline_start(
                logger, "QUARANTINE_FILES", f"quarantining {len(keys)} file(s) after upstream failure"
            )
            if not keys:
                log_pipeline_end(logger, "QUARANTINE_FILES", "no keys to quarantine", start_time)
                return 0
            try:
                from src.medenroll834.storage import blob_storage
                for key in keys:
                    blob_storage.archive_834(key, archive_prefix="failed/834/")
            except Exception:
                log_pipeline_error(
                    logger, "QUARANTINE_FILES", f"quarantining {len(keys)} file(s)", start_time
                )
                raise
            log_pipeline_end(logger, "QUARANTINE_FILES", f"quarantined={len(keys)}", start_time)
            return len(keys)

    local = generate_834()
    uploaded = upload_to_blob(local)
    pushed = push_to_uc_volume(uploaded)
    run_id = run_databricks_job(pushed)
    summary = load_oltp_database(run_id)
    report = generate_report(summary)
    genie = ask_genie(report)

    # archive_blob is wired to pushed/summary (the core chain's own outputs),
    # not to genie, so it only runs -- and only archives these exact files --
    # when the actual processing chain (push -> Databricks -> OLTP) succeeded.
    # The optional generate_report/ask_genie branch runs independently off
    # summary and never gates archiving in either direction.
    archive_blob(pushed) << summary
    # quarantine_failed_files has no data dependency on the chain's return
    # value (it only needs the keys), but must still be wired to the same
    # tasks so Airflow evaluates ONE_FAILED against their state.
    quarantine_failed_files(uploaded) << [pushed, run_id, summary]


med834_e2e()
