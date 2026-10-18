# 24. Observability

MedEnroll834's observability is deliberately lightweight for a POC —
structured, configurable logging with run-id correlation, pipeline-level
run summaries, and Airflow's own task UI — rather than a separate
metrics/tracing stack, but every signal that exists is real and wired into
actual code paths.

## Standardized, configurable logging

> Source: [`src/medenroll834/common/logging.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/common/logging.py)

```python
def setup_logging(name: str = "med834enrl") -> logging.Logger:
    """Console handler always on; env-driven level + optional rotating file
    handler. Every record also gets run_id/file_name injected via a
    logging.Filter backed by a contextvar (see run_context() below)."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = _resolve_level()  # MED834_LOG_LEVEL, default INFO
    logger.setLevel(level)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(_CONTEXT_FILTER)
    logger.addHandler(stream_handler)

    if os.environ.get("MED834_LOG_FILE"):   # optional rotating file handler
        ...
    return logger

def log_pipeline_start(logger, layer: str, details: str) -> float:
    logger.info("START %s | %s | %s", layer, details, datetime.now(timezone.utc).isoformat())
    return time.monotonic()  # feed into log_pipeline_end/_error for duration

def log_pipeline_end(logger, layer: str, details: str, start_time=None) -> None:
    if start_time is not None:
        details = f"{details} | duration_s={time.monotonic() - start_time:.2f}"
    logger.info("END %s | %s | %s", layer, details, datetime.now(timezone.utc).isoformat())

def log_pipeline_error(logger, layer: str, details: str, start_time=None) -> None:
    """Call from an except block -- logs full traceback via logger.exception()."""
    ...
```

Env vars: `MED834_LOG_LEVEL` (default `INFO`), `MED834_LOG_FILE` (optional,
adds a `RotatingFileHandler` alongside stdout), `MED834_LOG_MAX_BYTES`,
`MED834_LOG_BACKUP_COUNT`. Wired into `docker-compose.yaml` for the Airflow
containers and documented in `.env.example`.

Every layer (`BronzeLayer`, `SilverLayer`, `GoldLayer`,
`DeltaQuarantineManager`, the OLTP loader, the report generator) calls
`log_pipeline_start`/`log_pipeline_end` with a layer name and a details
string that includes the actual row count and elapsed duration, e.g.:

```text
2026-10-04 10:40:23 [INFO] bronze | run=3f9a21 file=- - START BRONZE | landing_path=10_data/raw/ | 2026-10-04T10:40:23+00:00
2026-10-04 10:40:25 [INFO] bronze | run=3f9a21 file=- - END BRONZE | files=2 segments_loaded=83 | duration_s=1.84 | 2026-10-04T10:40:25+00:00
2026-10-04 10:40:26 [INFO] silver | run=3f9a21 file=- - END SILVER | total=6 valid=6 invalid=0 | duration_s=0.92 | 2026-10-04T10:40:26+00:00
2026-10-04 10:40:27 [INFO] gold | run=3f9a21 file=- - END GOLD | records=6 | duration_s=0.41 | 2026-10-04T10:40:27+00:00
```


Logger names (`bronze`, `silver`, `gold`, `quarantine`, `pipeline`,
`validator`, `oltp_loader`, `report_generator`, `s3_utils`, `blob_storage`,
`edi834_parser`, `airflow_dag`) make it trivial to additionally
`grep`/filter logs by component, and the `logger.handlers` guard in
`setup_logging()` prevents duplicate log lines if it is called more than
once for the same name within a process.

## Pipeline run-context correlation

> `run_context()`/`set_run_context()` in `common/logging.py`

Every log line above carries `run=3f9a21` — a `contextvars.ContextVar`
bound once per pipeline invocation and injected into every `LogRecord` via
a `logging.Filter`, so Bronze/Silver/Gold/Quarantine (each of which calls
`setup_logging()` independently, since they are separate classes
instantiated by the orchestrator) share one correlation id without
threading `run_id` through every method signature:

```python
# src/medenroll834/ingestion/pipeline.py
def run(self, landing_path: str, run_id: Optional[str] = None) -> Dict:
    run_id = run_id or uuid.uuid4().hex[:12]
    with run_context(run_id=run_id):
        ...  # every log line emitted by bronze/silver/gold/quarantine
             # during this `with` block carries run=<run_id>
```

The Airflow DAG does the same per-task, binding the Airflow `dag_run.run_id`
(via `get_current_context()["run_id"]`) so a human can `grep` the Airflow
task log and the pipeline module logs together for one DAG run, and forwards
that same id to the Databricks job as a `job_parameters={"airflow_run_id": ...}`
value — the notebook reads it back via `dbutils.widgets.get("airflow_run_id")`
and includes it in its own `print()` lines, closing the loop from Airflow UI
-> task log -> Databricks job run log.

## Error logging

`log_pipeline_error()` is called from every layer's top-level `except`
block before re-raising, so failures are never silently swallowed: it logs
via `logger.exception()`, which captures the full traceback plus the
elapsed duration up to the point of failure. Modules with no "layer"
concept (`s3_utils`, `blob_storage`, `oltp_loader`'s Databricks read,
`report_generator`'s Genie call) use `logger.exception()`/`logger.error()`
directly at each external I/O call site (S3/blob upload-download-archive,
SQL warehouse statement execution, Genie conversation) with the operation's
key identifiers (bucket/key, warehouse_id, space_id) in the message --
credentials/tokens are never included.

## Pipeline-level run summaries

`DeltaMedallionPipeline.run()` returns a structured summary dict rather
than just logging — this makes the pipeline's outcome programmatically
consumable (by a test, by a future Airflow XCom, by a monitoring script):

```python
summary = {
    "run_id": run_id,
    "bronze_segments": bronze_df.count(),
    "silver_total": silver_df.count(),
    "silver_valid": valid_df.count(),
    "silver_invalid": invalid_df.count(),
    "gold_records": gold_df.count(),
}
```

The Databricks notebook job produces an equivalent structured result and
exits with it explicitly:

```python
result["airflow_run_id"] = RUN_ID
print("MEDALLION RESULT:", json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
```

`dbutils.notebook.exit()` makes this result retrievable from the
**Databricks Jobs API** by anything that calls the job (including the
Airflow task), without needing to parse log output.

## Airflow-native observability

- **Task graph UI** (`localhost:8080`) shows per-task state
  (queued/running/success/failed/retried) for every DAG run.
- **Per-task logs** are captured automatically by Airflow and viewable per
  task instance; the DAG's own tasks use `logging.getLogger`-based loggers
  (not bare `print()`) so their output is level-tagged and run-id-tagged
  consistently with the pipeline's own `setup_logging()` output, which still
  appears within the same task log.
- **DAG import error detection**:
  `airflow dags list-import-errors` — used as a CI-style sanity check
  (`07_scripts/03_utils/validate_dag.py`) before deploying a DAG change.

## OLTP and OLAP lineage columns

Both stores carry lineage back to the originating 834 file:

- OLTP: every `core_meen_enrollment` row FKs to an
  `enrl_edxn_transaction.txn_id`, which stores `file_nm`, `rcvd_ts`, and
  `proc_stat`.
- OLAP: `DIM_TRANSACTION_SOURCE.FILE_NAME` + `FACT_ENROLLMENT.TXN_SRC_KEY`
  trace every fact row back to its source file.
- Delta: `file_name` is carried as a column through Bronze → Silver → Gold
  → quarantine, so any row can be traced to its source file without a
  separate lineage table.

## What's deliberately out of scope for this POC

No Prometheus/Grafana metrics stack, no distributed tracing (OpenTelemetry),
and no alerting integration are wired up for this POC. See
[Chapter 37](37-future-improvements.md) for the monitoring roadmap.
