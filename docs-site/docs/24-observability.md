# 24. Observability

MedEnroll834's observability is deliberately lightweight for a POC —
structured logging, pipeline-level run summaries, and Airflow's own task
UI — rather than a separate metrics/tracing stack, but every signal that
exists is real and wired into actual code paths.

## Standardized logging

> Source: [`src/medenroll834/common/logging.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/common/logging.py)

```python
def setup_logging(name: str = "med834enrl") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def log_pipeline_start(logger, layer: str, details: str) -> None:
    logger.info("START %s | %s | %s", layer, details, datetime.now(timezone.utc).isoformat())

def log_pipeline_end(logger, layer: str, details: str) -> None:
    logger.info("END %s | %s | %s", layer, details, datetime.now(timezone.utc).isoformat())
```

Every layer (`BronzeLayer`, `SilverLayer`, `GoldLayer`,
`DeltaQuarantineManager`) calls `log_pipeline_start`/`log_pipeline_end`
with a layer name and a details string that includes the actual row
count, e.g.:

```text
2026-10-04 10:40:23 [INFO] bronze - START BRONZE | landing_path=10_data/raw/ | 2026-10-04T10:40:23+00:00
2026-10-04 10:40:25 [INFO] bronze - END BRONZE | segments_loaded=83 | 2026-10-04T10:40:25+00:00
2026-10-04 10:40:26 [INFO] silver - END SILVER | total=6 valid=6 invalid=0 | 2026-10-04T10:40:26+00:00
2026-10-04 10:40:27 [INFO] gold - END GOLD | records=6 | 2026-10-04T10:40:27+00:00
```

Logger names (`bronze`, `silver`, `gold`, `quarantine`, `pipeline`,
`validator`) make it trivial to `grep`/filter logs by layer, and the
`logger.handlers` guard prevents duplicate log lines if `setup_logging()`
is called more than once for the same name within a process.

## Pipeline-level run summaries

`DeltaMedallionPipeline.run()` returns a structured summary dict rather
than just logging — this makes the pipeline's outcome programmatically
consumable (by a test, by a future Airflow XCom, by a monitoring script):

```python
summary = {
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
print("MEDALLION RESULT:", json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
```

`dbutils.notebook.exit()` makes this result retrievable from the
**Databricks Jobs API** by anything that calls the job (including a future
Airflow task), without needing to parse log output.

## Airflow-native observability

- **Task graph UI** (`localhost:8080`) shows per-task state
  (queued/running/success/failed/retried) for every DAG run.
- **Per-task logs** are captured automatically by Airflow and viewable per
  task instance, separate from the pipeline's own `setup_logging()` output
  (which still appears within the task log).
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
and no alerting integration are wired up — `09_monitoring/` exists as a
placeholder directory for exactly this kind of future addition. See
[Chapter 38](38-future-improvements.md) for the monitoring roadmap.
