# 34. Legacy vs Modern Benchmark

!!! info "Measured vs. design target"
    This chapter clearly separates **numbers actually measured** in this
    repository from the legacy system's **documented constraint**, which
    was never independently re-measured against the new pipeline at
    equivalent scale. Presenting one as the other would be misleading —
    see [Chapter 26](26-benchmark-framework.md) for how to close this gap
    with real throughput testing.

## Legacy system (documented constraint)

> Source: [Chapter 2](02-business-problem.md), business-problem
> framing.

Enrollment processing jobs in the legacy system are observed to take
approximately **one hour** end to end for a full daily batch file, across
the full EDI → ECA → Legacy Programs → SCP → CORE Programs chain. This
number describes the *existing production workflow's* behavior as
reported by the business, not a benchmark run against this repository's
code.

## Modern pipeline (measured, smoke-test scale)

> Source: [`PIPELINE_STATUS.md`](https://github.com/yourusername/medenroll834/blob/main/PIPELINE_STATUS.md), "VERIFIED:
> End-to-End Run" section.

A real, reproducible run of this repository's `DeltaMedallionPipeline`
against 1 synthetic file (3 subscribers + 3 dependents) produced:

```text
BRONZE     : segments_loaded = 83
SILVER     : total = 6, valid = 6, invalid = 0
GOLD       : records = 6
QUARANTINE : records = 0
```

All four Delta tables were written with a valid `_delta_log`. This
confirms **functional correctness** end to end (parsing, HL resolution,
validation, dedup, Delta writes) — it is not yet a throughput/scale
measurement at anything near the legacy system's full daily batch volume.


## Qualitative comparison (architecturally grounded)

| Dimension | Legacy | Modern (this repo) |
|---|---|---|
| **Processing model** | Serial, single-machine, 6+ distinct tools (EDI/ECA/Legacy Programs/SCP/Bash/CORE Programs) | Distributed PySpark/Delta, 3-layer medallion (see [Ch. 13–18](13-bronze-layer.md)) |
| **Parallelism** | None — one file processed at a time, one machine | Spark partitions data across cores/executors automatically (AQE, see [Ch. 25](25-spark-performance-optimization.md)) |
| **Failure isolation** | A failure in any of 6 tools (EDI/ECA/Legacy Programs/SCP/Bash/CORE Programs) stops the chain with separate logs per tool | Per-layer logging with structured `log_pipeline_start/end` (see [Ch. 24](24-observability.md)); invalid *records* (not whole files) are quarantined, not fatal (see [Ch. 16](16-data-quality-reject-handling.md)) |
| **Recoverability** | Manual investigation across 6 tool logs | Idempotent re-runs (see [Ch. 22](22-idempotency-incremental-processing.md)); Airflow automatic retry (see [Ch. 23](23-error-handling-retry.md)) |
| **Analytical access** | None — CORE System keyword-file load only | OLAP star schema + Databricks SQL + Genie (see [Ch. 19](19-olap-star-schema.md), [Ch. 27–28](27-databricks-sql-dashboard.md)) |
| **Data lineage** | Proprietary intermediate format, no shared trace | `file_name` carried through every layer; `ENRL_EDXN_TRANSACTION`/`DIM_TRANSACTION_SOURCE` FK every record to its source file |
| **Observability** | 6 separate tool-specific logs | Unified structured logging + per-run summary dicts (see [Ch. 24](24-observability.md)) |
| **Infrastructure as Code** | Manually configured Tidal/SSIS/DataStage jobs | Terraform (S3), SQL DDL scripts, Docker Compose, all version-controlled |

## What a rigorous head-to-head benchmark would require

1. Generate synthetic 834 data at a representative daily-batch scale using
   `generate_batch()` with a large `subscribers_per_file`.
2. Run `DeltaMedallionPipeline.run()` against it on a representative
   Databricks cluster size, timed with the methodology proposed in
   [Chapter 26](26-benchmark-framework.md).
3. Compare wall-clock time directly against the legacy system's ~1 hour
   figure, at the same record count.


This has not yet been executed in this repository — the honest claim this
documentation makes is about **architecture** (distributed vs. serial,
quarantine vs. hard-fail, governed analytics vs. none), not a measured
speedup multiplier.
