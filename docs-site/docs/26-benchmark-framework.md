# 26. Benchmark Framework

!!! warning "What exists vs. what is proposed"
    This repository does **not** currently ship a dedicated, automated
    benchmark harness (e.g. a script that runs N file sizes and plots
    throughput). What exists is the **instrumentation a benchmark
    framework would consume** — pipeline run summaries, per-layer logging,
    and a validated smoke-test baseline. This chapter documents that real
    instrumentation, then proposes a concrete framework design that is not
    yet implemented. See [Chapter 34](34-legacy-vs-modern-benchmark.md) for
    the actual measured numbers vs. the legacy design targets.

## What already exists to build a benchmark on

### 1. Structured per-run summaries

`DeltaMedallionPipeline.run()` returns row counts for every layer on every
run:

```python
summary = {
    "bronze_segments": bronze_df.count(),
    "silver_total": silver_df.count(),
    "silver_valid": valid_df.count(),
    "silver_invalid": invalid_df.count(),
    "gold_records": gold_df.count(),
}
```

This is already a timed, structured artifact — wrapping the call in
`time.perf_counter()` before/after gives per-run wall-clock duration for
free, with no change to pipeline code.

### 2. Timestamped layer-boundary logs

`log_pipeline_start`/`log_pipeline_end` emit ISO-8601 timestamps at the
start and end of every layer (Bronze, Silver, Gold, Quarantine):

```text
START BRONZE | landing_path=10_data/raw/ | 2026-10-04T10:40:23+00:00
END BRONZE   | segments_loaded=83        | 2026-10-04T10:40:25+00:00
```

Parsing these gives **per-layer** duration breakdowns (Bronze vs. Silver vs.
Gold time), which is more actionable than a single end-to-end number when
deciding where to optimize.

### 3. A known, reproducible smoke-test baseline

```text
BRONZE     : segments_loaded = 83
SILVER     : total = 6, valid = 6, invalid = 0
GOLD       : records = 6
QUARANTINE : records = 0
```

(1 synthetic file, 3 subscribers + 3 dependents — see
[`PIPELINE_STATUS.md`](https://github.com/yourusername/medenroll834/blob/main/PIPELINE_STATUS.md)). This is a real, run
result, suitable as a regression baseline: any future change that alters
these counts for the same input is a signal worth investigating.

## Proposed framework design (not yet implemented)

A natural next step, using only what's already in the repo:

```python
# proposed: 07_scripts/03_utils/benchmark_pipeline.py
import time
from generator.synthetic_834_generator import generate_batch
from src.medenroll834.ingestion.pipeline import DeltaMedallionPipeline
from src.medenroll834.common.spark_session import get_local_spark

SIZES = [(1, 10), (5, 100), (10, 1000), (20, 5000)]  # (files, subscribers_per_file)

def run_benchmark():
    spark = get_local_spark("benchmark")
    results = []
    for num_files, subs in SIZES:
        generate_batch("10_data/bench_raw", num_files=num_files, subscribers_per_file=subs)
        pipeline = DeltaMedallionPipeline(spark)
        start = time.perf_counter()
        summary = pipeline.run("10_data/bench_raw")
        elapsed = time.perf_counter() - start
        results.append({"files": num_files, "subs": subs, "elapsed_s": elapsed, **summary})
    return results
```

This would reuse `generate_batch()` ([Chapter 10](10-synthetic-data-generation.md))
and `DeltaMedallionPipeline` ([Chapter 18](18-gold-layer.md)) exactly as-is
— no pipeline code changes needed, only a new orchestrating script that
scales input size and records elapsed time per run.

## What a benchmark should measure

| Dimension | Metric | Source |
|---|---|---|
| Volume | Records/sec at increasing file sizes | `summary["gold_records"] / elapsed_s` |
| Velocity | End-to-end wall-clock time per run | `time.perf_counter()` around `pipeline.run()` |
| Variety | Validation pass rate as plan-code/field variety increases | `silver_valid / silver_total` |
| Reliability | Quarantine rate under `--messy` synthetic data | `silver_invalid / silver_total` |

See [Chapter 34](34-legacy-vs-modern-benchmark.md) for how these dimensions
map back to the legacy system's documented constraints.
