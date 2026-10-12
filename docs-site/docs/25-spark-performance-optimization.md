# 25. Spark Performance Optimization

Every Spark tuning decision documented here is an **actual configuration
or code change in this repo**, most captured with an inline comment
explaining the real failure it fixed — not generic Spark tuning advice.

## Adaptive Query Execution (AQE)

```python
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.sql.shuffle.partitions", "8")
```

AQE lets Spark re-plan shuffle partition counts at runtime based on actual
data size rather than a fixed guess, and `coalescePartitions` merges small
post-shuffle partitions automatically — important for this pipeline's
small-to-medium local/POC data volumes where a fixed large partition count
would create excessive task overhead. `shuffle.partitions=8` is tuned down
from Spark's default of 200, appropriate for local/`local[*]` execution
rather than a large cluster.

## Driver memory: fixing a real OOM

```python
# Default JVM heap (1g) is not enough for the window functions in the
# Silver address/hierarchy steps on a multi-core local box: 16 slots
# each sort a partition of the shuffle and the driver OOMs. Raise the
# ceiling and give the driver a real off-heap/spill budget.
.config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
.config("spark.driver.maxResultSize", "1g")
.config("spark.sql.windowExec.buffer.spill.threshold", "4096")
```

This was a reproduced OOM, not a precaution: the Silver layer's HL
hierarchy resolution uses multiple `Window` operations (see
[Chapter 14](14-silver-layer.md)), and the default 1 GB driver heap could
not hold the window buffers on a multi-core local machine. Raising
`spark.driver.memory` to 4 GB and the window exec spill threshold to 4096
rows fixed it.

## Broadcast map instead of a self-join (biggest algorithmic win)

The single largest performance/correctness fix in the codebase: resolving
each dependent's subscriber via `hl_parent_id`/`hl_id` was originally a
**self-join**, which fanned out catastrophically because HL ids repeat
across transaction sets (see [Chapter 14](14-silver-layer.md)):

```python
# Implemented with a broadcast map rather than a self-join. A join here
# duplicated every dependent against every candidate subscriber and
# blew the executor heap; a broadcast lookup is O(1) per row and cannot
# fan out because the key set is collapsed with first().
subscriber_map = {
    f"{r['hl_key']}|{r['hl_id']}": r["member_id"]
    for r in base.filter(F.col("is_subscriber"))
    .select("hl_key", "hl_id", "member_id")
    .dropDuplicates(["hl_key", "hl_id"])
    .collect()
}
```

Collecting only the (small) distinct-subscriber set and broadcasting it as
a Spark `create_map()` expression turns an O(n²)-shaped join into an O(1)
per-row map lookup — this scales with the number of **subscribers**, not
the number of **members**, which is the correct scaling dimension for this
workload.

## Window-based "carry forward latest value" instead of a loop

```python
file_order = Window.partitionBy("file_name").orderBy(F.col("seg_row"))
ordered = ordered.withColumn(
    "txn_row",
    F.last(F.when(F.col("segment_id") == "ST", F.col("seg_row")), ignorenulls=True)
     .over(file_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
)
```

A previous version range-joined every segment against every `ST` row to
find "the most recent transaction start," which matched a segment against
**all preceding** transaction sets and multiplied member rows roughly 5x. A
cumulative-window `last(..., ignorenulls=True)` carries the most recent
value forward in a single pass with no join at all.

## Delta write-time optimization

```python
# Bronze
.option("delta.autoOptimize.optimizeWrite", "true")
```

Compacts small files at write time — relevant because 834 ingestion
produces many small per-segment row batches per file; without
`optimizeWrite`, Bronze would accumulate a large number of tiny Parquet
files under the Delta table over many pipeline runs, degrading future read
performance.

## Delta retention tuning

```yaml
silver:
  options:
    delta.deletedFileRetentionDuration: "interval 7 days"
```

Keeps a 7-day time-travel window on Silver for debugging/auditing without
retaining deleted files indefinitely (the Delta default is 30 days via
`VACUUM`'s retention check) — a deliberate storage-cost vs. debuggability
tradeoff for this layer specifically.

## Change Data Feed for future incremental reads

```yaml
gold:
  options:
    delta.enableChangeDataFeed: "true"
```

Enabled proactively so a future consumer can read only changed rows since
a given version instead of a full re-scan — see
[Chapter 22](22-idempotency-incremental-processing.md) for the current
state (not yet exploited by the OLTP loader) and
[Chapter 38](38-future-improvements.md) for the roadmap.

## Databricks-side: `read_files()` + `explode()` over a Python loop

The Databricks notebook job reads landing files with a single
distributed-SQL statement rather than iterating files in Python, because
it must scale across a cluster reading many files from a Unity Catalog
Volume at once (see [Chapter 13](13-bronze-layer.md)):

```python
raw = spark.sql(
    f"SELECT value, element_at(split(_metadata.file_path, '/'), -1) AS file_name "
    f"FROM read_files('{LANDING}', format => 'text', wholeText => true)"
)
bronze = raw.withColumn("segment", F.explode(F.split(F.regexp_replace("value", r"[\r\n]+", "~"), "~"))) ...
```

`monotonically_increasing_id()` is used for `seg_order` in this distributed
path (rather than a Python `enumerate()`), since it can be computed without
collecting data back to the driver.
