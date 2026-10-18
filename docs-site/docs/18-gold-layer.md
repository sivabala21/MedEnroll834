# 18. Gold Layer

> Source: [`src/medenroll834/ingestion/gold.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/ingestion/gold.py)

The Gold layer is the final, **analytics-ready** output: deduplicated,
valid-only enrollment records with derived convenience fields, written with
Change Data Feed enabled so downstream consumers (OLAP star schema builder,
OLTP loader) can read incrementally in the future.

## Full transform

```python
class GoldLayer:
    def transform(self, silver_df: DataFrame) -> DataFrame:
        self.logger = setup_logging("gold")
        start_time = log_pipeline_start(self.logger, "GOLD", "transforming silver to gold")

        try:
            gold_df = (
                silver_df.withColumn(
                    "subscriber_id",
                    F.when(F.col("is_subscriber"), F.col("member_id")).otherwise(
                        F.col("subscriber_id")
                    ),
                )
                .filter(F.col("is_valid") == True)
                .dropDuplicates(["file_name", "member_id"])
                .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
                .withColumn(
                    "age_years",
                    F.floor(F.datediff(F.current_date(), F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")) / 365.25),
                )
                .withColumn("ingested_at", F.current_timestamp().cast("string"))
            )

            table_path = self.config["layers"]["gold"]["path"]
            (
                gold_df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .option("delta.enableChangeDataFeed", "true")
                .save(table_path)
            )
        except Exception:
            log_pipeline_error(self.logger, "GOLD", "transforming silver to gold", start_time)
            raise

        log_pipeline_end(self.logger, "GOLD", f"records={gold_df.count()}", start_time)
        return gold_df
```

## What Gold does, step by step

1. **Finalize `subscriber_id`** — see
   [Chapter 15](15-business-rules-plan-assignment.md#gold-finalizes-the-subscriber-assignment)
   for why this does **no** re-derivation from `group_id` (a past bug).
2. **Filter to `is_valid == True`** — invalid rows never reach Gold; they
   were already routed to quarantine from Silver (see
   [Chapter 16](16-data-quality-reject-handling.md)).
3. **Deduplicate on `(file_name, member_id)`** — guards against the same
   member appearing twice within one file (e.g. duplicate `HL` blocks from
   a malformed or re-sent 834).
4. **Derive `full_name`** — `concat_ws(" ", first_name, last_name)`, a
   display convenience not present in Silver.
5. **Derive `age_years`** — `floor(datediff(today, dob) / 365.25)`,
   accounting for leap years via the `365.25` divisor rather than a flat
   `365`.
6. **Write with Change Data Feed enabled** — `delta.enableChangeDataFeed`
   lets a future incremental consumer read only the rows that changed
   since a given version, instead of re-scanning the whole Gold table.

## Write semantics

| Option | Value | Why |
|---|---|---|
| `mode` | `overwrite` | Gold reflects the current full state of valid enrollments per run |
| `mergeSchema` | `true` | Tolerates schema drift from upstream Silver changes |
| `delta.enableChangeDataFeed` | `true` | Enables future incremental/CDC-style consumption |

Configured path (`03_config/pipeline_config.yaml`):

```yaml
layers:
  gold:
    path: 10_data/gold/enrollments
    table: gold_enrollments
    format: delta
    options:
      mergeSchema: "true"
      delta.enableChangeDataFeed: "true"
```

On Databricks, this is `med834enrl.edi.gold_enrollments` — the table read
by both the OLTP loader (`_read_gold_from_databricks`) and the report
generator's `REPORT_QUERIES` (see [Chapter 27](27-databricks-sql-dashboard.md)).

## Gold's row count in the pipeline summary

```python
summary = {
    "bronze_segments": bronze_df.count(),
    "silver_total": silver_df.count(),
    "silver_valid": valid_df.count(),
    "silver_invalid": invalid_df.count(),
    "gold_records": gold_df.count(),
}
```

From the verified smoke-test run in [Chapter 1](01-project-overview.md):
`gold_records = 6` from 6 valid Silver rows (3 subscribers + 3 dependents),
matching 1:1 because none were duplicates.

Gold feeds two independent downstream paths — see
[Chapter 17](17-postgresql-bulk-load-upsert.md) for the OLTP load and
[Chapter 19](19-olap-star-schema.md) for the OLAP star schema build.
