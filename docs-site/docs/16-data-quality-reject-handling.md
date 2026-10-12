# 16. Data Quality & Reject Handling

MedEnroll834 validates every member record with Spark-SQL expressions
(no row-by-row Python), tags each row with its specific failure reasons,
and routes invalid rows to a dedicated Delta quarantine table instead of
dropping or silently failing them.

## Validation rules

> Source: [`src/medenroll834/quality/validator.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/quality/validator.py)
> (standalone) and `SilverLayer._apply_validation()` (inline, identical logic)

```python
class DataFrameValidator:
    VALID_PLANS = {"GOLD", "SILVER", "BRONZE", "PLATINUM", "HDHP", "EPO", "PPO", "HMO"}

    def validate(self, df: DataFrame) -> DataFrame:
        errors = F.filter(
            F.array(
                F.when(F.col("member_id") == "", F.lit("Member ID is required")),
                F.when(F.col("first_name") == "", F.lit("First name is required")),
                F.when(F.col("last_name") == "", F.lit("Last name is required")),
                F.when(F.col("plan_id") == "", F.lit("Plan ID is required")),
                F.when(
                    (F.col("coverage_start_date").isNotNull()) & (F.col("coverage_end_date").isNotNull()),
                    F.when(F.try_to_date(F.col("coverage_start_date")) > F.try_to_date(F.col("coverage_end_date")),
                           F.lit("Coverage start after end")),
                ),
                F.when(
                    (F.col("date_of_birth").isNotNull()) & (F.col("coverage_start_date").isNotNull()),
                    F.when(F.try_to_date(F.col("coverage_start_date")) < F.try_to_date(F.col("date_of_birth")),
                           F.lit("Coverage before DOB")),
                ),
                F.when(~F.col("plan_id").isin(list(self.VALID_PLANS)),
                       F.concat(F.lit("Unknown plan: "), F.col("plan_id"))),
            ),
            lambda x: x.isNotNull(),
        )
        return df.withColumn("validation_errors", errors).withColumn(
            "is_valid", F.size(F.col("validation_errors")) == 0
        )
```

## The six validation rules

| # | Rule | Error message |
|---|---|---|
| 1 | `member_id` required | `Member ID is required` |
| 2 | `first_name` required | `First name is required` |
| 3 | `last_name` required | `Last name is required` |
| 4 | `plan_id` required | `Plan ID is required` |
| 5 | coverage start ≤ coverage end (when both present) | `Coverage start after end` |
| 6 | coverage start ≥ date of birth (when both present) | `Coverage before DOB` |
| 7 | `plan_id` is one of the 8 valid plan codes | `Unknown plan: <code>` |

Each row can accumulate **multiple** errors at once — `validation_errors`
is an array, not a single flag, so a reject-remediation team sees every
problem with a record in one place rather than fixing issues one at a time
across repeated pipeline runs. `F.try_to_date()` is used (not `F.to_date()`)
so a malformed date string produces `null` and is excluded from the
comparison rather than crashing the whole DataFrame evaluation.

## `is_valid` flag

```python
.withColumn("is_valid", F.size(F.col("validation_errors")) == 0)
```

A row is valid if and only if its error array is empty — this single
boolean is what the pipeline orchestrator (`DeltaMedallionPipeline.run()`)
and the Gold layer filter on.

## Quarantine routing

> Source: [`src/medenroll834/quality/quarantine.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/quality/quarantine.py)

```python
class DeltaQuarantineManager:
    def write_invalid(self, invalid_df: DataFrame) -> None:
        quarantine_df = invalid_df.withColumn(
            "quarantined_at", F.current_timestamp().cast("string")
        ).withColumn("quarantine_id", F.expr("uuid()"))

        table_path = self.config["quarantine"]["path"]
        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(table_path)
        )
```

Quarantine writes are **appended**, not overwritten — every run's rejects
accumulate historically, each tagged with a unique `quarantine_id` and a
`quarantined_at` timestamp, so a remediation team can audit when and how
often a given record kept failing.

## Where quarantine fits in the pipeline

```python
# src/medenroll834/ingestion/pipeline.py
bronze_df = self.bronze.ingest(landing_path)
silver_df = self.silver.transform(bronze_df)
gold_df = self.gold.transform(silver_df)

valid_df = silver_df.filter(F.col("is_valid") == True)
invalid_df = silver_df.filter(F.col("is_valid") == False)
self.quarantine.write_invalid(invalid_df)
```

Quarantine operates on the **Silver** output (after HL resolution and
field extraction, before Gold deduplication/enrichment) — so a quarantined
record retains all the context Silver resolved about it (relationship,
subscriber hierarchy, address) even though it failed validation, which
makes remediation easier than quarantining raw Bronze segments.

## OLTP-side rejects (`ENRL_RJRW_REJECT`)

The OLTP schema has a parallel reject concept at the operational layer:

```sql
CREATE TABLE IF NOT EXISTS ENRL.ENRL_RJRW_REJECT (
  RJRW_ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  TXN_ID BIGINT NOT NULL,
  SRC_ROW TEXT NOT NULL,
  RJCT_CD VARCHAR(20) NOT NULL,
  RJCT_RSN VARCHAR(255) NOT NULL,
  RJCT_TS TIMESTAMPTZ NOT NULL
);
```

FK'd to `ENRL_EDXN_TRANSACTION`, so every reject traces back to the
specific inbound file transaction it came from — the OLTP equivalent of the
Delta quarantine table's `file_name`/`quarantine_id` lineage.

## Pipeline run summary

Quarantine counts are surfaced directly in the pipeline's return value, so
reject volume is visible without a separate query:

```python
summary = {
    "bronze_segments": bronze_df.count(),
    "silver_total": silver_df.count(),
    "silver_valid": valid_df.count(),
    "silver_invalid": invalid_df.count(),
    "gold_records": gold_df.count(),
}
```
