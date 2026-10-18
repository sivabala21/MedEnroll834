# 15. Business Rules / Plan Assignment

This chapter covers how a member is assigned to a benefit plan and rolled
up under the correct subscriber/group — the pipeline's core "enrollment
business rule" logic, spread across Silver (`HD` segment extraction, HL
resolution) and Gold (`subscriber_id` finalization).

## Where the plan code comes from

The `HD` (Health Coverage) segment carries the plan identifier. Silver's
`_extract_hd_data()` (see [Chapter 14](14-silver-layer.md)) pulls element
index 3 directly as the plan code:

```python
def _extract_hd_data(self, members_df):
    hd_seg = members_df.filter(F.col("segment_id") == "HD")
    return hd_seg.groupBy("file_name", "seg_start").agg(
        F.max(F.col("elements")[3]).alias("plan_code"),
    ).withColumn("plan_id", F.col("plan_code")).select("file_name", "seg_start", "plan_id")
```

## Valid plan codes (the business rule)

The complete set of accepted plan products is defined once and shared
between the generator, the validator, and `pipeline_config.yaml`:

```python
VALID_PLANS = {"100", "101", "102", "103", "104", "105", "106", "107"}
```

```yaml
# 03_config/pipeline_config.yaml
validation:
  require_plan_code: true
  valid_plan_codes:
    - "100"
    - "101"
    - "102"
    - "103"
    - "104"
    - "105"
    - "106"
    - "107"
```

Any `plan_id` outside this set produces a validation error
(`"Unknown plan: <code>"`) and the row is quarantined — see
[Chapter 16](16-data-quality-reject-handling.md).

## Relationship-driven subscriber assignment

Plan assignment is not just "attach a plan code" — it must correctly
determine **whose** enrollment record a plan applies to, by resolving the
X12 relationship hierarchy:

```python
RELATIONSHIP_MAP = {
    "18": "self",              # subscriber (primary policyholder)
    "01": "spouse",             # dependent
    "03": "child",               # dependent
    "19": "domestic_partner",   # dependent
}
```

HL level `20` always means subscriber; level `21` always means dependent.
Silver materializes `is_subscriber` directly from this, because Gold's
plan/group roll-up depends on it:

```python
# HL level 20 = subscriber, 21 = dependent. Gold depends on this flag to
# roll dependents up to their subscriber, so it must be materialised
# here; leaving it null silently produces null group_id in Gold.
base = base.withColumn(
    "is_subscriber",
    F.when(F.col("hl_level") == "20", F.lit(True)).otherwise(F.lit(False)),
)
```

## Gold finalizes the subscriber assignment

> Source: [`src/medenroll834/ingestion/gold.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/ingestion/gold.py)

```python
gold_df = (
    silver_df.withColumn(
        "subscriber_id",
        F.when(F.col("is_subscriber"), F.col("member_id")).otherwise(
            F.col("subscriber_id")
        ),
    )
    .filter(F.col("is_valid") == True)
    .dropDuplicates(["file_name", "member_id"])
    ...
)
```

This is a correction over an earlier version that joined on `group_id` —
which Silver had actually populated from the `REF*2L` *address-association*
key (a member id, not a group id). That join both failed to resolve
correctly and fanned out rows. Because `subscriber_id` is already resolved
in Silver via the HL hierarchy (see
[Chapter 14](14-silver-layer.md#subscriber-resolution-without-a-fan-out-join)),
Gold does no re-derivation and no positional guesswork — it only finalizes
the subscriber's own row to point at itself.

## Enrollment maintenance type (OLTP load)

When loading into the OLTP CORE schema, every enrollment event is written
with a fixed maintenance type code `"021"` (addition) and an `enrl_stat` of
`"ACTIVE"`:

```python
cur.execute(
    "INSERT INTO core.core_meen_enrollment (meme_id, plpl_id, txn_id, maint_typ_cd, "
    "eff_dt, term_dt, enrl_stat, crtd_ts, updt_ts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
    (meme_id, plpl_id, txn_id, "021", _as_date(r.get("coverage_start_date")) or now.date(),
     _as_date_open_ended(r.get("coverage_end_date")),
     "ACTIVE" if _as_date(r.get("coverage_end_date")) is None else "TERMED", now, now),
)
```

`_as_date_open_ended()` replaces a missing/`NULL` coverage end date with the
fixed `9999-12-31` sentinel (`OPEN_ENDED_DATE`) instead of `NULL`, so
`term_dt`/`END_DATE` is always a real, queryable date across both OLTP and
OLAP (see [Chapter 19](19-olap-star-schema.md)).

The OLAP `FACT_ENROLLMENT` builder in the Databricks notebook derives
`ENROLLMENT_STATUS` dynamically instead, based on whether a coverage end
date is present:

```python
.withColumn("ENROLLMENT_STATUS",
    F.when(F.col("coverage_end_date").isNull() | (F.col("coverage_end_date") == ""),
           F.lit("ACTIVE")).otherwise(F.lit("TERMED")))
```

See [Chapter 17](17-postgresql-bulk-load-upsert.md) and
[Chapter 19](19-olap-star-schema.md) for the full downstream loads.
