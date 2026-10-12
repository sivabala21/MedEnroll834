# 14. Silver Layer

> Source: [`src/medenroll834/ingestion/silver.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/ingestion/silver.py)

The Silver layer turns raw Bronze segments into one normalized row **per
member**, resolving the X12 `HL` (hierarchical level) structure, extracting
every relevant segment type, formatting dates, and applying validation —
all as Spark DataFrame/window operations, with **no row-by-row Python
loops** on the member data itself.

## Why HL resolution is hard (and how it's solved)

X12 834 files model subscriber/dependent relationships through `HL`
segments: `HL*<id>*<parent_id>*<level>*<child_count>`. The catch, documented
directly in the code, is that **HL ids are only unique within a
transaction set** (each `ST*834.../SE` pair restarts numbering at 1) — not
within the whole file:

```python
# HL ids are only unique *within a transaction set* (each ST*834.../SE
# pair restarts numbering at 1), not within the file. A file with 100
# enrolments contains 100 HL rows with hl_id=1, so resolving dependents
# against subscribers on hl_id alone matches every subscriber in the
# file and fans out 100x.
```

The fix is a transaction-scoped key, derived by carrying the most recent
`ST` row forward with a window function:

```python
file_order = Window.partitionBy("file_name").orderBy(F.col("seg_row"))
ordered = ordered.withColumn(
    "txn_row",
    F.last(
        F.when(F.col("segment_id") == "ST", F.col("seg_row")),
        ignorenulls=True,
    ).over(file_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
)
ordered = ordered.withColumn(
    "hl_key",
    F.concat_ws("-", F.col("file_name"), F.col("txn_row").cast("string")),
)
```

An earlier version range-joined against every `ST` row instead, which
matched a segment to all preceding transaction sets and multiplied member
rows ~5x. `last()` over a cumulative window yields exactly one key per
segment.

## Deriving member row ranges from HL boundaries

Each `HL` segment marks the start of one member's data block; the next
`HL` segment (within the same transaction) marks the end:

```python
hl_window = Window.partitionBy("hl_key").orderBy(F.col("seg_row"))
hl_with_next = hl_events.withColumn(
    "next_hl_row", F.lead("seg_row").over(hl_window)
).fillna(999999999, subset=["next_hl_row"])
## Segment extraction (NM1, INS, DTP, HD, EB, N3/N4, REF)

Each segment type is extracted with its own `groupBy(...).agg(F.max(F.when(...)))`
helper, keyed on `(file_name, seg_start)` — the member block identifier:

```python
def _extract_nm1_data(self, members_df):
    nm1_seg = members_df.filter(F.col("segment_id") == "NM1")
    return nm1_seg.groupBy("file_name", "seg_start", "hl_level").agg(
        F.max(F.when(F.col("elements")[1] == "IL", F.col("elements")[2])).alias("last_name"),
        F.max(F.when(F.col("elements")[1] == "IL", F.col("elements")[3])).alias("first_name"),
        F.max(F.when(F.col("elements")[1] == "IL", F.col("elements")[8])).alias("member_id"),
    )
```

| Helper | Segment | Extracts |
|---|---|---|
| `_extract_nm1_data` | `NM1` | last name, first name, member id (entity code `IL`) |
| `_extract_ins_data` | `INS` | relationship (via `RELATIONSHIP_MAP`; subscriber is hardcoded `"self"`) |
| `_extract_dtp_data` | `DTP` | DOB (`291`), coverage start (`472`/`348`), coverage end (`349`) |
| `_extract_hd_data` | `HD` | plan id |
| `_extract_eb_data` | `EB` | fallback coverage start/end (coalesced if `DTP` is absent) |
| `_extract_address_data` | `N3`/`N4`/`REF*2L` | street/city/state/zip, resolved to the member via the nearest preceding `REF*2L` |

## Subscriber resolution without a fan-out join

HL level `20` = subscriber, level `21` = dependent. A dependent must be tied
to its subscriber's `member_id`. The naive approach — a self-join on
`hl_parent_id == hl_id` — duplicated every dependent against every
candidate subscriber and **blew the executor heap**. The fix is a
broadcast map built from a small `collect()`:

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
subscriber_lookup = F.create_map(*[item for kv in subscriber_map.items() for item in (F.lit(kv[0]), F.lit(kv[1]))])
base = base.withColumn(
    "subscriber_id",
    F.when(F.col("is_subscriber"), F.col("member_id"))
     .otherwise(F.element_at(subscriber_lookup, F.concat_ws("|", F.col("hl_key"), F.col("hl_parent_id")))),
)
```

This only `collect()`s the (small) set of distinct subscribers, not the
full member population, so it scales with group count rather than member

## Date formatting

X12 dates arrive as `YYYYMMDD` strings; Silver reformats them to
`YYYY-MM-DD`:

```python
def _format_date(self, col_name: str):
    return F.when(
        (F.length(F.col(col_name)) == 8) & F.col(col_name).rlike(r"^\d{8}$"),
        F.concat(F.substring(F.col(col_name), 1, 4), F.lit("-"),
                  F.substring(F.col(col_name), 5, 2), F.lit("-"),
                  F.substring(F.col(col_name), 7, 2)),
    ).otherwise(F.col(col_name))
```

## Output schema

`SILVER_SCHEMA` adds `hl_id`, `hl_parent_id`, `subscriber_id`,
`address_member_id`, `validation_errors`, and `is_valid` on top of the
flattened member attributes — see
[`04_infrastructure/02_sql/01_delta/silver_members.sql`](https://github.com/yourusername/medenroll834/blob/main/04_infrastructure/02_sql/01_delta/silver_members.sql)
for the Delta table DDL. Written with
`delta.deletedFileRetentionDuration: "interval 7 days"` per
[`pipeline_config.yaml`](https://github.com/yourusername/medenroll834/blob/main/03_config/pipeline_config.yaml), giving a
7-day time-travel window for debugging.

See [Chapter 15](15-business-rules-plan-assignment.md) for how Gold
consumes `is_subscriber`/`subscriber_id`, and
[Chapter 16](16-data-quality-reject-handling.md) for `_apply_validation()`.

count.

```

A range join (`seg_row >= seg_start AND seg_row < seg_end`) then attaches
every segment between consecutive `HL`s to the right member block.
