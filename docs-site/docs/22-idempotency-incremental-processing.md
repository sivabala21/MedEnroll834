# 22. Idempotency & Incremental Processing

Every layer in MedEnroll834 is designed so **re-running it with the same
input produces the same result** — a core requirement for a pipeline that
Airflow may retry automatically.

## Delta layer idempotency: `overwrite` + natural grain

Bronze, Silver, and Gold all write with `mode("overwrite")`:

```python
# Bronze
parsed.write.format("delta").mode("overwrite").option("mergeSchema", "true")...

# Gold
gold_df.write.format("delta").mode("overwrite").option("mergeSchema", "true")...
```

Because each run re-derives its output **entirely** from the current
landing directory's files, re-running the pipeline against the same input
files produces byte-for-byte the same Bronze/Silver/Gold content — there is
no incremental merge logic to get out of sync. The one exception is
**quarantine**, which appends (see below), because quarantine history is
meant to accumulate across runs, not reflect only the latest run.

## Gold-level deduplication

```python
.dropDuplicates(["file_name", "member_id"])
```

Guards against a malformed or re-sent 834 file producing duplicate `HL`
blocks for the same member within one file — Gold's grain is
`(file_name, member_id)`, so duplicates collapse to one row regardless of
how many times a Silver row for that member appeared.

## OLTP idempotency: natural-key upserts

The PostgreSQL loader's reference-entity inserts use
`ON CONFLICT ... DO UPDATE`, keyed on each table's natural unique key:

```python
cur.execute(
    "INSERT INTO core.core_grgr_group (grgr_num, st_cd, eff_dt, crtd_ts, updt_ts) "
    "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (grgr_num) DO UPDATE SET updt_ts=EXCLUDED.updt_ts "
    "RETURNING grgr_id",
    ...
)
```

| Table | Natural key | Upsert behavior |
|---|---|---|
| `core_grgr_group` | `grgr_num` | Update `updt_ts` on conflict |
| `core_plpl_plan` | `plpl_cd` | Update `updt_ts` on conflict |
| `core_mesb_subscriber` | `sbsb_num` | Update `updt_ts` on conflict |

Re-running `load_gold_to_oltp()` against the same Gold data therefore does
**not** create duplicate groups, plans, or subscribers — only their
`updt_ts` advances. (Member and enrollment rows are plain inserts in this
simplified demo schema, not idempotent upserts — see
[Chapter 17](17-postgresql-bulk-load-upsert.md) and
[Chapter 37](37-future-improvements.md) for the natural extension.)

## OLAP idempotency: deterministic hashed keys

`DIM_TRANSACTION_SOURCE.TXN_ID` is derived with `F.abs(F.xxhash64("file_name"))`
rather than an auto-incrementing counter — the same file name always
produces the same `TXN_ID` across repeated notebook runs, so re-running the
medallion job does not fork transaction-source identity for the same file.

## Transactional atomicity

Both the OLTP loader and the OLTP schema deployment script wrap all work in
a **single transaction**:

```python
conn.autocommit = False
try:
    ...  # all inserts/upserts
    conn.commit()
except Exception:
    conn.rollback()
    raise
```

A failure partway through either operation leaves the database exactly as
it was before the attempt — no partial writes to reconcile manually.

## Incremental processing: Change Data Feed

Gold is written with `delta.enableChangeDataFeed: "true"`:

```yaml
layers:
  gold:
    options:
      delta.enableChangeDataFeed: "true"
```

This enables a future consumer to read only rows that changed since a
given Delta table version (`spark.read.format("delta").option("readChangeData", "true").option("startingVersion", N)`)
instead of re-scanning the whole table — the current OLTP loader and
report generator both still do a full `SELECT *` against Gold, so CDF is
provisioned but not yet exploited; see
[Chapter 37](37-future-improvements.md) for making the OLTP load
CDF-incremental.

## File-level idempotency boundary

The Airflow DAG's `archive_blob` task moves this run's exact processed
files out of `landing/834/` into `archive/834/` only after the core
processing chain (push -> Databricks -> OLTP) succeeds (default
`all_success` trigger rule, wired directly to that chain's XCom outputs —
see [Chapter 23](23-error-handling-retry.md)). This means a given 834 file
is only ever re-processed if it is manually moved back into `landing/` —
the pipeline itself does not re-ingest archived files on a subsequent
scheduled run, which is what keeps daily runs idempotent at the file level
even though Bronze/Silver/Gold are always full `overwrite`s of "whatever is
currently in landing." If the core chain fails instead, `quarantine_failed_files`
moves the same files to `failed/834/` rather than archiving or re-listing
`landing/834/` from scratch.
