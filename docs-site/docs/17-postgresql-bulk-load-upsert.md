# 17. PostgreSQL Bulk Load / Upsert

> Source: [`src/medenroll834/oltp/oltp_loader.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/oltp/oltp_loader.py)

`load_gold_to_oltp()` reads the Databricks Gold table via a SQL warehouse
and writes it into the PostgreSQL CORE/ENRL schemas, using natural-key
upserts so re-running the load does not duplicate reference data
(groups, plans, subscribers).

## Reading Gold from Databricks

```python
GOLD_TABLE = "med834enrl.edi.gold_enrollments"

def _read_gold_from_databricks(warehouse_id=None, profile=None) -> List[Dict]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient(**({"profile": profile} if profile else {}))
    if not warehouse_id:
        whs = list(w.warehouses.list())
        warehouse_id = whs[0].id

    sql = (
        "SELECT member_id, first_name, last_name, date_of_birth, gender, "
        "group_id, group_name, plan_id, coverage_start_date, coverage_end_date, "
        "street_1, street_2, city, state, zip_code, relationship, is_subscriber, "
        f"file_name FROM {GOLD_TABLE}"
    )
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s"
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Gold read failed: {resp.status.error.message}")

    cols = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(cols, r)) for r in (resp.result.data_array or [])]
```

Uses the `databricks-sdk`'s SQL Statement Execution API — no JDBC driver,
no cluster to keep warm; the serverless SQL warehouse spins up on demand.

## The load: one transaction, natural-key upserts

```python
def load_gold_to_oltp(oltp_db_url=None, warehouse_id=None, databricks_profile=None, rows=None) -> Dict:
    oltp_db_url = oltp_db_url or os.environ["OLTP_DATABASE_URL"]
    if rows is None:
        rows = _read_gold_from_databricks(warehouse_id, databricks_profile)

    conn = psycopg2.connect(oltp_db_url)
    conn.autocommit = False
    cur = conn.cursor()
    loaded = {"groups": 0, "plans": 0, "subscribers": 0, "members": 0,
              "addresses": 0, "transactions": 0, "enrollments": 0}
    try:
        # ... per-row processing (below) ...
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return loaded
```

The **entire load runs in one transaction** — any failure partway through
rolls back everything, so the OLTP database never ends up with a
half-loaded file's data.

## Upsert pattern (`ON CONFLICT ... DO UPDATE`)

Reference entities (group, plan, subscriber) are upserted on their natural
key, with an in-memory cache (`group_cache`, `plan_cache`) to avoid
redundant round-trips within the same load:

```python
if grp_num not in group_cache:
    cur.execute(
        "INSERT INTO core.core_grgr_group (grgr_num, st_cd, eff_dt, crtd_ts, updt_ts) "
        "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (grgr_num) DO UPDATE SET updt_ts=EXCLUDED.updt_ts "
        "RETURNING grgr_id",
        (grp_num, (r.get("state") or "NA")[:2], _as_date(r.get("coverage_start_date")) or now.date(), now, now),
    )
    group_cache[grp_num] = cur.fetchone()[0]
```

This is why the loader is **idempotent**: re-running it against the same
Gold data updates `updt_ts` on existing groups/plans/subscribers instead of
inserting duplicates — see [Chapter 22](22-idempotency-incremental-processing.md).

## Member and enrollment inserts (not upserted)

`core_meme_member` and `core_meen_enrollment` are plain inserts — a member
row is created fresh for each processed Gold record (member identity is
owned by the subscriber's natural key + relationship code, not a separately
upserted key in this simplified schema):

```python
cur.execute(
    "INSERT INTO core.core_meme_member (mesb_id, rltn_cd, fst_nm, lst_nm, dob, gndr_cd, "
    "is_act, crtd_ts, updt_ts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING meme_id",
    (mesb_id, _rel_code(r.get("relationship")), ..., True, now, now),
)
meme_id = cur.fetchone()[0]
```

## One transaction row created per file

```python
for file_nm in {r["file_name"] for r in rows if r.get("file_name")}:
    cur.execute(
        "INSERT INTO enrl.enrl_edxn_transaction "
        "(file_nm, isa_ctrl_num, gs_ctrl_num, st_ctrl_num, sndr_id, rcvr_id, "
        "rcvd_ts, proc_stat, rec_cnt) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING txn_id",
        (file_nm, "N/A", "N/A", "N/A", "SENDERID", "RECEIVERID", now, "PROCESSED", 0),
    )
```

`file_txn` maps each file name to its transaction id, so every
`core_meen_enrollment` row can be FK'd back to the specific 834 file it
came from — this is the OLTP-side equivalent of Gold's `file_name` lineage
column.

## Helper conversions

```python
def _as_date(value):
    """Try YYYY-MM-DD then YYYYMMDD; returns None if neither parses."""
    ...

def _rel_code(relationship):
    """Reverse-maps Silver's relationship string back to the X12 code (e.g. 'spouse' -> '01')."""
    mapping = {"self": "18", "spouse": "01", "child": "03", "domestic_partner": "19"}
    return mapping.get((relationship or "self").lower(), "18")
```

## Invoked from the Airflow DAG

```python
# med834_e2e_pipeline.py
@task
def load_oltp_database(_run_id: int) -> dict:
    from src.medenroll834.oltp.oltp_loader import load_gold_to_oltp
    summary = load_gold_to_oltp(oltp_db_url=_cfg("OLTP_DATABASE_URL"),
                                warehouse_id=_cfg("DATABRICKS_WAREHOUSE_ID"))
    return summary
```

See [Chapter 21](21-airflow-dag-implementation.md) for the full DAG task
graph this step sits in.
