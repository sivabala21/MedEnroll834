"""
src/medenroll834/oltp/oltp_loader.py
Load enrollment records from the Databricks GOLD Delta table into the
PostgreSQL OLTP database (CORE + ENRL schemas). Kept simple for the demo;
idempotent on natural keys so re-runs do not duplicate reference rows.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Dict, List

import psycopg2

GOLD_TABLE = "med834enrl.edi.gold_enrollments"


def _read_gold_from_databricks(warehouse_id=None, profile=None) -> List[Dict]:
    """Fetch gold enrollment rows from Databricks as a list of dicts."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    kwargs = {"profile": profile} if profile else {}
    w = WorkspaceClient(**kwargs)
    if not warehouse_id:
        whs = list(w.warehouses.list())
        if not whs:
            raise RuntimeError("No SQL warehouse available")
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
    sid = resp.statement_id
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(sid)
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"Gold read failed: {err.message if err else 'unknown'}")

    cols = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(cols, r)) for r in (resp.result.data_array or [])]


def _as_date(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def _rel_code(relationship):
    mapping = {"self": "18", "spouse": "01", "child": "03", "domestic_partner": "19"}
    return mapping.get((relationship or "self").lower(), "18")



def load_gold_to_oltp(oltp_db_url=None, warehouse_id=None, databricks_profile=None, rows=None) -> Dict:
    """Load gold enrollment rows into the OLTP database. Returns a summary dict."""
    oltp_db_url = oltp_db_url or os.environ["OLTP_DATABASE_URL"]
    if rows is None:
        rows = _read_gold_from_databricks(warehouse_id, databricks_profile)

    now = datetime.now(timezone.utc)
    conn = psycopg2.connect(oltp_db_url)
    conn.autocommit = False
    cur = conn.cursor()
    loaded = {"groups": 0, "plans": 0, "subscribers": 0, "members": 0,
              "addresses": 0, "transactions": 0, "enrollments": 0}
    try:
        file_txn: Dict[str, int] = {}
        for file_nm in {r["file_name"] for r in rows if r.get("file_name")}:
            cur.execute(
                "INSERT INTO enrl.enrl_edxn_transaction "
                "(file_nm, isa_ctrl_num, gs_ctrl_num, st_ctrl_num, sndr_id, rcvr_id, rcvd_ts, proc_stat, rec_cnt) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING txn_id",
                (file_nm, "N/A", "N/A", "N/A", "SENDERID", "RECEIVERID", now, "PROCESSED", 0),
            )
            file_txn[file_nm] = cur.fetchone()[0]
            loaded["transactions"] += 1

        group_cache: Dict[str, int] = {}
        plan_cache: Dict[str, int] = {}
        for r in rows:
            grp_num = (r.get("group_id") or "UNKNOWN")[:20]
            if grp_num not in group_cache:
                cur.execute(
                    "INSERT INTO core.core_grgr_group (grgr_num, st_cd, eff_dt, crtd_ts, updt_ts) "
                    "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (grgr_num) DO UPDATE SET updt_ts=EXCLUDED.updt_ts RETURNING grgr_id",
                    (grp_num, (r.get("state") or "NA")[:2], _as_date(r.get("coverage_start_date")) or now.date(), now, now),
                )
                group_cache[grp_num] = cur.fetchone()[0]
                loaded["groups"] += 1
            grgr_id = group_cache[grp_num]

            plan_cd = (r.get("plan_id") or "UNKNOWN")[:20]
            if plan_cd not in plan_cache:
                cur.execute(
                    "INSERT INTO core.core_plpl_plan (plpl_cd, plpl_nm, lob_cd, eff_dt, crtd_ts, updt_ts) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (plpl_cd) DO UPDATE SET updt_ts=EXCLUDED.updt_ts RETURNING plpl_id",
                    (plan_cd, plan_cd + " Plan", "COMMERCIAL", _as_date(r.get("coverage_start_date")) or now.date(), now, now),
                )
                plan_cache[plan_cd] = cur.fetchone()[0]
                loaded["plans"] += 1
            plpl_id = plan_cache[plan_cd]

            sbsb_num = (r.get("member_id") or "UNKNOWN")[:30]
            cur.execute(
                "INSERT INTO core.core_mesb_subscriber (sbsb_num, grgr_id, fst_nm, lst_nm, dob, gndr_cd, crtd_ts, updt_ts) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (sbsb_num) DO UPDATE SET updt_ts=EXCLUDED.updt_ts RETURNING mesb_id",
                (sbsb_num, grgr_id, (r.get("first_name") or "NA")[:100], (r.get("last_name") or "NA")[:100],
                 _as_date(r.get("date_of_birth")) or now.date(), (r.get("gender") or None), now, now),
            )
            mesb_id = cur.fetchone()[0]
            loaded["subscribers"] += 1

            cur.execute(
                "INSERT INTO core.core_meme_member (mesb_id, rltn_cd, fst_nm, lst_nm, dob, gndr_cd, is_act, crtd_ts, updt_ts) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING meme_id",
                (mesb_id, _rel_code(r.get("relationship")), (r.get("first_name") or "NA")[:100],
                 (r.get("last_name") or "NA")[:100], _as_date(r.get("date_of_birth")) or now.date(),
                 (r.get("gender") or None), True, now, now),
            )
            meme_id = cur.fetchone()[0]
            loaded["members"] += 1

            if r.get("street_1"):
                cur.execute(
                    "INSERT INTO core.core_sbad_address (meme_id, adr_typ, adr_ln1, adr_ln2, city_nm, st_cd, zip_cd, eff_dt, crtd_ts) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (meme_id, "HOME", r["street_1"][:150], (r.get("street_2") or None),
                     (r.get("city") or "NA")[:100], (r.get("state") or "NA")[:2],
                     (r.get("zip_code") or "00000")[:10], _as_date(r.get("coverage_start_date")) or now.date(), now),
                )
                loaded["addresses"] += 1

            txn_id = file_txn.get(r.get("file_name"))
            if txn_id:
                cur.execute(
                    "INSERT INTO core.core_meen_enrollment (meme_id, plpl_id, txn_id, maint_typ_cd, eff_dt, term_dt, enrl_stat, crtd_ts, updt_ts) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (meme_id, plpl_id, txn_id, "021", _as_date(r.get("coverage_start_date")) or now.date(),
                     _as_date(r.get("coverage_end_date")), "ACTIVE", now, now),
                )
                loaded["enrollments"] += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return loaded
