#!/usr/bin/env python3
"""
Backout (rollback) the MedEnroll834 OLTP schema on a PostgreSQL database.

Reverses 07_scripts/02_deployment/deploy_oltp_database.py by running
04_infrastructure/02_sql/02_oltp/SCHEMA_OLTP_BACKOUT.sql, which drops all
foreign keys, tables, and the CORE/ENRL/UTIL schemas (idempotent, IF EXISTS).

Usage:
    python 07_scripts/02_deployment/backout_oltp_database.py "<connection_url>"
"""
import sys
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKOUT_SQL = REPO_ROOT / "04_infrastructure" / "02_sql" / "02_oltp" / "SCHEMA_OLTP_BACKOUT.sql"


def main() -> int:
    if len(sys.argv) < 2:
        print("ERROR: connection URL required as first argument")
        return 1

    conn_url = sys.argv[1]
    sql = BACKOUT_SQL.read_text(encoding="utf-8")

    conn = psycopg2.connect(conn_url)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute(sql)
        conn.commit()
        print(f"OK: executed {BACKOUT_SQL.name}")
        print("\nAll OLTP objects (CORE/ENRL/UTIL schemas and tables) removed.")
        print("Re-run deploy_oltp_database.py to redeploy.")
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        print(f"\nBACKOUT FAILED (rolled back): {exc}")
        return 1
    finally:
        cur.close()
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
