#!/usr/bin/env python3
"""
Deploy the MedEnroll834 OLTP schema to a PostgreSQL database.

Executes the SQL files under 04_infrastructure/02_sql/02_oltp in dependency
order: schemas -> tables -> indexes -> constraints -> comments.

Usage:
    python 07_scripts/02_deployment/deploy_oltp_database.py "<connection_url>"
"""
import sys
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[2]
OLTP = REPO_ROOT / "04_infrastructure" / "02_sql" / "02_oltp"

# Deterministic deployment order honouring FK dependencies.
DEPLOY_ORDER = [
    # 1. Schemas
    "01_schemas/01_create_core_schema.sql",
    "01_schemas/02_create_enrl_schema.sql",
    "01_schemas/03_create_util_schema.sql",
    # 2. Tables (parents before children)
    "02_tables/CORE_GRGR_GROUP.sql",
    "02_tables/CORE_PLPL_PLAN.sql",
    "02_tables/CORE_MESB_SUBSCRIBER.sql",
    "02_tables/CORE_MEME_MEMBER.sql",
    "02_tables/CORE_SBAD_ADDRESS.sql",
    "02_tables/ENRL_EDXN_TRANSACTION.sql",
    "02_tables/CORE_MEEN_ENROLLMENT.sql",
    "02_tables/ENRL_RJRW_REJECT.sql",
    # 3. Indexes
    "03_indexes/INDEXES.sql",
    # 4. Constraints (foreign keys)
    "04_constraints/CONSTRAINTS.sql",
    # 5. Comments / documentation
    "05_views/COMMENTS.sql",
]


def main() -> int:
    if len(sys.argv) < 2:
        print("ERROR: connection URL required as first argument")
        return 1

    conn_url = sys.argv[1]

    conn = psycopg2.connect(conn_url)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        for rel_path in DEPLOY_ORDER:
            sql_file = OLTP / rel_path
            if not sql_file.exists():
                print(f"  SKIP (missing): {rel_path}")
                continue
            sql = sql_file.read_text(encoding="utf-8")
            cur.execute(sql)
            print(f"  OK: {rel_path}")

        conn.commit()
        print("\nAll OLTP objects deployed and committed.")
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        print(f"\nDEPLOY FAILED (rolled back): {exc}")
        return 1
    finally:
        cur.close()
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
