#!/usr/bin/env python3
"""Verify the deployed OLTP schema in the PostgreSQL database."""
import sys
import psycopg2

URL = (
    "postgresql://neondb_owner:npg_O3NChGKwjnB5@"
    "ep-late-leaf-b3moj6iv-pooler.c-4.ap-southeast-1.aws.neon.tech/"
    "med834enrl?sslmode=require"
)


def main():
    conn = psycopg2.connect(URL)
    cur = conn.cursor()

    cur.execute(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name IN ('core','enrl','util') ORDER BY schema_name"
    )
    print("SCHEMAS:", [r[0] for r in cur.fetchall()])

    cur.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema IN ('core','enrl','util') "
        "ORDER BY table_schema, table_name"
    )
    print("\nTABLES:")
    for schema, table in cur.fetchall():
        print(f"  {schema}.{table}")

    cur.execute(
        "SELECT count(*) FROM pg_indexes WHERE schemaname IN ('core','enrl')"
    )
    print("\nINDEXES (total):", cur.fetchone()[0])

    cur.execute(
        "SELECT count(*) FROM information_schema.table_constraints "
        "WHERE constraint_type='FOREIGN KEY' "
        "AND constraint_schema IN ('core','enrl')"
    )
    print("FOREIGN KEYS:", cur.fetchone()[0])

    # Column count sanity check per table
    cur.execute(
        "SELECT table_schema, table_name, count(*) "
        "FROM information_schema.columns "
        "WHERE table_schema IN ('core','enrl') "
        "GROUP BY table_schema, table_name ORDER BY 1,2"
    )
    print("\nCOLUMN COUNTS:")
    for schema, table, cols in cur.fetchall():
        print(f"  {schema}.{table}: {cols} columns")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
