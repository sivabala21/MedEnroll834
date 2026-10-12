#!/usr/bin/env python3
"""
Deploy the MedEnroll834 OLAP medallion + star schema to Databricks.

Creates schemas med834enrl.{bronze,silver,gold} and the GOLD star-schema
(5 dimensions + FACT_ENROLLMENT) with PK/FK constraints.

Uses the Databricks SDK with the OAuth profile from `databricks auth login`.
Idempotent (CREATE ... IF NOT EXISTS); constraints are skipped if they already
exist.

Usage:
    python 07_scripts/02_deployment/deploy_databricks_olap.py [--profile <name>]
"""
import argparse
import os
import re
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = REPO_ROOT / "07_scripts" / "02_deployment"
TABLES_SQL = DEPLOY_DIR / "databricks_olap_deploy.sql"
CONSTRAINTS_SQL = DEPLOY_DIR / "databricks_olap_constraints.sql"

CATALOG = "med834enrl"
GOLD_TABLES = [
    "dim_date",
    "dim_member",
    "dim_plan",
    "dim_group",
    "dim_transaction_source",
    "fact_enrollment",
]

# Point the SDK at the winget-installed CLI for OAuth token refresh on Windows.
_DEFAULT_CLI = (
    Path(os.environ.get("LOCALAPPDATA", ""))
    / "Microsoft" / "WinGet" / "Packages"
    / "Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe"
    / "databricks.exe"
)
if "DATABRICKS_CLI_PATH" not in os.environ and _DEFAULT_CLI.exists():
    os.environ["DATABRICKS_CLI_PATH"] = str(_DEFAULT_CLI)


def split_statements(sql_text: str) -> list[str]:
    no_comments = re.sub(r"--[^\n]*", "", sql_text)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def get_warehouse_id(w: WorkspaceClient, override):
    if override:
        return override
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse found. Create one or pass --warehouse-id.")
    for wh in warehouses:
        if getattr(wh.state, "value", str(wh.state)) == "RUNNING":
            print(f"  Using RUNNING warehouse: {wh.name} ({wh.id})")
            return wh.id
    wh = warehouses[0]
    print(f"  Using warehouse: {wh.name} ({wh.id})")
    return wh.id


def run_sql(w, warehouse_id, statement, tolerate_exists=False):
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="50s"
    )
    statement_id = resp.statement_id
    state = resp.status.state
    while state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(statement_id)
        state = resp.status.state
    if state != StatementState.SUCCEEDED:
        msg = resp.status.error.message if resp.status.error else "unknown error"
        low = msg.lower()
        already = (
            "already exists" in low
            or "already has a primary key" in low
            or "already_exist" in low
            or "resource_already_exists" in low
        )
        if tolerate_exists and already:
            return "SKIP"
        raise RuntimeError(f"Statement failed: {msg}\n  SQL: {statement[:120]}...")
    return "OK"


def execute_file(w, warehouse_id, path, tolerate_exists=False):
    statements = split_statements(path.read_text(encoding="utf-8"))
    print(f"\nExecuting {len(statements)} statements from {path.name}...")
    for i, stmt in enumerate(statements, 1):
        first_line = stmt.splitlines()[0][:70]
        status = run_sql(w, warehouse_id, stmt, tolerate_exists=tolerate_exists)
        print(f"  [{i}/{len(statements)}] {status}: {first_line}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="dbc-645321b4-3ef9")
    parser.add_argument("--warehouse-id", default=None)
    args = parser.parse_args()

    print(f"Connecting to Databricks (profile={args.profile})...")
    w = WorkspaceClient(profile=args.profile)
    print(f"  Authenticated as: {w.current_user.me().user_name}")

    warehouse_id = get_warehouse_id(w, args.warehouse_id)

    # 1. schemas + tables
    execute_file(w, warehouse_id, TABLES_SQL)
    # 2. PK/FK constraints (tolerate re-runs)
    execute_file(w, warehouse_id, CONSTRAINTS_SQL, tolerate_exists=True)

    # 3. verify
    print("\nVerifying GOLD star schema...")
    verify = (
        f"SELECT table_schema, table_name FROM {CATALOG}.information_schema.tables "
        f"WHERE table_schema = 'gold' ORDER BY table_name"
    )
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=verify, wait_timeout="50s"
    )
    found = [row[1] for row in (resp.result.data_array or [])] if resp.result else []
    print(f"  Tables in {CATALOG}.gold: {found}")

    missing = [t for t in GOLD_TABLES if t not in found]
    if missing:
        print(f"\nWARNING: missing tables: {missing}")
        return 1

    print("\nOLAP deploy complete. All 6 star-schema tables present in med834enrl.gold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
