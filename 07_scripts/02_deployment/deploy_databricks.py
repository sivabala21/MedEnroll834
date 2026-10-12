#!/usr/bin/env python3
"""
Clean-deploy the MedEnroll834 Unity Catalog objects to Databricks.

Uses the Databricks SDK, which automatically picks up the OAuth credentials
created by `databricks auth login` (from ~/.databrickscfg). No tokens are
stored in this repo.

What it does (DESTRUCTIVE):
  1. DROP CATALOG med834enrl CASCADE   (removes everything in the catalog)
  2. Recreate catalog + schema + volume
  3. Recreate the 4 Delta tables (bronze / silver / gold / quarantine)
  4. Verify the objects exist

Usage:
    python 07_scripts/02_deployment/deploy_databricks.py [--profile DEFAULT] [--warehouse-id <id>]
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
SQL_FILE = REPO_ROOT / "07_scripts" / "02_deployment" / "databricks_clean_deploy.sql"

# The databricks-cli OAuth auth type shells out to the CLI binary to refresh
# tokens. On Windows the winget-installed CLI is not on PATH, so point the SDK
# at it explicitly unless the caller already set DATABRICKS_CLI_PATH.
_DEFAULT_CLI = (
    Path(os.environ.get("LOCALAPPDATA", ""))
    / "Microsoft"
    / "WinGet"
    / "Packages"
    / "Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe"
    / "databricks.exe"
)
if "DATABRICKS_CLI_PATH" not in os.environ and _DEFAULT_CLI.exists():
    os.environ["DATABRICKS_CLI_PATH"] = str(_DEFAULT_CLI)

CATALOG = "med834enrl"
SCHEMA = "edi"
EXPECTED_TABLES = [
    "bronze_edi_segments",
    "silver_members",
    "gold_enrollments",
    "quarantine_invalid_members",
]


def split_statements(sql_text: str) -> list[str]:
    """Split a SQL script into individual statements, stripping comments."""
    # Remove line comments
    no_comments = re.sub(r"--[^\n]*", "", sql_text)
    parts = [s.strip() for s in no_comments.split(";")]
    return [p for p in parts if p]


def get_warehouse_id(w: WorkspaceClient, override: str | None) -> str:
    if override:
        return override
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError(
            "No SQL warehouse found. Create one in the Databricks UI "
            "(SQL > SQL Warehouses) or pass --warehouse-id."
        )
    # Prefer a running warehouse, else the first one.
    for wh in warehouses:
        state = getattr(wh.state, "value", str(wh.state))
        if state == "RUNNING":
            print(f"  Using RUNNING warehouse: {wh.name} ({wh.id})")
            return wh.id
    wh = warehouses[0]
    print(f"  Using warehouse: {wh.name} ({wh.id})")
    return wh.id


def run_sql(w: WorkspaceClient, warehouse_id: str, statement: str) -> None:
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    # Poll if still running
    statement_id = resp.statement_id
    state = resp.status.state
    while state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(statement_id)
        state = resp.status.state

    if state != StatementState.SUCCEEDED:
        err = resp.status.error
        msg = err.message if err else "unknown error"
        raise RuntimeError(f"Statement failed: {msg}\n  SQL: {statement[:120]}...")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="dbc-645321b4-3ef9")
    parser.add_argument("--warehouse-id", default=None)
    args = parser.parse_args()

    print(f"Connecting to Databricks (profile={args.profile})...")
    w = WorkspaceClient(profile=args.profile)
    me = w.current_user.me()
    print(f"  Authenticated as: {me.user_name}")

    warehouse_id = get_warehouse_id(w, args.warehouse_id)

    statements = split_statements(SQL_FILE.read_text(encoding="utf-8"))
    print(f"\nExecuting {len(statements)} statements...")
    for i, stmt in enumerate(statements, 1):
        first_line = stmt.splitlines()[0][:70]
        run_sql(w, warehouse_id, stmt)
        print(f"  [{i}/{len(statements)}] OK: {first_line}")

    print("\nVerifying deployment...")
    verify = (
        f"SELECT table_name FROM {CATALOG}.information_schema.tables "
        f"WHERE table_schema = '{SCHEMA}' ORDER BY table_name"
    )
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=verify, wait_timeout="50s"
    )
    found = []
    if resp.result and resp.result.data_array:
        found = [row[0] for row in resp.result.data_array]
    print(f"  Tables in {CATALOG}.{SCHEMA}: {found}")

    missing = [t for t in EXPECTED_TABLES if t not in found]
    if missing:
        print(f"\nWARNING: missing tables: {missing}")
        return 1

    print("\nClean deploy complete. All 4 Delta tables present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
