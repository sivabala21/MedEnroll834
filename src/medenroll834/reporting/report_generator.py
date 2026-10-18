"""
src/medenroll834/reporting/report_generator.py
Run predefined analytics SQL against the Databricks GOLD layer and render an
HTML report. Optionally ask Databricks Genie natural-language questions if a
Genie space is configured.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

# Analytics questions answered from the gold enrollment table (flat) and the
# OLAP star schema (FACT_ENROLLMENT joined to its dimensions).
REPORT_QUERIES: List[Tuple[str, str]] = [
    ("Total enrollments", "SELECT COUNT(*) AS total FROM med834enrl.edi.gold_enrollments"),
    ("Members by plan",
     "SELECT plan_id, COUNT(*) AS members FROM med834enrl.edi.gold_enrollments "
     "GROUP BY plan_id ORDER BY members DESC"),
    ("Members by state",
     "SELECT state, COUNT(*) AS members FROM med834enrl.edi.gold_enrollments "
     "WHERE state IS NOT NULL GROUP BY state ORDER BY members DESC LIMIT 15"),
    ("Subscriber vs dependent",
     "SELECT is_subscriber, COUNT(*) AS members FROM med834enrl.edi.gold_enrollments "
     "GROUP BY is_subscriber"),
    ("Average age by plan",
     "SELECT plan_id, ROUND(AVG(age_years),1) AS avg_age FROM med834enrl.edi.gold_enrollments "
     "GROUP BY plan_id ORDER BY avg_age DESC"),
    ("Enrollments by plan (FACT_ENROLLMENT star schema)",
     "SELECT p.PLAN_CODE, COUNT(*) AS enrollments "
     "FROM med834enrl.gold.FACT_ENROLLMENT f "
     "JOIN med834enrl.gold.DIM_PLAN p ON f.PLPL_KEY = p.PLPL_KEY "
     "GROUP BY p.PLAN_CODE ORDER BY enrollments DESC"),
    ("Enrollments by group and plan (FACT_ENROLLMENT star schema)",
     "SELECT gr.GROUP_NUMBER, p.PLAN_CODE, COUNT(*) AS enrollments "
     "FROM med834enrl.gold.FACT_ENROLLMENT f "
     "JOIN med834enrl.gold.DIM_GROUP gr ON f.GRGR_KEY = gr.GRGR_KEY "
     "JOIN med834enrl.gold.DIM_PLAN p ON f.PLPL_KEY = p.PLPL_KEY "
     "GROUP BY gr.GROUP_NUMBER, p.PLAN_CODE ORDER BY enrollments DESC LIMIT 15"),
]


def _client(profile=None):
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def _warehouse_id(w, override=None):
    if override:
        return override
    whs = list(w.warehouses.list())
    if not whs:
        raise RuntimeError("No SQL warehouse available")
    return whs[0].id


def _run_query(w, warehouse_id, sql):
    from databricks.sdk.service.sql import StatementState
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s"
    )
    sid = resp.statement_id
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(sid)
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"Query failed: {err.message if err else 'unknown'}")
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array or []
    return cols, rows


def _table_html(title: str, cols: List[str], rows: List[List]) -> str:
    head = "".join(f"<th>{c}</th>" for c in cols)
    body = "".join(
        "<tr>" + "".join(f"<td>{'' if v is None else v}</td>" for v in row) + "</tr>"
        for row in rows
    )
    return (
        f"<h2>{title}</h2>"
        f"<table border='1' cellpadding='6' cellspacing='0' "
        f"style='border-collapse:collapse;margin-bottom:24px'>"
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
    )


def generate_report(profile=None, warehouse_id=None) -> str:
    """Run all report queries and return a self-contained HTML string."""
    w = _client(profile)
    wid = _warehouse_id(w, warehouse_id)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    sections = []
    for title, sql in REPORT_QUERIES:
        try:
            cols, rows = _run_query(w, wid, sql)
            sections.append(_table_html(title, cols, rows))
        except Exception as exc:  # noqa: BLE001
            sections.append(f"<h2>{title}</h2><p style='color:red'>Error: {exc}</p>")

    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        "<title>MedEnroll834 Enrollment Report</title>"
        "<style>body{font-family:Segoe UI,Arial,sans-serif;margin:40px;color:#222}"
        "h1{color:#0b5}th{background:#0b5;color:#fff}tr:nth-child(even){background:#f3f3f3}"
        "</style></head><body>"
        "<h1>MedEnroll834 - Enrollment Analytics Report</h1>"
        f"<p>Generated: {generated} | Source: med834enrl.edi.gold_enrollments</p>"
        + "".join(sections)
        + "</body></html>"
    )


def ask_genie(space_id: str, question: str, profile=None) -> Dict:
    """Ask a Databricks Genie space a natural-language question.

    Requires a Genie space (space_id) in a workspace with Genie enabled.
    Returns the final message payload. Best-effort: raises if Genie API or
    space is unavailable.
    """
    w = _client(profile)
    conv = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
    out = {"question": question, "attachments": []}
    for att in (conv.attachments or []):
        if att.text:
            out["attachments"].append({"type": "text", "content": att.text.content})
        if att.query:
            out["attachments"].append({"type": "query", "query": att.query.query,
                                        "description": att.query.description})
    return out
