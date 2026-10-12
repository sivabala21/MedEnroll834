# 27. Databricks SQL Dashboard

> Source: [`src/medenroll834/reporting/report_generator.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/reporting/report_generator.py)

Rather than a separate BI tool, MedEnroll834's analytics layer runs a fixed
set of SQL queries against Databricks SQL warehouses and renders them as a
single self-contained HTML report — a deliberately simple "traditional
analytics" consumption path that pairs with Genie's "AI analytics" path
(see [Chapter 4](04-modern-architecture.md#4-analytical-consumption-two-modes)).

## The seven report queries

```python
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
```

Notice the first five queries hit the **flat** `gold_enrollments` table
directly, while the last two join through the **OLAP star schema**
(`FACT_ENROLLMENT` + `DIM_PLAN`/`DIM_GROUP`) — demonstrating both
consumption shapes from the same analytics entry point.

## Executing against a SQL warehouse

```python
def _run_query(w, warehouse_id, sql):
    from databricks.sdk.service.sql import StatementState
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s"
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Query failed: {resp.status.error.message}")
    return [c.name for c in resp.manifest.schema.columns], resp.result.data_array or []
```

Uses the Databricks SQL Statement Execution API — the serverless warehouse
spins up on demand if not already running, with a 2-second poll loop while
`PENDING`/`RUNNING`.

## Rendering self-contained HTML

```python
def generate_report(profile=None, warehouse_id=None) -> str:
    w = _client(profile)
    wid = _warehouse_id(w, warehouse_id)
    sections = []
    for title, sql in REPORT_QUERIES:
        try:
            cols, rows = _run_query(w, wid, sql)
            sections.append(_table_html(title, cols, rows))
        except Exception as exc:
            sections.append(f"<h2>{title}</h2><p style='color:red'>Error: {exc}</p>")
    return (
        "<!DOCTYPE html><html>...<title>MedEnroll834 Enrollment Report</title>..."
        f"<p>Generated: {generated} | Source: med834enrl.edi.gold_enrollments</p>"
        + "".join(sections) + "</body></html>"
    )
```

The report is a single HTML string with inline CSS — no external
dependencies, no JavaScript — so it can be written straight to blob storage
(`audit/reports/enrollment_report_<timestamp>.html`) and opened in any
browser with no server required. Each query's failure is isolated (see
[Chapter 23](23-error-handling-retry.md)), so one broken query renders a
red error message inline rather than failing the whole report.

## Invoked from the Airflow DAG

```python
@task
def generate_report(_summary: dict) -> str:
    from src.medenroll834.reporting.report_generator import generate_report as gen
    from src.medenroll834.storage import blob_storage
    html = gen(warehouse_id=_cfg("DATABRICKS_WAREHOUSE_ID"))
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    uri = blob_storage.put_text(f"audit/reports/enrollment_report_{stamp}.html",
                                html, content_type="text/html")
    return uri
```

Each run produces a uniquely timestamped report file — historical reports
accumulate under `audit/reports/` rather than overwriting each other.
