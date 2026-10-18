# 37. Future Improvements

A concrete, prioritized roadmap grounded in gaps and TODOs actually
identified while building this documentation — not a generic wishlist.

## Testing


- **Extend coverage to `oltp_loader.py` and `report_generator.py`** — both
  currently require a live Databricks workspace and PostgreSQL instance to
  exercise (see [Chapter 30](30-testing.md)). Mocking
  `databricks.sdk.WorkspaceClient` and `psycopg2.connect` would let these
  modules run in CI without live infrastructure.
- **Add a true end-to-end Airflow task-execution test** — current DAG
  validation (`validate_dag.py`) only checks import/parse correctness, not
  that tasks actually execute correctly against mocked dependencies.
- **Add `--messy` synthetic data test cases** so the quarantine path
  ([Chapter 16](16-data-quality-reject-handling.md)) has automated test
  coverage, not just manual verification.

## Performance & benchmarking

- **Implement the benchmark framework proposed in
  [Chapter 26](26-benchmark-framework.md)** — scale synthetic data
  generation across a range of file/record counts and measure actual
  wall-clock throughput, to get a real number against the legacy system's
  documented ~1 hour per batch constraint (see
  [Chapter 34](34-legacy-vs-modern-benchmark.md)).

- **Exploit Delta Change Data Feed for incremental OLTP loads** — `gold`
  is already written with `delta.enableChangeDataFeed: "true"`, but
  `load_gold_to_oltp()` still does a full `SELECT *` against Gold on every
  run. Switching to `readChangeData`/`startingVersion` would make repeated
  OLTP loads proportional to what changed, not the full table size.

## Data model

- **Make `core_meme_member` / `core_meen_enrollment` upsertable** — these
  are currently plain inserts (see [Chapter 17](17-postgresql-bulk-load-upsert.md)),
  unlike the natural-key-upserted group/plan/subscriber tables. A natural
  key (e.g. `mesb_id + rltn_cd + eff_dt`) would let member/enrollment loads
  be fully idempotent rather than relying on upstream Gold deduplication
  alone.
- **Add SCD Type 2 history tracking to the OLTP schema**, mirroring the
  `CURRENT_FLAG` pattern already used in the OLAP dimensions
  ([Chapter 9](09-database-schema.md)), so an OLTP member's historical
  plan changes are queryable, not just the current state.

## Observability

- **Add a `09_monitoring/` metrics/alerting stack** (e.g. Prometheus +
  Grafana, or a lightweight Airflow-native alerting integration) — none
  currently exists (see [Chapter 24](24-observability.md)).
- **Add structured JSON logging** (vs. the current
  human-readable-formatted log lines) so logs can be ingested by a log
  aggregation platform without custom parsing.
- **Add access/audit logging** for PHI/PII-adjacent operations, per the
  compliance posture discussed in [Chapter 29](29-security.md) — not yet
  implemented since this POC processes only synthetic data.

## Architecture

- **Complete the AWS S3 migration** described in
  [Chapter 7](07-aws-s3-setup.md) and [Chapter 33](33-end-to-end-execution.md)
  — swap `upload_to_blob`/`archive_blob` DAG tasks for the already-built
  `src/medenroll834/s3/s3_utils.py` equivalents, eliminating the POC's
  blob-storage-to-UC-Volume staging hop entirely (a Databricks job with an
  instance profile can read S3 directly).
- **Generalize the parser** to other X12 transaction types (837 claims,
  270/271 eligibility) to validate the "reusable platform" claim made in
  [Chapter 1](01-project-overview.md) beyond the 834 module alone.
- **CD pipeline's `deploy` job** is currently a documented placeholder
  (see [Chapter 31](31-ci-cd.md)) — wiring an actual Kubernetes/Helm (or
  Databricks Asset Bundle) deployment target would complete the CI/CD
  story end to end.

## Documentation

- **Add a benchmark results page** once [Chapter 26](26-benchmark-framework.md)'s
  framework is implemented, with real throughput numbers and charts.
- **Keep this documentation in sync with code changes** going forward — treat
  `docs-site/docs/*.md` updates as a required part of any PR that changes
  the behavior a chapter documents (this is the core discipline of
  "docs-as-code").
