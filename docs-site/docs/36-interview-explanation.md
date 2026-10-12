# 36. Interview Explanation

A structured way to talk through this project in a technical interview,
organized as a narrative arc with concrete, code-grounded details to back
up each claim — not generic buzzwords.

## The 60-second version

> "I built a modernization prototype for healthcare 834 enrollment
> processing. The legacy system chains six different tools — EDI, ECA,
> .NET, TTK, Bash, W2K — serially on one machine, and a 300K-record batch
> takes about an hour. I replaced that with a distributed PySpark/Delta
> medallion pipeline — Bronze, Silver, Gold — orchestrated by Airflow, that
> writes to both a PostgreSQL OLTP store for operational use and a Delta
> Lake OLAP star schema for analytics, with two separate consumption
> paths: a traditional HTML report and a Databricks Genie natural-language
> interface. Everything's idempotent, data-quality failures are quarantined
> with explicit reasons instead of hard-failing, and the whole stack is
> containerized and CI/CD'd."

## If asked "walk me through the architecture"

Use the [Modern Architecture diagram](04-modern-architecture.md) narrative:
Source (834 file) → object storage landing → Databricks/PySpark → Bronze
→ Silver → Gold, then **two** downstream paths fan out from Gold: an OLTP
load into PostgreSQL for operational apps, and an OLAP star-schema build
for BI/Genie. Emphasize that Airflow sits *above* this as orchestration,
not as a data layer itself — a distinction many candidates get wrong.

## If asked "what was the hardest technical problem?"

The HL hierarchy resolution bug in Silver (see
[Chapter 14](14-silver-layer.md) and
[Chapter 25](25-spark-performance-optimization.md)): X12 `HL` segment ids
are only unique **within a transaction set**, not within a whole file, so
a naive join to resolve a dependent's subscriber matched every subscriber
in the file and fanned out rows ~100x. The fix was two-fold: a
transaction-scoped key derived via a cumulative window function
(`last(..., ignorenulls=True)` over `ST` boundaries), and replacing a
self-join with a broadcast map built from a small `collect()` of just the
distinct subscribers — turning an O(n²)-shaped operation into an O(1)
per-row lookup. This is a good story because it shows debugging a subtle
correctness bug that silently *multiplied* data rather than crashing
outright — the kind of bug that's dangerous precisely because it doesn't
raise an exception.

## If asked "how do you handle data quality?"

Point to [Chapter 16](16-data-quality-reject-handling.md): seven
Spark-SQL validation rules producing a `validation_errors` array (not a
single boolean), so a record can carry multiple simultaneous failure
reasons. Invalid rows are routed to a Delta quarantine table (`append`
mode, with a `quarantine_id` and timestamp) rather than being dropped or
crashing the pipeline — a record that fails validation is expected,
handled output, not a pipeline failure (see
[Chapter 23](23-error-handling-retry.md) for that distinction).

## If asked "how do you ensure idempotency?"

Three concrete mechanisms (see [Chapter 22](22-idempotency-incremental-processing.md)):
Delta `overwrite` writes derived entirely from current input (no merge
drift), PostgreSQL natural-key `ON CONFLICT ... DO UPDATE` upserts for
reference entities, and deterministic hashed surrogate keys
(`xxhash64(file_name)`) for the transaction-source dimension — so
re-running the pipeline against the same input never duplicates data.

## If asked "why two separate databases (OLTP and OLAP)?"

Separating PostgreSQL (operational reads/writes) from the Delta star
schema (analytical queries) means a heavy BI/Genie query load never
contends with operational application traffic, and each store is modeled
for its actual access pattern — normalized/FK-enforced for OLTP,
dimensional/denormalized for OLAP. This is explicitly called out as a
deliberate design decision in [Chapter 4](04-modern-architecture.md)'s
"Alignment Summary."

## If asked "what would you do differently / what's next?"

Be honest and specific — see [Chapter 38](38-future-improvements.md):
exploit the already-enabled Delta Change Data Feed for true incremental
OLTP loads instead of full re-reads, build the benchmark framework
proposed in [Chapter 26](26-benchmark-framework.md) to get a real
measured throughput number against the legacy system's 300K/hour
baseline, extend test coverage to the OLTP loader and reporting modules
(currently untested because they require live Databricks/Postgres), and
fix the `ci.yml` / `02_tests/` path mismatch flagged in
[Chapter 31](31-ci-cd.md).

## Questions to anticipate and have answers ready for

- "Is this using real PHI data?" → No, synthetic only (Chapter 10) — be
  ready to explain exactly how (`generator/synthetic_834_generator.py`).
- "What happens if Databricks is down mid-run?" → Airflow's `retries=1`
  catches transient failures; a hard Databricks job failure raises and the
  DAG run fails cleanly, with `archive_blob` still running via
  `trigger_rule="all_done"` so files aren't stuck.
- "How would this scale to 837 claims or 270/271 eligibility?" → The
  medallion pattern (Bronze/Silver/Gold), OLTP/OLAP separation, and
  orchestration shape are transaction-type-agnostic; only the parser and
  schema would change per [Chapter 1](01-project-overview.md)'s framing.
