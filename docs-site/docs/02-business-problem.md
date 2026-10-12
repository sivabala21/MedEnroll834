# 02. Business Problem

## Business drivers

Healthcare enrollment processing relies on timely, accurate HIPAA 834
transactions. The current (legacy) workflow uses a chain of specialized
tools and scheduled jobs rather than a unified, distributed pipeline.

An inbound 834 file arrives in a designated folder. An EDI process converts
the transaction into a proprietary file. An ECA process evaluates each
member and assigns a plan. The process generates a keyword file for the
FACETS core administration platform. FACETS then loads the enrollment
result.

The legacy implementation chains together **Tidal** scheduling, **SSIS**,
**DataStage**, **.NET**, **TTK** file movement, **Bash** validations, and
**W2K** loading. Each handoff adds an operational dependency and a
potential failure point — see [Chapter 3](03-legacy-architecture.md) for the
full flow diagram.

## Current constraint (observed)

An observed workload of approximately **300,000 records takes nearly one
hour** end to end. Database locks and single-machine processing limit
throughput. Investigating a failure across six-plus disparate tools can
delay recovery significantly, since each tool keeps its own logs and none
share a common run/lineage identifier.

## The three-V challenge

### Velocity

Enrollment changes require prompt processing — a member's coverage often
needs to be active before a claim is submitted. Long batch windows delay
downstream availability and operational response.

### Volume

Large 834 files create pressure on serial processing and a shared
database. The modernized solution must scale throughput **without**
extending the batch window, by distributing work across a Spark cluster
instead of a single machine.

### Variety

834 transactions contain varying member, employer, subscriber, group, and
plan information — different relationship codes (self/spouse/child/domestic
partner), different plan products (`GOLD`, `SILVER`, `BRONZE`, `PLATINUM`,
`HDHP`, `EPO`, `PPO`, `HMO` in this prototype's validation rules — see
`DataFrameValidator.VALID_PLANS` in
[`src/medenroll834/quality/validator.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/quality/validator.py)),
and segments that may be mixed `~`-terminated or newline-terminated within
the same file (`split_segments()` in
[`src/medenroll834/ingestion/bronze.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/ingestion/bronze.py)
normalizes both). The pipeline must validate these differences without
losing source traceability back to the raw segment.

## Business objective

The prototype shows how a modern, distributed data platform can address all
three constraints simultaneously: it improves throughput (parallel Spark
execution vs. serial single-machine jobs), observability (structured logs +
run summaries + Delta table lineage instead of six separate tool logs),
recoverability (idempotent re-runs, quarantine instead of hard failure), and
analytical access (an OLAP star schema and Databricks SQL/Genie that the
legacy FACETS-only flow does not provide). It is also a practical
modernization blueprint to present to stakeholders — see
[Chapter 36](36-interview-explanation.md) for how to talk through it in an
interview setting.
