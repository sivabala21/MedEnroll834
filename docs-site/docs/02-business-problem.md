# 02. Business Problem

## Business drivers

Healthcare enrollment processing relies on timely, accurate HIPAA 834
transactions. The current (legacy) workflow uses a chain of specialized
tools and scheduled jobs rather than a unified, distributed pipeline.

An inbound 834 file arrives in a designated folder. An **EDI** job
converts the transaction into a proprietary format. An **ECA** job
evaluates each member and assigns a plan. A **Legacy Programs** process
generates a keyword file for the downstream **CORE System**. **CORE
Programs** then loads the enrollment result.

The legacy implementation chains together **Tidal** scheduling, **SSIS**,
**DataStage**, **Legacy Programs**, **SCP** file movement, **Bash**
validations, and the **CORE Programs** load. Each handoff adds an
operational dependency and a potential failure point — see
[Chapter 3](03-legacy-architecture.md) for the full flow diagram.


## Current constraint (observed)

Enrollment processing jobs in the legacy system are observed to take
approximately **one hour** end to end for a full daily batch file. Database
locks and single-machine processing limit throughput. Investigating a
failure across six-plus disparate tools can delay recovery significantly,
since each tool keeps its own logs and none share a common run/lineage
identifier.

## The three-V challenge

### Volume — an industry-wide growth trend, not a one-time spike

Total U.S. health insurance enrollment grew significantly through the
COVID-19 pandemic years, reaching roughly **297 million covered
individuals** by late 2025, before entering a period of policy-driven
contraction heading into 2026 — largely due to the "Great Unwinding" of
continuous Medicaid eligibility and the expiration of pandemic-era ACA
premium tax credits. Even accounting for that contraction, the underlying
structural trend for managed care organizations has been sustained,
multi-decade membership growth punctuated by sharp step-changes (pandemic
enrollment surges, Medicaid expansion, new ACA Marketplace lines of
business). A fixed-capacity, single-machine legacy pipeline has no way to
absorb that kind of growth or a sudden policy-driven enrollment swing
without a hard redesign — only horizontally scalable, distributed
processing can expand capacity on demand rather than hitting a hard
ceiling.

### Velocity

Enrollment changes require prompt processing — a member's coverage often
needs to be active before a claim is submitted. Long batch windows delay
downstream availability and operational response.

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
legacy CORE System-only flow does not provide). It is also a practical
modernization blueprint for presenting an architecture decision to
stakeholders.

