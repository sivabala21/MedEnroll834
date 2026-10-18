# 02. Business Problem

!!! warning "Confidential — internal use only"
    This document references the client this project's requirements are
    modeled on and the real legacy toolchain names. Do not distribute
    this file externally.

## Client context: AmeriHealth Caritas

AmeriHealth Caritas is a managed care organization with more than 40 years
of experience serving **Medicaid and CHIP** populations, plus Medicare
dual-eligible and individual/family marketplace plans, across multiple
state health plans. Member enrollment — adding a subscriber, adding or
removing a dependent, changing a benefit plan, terming coverage — is the
transaction type this project models (HIPAA X12 834), because it is the
highest-volume, most time-sensitive data feed a managed care organization
like this ingests: a member's coverage status has to be correct and current
before a claim, a prior authorization, or a pharmacy fill can be processed
against it.

## Business drivers

Healthcare enrollment processing relies on timely, accurate HIPAA 834
transactions. The current (legacy) workflow uses a chain of specialized
tools and scheduled jobs rather than a unified, distributed pipeline.

An inbound 834 file arrives in a designated folder. An **EDI** process
converts the transaction into a proprietary file. An **ECA** process
evaluates each member and assigns a plan. The process generates a keyword
file for the **FACETS** core administration platform. FACETS then loads
the enrollment result.

The legacy implementation chains together **Tidal** scheduling, **SSIS**,
**DataStage**, **.NET**, **TTK** file movement, **Bash** validations, and
**W2K** loading. Each handoff adds an operational dependency and a
potential failure point — see [Chapter 3](03-legacy-architecture.md) for the
full flow diagram.

## Current constraint (observed)

Enrollment processing jobs in the legacy system are observed to take
approximately **one hour** end to end for a full daily batch file. Database
locks and single-machine processing limit throughput. Investigating a
failure across six-plus disparate tools can delay recovery significantly,
since each tool keeps its own logs and none share a common run/lineage
identifier.


## The three-V challenge

### Volume — four decades of sustained, compounding growth

AmeriHealth Caritas's own enrollment history illustrates why a fixed-
capacity, single-machine legacy pipeline becomes an increasingly acute
bottleneck over time, not a one-time scaling problem to solve once:

| Era | Milestone | Approx. membership |
|---|---|---|
| 1983 | Founded as Mercy Health Plan, a voluntary Medicaid managed care pilot inside a West Philadelphia hospital | ~300 |
| 1988 | Expanded past Philadelphia into 15 central/northeastern Pennsylvania counties; first Medical Assistance plan to earn full NCQA accreditation | 10,000+ |
| Early 2000s | Solidified state-wide Pennsylvania leadership through the HealthChoices program | ~1 million |
| 2013 | Rebranded AmeriHealth Mercy → AmeriHealth Caritas; acquired D.C. Chartered Health Plan, becoming D.C.'s largest Medicaid managed care provider | ~4 million |
| 2023 | Launched AmeriHealth Caritas Next, expanding into ACA Marketplace plans in Delaware, Florida, South Carolina, and North Carolina | — |
| 2026 | Continued expansion of ACA Marketplace and Medicare D-SNP plans into new territories (Baton Rouge, Metro Detroit); footprint now spans 13 states + D.C. | ~5.3 million |

A membership base that grew roughly **17,000x** over four decades — and
kept adding new lines of business (Medicaid, Medicare D-SNP, ACA
Marketplace) along the way — cannot be served indefinitely by a pipeline
whose only scaling lever is a bigger single machine. Each new state, each
new line of business, and each acquisition (like the D.C. Chartered Health
Plan integration) adds another enrollment data stream the legacy chain has
to absorb without redesigning its fundamentally serial processing model.

### Velocity

A member's Medicaid/CHIP coverage often needs to be active **before** a
claim, prior authorization, or pharmacy fill is submitted against it — a
delayed 834 batch window can mean a pharmacy point-of-sale rejection for a
member who, per the state, is already enrolled. Long batch windows delay
downstream availability and operational response across every one of the
plan's lines of business (Medicaid managed care, Medicare dual-eligible,
individual/family marketplace).

### Variety

834 transactions contain varying member, employer, subscriber, group, and
plan information — different relationship codes (self/spouse/child/domestic
partner), different plan products across lines of business (Medicaid
managed care, Medicare dual-eligible, LTSS, individual/family marketplace —
`GOLD`, `SILVER`, `BRONZE`, `PLATINUM`, `HDHP`, `EPO`, `PPO`, `HMO` in this
prototype's validation rules — see `DataFrameValidator.VALID_PLANS` in
[`src/medenroll834/quality/validator.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/quality/validator.py)),
and segments that may be mixed `~`-terminated or newline-terminated within
the same file (`split_segments()` in
[`src/medenroll834/ingestion/bronze.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/ingestion/bronze.py)
normalizes both). Different state Medicaid agencies also submit files with
state-specific field-population quirks, so the pipeline must validate these
differences without losing source traceability back to the raw segment.

## Business objective

The prototype shows how a modern, distributed data platform can address all
three constraints simultaneously: it improves throughput (parallel Spark
execution vs. serial single-machine jobs), observability (structured logs +
run summaries + Delta table lineage instead of six separate tool logs),
recoverability (idempotent re-runs, quarantine instead of hard failure), and
analytical access (an OLAP star schema and Databricks SQL/Genie that the
legacy FACETS-only flow does not provide). It is also a practical
modernization blueprint for presenting an architecture decision to
stakeholders.


