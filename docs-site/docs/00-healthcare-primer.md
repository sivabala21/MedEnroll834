# 00. Healthcare Enrollment Primer

This chapter is for readers new to U.S. healthcare administration. It
defines the terms, programs, and regulatory concepts used throughout this
documentation — read it first if phrases like "834," "Medicaid managed
care," or "HIPAA X12" aren't already familiar.

## What is an "834"?

**834** is the name of a specific electronic document format — formally,
the **ANSI ASC X12 834 Benefit Enrollment and Maintenance transaction**.
Health insurers, employers, and government agencies use it to tell each
other "this person is enrolling in (or changing, or ending) this health
plan." It is one of dozens of numbered X12 transaction types used in U.S.
healthcare administration; others include **837** (claims), **835**
(payment/remittance), and **270/271** (eligibility inquiry/response). This
project processes only 834 files.

An 834 file is not human-readable prose — it is a structured text format
with fixed segment codes (`ISA`, `GS`, `ST`, `HL`, `NM1`, `DTP`, `HD`, ...),
each carrying specific pieces of information (a name, a date, a plan code)
in a fixed position. [Chapter 12](12-834-parser.md) covers exactly how
this project parses that structure.

## Why "HIPAA X12"?

**HIPAA** (the Health Insurance Portability and Accountability Act of
1996) is the U.S. federal law best known for patient-privacy rules, but it
also *mandates* that health plans and providers use standardized
electronic transaction formats — specifically, the **X12** standard
published by ASC X12 — for exchanges like enrollment, claims, and
eligibility checks. "HIPAA X12 834" is shorthand for "the federally
mandated standard enrollment transaction format." Using a non-standard,
proprietary format for these exchanges is not legally permitted for
covered entities, which is part of why every health plan ingests the same
834 structure regardless of which vendor built its systems.

## Who is enrolling in what? The core entities

| Term | Plain-language meaning |
|---|---|
| **Member** | A specific person covered by a health plan — could be the policyholder or one of their dependents. |
| **Subscriber** | The member who is the primary policyholder — the person whose employment, Medicaid eligibility, or marketplace application the coverage is based on. |
| **Dependent** | A member covered *through* a subscriber's policy — a spouse, child, or domestic partner, per the 834's relationship code. |
| **Employer group** | The organization (an employer, or a state Medicaid program) whose contract with the health plan covers a population of subscribers. |
| **Benefit plan** | The specific product a member is enrolled in (e.g. an HMO, a PPO, a specific metal tier) — determines what's covered and how. |
| **Enrollment** | The event of a member being added, changed, or termed on a benefit plan — the "transaction" an 834 file communicates. |

These six concepts map directly onto this project's data model — see
[Chapter 1](01-project-overview.md)'s conceptual model and
[Chapter 9](09-database-schema.md)'s schema.

## Medicaid, Medicare, and the marketplace — who pays, and who's eligible

| Program | Who it's for | Who runs it |
|---|---|---|
| **Medicaid** | Low-income individuals and families; eligibility rules vary by state | Jointly funded by federal and state governments; administered by each state, often through contracted **managed care organizations** |
| **CHIP** (Children's Health Insurance Program) | Children in families who earn too much for Medicaid but can't afford private coverage | Same federal/state structure as Medicaid, often administered alongside it |
| **Medicare** | People 65+, or younger people with certain disabilities | Fully federal (via CMS — the Centers for Medicare & Medicaid Services) |
| **Medicare dual-eligible** | People who qualify for *both* Medicare and Medicaid | Coordinated between federal Medicare and state Medicaid benefits |
| **ACA Marketplace** (also "Affordable Care Act Marketplace" or "Exchange") | Individuals/families buying private coverage directly, often with income-based federal subsidies | Federal framework (HealthCare.gov or a state-run exchange); plans sold by private insurers |
| **Commercial/employer-sponsored** | Employees (and dependents) covered through an employer's group health plan | Fully private, negotiated between employer and insurer |

A **managed care organization (MCO)** is a private health plan that a
state *contracts with* to administer Medicaid (and sometimes Medicare
dual-eligible and Marketplace) benefits on the state's behalf — this is
the kind of organization whose enrollment processing this project models
(see [Chapter 2](02-business-problem.md)).

## Why enrollment data is time-sensitive

A member's coverage record has to be accurate and *current* before a
pharmacy, doctor's office, or hospital can rely on it — a claim submitted
for a service the plan doesn't yet show the member as covered for will be
rejected or delayed, even if the member is, per the state or employer,
actually enrolled. This is the real-world reason enrollment file
processing speed ("Velocity" in the Three-V framing used throughout this
documentation) matters operationally, not just as an abstract performance
metric.

## Relationship codes used in this project

The 834's `INS` segment carries a numeric relationship code identifying
how a dependent relates to their subscriber. This prototype's generator
and validator use four of the most common codes:

| Code | Relationship |
|---|---|
| `18` | Self (the subscriber) |
| `01` | Spouse |
| `03` | Child |
| `19` | Domestic partner |

## Terms you'll see throughout this documentation

| Term | Meaning |
|---|---|
| **EDI** (Electronic Data Interchange) | The general practice of exchanging structured business documents (like 834s) electronically between organizations, instead of paper/fax. |
| **Plan code / plan tier** | A short code identifying a specific benefit plan product (this project's synthetic data uses tiers like `GOLD`, `SILVER`, `BRONZE`, `PLATINUM`, `HDHP`, `EPO`, `PPO`, `HMO`). |
| **Coverage start/end date** | The date range a member's enrollment in a plan is effective — an open-ended (still-active) enrollment has no end date yet. |
| **Termination ("term")** | Ending a member's coverage on a plan, as opposed to adding or changing it. |
| **Group number** | An identifier for the employer group or state contract a subscriber's coverage is issued under. |
| **PHI / PII** | Protected Health Information / Personally Identifiable Information — the categories of sensitive data HIPAA and general privacy law protect. This project uses **zero** real PHI/PII; all data is synthetically generated (see [Chapter 10](10-synthetic-data-generation.md)). |

With these fundamentals in place, [Chapter 1](01-project-overview.md)
picks up with how this project's own pipeline is scoped and structured.

