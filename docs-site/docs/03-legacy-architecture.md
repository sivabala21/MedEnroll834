# 03. Legacy Architecture

## Existing platform

The legacy process is a scheduled, file-based enrollment workflow. It moves
data across specialized tools before FACETS receives the final keyword
file — there is no single distributed processing engine or shared data
lake underneath it.

## Processing flow

1. A federal or external source places an 834 file in a monitored folder.
2. An **EDI** job converts the 834 transaction into a proprietary format.
3. An **ECA** job processes each member and assigns a benefit plan.
4. A **.NET** process generates the keyword file.
5. **TTK** and Linux jobs move files and run **Bash** validations.
6. **W2K** loads the keyword file into the **FACETS** core platform.

**Tidal** schedules and coordinates the jobs. **SSIS** and **DataStage**
support loading and transformation activities elsewhere in the estate.

```mermaid
flowchart LR
    SRC["834 File<br/>(federal / external source)"] --> EDI["EDI Job<br/>(proprietary conversion)"]
    EDI --> ECA["ECA Job<br/>(plan assignment)"]
    ECA --> NET[".NET Process<br/>(keyword file generation)"]
    NET --> TTK["TTK + Bash<br/>(file movement & validation)"]
    TTK --> W2K["W2K Load"]
    W2K --> FACETS["FACETS<br/>(core administration platform)"]

    TIDAL["Tidal Scheduler"] -.schedules.-> EDI
    TIDAL -.schedules.-> ECA
    TIDAL -.schedules.-> NET
    TIDAL -.schedules.-> TTK
    TIDAL -.schedules.-> W2K

    SSIS["SSIS / DataStage"] -.supports.-> EDI
    SSIS -.supports.-> ECA
```

## Operational limitations

- **Sequential file handoffs.** Each stage must complete and hand a file to
  the next tool; there is no parallelism within a stage.
- **Fragmented logging.** Each tool maintains separate logs and operational
  controls — there is no common run ID connecting an EDI failure to its
  downstream ECA or FACETS load.
- **Database contention.** Single-machine, serial processing against a
  shared database can create lock-related delays or failures under load.
- **Limited parallelism.** All transformation work happens on one machine,
  so the observed ~1 hour processing-time constraint (see
  [Chapter 2](02-business-problem.md)) cannot be improved without adding
  distributed compute.

- **Proprietary intermediate format.** The EDI→proprietary conversion
  complicates reuse: the data cannot easily be queried, re-processed, or fed
  into an analytics stack without a custom reader.
- **Operational-only data shape.** The data produced is optimized purely for
  the FACETS keyword-file load, not for broad analytics, historical
  reporting, or ad hoc BI/AI querying.

## Modernization boundary

**FACETS remains the downstream system of record** in this prototype's
conceptual framing. The modernization effort documented in this
documentation focuses on **ingestion, transformation, validation, and data
delivery** —
replacing the EDI → ECA → .NET → TTK → Bash → W2K chain with a distributed
PySpark/Delta/Airflow pipeline (see [Chapter 4](04-modern-architecture.md))
while keeping the downstream operational contract (an enrollment record
reaching a core administration system) conceptually the same.
