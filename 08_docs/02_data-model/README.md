# Diagrams

This directory contains all architectural diagrams and database schema visualizations for the MedEnroll834 project.

## Structure

```
docs/diagrams/
├── conceptual/
│   ├── med834enrl-conceptual-model.mmd
│   ├── med834enrl-conceptual-model-formatted.mmd
│   ├── med834enrl-conceptual-model.pdf
│   ├── med834enrl-conceptual-model.svg
│   └── 834-enrollment-transaction.png
├── logical/
│   └── architecture/
│       ├── med834enrl-medellion-architecture.mmd
│       ├── med834enrl-medellion-architecture.svg
│       └── med834enrl-medellion-architecture.png
└── infrastructure/
    ├── application-architecture/
    │   ├── modern-architecture.mmd
    │   ├── modern-architecture.svg
    │   └── modern-architecture.png
    └── database/
        ├── oltp/
        │   ├── med834enrl-oltp-db
        │   ├── med834enrl-oltp.svg
        │   ├── med834enrl-oltp.sql
        │   └── med834enrl-oltp.png
        └── olap/
            ├── med834enrl-olap-db
            ├── med834enrl-olap.svg
            ├── med834enrl-olap.sql
            └── med834enrl-olap.png
```

## Naming Convention

- All filenames use **lowercase kebab-case**
- Timestamps have been removed from filenames
- Source files (`.mmd`, `.sql`) are kept alongside rendered outputs (`.svg`, `.png`, `.pdf`)

## Categories

| Category | Description |
|----------|-------------|
| `conceptual/` | High-level domain model and enrollment transaction flow |
| `logical/architecture/` | Medellion architecture logical design |
| `infrastructure/application-architecture/` | Modern application architecture overview |
| `infrastructure/database/oltp/` | Online Transaction Processing schema |
| `infrastructure/database/olap/` | Online Analytical Processing schema |
