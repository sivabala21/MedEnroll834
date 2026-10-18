# Project Structure - Enterprise Standard

## Directory Numbering Convention

All directories are prefixed with numbers (01_, 02_, etc.) following enterprise delivery standards.

## Root Structure

```
MedEnroll834/
├── src/                       # Source code (not numbered — Python import paths
│                               #   cannot start with a digit, e.g. `01_src`)
├── 02_tests/                  # Test suite
├── 03_config/                 # Configuration
├── 04_infrastructure/         # IaC & SQL
├── 05_orchestration/          # Airflow workflows
├── 06_deploy/                 # Deployment configs
├── 07_scripts/                # Utility scripts
├── 08_docs/                   # Documentation
├── 10_data/                   # Data storage
├── 11_build/                  # Build artifacts
└── 12_tools/                  # Dev tools
```

## Benefits

1. **Clarity**: Numbers indicate order and priority
2. **Sorting**: Logical directory ordering
3. **Standards**: Enterprise best practices
4. **Professional**: Industry-standard organization

See full structure details in documentation.

---
Last Updated: 2026-10-04

