# Quick Start Guide

## Installation

```bash
pip install -e .
```

## Run Tests

```bash
make test
```

## Docker

```bash
# Start all services
docker-compose up -d

# Stop services
docker-compose down
```

## Access Services

- Airflow: http://localhost:8080
- MinIO: http://localhost:9001
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

## Project Structure

```
01_src/          Source code
02_tests/        Test suite
03_config/       Configuration
04_infrastructure/ IaC & SQL
05_orchestration/ Airflow
06_deploy/       Deployment
07_scripts/      Utilities
08_docs/         Documentation
09_monitoring/   Observability
10_data/         Data storage
```

## Common Commands

```bash
make install     # Install dependencies
make test        # Run tests
make lint        # Run linters
make format      # Format code
make clean       # Clean artifacts
make docker-up   # Start Docker
make docker-down # Stop Docker
```

## Documentation

- README.md - Project overview
- PROJECT_STRUCTURE.md - Structure details
- CONTRIBUTING.md - How to contribute
- 08_docs/ - Full documentation

## Support

See CONTRIBUTING.md for guidelines.
