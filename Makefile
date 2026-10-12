.PHONY: help install test lint format clean docker-up docker-down setup-db setup-databricks

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt
	pip install -e .

test: ## Run tests
	pytest 02_tests/ -v --cov=src --cov-report=term-missing

lint: ## Run linters
	ruff check src/ 02_tests/
	mypy src/

format: ## Format code
	black src/ 02_tests/
	ruff check --fix src/ 02_tests/

clean: ## Clean artifacts
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
	find . -type f -name '*.pyo' -delete
	find . -type d -name '*.egg-info' -exec rm -rf {} +
	find . -type d -name '.pytest_cache' -exec rm -rf {} +
	find . -type d -name '.mypy_cache' -exec rm -rf {} +

docker-up: ## Start Docker Compose
	docker-compose up -d

docker-down: ## Stop Docker Compose
	docker-compose down

setup-db: ## Initialize PostgreSQL schema
	psql -U postgres -f 04_infrastructure/02_sql/02_oltp/01_schemas/01_create_core_schema.sql
	psql -U postgres -f 04_infrastructure/02_sql/02_oltp/01_schemas/02_create_enrl_schema.sql
	psql -U postgres -f 04_infrastructure/02_sql/02_oltp/01_schemas/03_create_util_schema.sql
	psql -U postgres -f 04_infrastructure/02_sql/02_oltp/02_tables/*.sql
	psql -U postgres -f 04_infrastructure/02_sql/02_oltp/03_indexes/INDEXES.sql
	psql -U postgres -f 04_infrastructure/02_sql/02_oltp/04_constraints/CONSTRAINTS.sql

setup-databricks: ## Initialize Databricks schemas and Delta tables
	databricks workspace import 04_infrastructure/02_sql/01_delta/*.sql /Shared/med834enrl/sql/delta/
	databricks sql warehouses create --name med834enrl-wh --cluster-size 2X-Small --min-num-clusters 1 --max-num-clusters 5 --auto-stop-mins 10 --spot-instance-policy COST_OPTIMIZED
