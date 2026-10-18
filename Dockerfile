# Multi-stage Dockerfile for MedEnroll834 Production

# Stage 1: Base image with common dependencies
FROM python:3.10-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Stage 2: Dependencies
FROM base as dependencies

# Copy requirements
COPY requirements.txt pyproject.toml ./

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Stage 3: Application
FROM dependencies as application

# Copy application code
COPY src/ ./src/
COPY 03_config/ ./config/
COPY README.md LICENSE ./

# Install the package
RUN pip install -e .

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Expose ports (if needed)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command
CMD ["python", "-m", "medenroll834.cli"]

# Stage 4: Development (optional)
FROM application as development

USER root

# Install development dependencies
RUN pip install pytest pytest-cov black ruff mypy

USER appuser

CMD ["/bin/bash"]
