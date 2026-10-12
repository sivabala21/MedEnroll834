"""
src/utils/logging.py
Standardized logging for Databricks pipeline.
"""
import logging
import sys
from datetime import datetime, timezone


def setup_logging(name: str = "med834enrl") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def log_pipeline_start(logger: logging.Logger, layer: str, details: str) -> None:
    logger.info("START %s | %s | %s", layer, details, datetime.now(timezone.utc).isoformat())


def log_pipeline_end(logger: logging.Logger, layer: str, details: str) -> None:
    logger.info("END %s | %s | %s", layer, details, datetime.now(timezone.utc).isoformat())
