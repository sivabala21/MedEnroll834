"""
src/utils/logging.py
Standardized, configurable logging for the MedEnroll834 pipeline.

Features:
* Env-driven log level (MED834_LOG_LEVEL) and optional rotating file handler
  (MED834_LOG_FILE) in addition to the always-on stdout handler, so the same
  setup_logging() call works unchanged locally, in pytest, inside an Airflow
  task (stdout is captured into the task log automatically) and in a
  Databricks job.
* Pipeline run-context correlation: set_run_context()/run_context() bind a
  run_id (and optionally a file_name) to a contextvar that every log record
  picks up automatically via a logging.Filter, so call sites do not need to
  thread run_id through every function signature just to log it.
"""
import logging
import os
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterator, Optional

# Default value for context fields when no run is bound, so the format string
# below can reference %(run_id)s/%(file_name)s unconditionally.
_NO_CONTEXT = "-"

_run_id_var: ContextVar[str] = ContextVar("med834_run_id", default=_NO_CONTEXT)
_file_name_var: ContextVar[str] = ContextVar("med834_file_name", default=_NO_CONTEXT)

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s | run=%(run_id)s file=%(file_name)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class _ContextFilter(logging.Filter):
    """Injects the currently bound run_id/file_name into every LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _run_id_var.get()
        record.file_name = _file_name_var.get()
        return True


_CONTEXT_FILTER = _ContextFilter()



def set_run_context(run_id: Optional[str] = None, file_name: Optional[str] = None) -> dict:
    """Bind run_id/file_name for subsequent log records; returns reset tokens."""
    tokens: dict = {}
    if run_id is not None:
        tokens["run_id"] = _run_id_var.set(run_id)
    if file_name is not None:
        tokens["file_name"] = _file_name_var.set(file_name)
    return tokens


def reset_run_context(tokens: dict) -> None:
    """Undo a prior set_run_context() call using the tokens it returned."""
    if "run_id" in tokens:
        _run_id_var.reset(tokens["run_id"])
    if "file_name" in tokens:
        _file_name_var.reset(tokens["file_name"])


@contextmanager
def run_context(run_id: Optional[str] = None, file_name: Optional[str] = None) -> Iterator[None]:
    """Context manager binding run_id/file_name for every log record emitted
    within the block, across any layer/module that calls setup_logging().

    Usage:
        with run_context(run_id="abc123"):
            bronze.ingest(...)   # logs now show run=abc123
    """
    tokens = set_run_context(run_id=run_id, file_name=file_name)
    try:
        yield
    finally:
        reset_run_context(tokens)


def current_run_id() -> str:
    """Return the run_id bound by the innermost active run_context(), or '-'."""
    return _run_id_var.get()


def _resolve_level() -> int:
    level_name = os.environ.get("MED834_LOG_LEVEL", "INFO").strip().upper()
    resolved = logging.getLevelName(level_name)
    return resolved if isinstance(resolved, int) else logging.INFO


def setup_logging(name: str = "med834enrl") -> logging.Logger:
    """Return a configured logger, console-only by default.

    Env vars:
      MED834_LOG_LEVEL        - e.g. DEBUG/INFO/WARNING (default INFO)
      MED834_LOG_FILE         - optional path; when set, a RotatingFileHandler
                                 is added alongside the stdout handler
      MED834_LOG_MAX_BYTES    - rotation size (default 10 MiB)
      MED834_LOG_BACKUP_COUNT - rotated file count to keep (default 5)
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = _resolve_level()
    logger.setLevel(level)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(_CONTEXT_FILTER)
    logger.addHandler(stream_handler)

    log_file = os.environ.get("MED834_LOG_FILE")
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        max_bytes = int(os.environ.get("MED834_LOG_MAX_BYTES", 10 * 1024 * 1024))
        backup_count = int(os.environ.get("MED834_LOG_BACKUP_COUNT", 5))
        file_handler = RotatingFileHandler(
            log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(_CONTEXT_FILTER)
        logger.addHandler(file_handler)

    return logger


def log_pipeline_start(logger: logging.Logger, layer: str, details: str) -> float:
    """Log the start of a pipeline stage and return a monotonic start time to
    hand to log_pipeline_end()/log_pipeline_error() for duration reporting."""
    logger.info("START %s | %s | %s", layer, details, datetime.now(timezone.utc).isoformat())
    return time.monotonic()


def log_pipeline_end(
    logger: logging.Logger, layer: str, details: str, start_time: Optional[float] = None
) -> None:
    if start_time is not None:
        details = f"{details} | duration_s={time.monotonic() - start_time:.2f}"
    logger.info("END %s | %s | %s", layer, details, datetime.now(timezone.utc).isoformat())


def log_pipeline_error(
    logger: logging.Logger, layer: str, details: str, start_time: Optional[float] = None
) -> None:
    """Call from inside an ``except`` block: logs the layer/details plus the
    current exception's traceback. Does not suppress the exception -- callers
    must still re-raise."""
    if start_time is not None:
        details = f"{details} | duration_s={time.monotonic() - start_time:.2f}"
    logger.exception("FAILED %s | %s | %s", layer, details, datetime.now(timezone.utc).isoformat())
