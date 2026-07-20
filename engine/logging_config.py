"""Structured JSON logging for the application core.

Every log line is one JSON object (timestamp, level, logger name, message,
plus whatever a call site passes via `extra={...}`) written to stdout --
the shape log shippers, `jq`, and hosted-log-search tools expect, instead of
the default plain-text `logging` format. All of it lives under one root
logger ("aiboss") so a single LOG_LEVEL env var controls verbosity
everywhere at once; module loggers (get_logger(__name__)) are children of
it and propagate up to its one handler rather than each installing their
own. Lives in engine/ (not webapp/) so engine modules (the orchestrator,
the state store) can use it without creating a reverse dependency on the
webapp package.
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone

_ROOT_LOGGER_NAME = "aiboss"

# Every attribute a plain LogRecord carries by default -- used to separate
# "the record's own bookkeeping" from whatever a call site added via
# `extra={...}`, so only the latter ends up as extra top-level JSON fields.
_STANDARD_RECORD_KEYS = set(logging.makeLogRecord({}).__dict__.keys())


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_KEYS:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = None) -> logging.Logger:
    """Set up the 'aiboss' root logger's single stdout handler. Idempotent
    -- safe to call from every module that wants a logger (each does, via
    get_logger()) without stacking duplicate handlers on repeated calls or
    across test-suite reimports."""
    resolved_level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    logger = logging.getLogger(_ROOT_LOGGER_NAME)
    logger.setLevel(resolved_level)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    return logger


def get_logger(name: str) -> logging.Logger:
    """A '{_ROOT_LOGGER_NAME}.<name>' logger -- pass __name__ from the
    calling module. Ensures the root logger is configured first, since a
    child logger with no handlers of its own relies on the root's."""
    configure_logging()
    return logging.getLogger(f"{_ROOT_LOGGER_NAME}.{name}")
