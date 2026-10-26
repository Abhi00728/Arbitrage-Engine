"""
logger.py — Structured logging factory.

Provides:
  - Rotating file handler (JSON lines format) for machine-readable audit logs.
  - Console handler with colored output for developer visibility.
  - Single get_logger(name) factory used by all modules.

Usage:
    from logger import get_logger
    log = get_logger(__name__)
    log.info("Order placed", extra={"order_id": "123", "symbol": "ETH/USDT"})
"""

import logging
import logging.handlers
import os
import json
import time
from typing import Optional


# ─── ANSI Colors for Console ─────────────────────────────────────────────────

RESET  = "\033[0m"
COLORS = {
    "DEBUG":    "\033[36m",   # Cyan
    "INFO":     "\033[32m",   # Green
    "WARNING":  "\033[33m",   # Yellow
    "ERROR":    "\033[31m",   # Red
    "CRITICAL": "\033[35m",   # Magenta
}


class ColoredConsoleFormatter(logging.Formatter):
    """Human-readable colored formatter for stdout."""

    def format(self, record: logging.LogRecord) -> str:
        color = COLORS.get(record.levelname, RESET)
        ts    = time.strftime("%H:%M:%S", time.localtime(record.created))
        ms    = int((record.created % 1) * 1000)
        name  = record.name.split(".")[-1]  # Use only the leaf module name

        base  = f"{color}[{ts}.{ms:03d}] [{record.levelname:8s}] [{name:20s}]{RESET}  {record.getMessage()}"

        # Append any structured extra fields
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in logging.LogRecord.__dict__ and k not in (
                "args", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "filename", "module",
                "created", "msecs", "relativeCreated", "thread",
                "threadName", "processName", "process", "message",
                "taskName",
            )
        }
        if extras:
            base += f"  {color}{json.dumps(extras)}{RESET}"

        return base


class JsonFileFormatter(logging.Formatter):
    """JSON-lines formatter for file-based audit trail."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "ts":      record.created,
            "level":   record.levelname,
            "logger":  record.name,
            "msg":     record.getMessage(),
        }
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in logging.LogRecord.__dict__ and k not in (
                "args", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "filename", "module",
                "created", "msecs", "relativeCreated", "thread",
                "threadName", "processName", "process", "message",
                "taskName",
            )
        }
        log_entry.update(extras)
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


# ─── Logger Factory ───────────────────────────────────────────────────────────

_initialized: bool = False


def _initialize_root_logger(log_dir: str, log_level: str) -> None:
    """Set up root logger handlers once. Called by setup_logging()."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    os.makedirs(log_dir, exist_ok=True)
    level = getattr(logging, log_level, logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Root captures everything; handlers filter

    # ── Console handler ──────────────────────────────────────────────────────
    console_h = logging.StreamHandler()
    console_h.setLevel(level)
    console_h.setFormatter(ColoredConsoleFormatter())
    root.addHandler(console_h)

    # ── Rotating JSON file — all events ──────────────────────────────────────
    all_log_path = os.path.join(log_dir, "engine.log.jsonl")
    file_h = logging.handlers.RotatingFileHandler(
        all_log_path, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_h.setLevel(logging.DEBUG)
    file_h.setFormatter(JsonFileFormatter())
    root.addHandler(file_h)

    # ── Rotating JSON file — trade events only ────────────────────────────────
    trade_log_path = os.path.join(log_dir, "trades.log.jsonl")
    trade_h = logging.handlers.RotatingFileHandler(
        trade_log_path, maxBytes=10 * 1024 * 1024, backupCount=10
    )
    trade_h.setLevel(logging.INFO)
    trade_h.setFormatter(JsonFileFormatter())
    trade_h.addFilter(lambda r: getattr(r, "is_trade_event", False))
    root.addHandler(trade_h)


def setup_logging(log_dir: str = "logs", log_level: str = "INFO") -> None:
    """Call once at application startup."""
    _initialize_root_logger(log_dir, log_level)


def get_logger(name: str, log_dir: Optional[str] = None, log_level: Optional[str] = None) -> logging.Logger:
    """
    Get a named logger. If this is the first call, initialize root handlers.
    Subsequent calls just return the named child logger.
    """
    if log_dir and log_level:
        _initialize_root_logger(log_dir, log_level)
    return logging.getLogger(name)


def log_trade_event(logger: logging.Logger, level: str, msg: str, **kwargs) -> None:
    """
    Emit a log record tagged as a trade event (routed to trades.log.jsonl).
    """
    extra = {"is_trade_event": True}
    extra.update(kwargs)
    getattr(logger, level.lower())(msg, extra=extra)
