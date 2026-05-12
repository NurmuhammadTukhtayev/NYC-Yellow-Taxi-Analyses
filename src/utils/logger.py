"""
Logging configuration for the NYC Taxi pipeline.

One log file is created per run, named with the run's start timestamp:

    logs/pipeline_YYYY-MM-DD_HH-MM-SS.log

This makes each run independently searchable — no need to grep through a
shared append-only file.  Old files are pruned automatically; by default the
last 30 runs are kept (configurable via MAX_LOG_FILES).

Two handlers:
  Console (stderr)  -- INFO and above, no line numbers.
  Per-run file      -- DEBUG and above, with module name and line number.

Call setup_logging() once at the start of main() before any other module
performs I/O.  Subsequent calls are no-ops (idempotent).
"""
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

LOG_DIR = Path("logs")
MAX_LOG_FILES = 30

_CONSOLE_FMT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_FILE_FMT = "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(console_level: int = logging.INFO) -> Path:
    """
    Configure the root logger with a console handler and a per-run file handler.

    Args:
        console_level: Minimum level shown on the terminal.
                       Defaults to INFO; pass logging.DEBUG for verbose output.

    Returns:
        Path to the log file created for this run.
    """
    root = logging.getLogger()
    if root.handlers:
        # Already configured; return the existing file handler's path.
        for h in root.handlers:
            if isinstance(h, logging.FileHandler):
                return Path(h.baseFilename)
        return LOG_DIR

    root.setLevel(logging.DEBUG)  # root passes everything; handlers filter by level

    # -- Console --------------------------------------------------------------
    console = logging.StreamHandler()
    console.setLevel(console_level)
    console.setFormatter(logging.Formatter(_CONSOLE_FMT, datefmt=_DATE_FMT))

    # -- Per-run file ---------------------------------------------------------
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = LOG_DIR / f"pipeline_{run_ts}.log"

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_FILE_FMT, datefmt=_DATE_FMT))

    root.addHandler(console)
    root.addHandler(file_handler)

    _prune_old_logs()

    return log_file


def _prune_old_logs() -> None:
    """Delete log files beyond MAX_LOG_FILES, oldest first."""
    logs = sorted(
        LOG_DIR.glob("pipeline_*.log"),
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )
    for old_log in logs[MAX_LOG_FILES:]:
        old_log.unlink(missing_ok=True)
