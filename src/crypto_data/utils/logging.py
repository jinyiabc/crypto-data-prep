#!/usr/bin/env python3
"""
Logging utilities for BTC Basis Trade toolkit.

Consolidated logging pattern from multiple files with Windows encoding fallback.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logging(
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    format_string: Optional[str] = None,
) -> logging.Logger:
    """
    Setup logging to file and console.

    Args:
        log_file: Path to log file (optional)
        level: Logging level
        format_string: Custom format string

    Returns:
        Configured logger
    """
    log_format = format_string or "%(asctime)s - %(levelname)s - %(message)s"

    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=log_format, handlers=handlers)

    return logging.getLogger()


class LoggingMixin:
    """
    Mixin class providing logging functionality with Windows encoding fallback.

    Used by fetchers and other classes that need timestamped logging.
    """

    def log(self, message: str, level: str = "info") -> None:
        """
        Print timestamped log message with encoding fallback.

        Args:
            message: Message to log
            level: Log level (info, warning, error, debug)
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            formatted = f"[{timestamp}] {message}"
            print(formatted)
        except UnicodeEncodeError:
            # Windows console encoding fallback
            safe_message = message.encode("ascii", "replace").decode("ascii")
            formatted = f"[{timestamp}] {safe_message}"
            print(formatted)

        # Also log to Python logger if configured
        logger = logging.getLogger(self.__class__.__name__)
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message)

    def log_info(self, message: str) -> None:
        """Log info message."""
        self.log(message, "info")

    def log_warning(self, message: str) -> None:
        """Log warning message."""
        self.log(message, "warning")

    def log_error(self, message: str) -> None:
        """Log error message."""
        self.log(message, "error")

    def log_debug(self, message: str) -> None:
        """Log debug message."""
        self.log(message, "debug")
