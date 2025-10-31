"""Logging utilities built on top of :mod:`loguru`."""
from __future__ import annotations

import logging
from typing import Optional

from loguru import logger


def configure_logging(level: str = "INFO") -> None:
    """Configure loguru and the standard logging bridge."""

    logger.remove()
    logger.add(lambda msg: print(msg, end=""), level=level)

    class LoguruHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            logger.opt(depth=6, exception=record.exc_info).log(record.levelname, record.getMessage())

    logging.basicConfig(handlers=[LoguruHandler()], level=level)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a standard-library logger tied to loguru."""

    return logging.getLogger(name or __name__)
