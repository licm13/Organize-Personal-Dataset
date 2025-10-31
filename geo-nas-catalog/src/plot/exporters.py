"""Utilities for exporting figures to multiple formats with consistent settings."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt

from ..utils.logging import get_logger

LOGGER = get_logger(__name__)


def export_figure(path: Path, formats: Iterable[str] = ("png", "eps")) -> None:
    """Export the current Matplotlib figure to the given formats at 600 dpi."""

    figure = plt.gcf()
    for extension in formats:
        target = path.with_suffix(f".{extension}")
        LOGGER.info("Saving figure to %s", target)
        figure.savefig(target, dpi=600, bbox_inches="tight")
