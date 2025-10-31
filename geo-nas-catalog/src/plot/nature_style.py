"""Helpers for applying Nature/Science inspired styling to Matplotlib plots."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import matplotlib.pyplot as plt

NATURE_PARAMS = {
    "figure.figsize": (3.5, 3.2),
    "font.family": "sans-serif",
    "font.size": 8,
    "axes.titlesize": 9,
    "axes.labelsize": 8,
    "axes.linewidth": 0.6,
    "axes.edgecolor": "black",
    "axes.labelpad": 4,
    "axes.grid": False,
    "grid.linewidth": 0.3,
    "lines.linewidth": 1.2,
    "lines.markersize": 4,
    "legend.frameon": False,
    "savefig.dpi": 600,
    "savefig.transparent": True,
}


@contextmanager
def apply_nature_style() -> Iterator[None]:
    """Temporarily apply Nature-style parameters."""

    original = plt.rcParams.copy()
    plt.rcParams.update(NATURE_PARAMS)
    try:
        yield
    finally:
        plt.rcParams.update(original)
