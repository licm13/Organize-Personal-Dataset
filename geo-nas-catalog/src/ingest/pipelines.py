"""Reusable ingestion pipelines built on Dask for scalable processing."""
from __future__ import annotations

from pathlib import Path
from typing import Callable

import dask.dataframe as dd

from ..utils.logging import get_logger

LOGGER = get_logger(__name__)


def build_lazy_pipeline(path: Path, transform: Callable[[dd.DataFrame], dd.DataFrame]) -> dd.DataFrame:
    """Create a lazy Dask pipeline for tabular data."""

    LOGGER.info("Building lazy pipeline for %s", path)
    ddf = dd.read_csv(path) if path.suffix.lower() in {".csv", ".txt"} else dd.read_parquet(path)
    return transform(ddf)
