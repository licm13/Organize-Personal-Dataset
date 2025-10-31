"""Convenience readers for heterogeneous geoscience datasets."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import xarray as xr

from ..utils.logging import get_logger

LOGGER = get_logger(__name__)


def load_dataset(path: Path) -> Any:
    """Load a dataset using a sensible engine inferred from the file suffix."""

    suffix = path.suffix.lower()
    if suffix in {".nc", ".cdf", ".netcdf"}:
        LOGGER.info("Opening NetCDF dataset at %s", path)
        return xr.open_dataset(path, engine="netcdf4")
    if suffix in {".csv", ".txt"}:
        LOGGER.info("Reading tabular dataset at %s", path)
        return pd.read_csv(path)
    if suffix in {".xls", ".xlsx"}:
        LOGGER.info("Reading Excel dataset at %s", path)
        return pd.read_excel(path)
    raise ValueError(f"Unsupported dataset format for {path}")
