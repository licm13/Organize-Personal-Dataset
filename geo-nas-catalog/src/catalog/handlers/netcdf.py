"""Handler for NetCDF datasets."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, TYPE_CHECKING

import xarray as xr

from utils.logging import get_logger

from .base import FileHandler

if TYPE_CHECKING:  # pragma: no cover
    from ..scanner import ScanContext


LOGGER = get_logger(__name__)


class NetCDFHandler(FileHandler):
    """Extract metadata for NetCDF files using :mod:`xarray`."""

    extensions = (".nc", ".nc4", ".cdf", ".netcdf")

    def extract(self, path: Path, rel_path: Path, *, context: "ScanContext") -> Dict[str, object]:  # type: ignore[override]
        metadata: Dict[str, object] = {"format": "netcdf"}
        try:
            dataset = xr.open_dataset(path, decode_cf=True, engine="netcdf4", chunks=None)
        except Exception as exc:  # pragma: no cover - caller logs
            LOGGER.debug("Failed to open NetCDF %s: %s", path, exc)
            return metadata
        try:
            variables = sorted(dataset.data_vars)
            metadata["variables"] = variables
            units = {
                name: str(dataset[name].attrs.get("units"))
                for name in dataset.data_vars
                if "units" in dataset[name].attrs
            }
            metadata["units"] = units
            if "time" in dataset.coords:
                time_index = dataset.indexes.get("time")
                if time_index is not None and len(time_index) > 0:
                    start = str(time_index[0])
                    end = str(time_index[-1])
                    metadata["time_coverage"] = (start, end)
                    if len(time_index) > 1:
                        metadata["temporal_resolution"] = str(time_index[1] - time_index[0])
            sample_var = next(iter(variables), None)
            if sample_var:
                sample = dataset[sample_var]
                indexers = {dim: 0 for dim in sample.dims}
                try:
                    _ = sample.isel(**indexers).values
                except Exception as exc:  # pragma: no cover - diagnostic only
                    LOGGER.debug("Failed to sample %s: %s", path, exc)
            extent = {}
            for key in (
                "geospatial_lat_min",
                "geospatial_lat_max",
                "geospatial_lon_min",
                "geospatial_lon_max",
            ):
                if key in dataset.attrs:
                    value = dataset.attrs[key]
                    if hasattr(value, "item"):
                        try:
                            value = value.item()
                        except Exception:  # pragma: no cover - fallback to repr
                            value = str(value)
                    extent[key] = value
            if extent:
                metadata["extent"] = extent
            spatial_ref = dataset.attrs.get("spatial_ref") or dataset.attrs.get("crs")
            if spatial_ref:
                metadata["spatial_ref"] = str(spatial_ref)
            metadata["data_type"] = "grid" if dataset.dims else None
        finally:
            dataset.close()
        return metadata
