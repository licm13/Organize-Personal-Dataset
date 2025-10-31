"""Handler for NetCDF datasets."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import xarray as xr

from ..metadata import CatalogEntry
from .base import FileHandler


class NetCDFHandler(FileHandler):
    """Extract metadata for NetCDF files using :mod:`xarray`."""

    formats = (".nc", ".cdf", ".netcdf")

    def sniff(self, path: Path) -> bool:
        return path.suffix.lower() in self.formats

    def extract(self, path: Path, rel_path: Path) -> Optional[CatalogEntry]:
        stat = path.stat()
        try:
            ds = xr.open_dataset(path, engine="netcdf4")
        except Exception:  # pragma: no cover - logged by caller
            return None
        variables = [var for var in ds.data_vars]
        units = {
            name: str(ds[name].attrs.get("units", ""))
            for name in ds.data_vars
            if "units" in ds[name].attrs
        }
        temporal_resolution = ds.attrs.get("time_resolution")
        coverage = None
        if "time" in ds.coords:
            times = ds.indexes["time"]
            if len(times) >= 1:
                start = times[0].item()
                end = times[-1].item()
                coverage = (str(start), str(end))
        ds.close()
        return CatalogEntry(
            path=path,
            rel_path=rel_path,
            format="netcdf",
            size_bytes=stat.st_size,
            modified_utc=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            variables=variables,
            units=units,
            temporal_resolution=temporal_resolution,
            time_coverage=coverage,
            data_type="grid" if ds.dims else None,
        )
