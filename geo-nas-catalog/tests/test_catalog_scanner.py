from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import xarray as xr

from catalog import CatalogScanner, ScanConfig


def _create_csv(path: Path) -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df.to_csv(path, index=False)


def _create_netcdf(path: Path) -> None:
    ds = xr.Dataset({"temperature": ("time", [10.0, 11.0, 12.0])})
    ds.to_netcdf(path)


@pytest.mark.parametrize("filenames", [["data.csv"], ["data.txt"]])
def test_scanner_discovers_tabular_files(tmp_path: Path, filenames: list[str]) -> None:
    for name in filenames:
        _create_csv(tmp_path / name)
    scanner = CatalogScanner()
    entries = list(scanner.scan(ScanConfig(root=tmp_path, output_dir=tmp_path / "out")))
    assert len(entries) == len(filenames)
    assert all(entry.format in {"csv", "txt"} for entry in entries)


def test_scanner_discovers_netcdf(tmp_path: Path) -> None:
    pytest.importorskip("netCDF4")
    netcdf_path = tmp_path / "test.nc"
    _create_netcdf(netcdf_path)
    scanner = CatalogScanner()
    entries = list(
        scanner.scan(
            ScanConfig(
                root=tmp_path,
                output_dir=tmp_path / "out",
                limit_extensions=(".nc",),
            )
        )
    )
    assert entries[0].format == "netcdf"
    assert "temperature" in entries[0].variables
