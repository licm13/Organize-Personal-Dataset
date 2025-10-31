from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import xarray as xr

from ingest import load_dataset


def test_load_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(csv_path, index=False)
    df = load_dataset(csv_path)
    assert list(df.columns) == ["x"]


def test_load_netcdf(tmp_path: Path) -> None:
    path = tmp_path / "data.nc"
    xr.Dataset({"value": ("time", [1, 2])}).to_netcdf(path)
    ds = load_dataset(path)
    assert "value" in ds


def test_load_invalid_format(tmp_path: Path) -> None:
    path = tmp_path / "data.bin"
    path.write_bytes(b"123")
    with pytest.raises(ValueError):
        load_dataset(path)
