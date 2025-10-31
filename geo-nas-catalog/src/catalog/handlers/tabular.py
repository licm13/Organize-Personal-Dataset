"""Handlers for text-based tabular datasets (CSV, TXT, Excel)."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from ..metadata import CatalogEntry
from .base import FileHandler


class CSVHandler(FileHandler):
    formats = (".csv", ".txt")

    def sniff(self, path: Path) -> bool:
        return path.suffix.lower() in self.formats

    def extract(self, path: Path, rel_path: Path) -> Optional[CatalogEntry]:
        stat = path.stat()
        try:
            df = pd.read_csv(path, nrows=5)
        except Exception:  # pragma: no cover
            return None
        return CatalogEntry(
            path=path,
            rel_path=rel_path,
            format="csv" if path.suffix.lower() == ".csv" else "txt",
            size_bytes=stat.st_size,
            modified_utc=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            variables=list(df.columns),
            data_type="table",
        )


class ExcelHandler(FileHandler):
    formats = (".xls", ".xlsx")

    def sniff(self, path: Path) -> bool:
        return path.suffix.lower() in self.formats

    def extract(self, path: Path, rel_path: Path) -> Optional[CatalogEntry]:
        stat = path.stat()
        try:
            df = pd.read_excel(path, nrows=5)
        except Exception:  # pragma: no cover
            return None
        return CatalogEntry(
            path=path,
            rel_path=rel_path,
            format="xlsx",
            size_bytes=stat.st_size,
            modified_utc=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            variables=list(df.columns),
            data_type="table",
        )
