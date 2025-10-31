"""Fallback handler for unknown formats."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ..metadata import CatalogEntry
from .base import FileHandler


class GenericHandler(FileHandler):
    """Return minimal metadata for files with unrecognised formats."""

    formats = ()

    def sniff(self, path: Path) -> bool:
        return True

    def extract(self, path: Path, rel_path: Path) -> Optional[CatalogEntry]:
        stat = path.stat()
        return CatalogEntry(
            path=path,
            rel_path=rel_path,
            format="other",
            size_bytes=stat.st_size,
            modified_utc=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
        )
