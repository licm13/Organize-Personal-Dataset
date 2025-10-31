"""Fallback handler for unknown formats."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, TYPE_CHECKING

from .base import FileHandler

if TYPE_CHECKING:  # pragma: no cover
    from ..scanner import ScanContext


class GenericHandler(FileHandler):
    """Return minimal metadata for files with unrecognised formats."""

    extensions = ()

    def extract(self, path: Path, rel_path: Path, *, context: "ScanContext") -> Dict[str, object]:  # type: ignore[override]
        return {"format": "other"}
