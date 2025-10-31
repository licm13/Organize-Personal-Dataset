"""Base protocol for file type handlers used by the catalog scanner."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Iterable

if False:  # pragma: no cover - used for typing only
    from ..scanner import ScanContext


class FileHandler(ABC):
    """Abstract base class for dataset handlers."""

    extensions: Iterable[str] = ()

    def sniff(self, path: Path) -> bool:
        """Return ``True`` if the handler can process the given path."""

        if not self.extensions:
            return True
        suffix = path.suffix.lower()
        return suffix in {ext.lower() for ext in self.extensions}

    @abstractmethod
    def extract(self, path: Path, rel_path: Path, *, context: "ScanContext") -> Dict[str, object]:
        """Return metadata updates for ``path`` or an empty mapping."""
