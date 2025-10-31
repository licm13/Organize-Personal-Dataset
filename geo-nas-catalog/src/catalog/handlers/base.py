"""Base protocol for file type handlers used by the catalog scanner."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, Optional

from ..metadata import CatalogEntry


class FileHandler(ABC):
    """Abstract base class for dataset handlers."""

    formats: Iterable[str]

    @abstractmethod
    def sniff(self, path: Path) -> bool:
        """Return ``True`` if the handler can process the given path."""

    @abstractmethod
    def extract(self, path: Path, rel_path: Path) -> Optional[CatalogEntry]:
        """Extract a :class:`CatalogEntry` from ``path`` if possible."""
