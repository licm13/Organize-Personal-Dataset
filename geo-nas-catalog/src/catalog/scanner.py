"""Filesystem scanning utilities for building the dataset catalog."""
from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence

from rich.progress import Progress

from .handlers.base import FileHandler
from .handlers.netcdf import NetCDFHandler
from .handlers.other import GenericHandler
from .handlers.tabular import CSVHandler, ExcelHandler
from .metadata import CatalogEntry


@dataclass(slots=True)
class ScanConfig:
    """Configuration parameters controlling scan behaviour."""

    root: Path
    include_suffixes: Optional[Sequence[str]] = None
    exclude_dirs: Sequence[str] = (".git", "__pycache__")
    follow_symlinks: bool = False
    handler_overrides: Optional[Iterable[FileHandler]] = None


@dataclass
class CatalogScanner:
    """Scan directories and dispatch to registered file handlers."""

    handlers: List[FileHandler] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.handlers:
            self.handlers = [NetCDFHandler(), CSVHandler(), ExcelHandler(), GenericHandler()]

    def _select_handler(self, path: Path, handlers: Sequence[FileHandler]) -> FileHandler:
        for handler in handlers:
            if handler.sniff(path):
                return handler
        return handlers[-1]

    def scan(self, config: ScanConfig) -> Iterator[CatalogEntry]:
        root = config.root
        if not root.exists():
            raise FileNotFoundError(f"Scan root {root} does not exist")
        include_suffixes = (
            tuple(s.lower() for s in config.include_suffixes)
            if config.include_suffixes
            else None
        )
        exclude_dirs = {name.lower() for name in config.exclude_dirs}
        handlers: Sequence[FileHandler] = list(config.handler_overrides or self.handlers)
        with Progress() as progress:
            task = progress.add_task("Scanning", start=False)
            progress.start_task(task)
            for dirpath, dirnames, filenames in os.walk(root, followlinks=config.follow_symlinks):
                dirnames[:] = [d for d in dirnames if d.lower() not in exclude_dirs]
                progress.update(task, advance=0, description=f"Scanning {dirpath}")
                for filename in filenames:
                    path = Path(dirpath) / filename
                    if include_suffixes and path.suffix.lower() not in include_suffixes:
                        continue
                    handler = self._select_handler(path, handlers)
                    rel_path = path.relative_to(root)
                    entry = handler.extract(path, rel_path)
                    if entry is not None:
                        yield entry

    def summarize(self, entries: Iterable[CatalogEntry]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for entry in entries:
            counts[entry.format] = counts.get(entry.format, 0) + 1
        return counts
