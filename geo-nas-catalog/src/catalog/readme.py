"""Utilities for discovering and summarising README files alongside datasets."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

from rich.console import Console

console = Console()


def find_readme(start: Path) -> Optional[Path]:
    """Return the closest README-like file relative to ``start``."""

    for candidate in start.parents:
        for name in ("README.md", "readme.txt", "README.txt"):
            path = candidate / name
            if path.exists():
                return path
    return None


def summarise_readme(path: Path, max_chars: int = 500) -> str:
    """Return a truncated snippet of a README for quick inspection."""

    text = path.read_text(encoding="utf-8", errors="ignore")
    snippet = text.strip().replace("\n", " ")[:max_chars]
    if len(text) > max_chars:
        snippet += "…"
    console.log(f"Summarised README at {path}")
    return snippet


def attach_readme(entries: Iterable[dict]) -> Iterable[dict]:
    """Attach README summaries to catalog entries."""

    for entry in entries:
        readme_path = find_readme(Path(entry["path"]))
        if readme_path:
            entry["readme_path"] = str(readme_path)
            entry["readme_summary"] = summarise_readme(readme_path)
        yield entry
