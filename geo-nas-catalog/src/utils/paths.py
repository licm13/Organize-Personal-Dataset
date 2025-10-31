"""Path utility helpers."""
from __future__ import annotations

from pathlib import Path


def normalise_path(path: Path) -> Path:
    """Return a normalised path handling Windows drive casing."""

    return Path(str(path).replace("\\", "/")).expanduser().resolve()
