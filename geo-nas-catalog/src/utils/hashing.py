"""Hashing helpers for large files."""
from __future__ import annotations

import hashlib
from pathlib import Path


def file_sha1(path: Path, chunk_size: int = 2**20) -> str:
    """Compute a streaming SHA1 checksum for ``path``."""

    digest = hashlib.sha1()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()
