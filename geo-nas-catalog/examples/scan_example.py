"""Example script showing how to run the catalog scanner programmatically."""
from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from catalog import CatalogScanner, ScanConfig  # type: ignore  # noqa: E402


def main() -> None:
    root = PROJECT_ROOT
    scanner = CatalogScanner()
    config = ScanConfig(root=root, include_suffixes=(".nc", ".csv", ".txt", ".xlsx"))
    entries = list(scanner.scan(config))
    for entry in entries:
        print(entry.json())


if __name__ == "__main__":
    main()
