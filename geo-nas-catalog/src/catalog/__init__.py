"""Catalog package providing scanning and metadata extraction utilities."""

from .metadata import CatalogEntry, CatalogSummary
from .scanner import CatalogScanner, ScanConfig

__all__ = [
    "CatalogEntry",
    "CatalogSummary",
    "CatalogScanner",
    "ScanConfig",
]
