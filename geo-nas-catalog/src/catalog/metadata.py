"""Pydantic models describing catalog schema and summarisation helpers."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field, validator


class CatalogEntry(BaseModel):
    """Structured representation of a single dataset discovered on disk."""

    path: Path
    rel_path: Path
    format: str
    size_bytes: int = Field(ge=0)
    modified_utc: datetime
    checksum_sha1: Optional[str] = None
    producer: Optional[str] = None
    producer_inferred_from: Optional[str] = None
    method_principle: Optional[str] = None
    variables: List[str] = Field(default_factory=list)
    units: Dict[str, str] = Field(default_factory=dict)
    temporal_resolution: Optional[str] = None
    time_coverage: Optional[Tuple[str, str]] = None
    spatial_resolution: Optional[str] = None
    spatial_ref: Optional[str] = None
    extent: Optional[Dict[str, Union[str, float]]] = None
    data_type: Optional[str] = None
    license: Optional[str] = None
    citation: Optional[str] = None
    doi: Optional[str] = None
    readme_path: Optional[Path] = None
    readme_summary: Optional[str] = None
    application_scope: Optional[str] = None
    curation_notes: Optional[str] = None
    read_example_path: Optional[Path] = None
    plot_example_path: Optional[Path] = None
    quality_flags: Optional[List[str]] = None
    missing_value: Optional[Union[float, int, str]] = None

    @validator("format")
    def validate_format(cls, value: str) -> str:
        allowed = {"netcdf", "csv", "txt", "xlsx", "other"}
        if value not in allowed:
            raise ValueError(f"format must be one of {sorted(allowed)}; got {value!r}")
        return value


class CatalogSummary(BaseModel):
    """Aggregate summary information of a catalog."""

    total_entries: int
    total_size_bytes: int
    formats: Dict[str, int]

    @classmethod
    def from_entries(cls, entries: List[CatalogEntry]) -> "CatalogSummary":
        total_size = sum(entry.size_bytes for entry in entries)
        counts: Dict[str, int] = {}
        for entry in entries:
            counts[entry.format] = counts.get(entry.format, 0) + 1
        return cls(total_entries=len(entries), total_size_bytes=total_size, formats=counts)
