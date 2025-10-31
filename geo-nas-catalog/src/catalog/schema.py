"""Pydantic models and helpers describing the on-disk catalog schema."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from typing import Literal

from pydantic import BaseModel, Field, validator


CatalogFormat = Literal["netcdf", "csv", "txt", "xlsx", "other"]


class CatalogEntry(BaseModel):
    """Structured metadata describing a single discovered dataset."""

    path: Path
    rel_path: Path
    format: CatalogFormat
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
    extent: Optional[Dict[str, Union[str, float, int]]] = None
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
    error: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            Path: str,
            datetime: lambda value: value.astimezone(timezone.utc).isoformat(),
        }

    @validator("format")
    def validate_format(cls, value: str) -> str:
        allowed = {"netcdf", "csv", "txt", "xlsx", "other"}
        if value not in allowed:
            raise ValueError(f"format must be one of {sorted(allowed)}; got {value!r}")
        return value

    def as_record(self) -> Dict[str, Any]:
        """Return a JSON-serialisable mapping representing this entry."""

        data = self.model_dump()
        data["path"] = str(self.path)
        data["rel_path"] = str(self.rel_path)
        data["modified_utc"] = self.modified_utc.astimezone(timezone.utc).isoformat()
        if self.readme_path is not None:
            data["readme_path"] = str(self.readme_path)
        if self.read_example_path is not None:
            data["read_example_path"] = str(self.read_example_path)
        if self.plot_example_path is not None:
            data["plot_example_path"] = str(self.plot_example_path)
        if self.time_coverage is not None:
            start, end = self.time_coverage
            data["time_coverage"] = [start, end]
            data["time_coverage_start"] = start
            data["time_coverage_end"] = end
        else:
            data["time_coverage"] = None
            data["time_coverage_start"] = None
            data["time_coverage_end"] = None
        return data

    def jsonl(self) -> str:
        """Serialise the entry to a JSONL-compatible string."""

        return json.dumps(self.as_record(), ensure_ascii=False)


class CatalogSummary(BaseModel):
    """Aggregate summary information of a catalog."""

    total_entries: int
    total_size_bytes: int
    formats: Dict[str, int]

    @classmethod
    def from_entries(cls, entries: Iterable[CatalogEntry]) -> "CatalogSummary":
        entries_list = list(entries)
        total_size = sum(entry.size_bytes for entry in entries_list)
        counts: Dict[str, int] = {}
        for entry in entries_list:
            counts[entry.format] = counts.get(entry.format, 0) + 1
        return cls(total_entries=len(entries_list), total_size_bytes=total_size, formats=counts)


class ScanState(BaseModel):
    """State describing previously processed files for resumable scans."""

    processed: Dict[str, float] = Field(default_factory=dict)

    def should_skip(self, path: Path, mtime: float) -> bool:
        stored = self.processed.get(str(path))
        return stored is not None and stored >= mtime

    def update(self, path: Path, mtime: float) -> None:
        self.processed[str(path)] = mtime


