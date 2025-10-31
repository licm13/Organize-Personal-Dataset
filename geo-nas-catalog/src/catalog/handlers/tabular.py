"""Handlers for text-based tabular datasets (CSV, TXT, Excel)."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, TYPE_CHECKING

import pandas as pd

from utils.logging import get_logger

from .base import FileHandler

if TYPE_CHECKING:  # pragma: no cover
    from ..scanner import ScanContext


LOGGER = get_logger(__name__)


class CSVHandler(FileHandler):
    extensions = (".csv", ".txt")

    def extract(self, path: Path, rel_path: Path, *, context: "ScanContext") -> Dict[str, object]:  # type: ignore[override]
        encoding = "utf-8"
        head_lines: list[str] = []
        try:
            with path.open("r", encoding=encoding, errors="ignore") as handle:
                for _ in range(context.text_head_lines):
                    line = handle.readline()
                    if not line:
                        break
                    head_lines.append(line.rstrip("\n"))
        except Exception as exc:  # pragma: no cover - logged by caller
            LOGGER.debug("Failed reading head of %s: %s", path, exc)

        sample = "\n".join(head_lines)
        delimiter = ","
        headers: list[str] = []
        if sample:
            try:
                dialect = csv.Sniffer().sniff(sample)
                delimiter = dialect.delimiter
            except csv.Error:
                delimiter = ","
            reader = csv.reader(head_lines, delimiter=delimiter)
            try:
                headers = next(reader)
            except StopIteration:
                headers = []
        if not headers:
            try:
                df = pd.read_csv(path, nrows=context.text_head_lines)
                headers = list(df.columns)
            except Exception as exc:  # pragma: no cover
                LOGGER.debug("Fallback header extraction failed for %s: %s", path, exc)

        time_columns = [col for col in headers if "time" in col.lower() or col.lower().endswith("date")]
        format_name = "csv" if path.suffix.lower() == ".csv" else "txt"
        notes = None
        if time_columns:
            notes = f"Detected potential time-like columns: {', '.join(time_columns[:5])}"
        return {
            "format": format_name,
            "variables": headers,
            "data_type": "table",
            "method_principle": f"delimiter={delimiter}",
            "curation_notes": notes,
        }


class ExcelHandler(FileHandler):
    extensions = (".xls", ".xlsx")

    def extract(self, path: Path, rel_path: Path, *, context: "ScanContext") -> Dict[str, object]:  # type: ignore[override]
        metadata: Dict[str, object] = {"format": "xlsx", "data_type": "table"}
        try:
            excel = pd.ExcelFile(path)
        except Exception as exc:  # pragma: no cover - logged by caller
            LOGGER.debug("Failed to open Excel file %s: %s", path, exc)
            return metadata
        try:
            metadata["variables"] = []
            metadata["method_principle"] = ",".join(excel.sheet_names)
            if excel.sheet_names:
                first_sheet = excel.sheet_names[0]
                df = excel.parse(first_sheet, nrows=context.text_head_lines)
                metadata["variables"] = list(df.columns)
        finally:
            excel.close()
        return metadata
