"""Filesystem scanning utilities for building the dataset catalog."""
from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from utils.logging import get_logger

from .handlers.base import FileHandler
from .handlers.netcdf import NetCDFHandler
from .handlers.other import GenericHandler
from .handlers.tabular import CSVHandler, ExcelHandler
from .schema import CatalogEntry, ScanState


LOGGER = get_logger(__name__)
HANDLER_ENTRYPOINT_GROUP = "geo_nas_catalog.handlers"


def _normalise_extension(ext: str) -> str:
    ext = ext.strip().lower()
    if not ext:
        return ext
    return ext if ext.startswith(".") else f".{ext}"


def _ensure_windows_path(path: Path) -> Path:
    if os.name == "nt":
        path_str = str(path)
        if not path_str.startswith(r"\\?\") and len(path_str) > 240:
            return Path(r"\\?\" + path_str)
    return path


@dataclass(slots=True)
class ScanConfig:
    """Configuration parameters controlling scan behaviour."""

    root: Path
    output_dir: Path = Path("./outputs/catalog")
    limit_extensions: Optional[Sequence[str]] = None
    exclude_dirs: Sequence[str] = (".git", "__pycache__")
    follow_symlinks: bool = False
    compute_hash: bool = False
    include_readmes: bool = False
    resume: bool = False
    workers: int = 1
    handler_overrides: Optional[Iterable[FileHandler]] = None
    state_path: Optional[Path] = None
    text_head_lines: int = 10
    sniff_bytes: int = 8192

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        self.output_dir = Path(self.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.state_path is None:
            self.state_path = self.output_dir / "scan_state.json"
        if self.limit_extensions:
            self.limit_extensions = tuple({_normalise_extension(ext) for ext in self.limit_extensions})
        if self.workers < 1:
            raise ValueError("workers must be >= 1")


@dataclass(slots=True)
class ScanContext:
    """Context shared with file handlers during extraction."""

    root: Path
    text_head_lines: int
    sniff_bytes: int


class CatalogWriter:
    """Incrementally persist catalog entries to JSONL and Parquet outputs."""

    def __init__(self, output_dir: Path, resume: bool) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.output_dir / "catalog.jsonl"
        self.parquet_path = self.output_dir / "catalog.parquet"
        if not resume:
            for path in (self.jsonl_path, self.parquet_path):
                if path.exists():
                    path.unlink()
        self._jsonl_handle = self.jsonl_path.open("a", encoding="utf-8")

    def append(self, entry: CatalogEntry) -> None:
        record = entry.as_record()
        self._jsonl_handle.write(entry.jsonl() + "\n")
        table = pa.Table.from_pylist([record])
        pq.write_table(
            table,
            self.parquet_path,
            compression="snappy",
            append=self.parquet_path.exists(),
        )

    def close(self) -> None:
        self._jsonl_handle.close()


class CatalogScanner:
    """Scan directories and dispatch to registered file handlers."""

    handlers: List[FileHandler]

    def __init__(self, handlers: Optional[List[FileHandler]] = None) -> None:
        self.handlers = list(handlers or self._load_handlers())

    def _load_handlers(self) -> List[FileHandler]:
        handler_instances: List[FileHandler] = [NetCDFHandler(), CSVHandler(), ExcelHandler()]
        try:
            entry_points = metadata.entry_points().select(group=HANDLER_ENTRYPOINT_GROUP)
        except AttributeError:  # pragma: no cover - fallback for legacy Python
            entry_points = metadata.entry_points().get(HANDLER_ENTRYPOINT_GROUP, [])  # type: ignore[arg-type]
        for ep in entry_points:
            try:
                loaded = ep.load()
                handler = loaded() if isinstance(loaded, type) else loaded
                if isinstance(handler, FileHandler):
                    handler_instances.append(handler)
                else:  # pragma: no cover - defensive
                    LOGGER.warning("Entry point %s did not yield a FileHandler", ep.name)
            except Exception as exc:  # pragma: no cover - plugin safety
                LOGGER.warning("Failed to load handler plugin %s: %s", ep.name, exc)
        handler_instances.append(GenericHandler())
        return handler_instances

    def _select_handler(self, path: Path, handlers: Sequence[FileHandler]) -> FileHandler:
        for handler in handlers:
            if handler.sniff(path):
                return handler
        return GenericHandler()

    def _load_state(self, path: Path, resume: bool) -> ScanState:
        if resume and path.exists():
            try:
                content = json.loads(path.read_text(encoding="utf-8"))
                return ScanState(**content)
            except Exception as exc:  # pragma: no cover - corrupted state
                LOGGER.warning("Ignoring corrupt state file %s: %s", path, exc)
        return ScanState()

    def _save_state(self, state: ScanState, path: Path) -> None:
        path.write_text(json.dumps(state.model_dump(), indent=2), encoding="utf-8")

    def _compute_sha1(self, path: Path) -> str:
        sha1 = hashlib.sha1()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                sha1.update(chunk)
        return sha1.hexdigest()

    def _detect_format(self, path: Path) -> str:
        ext = path.suffix.lower()
        if ext in {".nc", ".nc4", ".cdf", ".netcdf"}:
            return "netcdf"
        if ext == ".csv":
            return "csv"
        if ext == ".txt":
            return "txt"
        if ext in {".xls", ".xlsx"}:
            return "xlsx"
        return "other"

    def _find_readme(self, path: Path, root: Path) -> Tuple[Optional[Path], Optional[str]]:
        search_dirs = [path.parent]
        if path.parent != root:
            search_dirs.append(path.parent.parent)
        patterns = ("README", "readme")
        for directory in filter(None, search_dirs):
            try:
                for candidate in directory.iterdir():
                    if not candidate.is_file():
                        continue
                    name_lower = candidate.name.lower()
                    if any(name_lower.startswith(pattern.lower()) for pattern in patterns) or name_lower.endswith((".md", ".txt")):
                        try:
                            with candidate.open("r", encoding="utf-8", errors="ignore") as handle:
                                preview = handle.read(500)
                        except Exception as exc:  # pragma: no cover - log and continue
                            LOGGER.debug("Failed to read README %s: %s", candidate, exc)
                            continue
                        return candidate, preview.strip()
            except PermissionError as exc:
                LOGGER.warning("Permission denied listing %s: %s", directory, exc)
            except FileNotFoundError:
                continue
        return None, None

    def scan(self, config: ScanConfig) -> Iterator[CatalogEntry]:
        root = config.root
        if not root.exists():
            raise FileNotFoundError(f"Scan root {root} does not exist")

        limit_extensions = (
            {ext.lower() for ext in config.limit_extensions} if config.limit_extensions else None
        )
        exclude_dirs = {name.lower() for name in config.exclude_dirs}
        handlers = list(config.handler_overrides) if config.handler_overrides else self.handlers
        state = self._load_state(config.state_path, config.resume)
        writer = CatalogWriter(config.output_dir, config.resume)
        context = ScanContext(root=root, text_head_lines=config.text_head_lines, sniff_bytes=config.sniff_bytes)
        executor: Optional[ThreadPoolExecutor] = None
        if config.compute_hash and config.workers > 1:
            executor = ThreadPoolExecutor(max_workers=config.workers)

        def on_walk_error(err: OSError) -> None:
            LOGGER.warning("Error walking directory %s: %s", err.filename, err)

        try:
            for dirpath, dirnames, filenames in os.walk(
                root, followlinks=config.follow_symlinks, onerror=on_walk_error
            ):
                dirnames[:] = [d for d in dirnames if d.lower() not in exclude_dirs]
                for filename in filenames:
                    path = _ensure_windows_path(Path(dirpath) / filename)
                    if limit_extensions and path.suffix.lower() not in limit_extensions:
                        continue
                    try:
                        stat = path.stat()
                    except (OSError, PermissionError) as exc:
                        LOGGER.warning("Unable to stat %s: %s", path, exc)
                        continue
                    if state.should_skip(path, stat.st_mtime):
                        continue

                    rel_path = path.relative_to(root)
                    base_kwargs: Dict[str, object] = {
                        "path": path,
                        "rel_path": rel_path,
                        "format": self._detect_format(path),
                        "size_bytes": stat.st_size,
                        "modified_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                    }

                    hash_future: Optional[Future[str]] = None
                    checksum: Optional[str] = None
                    if config.compute_hash:
                        if executor:
                            hash_future = executor.submit(self._compute_sha1, path)
                        else:
                            checksum = self._compute_sha1(path)

                    handler = self._select_handler(path, handlers)
                    metadata: Dict[str, object] = {}
                    try:
                        metadata = handler.extract(path, rel_path, context=context) or {}
                    except PermissionError as exc:
                        LOGGER.warning("Permission denied reading %s: %s", path, exc)
                        metadata = {"error": f"permission_error: {exc}"}
                    except Exception as exc:
                        LOGGER.warning("Failed to extract metadata for %s: %s", path, exc)
                        metadata = {"error": f"extract_error: {exc}"}

                    if hash_future is not None:
                        checksum = hash_future.result()
                    if checksum is not None:
                        base_kwargs["checksum_sha1"] = checksum

                    if config.include_readmes:
                        readme_path, readme_preview = self._find_readme(path, root)
                        if readme_path is not None:
                            base_kwargs["readme_path"] = readme_path
                            base_kwargs["readme_summary"] = readme_preview

                    entry = CatalogEntry(**{**base_kwargs, **metadata})
                    writer.append(entry)
                    state.update(path, stat.st_mtime)
                    yield entry
        finally:
            writer.close()
            self._save_state(state, config.state_path)
            if executor is not None:
                executor.shutdown()

    def summarize(self, entries: Iterable[CatalogEntry]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for entry in entries:
            counts[entry.format] = counts.get(entry.format, 0) + 1
        return counts
