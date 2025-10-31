"""Typer-based command line interface for geo-nas-catalog."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Optional

import typer

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from catalog import CatalogEntry, CatalogScanner, ScanConfig  # type: ignore  # noqa: E402
from catalog.schema import CatalogSummary  # type: ignore  # noqa: E402
from ingest import load_dataset  # type: ignore  # noqa: E402
from plot import apply_nature_style, export_figure  # type: ignore  # noqa: E402
from utils.logging import configure_logging  # type: ignore  # noqa: E402

app = typer.Typer(add_completion=False)


def _resolve_root(path: Path) -> Path:
    if not path.exists():
        raise typer.BadParameter(f"Path {path} does not exist")
    return path


@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging.")) -> None:
    configure_logging("DEBUG" if verbose else "INFO")


def _parse_extensions(raw: str) -> Optional[List[str]]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _read_catalog(path: Path) -> Iterable[CatalogEntry]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            yield CatalogEntry.model_validate(record)


@app.command()
def scan(
    root: Path = typer.Argument(..., help="Root directory to scan."),
    out: Path = typer.Option(Path("./outputs/catalog"), "--out", help="Output directory for catalog artifacts."),
    hash_: bool = typer.Option(False, "--hash/--no-hash", help="Compute SHA1 checksums during scanning."),
    readme: bool = typer.Option(False, "--readme/--no-readme", help="Attempt to detect and summarise README files."),
    limit_ext: str = typer.Option("", "--limit-ext", help="Comma separated list of extensions to include (e.g. .nc,.csv)."),
    resume: bool = typer.Option(False, "--resume/--fresh", help="Resume from previous scan state."),
    workers: int = typer.Option(1, "--workers", min=1, help="Number of worker threads for hashing."),
) -> None:
    root = _resolve_root(root)
    output_dir = out
    extensions = _parse_extensions(limit_ext)
    config = ScanConfig(
        root=root,
        output_dir=output_dir,
        limit_extensions=extensions,
        compute_hash=hash_,
        include_readmes=readme,
        resume=resume,
        workers=workers,
    )
    scanner = CatalogScanner()
    count = 0
    for _ in scanner.scan(config):
        count += 1
    typer.echo(f"Processed {count} entries into {output_dir}")


@app.command()
def summarize(catalog_path: Path = typer.Argument(..., help="Catalog JSONL path.")) -> None:
    if not catalog_path.exists():
        raise typer.BadParameter(f"Catalog {catalog_path} not found")
    entries = list(_read_catalog(catalog_path))
    summary = CatalogSummary.from_entries(entries)
    typer.echo(summary.json(indent=2))


@app.command()
def export(
    catalog_path: Path = typer.Argument(..., help="Catalog JSONL path."),
    parquet_path: Path = typer.Option(Path("catalog/catalog.parquet"), help="Output Parquet path."),
) -> None:
    import pandas as pd

    entries = [entry.as_record() for entry in _read_catalog(catalog_path)]
    df = pd.DataFrame(entries)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    typer.echo(f"Exported catalog to {parquet_path}")


@app.command()
def examples(path: Path = typer.Argument(..., help="Example dataset to read.")) -> None:
    dataset = load_dataset(path)
    typer.echo(f"Loaded dataset type: {type(dataset).__name__}")


@app.command()
def plot_demo(output: Path = typer.Argument(Path("plots/demo"), help="Output base path for demo plot.")) -> None:
    import numpy as np
    import matplotlib.pyplot as plt

    with apply_nature_style():
        x = np.linspace(0, 10, 100)
        y = np.sin(x)
        plt.figure()
        plt.plot(x, y, label="sin(x)")
        plt.xlabel("x")
        plt.ylabel("sin(x)")
        plt.legend()
        export_figure(output)
    typer.echo(f"Saved demo plot to {output.with_suffix('.png')} and {output.with_suffix('.eps')}")


if __name__ == "__main__":
    app()
