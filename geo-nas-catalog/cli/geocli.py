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
from catalog.metadata import CatalogSummary  # type: ignore  # noqa: E402
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


@app.command()
def scan(
    root: Path = typer.Argument(..., help="Root directory to scan."),
    output: Path = typer.Option(Path("catalog/catalog.jsonl"), help="Path to JSONL output."),
) -> None:
    root = _resolve_root(root)
    scanner = CatalogScanner()
    config = ScanConfig(root=root)
    entries: List[CatalogEntry] = list(scanner.scan(config))
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(entry.json())
            handle.write("\n")
    typer.echo(f"Wrote {len(entries)} entries to {output}")


@app.command()
def summarize(catalog_path: Path = typer.Argument(..., help="Catalog JSONL path.")) -> None:
    if not catalog_path.exists():
        raise typer.BadParameter(f"Catalog {catalog_path} not found")
    entries = [CatalogEntry.parse_raw(line) for line in catalog_path.read_text().splitlines()]
    summary = CatalogSummary.from_entries(entries)
    typer.echo(summary.json(indent=2))


@app.command()
def export(
    catalog_path: Path = typer.Argument(..., help="Catalog JSONL path."),
    parquet_path: Path = typer.Option(Path("catalog/catalog.parquet"), help="Output Parquet path."),
) -> None:
    import pandas as pd

    entries = [CatalogEntry.parse_raw(line).dict() for line in catalog_path.read_text().splitlines()]
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
