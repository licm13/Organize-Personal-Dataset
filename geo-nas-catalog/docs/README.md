# geo-nas-catalog

`geo-nas-catalog` is a production-grade toolkit for scanning, cataloguing, and curating
heterogeneous Earth system datasets stored on personal NAS infrastructure. The repository
focuses on non-destructive metadata discovery, reproducible ingestion pipelines, and
publication-ready visualisation utilities.

## Repository Layout

- `src/catalog/`: recursive scanning, file-type handlers, metadata schema, README mining.
- `src/ingest/`: loaders and reproducible pipelines using pandas/xarray/dask.
- `src/plot/`: Nature/Science-inspired styling helpers and exporters.
- `src/utils/`: shared utilities for configuration, logging, paths, hashing, and parallelism.
- `cli/`: Typer-based command line interface for scanning and reporting.
- `tests/`: unit, parametrised, and property-based tests covering edge cases.
- `examples/`: runnable scripts and notebooks demonstrating data access and plotting.
- `docs/`: project documentation, research thinking, plotting style guide.

## Getting Started

```bash
hatch env create
hatch run geo-nas-catalog:python -m cli.geocli --help
```

Refer to `Milestones.md` for the delivery roadmap.
