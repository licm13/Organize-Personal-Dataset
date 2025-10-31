"""Data ingestion utilities for reading catalogued datasets."""

from .loaders import load_dataset
from .pipelines import build_lazy_pipeline

__all__ = ["load_dataset", "build_lazy_pipeline"]
