"""Configuration helpers for geo-nas-catalog."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml
from pydantic import BaseModel, Field


class AppConfig(BaseModel):
    """Application level configuration."""

    nas_root: Path = Field(default=Path("Z:/"))
    research_idea_path: Path = Field(default=Path("docs/Research_Thinking.md"))
    reference_dir: Path = Field(default=Path("reference"))


def load_config(path: Path) -> AppConfig:
    """Load configuration from a YAML file."""

    data: Dict[str, Any] = {}
    if path.exists():
        data = yaml.safe_load(path.read_text()) or {}
    return AppConfig(**data)
