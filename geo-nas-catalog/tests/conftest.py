from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _configure_test_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RICH_PROGRESS_BAR", "0")
    project_root = Path(__file__).resolve().parents[1]
    src_root = project_root / "src"
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))
