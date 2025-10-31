from __future__ import annotations

from pathlib import Path

from utils.config import AppConfig, load_config
from utils.paths import normalise_path


def test_load_config_defaults(tmp_path: Path) -> None:
    config = load_config(tmp_path / "missing.yml")
    assert isinstance(config, AppConfig)
    assert str(config.nas_root).lower().startswith("z")


def test_normalise_path(tmp_path: Path) -> None:
    test_path = tmp_path / "folder" / "file.txt"
    test_path.parent.mkdir(parents=True)
    test_path.write_text("data")
    resolved = normalise_path(Path(str(test_path)))
    assert resolved.exists()
