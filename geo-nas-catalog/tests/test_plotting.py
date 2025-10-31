from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from plot import apply_nature_style, export_figure


def test_apply_nature_style_context() -> None:
    before = plt.rcParams["font.size"]
    with apply_nature_style():
        assert plt.rcParams["font.size"] == 8
    assert plt.rcParams["font.size"] == before


def test_export_figure(tmp_path: Path) -> None:
    plt.figure()
    plt.plot([0, 1], [0, 1])
    export_figure(tmp_path / "figure")
    assert (tmp_path / "figure.png").exists()
    assert (tmp_path / "figure.eps").exists()
