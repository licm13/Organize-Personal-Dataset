from __future__ import annotations

from pathlib import Path

from hypothesis import given, strategies as st

from utils.hashing import file_sha1


@given(st.binary(min_size=0, max_size=128))
def test_sha1_matches_python_hashlib(tmp_path: Path, payload: bytes) -> None:
    import hashlib

    path = tmp_path / "blob.bin"
    path.write_bytes(payload)
    expected = hashlib.sha1(payload).hexdigest()
    assert file_sha1(path) == expected
