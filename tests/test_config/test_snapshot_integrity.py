from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from agrobr import snapshots
from agrobr.exceptions import SnapshotError
from tests.helpers import make_snapshot_source


@pytest.fixture
def snapshot_root(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshots, "get_snapshots_dir", lambda: tmp_path)
    monkeypatch.setattr(snapshots.importlib.util, "find_spec", lambda _name: object())
    return tmp_path


async def test_manifest_failure_cleans_staging(snapshot_root, monkeypatch):
    def fail_dump(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(snapshots, "_snapshot_cepea", make_snapshot_source)
    monkeypatch.setattr(snapshots.json, "dump", fail_dump)
    with pytest.raises(SnapshotError, match="disk full"):
        await snapshots.create_snapshot("failed", ["cepea"])
    assert list(snapshot_root.iterdir()) == []


async def test_checksum_detects_changed_file_before_parquet_read(snapshot_root, monkeypatch):
    monkeypatch.setattr(snapshots, "_snapshot_cepea", make_snapshot_source)
    result = await snapshots.create_snapshot("integrity", ["cepea"])
    assert result.path.parent == snapshot_root
    manifest = json.loads((result.path / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["files"]["cepea/sample.parquet"]["sha256"]) == 64
    (result.path / "cepea/sample.parquet").write_bytes(b"changed")
    read = AsyncMock()
    monkeypatch.setattr(snapshots.pd, "read_parquet", read)
    with pytest.raises(SnapshotError, match="SHA-256"):
        snapshots.load_from_snapshot("cepea", "sample", "integrity")
    read.assert_not_called()
