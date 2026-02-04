"""Tests for snapshots module."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from agrobr.snapshots import (
    SnapshotManifest,
    delete_snapshot,
    get_snapshot,
    list_snapshots,
    load_from_snapshot,
)

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

requires_pyarrow = pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")


class TestSnapshotManifest:
    def test_manifest_creation(self):
        manifest = SnapshotManifest(
            name="2025-01-15",
            created_at=datetime(2025, 1, 15, 10, 30),
            agrobr_version="0.3.0",
            sources=["cepea", "conab"],
        )
        assert manifest.name == "2025-01-15"
        assert manifest.agrobr_version == "0.3.0"
        assert "cepea" in manifest.sources

    def test_manifest_to_dict(self):
        manifest = SnapshotManifest(
            name="test",
            created_at=datetime(2025, 1, 15, 10, 30),
            agrobr_version="0.3.0",
        )
        d = manifest.to_dict()
        assert d["name"] == "test"
        assert d["created_at"] == "2025-01-15T10:30:00"
        assert d["agrobr_version"] == "0.3.0"

    def test_manifest_from_dict(self):
        data = {
            "name": "2025-01-15",
            "created_at": "2025-01-15T10:30:00",
            "agrobr_version": "0.3.0",
            "sources": ["cepea"],
            "files": {},
            "metadata": {},
        }
        manifest = SnapshotManifest.from_dict(data)
        assert manifest.name == "2025-01-15"
        assert manifest.created_at == datetime(2025, 1, 15, 10, 30)

    def test_manifest_roundtrip(self):
        original = SnapshotManifest(
            name="test",
            created_at=datetime(2025, 6, 15, 14, 0),
            agrobr_version="0.3.0",
            sources=["cepea", "ibge"],
            files={"cepea/soja.parquet": {"rows": 100}},
        )
        d = original.to_dict()
        restored = SnapshotManifest.from_dict(d)
        assert restored.name == original.name
        assert restored.sources == original.sources
        assert restored.files == original.files


class TestSnapshotOperations:
    def test_list_snapshots_empty(self, tmp_path):
        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            snapshots = list_snapshots()
            assert snapshots == []

    def test_list_snapshots_with_data(self, tmp_path):
        snapshot_dir = tmp_path / "2025-01-15"
        snapshot_dir.mkdir()

        manifest = SnapshotManifest(
            name="2025-01-15",
            created_at=datetime(2025, 1, 15, 10, 0),
            agrobr_version="0.3.0",
            sources=["cepea"],
        )
        with open(snapshot_dir / "manifest.json", "w") as f:
            json.dump(manifest.to_dict(), f)

        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            snapshots = list_snapshots()
            assert len(snapshots) == 1
            assert snapshots[0].name == "2025-01-15"

    def test_get_snapshot_found(self, tmp_path):
        snapshot_dir = tmp_path / "test-snapshot"
        snapshot_dir.mkdir()

        manifest = SnapshotManifest(
            name="test-snapshot",
            created_at=datetime(2025, 1, 15, 10, 0),
            agrobr_version="0.3.0",
        )
        with open(snapshot_dir / "manifest.json", "w") as f:
            json.dump(manifest.to_dict(), f)

        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            snapshot = get_snapshot("test-snapshot")
            assert snapshot is not None
            assert snapshot.name == "test-snapshot"

    def test_get_snapshot_not_found(self, tmp_path):
        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            snapshot = get_snapshot("nonexistent")
            assert snapshot is None

    def test_delete_snapshot_success(self, tmp_path):
        snapshot_dir = tmp_path / "to-delete"
        snapshot_dir.mkdir()

        manifest = SnapshotManifest(
            name="to-delete",
            created_at=datetime(2025, 1, 15, 10, 0),
            agrobr_version="0.3.0",
        )
        with open(snapshot_dir / "manifest.json", "w") as f:
            json.dump(manifest.to_dict(), f)

        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            result = delete_snapshot("to-delete")
            assert result is True
            assert not snapshot_dir.exists()

    def test_delete_snapshot_not_found(self, tmp_path):
        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            result = delete_snapshot("nonexistent")
            assert result is False


class TestLoadFromSnapshot:
    @requires_pyarrow
    def test_load_parquet_success(self, tmp_path):
        snapshot_dir = tmp_path / "2025-01-15" / "cepea"
        snapshot_dir.mkdir(parents=True)

        df = pd.DataFrame({"produto": ["soja", "milho"], "valor": [150.0, 80.0]})
        df.to_parquet(snapshot_dir / "soja.parquet", index=False)

        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch("agrobr.config.get_config") as mock_config,
        ):
            mock_config.return_value.snapshot_date = None
            loaded = load_from_snapshot("cepea", "soja", snapshot_name="2025-01-15")

            assert loaded is not None
            assert len(loaded) == 2
            assert "soja" in loaded["produto"].values

    def test_load_file_not_found(self, tmp_path):
        snapshot_dir = tmp_path / "2025-01-15" / "cepea"
        snapshot_dir.mkdir(parents=True)

        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch("agrobr.config.get_config") as mock_config,
        ):
            mock_config.return_value.snapshot_date = None
            loaded = load_from_snapshot("cepea", "trigo", snapshot_name="2025-01-15")
            assert loaded is None

    def test_load_no_snapshot_specified(self, tmp_path):
        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch("agrobr.config.get_config") as mock_config,
        ):
            mock_config.return_value.snapshot_date = None
            with pytest.raises(ValueError, match="No snapshot specified"):
                load_from_snapshot("cepea", "soja")
