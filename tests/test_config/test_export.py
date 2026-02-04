"""Tests for export module."""

from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import pytest

from agrobr.export import (
    export_csv,
    export_json,
    export_parquet,
    verify_export,
)
from agrobr.models import MetaInfo

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

requires_pyarrow = pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "data": pd.date_range("2025-01-01", periods=5),
            "produto": ["soja"] * 5,
            "valor": [150.0, 151.0, 152.0, 153.0, 154.0],
        }
    )


@pytest.fixture
def sample_meta():
    return MetaInfo(
        source="cepea",
        source_url="https://example.com/data",
        source_method="httpx",
        fetched_at=datetime(2025, 1, 15, 10, 30),
    )


@requires_pyarrow
class TestExportParquet:
    def test_export_basic(self, tmp_path, sample_df):
        path = tmp_path / "test.parquet"
        result = export_parquet(sample_df, path)

        assert result == path
        assert path.exists()

    def test_export_with_meta(self, tmp_path, sample_df, sample_meta):
        path = tmp_path / "test.parquet"
        export_parquet(sample_df, path, meta=sample_meta)

        import pyarrow.parquet as pq

        table = pq.read_table(path)
        metadata = table.schema.metadata

        assert b"source" in metadata
        assert metadata[b"source"] == b"cepea"

    def test_export_creates_dirs(self, tmp_path, sample_df):
        path = tmp_path / "nested" / "dir" / "test.parquet"
        export_parquet(sample_df, path)

        assert path.exists()

    def test_export_roundtrip(self, tmp_path, sample_df):
        path = tmp_path / "test.parquet"
        export_parquet(sample_df, path)

        loaded = pd.read_parquet(path)
        assert len(loaded) == len(sample_df)
        assert list(loaded.columns) == list(sample_df.columns)


class TestExportCSV:
    def test_export_basic(self, tmp_path, sample_df):
        path = tmp_path / "test.csv"
        csv_path, sidecar_path = export_csv(sample_df, path)

        assert csv_path == path
        assert path.exists()

    def test_export_with_sidecar(self, tmp_path, sample_df):
        path = tmp_path / "test.csv"
        csv_path, sidecar_path = export_csv(sample_df, path, include_sidecar=True)

        assert sidecar_path is not None
        assert sidecar_path.exists()

        with open(sidecar_path) as f:
            sidecar = json.load(f)

        assert "file_info" in sidecar
        assert sidecar["file_info"]["row_count"] == 5

    def test_export_without_sidecar(self, tmp_path, sample_df):
        path = tmp_path / "test.csv"
        csv_path, sidecar_path = export_csv(sample_df, path, include_sidecar=False)

        assert sidecar_path is None

    def test_export_with_meta(self, tmp_path, sample_df, sample_meta):
        path = tmp_path / "test.csv"
        _, sidecar_path = export_csv(sample_df, path, meta=sample_meta)

        with open(sidecar_path) as f:
            sidecar = json.load(f)

        assert "provenance" in sidecar
        assert sidecar["provenance"]["source"] == "cepea"

    def test_export_roundtrip(self, tmp_path, sample_df):
        path = tmp_path / "test.csv"
        export_csv(sample_df, path)

        loaded = pd.read_csv(path)
        assert len(loaded) == len(sample_df)


class TestExportJSON:
    def test_export_basic(self, tmp_path, sample_df):
        path = tmp_path / "test.json"
        result = export_json(sample_df, path)

        assert result == path
        assert path.exists()

    def test_export_with_metadata(self, tmp_path, sample_df):
        path = tmp_path / "test.json"
        export_json(sample_df, path, include_metadata=True)

        with open(path) as f:
            data = json.load(f)

        assert "metadata" in data
        assert "data" in data
        assert len(data["data"]) == 5

    def test_export_without_metadata(self, tmp_path, sample_df):
        path = tmp_path / "test.json"
        export_json(sample_df, path, include_metadata=False)

        with open(path) as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 5

    def test_export_with_meta(self, tmp_path, sample_df, sample_meta):
        path = tmp_path / "test.json"
        export_json(sample_df, path, meta=sample_meta, include_metadata=True)

        with open(path) as f:
            data = json.load(f)

        assert data["metadata"]["provenance"]["source"] == "cepea"


class TestVerifyExport:
    @requires_pyarrow
    def test_verify_parquet(self, tmp_path, sample_df):
        path = tmp_path / "test.parquet"
        export_parquet(sample_df, path)

        result = verify_export(path)

        assert result["valid"] is True
        assert result["row_count"] == 5

    def test_verify_csv(self, tmp_path, sample_df):
        path = tmp_path / "test.csv"
        export_csv(sample_df, path)

        result = verify_export(path)

        assert result["valid"] is True
        assert result["row_count"] == 5
        assert "computed_hash" in result

    def test_verify_file_not_found(self, tmp_path):
        result = verify_export(tmp_path / "nonexistent.csv")

        assert result["valid"] is False
        assert "error" in result

    def test_verify_with_expected_hash(self, tmp_path, sample_df):
        path = tmp_path / "test.csv"
        export_csv(sample_df, path)

        result = verify_export(path)
        computed_hash = result["computed_hash"]

        result2 = verify_export(path, expected_hash=computed_hash)
        assert result2["hash_match"] is True

        result3 = verify_export(path, expected_hash="sha256:wronghash")
        assert result3["hash_match"] is False
