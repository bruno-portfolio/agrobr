"""Tests for models module - MetaInfo."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from agrobr.models import MetaInfo


class TestMetaInfo:
    def test_creation_basic(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert meta.source == "cepea"
        assert meta.source_url == "https://example.com"
        assert meta.source_method == "httpx"
        assert meta.from_cache is False
        assert meta.records_count == 0

    def test_auto_version_fill(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime.now(),
        )

        assert meta.agrobr_version != ""
        assert meta.python_version != ""

    def test_to_dict(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime(2024, 1, 1, 12, 0, 0),
            from_cache=True,
            records_count=10,
            columns=["data", "valor"],
        )

        d = meta.to_dict()

        assert d["source"] == "cepea"
        assert d["from_cache"] is True
        assert d["records_count"] == 10
        assert d["columns"] == ["data", "valor"]
        assert "fetched_at" in d
        assert isinstance(d["fetched_at"], str)

    def test_to_json(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        json_str = meta.to_json()

        assert isinstance(json_str, str)
        assert "cepea" in json_str
        assert "source_url" in json_str

    def test_from_dict(self):
        original = MetaInfo(
            source="conab",
            source_url="https://conab.gov.br",
            source_method="httpx",
            fetched_at=datetime(2024, 6, 15, 10, 30, 0),
            from_cache=False,
            records_count=50,
        )

        d = original.to_dict()
        restored = MetaInfo.from_dict(d)

        assert restored.source == original.source
        assert restored.source_url == original.source_url
        assert restored.records_count == original.records_count

    def test_compute_dataframe_hash(self):
        meta = MetaInfo(
            source="test",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime.now(),
        )

        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        hash1 = meta.compute_dataframe_hash(df)

        assert hash1.startswith("sha256:")
        assert len(hash1) > 10

        hash2 = meta.compute_dataframe_hash(df)
        assert hash1 == hash2

        df2 = pd.DataFrame({"a": [1, 2, 4], "b": ["x", "y", "z"]})
        hash3 = meta.compute_dataframe_hash(df2)
        assert hash1 != hash3

    def test_verify_hash_no_hash(self):
        meta = MetaInfo(
            source="test",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime.now(),
            raw_content_hash=None,
        )

        df = pd.DataFrame({"a": [1, 2, 3]})
        assert meta.verify_hash(df) is True

    def test_cache_metadata(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="cache",
            fetched_at=datetime.now(),
            from_cache=True,
            cache_key="cepea:soja:all",
            cache_expires_at=datetime(2024, 12, 31, 18, 0, 0),
        )

        assert meta.from_cache is True
        assert meta.cache_key == "cepea:soja:all"
        assert meta.cache_expires_at is not None

    def test_validation_warnings(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime.now(),
            validation_passed=False,
            validation_warnings=["price_out_of_range", "missing_dates"],
        )

        assert meta.validation_passed is False
        assert len(meta.validation_warnings) == 2
        assert "price_out_of_range" in meta.validation_warnings

    def test_timing_metadata(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime.now(),
            fetch_duration_ms=500,
            parse_duration_ms=100,
        )

        assert meta.fetch_duration_ms == 500
        assert meta.parse_duration_ms == 100

    def test_parser_version(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime.now(),
            parser_version=2,
            schema_version="1.1",
        )

        assert meta.parser_version == 2
        assert meta.schema_version == "1.1"

    def test_provenance_fields_default(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime.now(),
        )

        assert meta.attempted_sources == []
        assert meta.selected_source == ""
        assert meta.fetch_timestamp is None

    def test_provenance_fields_populated(self):
        now = datetime.now()
        meta = MetaInfo(
            source="datasets.preco_diario/cepea",
            source_url="https://example.com",
            source_method="dataset",
            fetched_at=now,
            attempted_sources=["cepea", "cache"],
            selected_source="cepea",
            fetch_timestamp=now,
        )

        assert meta.attempted_sources == ["cepea", "cache"]
        assert meta.selected_source == "cepea"
        assert meta.fetch_timestamp == now

    def test_provenance_fallback_chain(self):
        meta = MetaInfo(
            source="datasets.estimativa_safra/ibge_lspa",
            source_url="https://sidra.ibge.gov.br",
            source_method="dataset",
            fetched_at=datetime.now(),
            attempted_sources=["conab", "ibge_lspa"],
            selected_source="ibge_lspa",
            fetch_timestamp=datetime.now(),
        )

        assert len(meta.attempted_sources) == 2
        assert meta.attempted_sources[0] == "conab"
        assert meta.selected_source == "ibge_lspa"
        assert meta.selected_source == meta.attempted_sources[-1]

    def test_provenance_to_dict(self):
        now = datetime(2024, 6, 15, 10, 30, 0)
        meta = MetaInfo(
            source="datasets.preco_diario/cepea",
            source_url="https://example.com",
            source_method="dataset",
            fetched_at=now,
            attempted_sources=["cepea"],
            selected_source="cepea",
            fetch_timestamp=now,
        )

        d = meta.to_dict()
        assert d["attempted_sources"] == ["cepea"]
        assert d["selected_source"] == "cepea"
        assert d["fetch_timestamp"] == now.isoformat()

    def test_provenance_roundtrip(self):
        now = datetime(2024, 6, 15, 10, 30, 0)
        original = MetaInfo(
            source="datasets.preco_diario/cepea",
            source_url="https://example.com",
            source_method="dataset",
            fetched_at=now,
            attempted_sources=["cepea", "cache"],
            selected_source="cepea",
            fetch_timestamp=now,
        )

        d = original.to_dict()
        restored = MetaInfo.from_dict(d)

        assert restored.attempted_sources == original.attempted_sources
        assert restored.selected_source == original.selected_source
        assert restored.fetch_timestamp == original.fetch_timestamp
