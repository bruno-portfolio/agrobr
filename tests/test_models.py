"""Tests for models module - MetaInfo."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from agrobr.constants import Fonte
from agrobr.models import Indicador, MetaInfo


class TestMetaInfo:
    def test_auto_version_fill(self):
        meta = MetaInfo(
            source="cepea",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime.now(),
        )

        assert meta.agrobr_version != ""
        assert meta.python_version != ""

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

    @pytest.mark.parametrize(
        "value, expected",
        [
            (datetime(2024, 6, 15, 23, 30), "2024-06-15T23:30:00+00:00"),
            (
                datetime(2024, 6, 15, 23, 30, tzinfo=timezone(timedelta(hours=-3))),
                "2024-06-16T02:30:00+00:00",
            ),
        ],
        ids=["naive_utc", "offset_date_rollover"],
    )
    def test_timestamps_normalized_on_assignment(self, value: datetime, expected: str):
        meta = MetaInfo(
            source="conab",
            source_url="https://example.com",
            source_method="httpx",
            fetched_at=datetime(2024, 1, 1),
        )
        for field in ("fetched_at", "timestamp", "cache_expires_at", "fetch_timestamp"):
            setattr(meta, field, value)
            assert getattr(meta, field).utcoffset() == timedelta(0), field
            assert meta.to_dict()[field] == expected, field
        assert meta.cache_expires_at - meta.fetched_at == timedelta(0)
        meta.cache_expires_at = None
        meta.fetch_timestamp = None
        meta.source_details = {"published_at": value}
        meta.records_count = 4
        assert meta.to_dict()["cache_expires_at"] is None
        assert meta.to_dict()["fetch_timestamp"] is None
        assert meta.source_details["published_at"] is value
        assert meta.records_count == 4


def test_indicador_normaliza_o_produto():
    indicador = Indicador(
        fonte=Fonte.CEPEA,
        produto=" Soja ",
        data=datetime(2025, 1, 2).date(),
        valor=1,
        unidade="BRL/sc",
    )
    assert indicador.produto == "soja"
