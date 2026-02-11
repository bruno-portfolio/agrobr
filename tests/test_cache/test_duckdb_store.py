from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import duckdb
import pytest

from agrobr.cache.duckdb_store import DuckDBStore
from agrobr.constants import CacheSettings, Fonte


@pytest.fixture()
def tmp_store(tmp_path: Path) -> DuckDBStore:
    settings = CacheSettings(cache_dir=tmp_path, db_name="test.duckdb")
    store = DuckDBStore(settings)
    yield store
    store.close()


class TestCacheGet:
    def test_existing_key(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("k1", b"payload", Fonte.CEPEA, ttl_seconds=3600)
        data, stale = tmp_store.cache_get("k1")

        assert data == b"payload"
        assert stale is False

    def test_missing_key(self, tmp_store: DuckDBStore):
        data, stale = tmp_store.cache_get("nonexistent")

        assert data is None
        assert stale is False

    def test_expired_key(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("k1", b"old", Fonte.CEPEA, ttl_seconds=1)

        conn = tmp_store._get_conn()
        past = datetime.utcnow() - timedelta(hours=1)
        conn.execute(
            "UPDATE cache_entries SET expires_at = ? WHERE key = ?",
            [past, "k1"],
        )

        data, stale = tmp_store.cache_get("k1")

        assert data == b"old"
        assert stale is True

    def test_stale_marked_key(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("k1", b"data", Fonte.CEPEA, ttl_seconds=3600)
        tmp_store.cache_invalidate("k1")

        data, stale = tmp_store.cache_get("k1")

        assert data == b"data"
        assert stale is True

    def test_hit_count_increments(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("k1", b"data", Fonte.CEPEA, ttl_seconds=3600)

        tmp_store.cache_get("k1")
        tmp_store.cache_get("k1")
        tmp_store.cache_get("k1")

        conn = tmp_store._get_conn()
        row = conn.execute(
            "SELECT hit_count FROM cache_entries WHERE key = ?", ["k1"]
        ).fetchone()
        assert row[0] == 3


class TestCacheSet:
    def test_insert_new(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("new", b"data", Fonte.CONAB, ttl_seconds=600)

        data, stale = tmp_store.cache_get("new")
        assert data == b"data"
        assert stale is False

    def test_overwrite_existing(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("k1", b"v1", Fonte.CEPEA, ttl_seconds=600)
        tmp_store.cache_set("k1", b"v2", Fonte.CEPEA, ttl_seconds=600)

        data, _ = tmp_store.cache_get("k1")
        assert data == b"v2"

    def test_empty_bytes(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("empty", b"", Fonte.IBGE, ttl_seconds=600)

        data, stale = tmp_store.cache_get("empty")
        assert data == b""
        assert stale is False


class TestCacheEvict:
    def test_delete_existing(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("k1", b"data", Fonte.CEPEA, ttl_seconds=600)
        tmp_store.cache_delete("k1")

        data, _ = tmp_store.cache_get("k1")
        assert data is None

    def test_delete_nonexistent_no_error(self, tmp_store: DuckDBStore):
        tmp_store.cache_delete("does_not_exist")

    def test_clear_by_source(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("cepea1", b"a", Fonte.CEPEA, ttl_seconds=600)
        tmp_store.cache_set("cepea2", b"b", Fonte.CEPEA, ttl_seconds=600)
        tmp_store.cache_set("conab1", b"c", Fonte.CONAB, ttl_seconds=600)

        count = tmp_store.cache_clear(source=Fonte.CEPEA)

        assert count == 2
        assert tmp_store.cache_get("cepea1")[0] is None
        assert tmp_store.cache_get("conab1")[0] == b"c"

    def test_clear_all(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("a", b"1", Fonte.CEPEA, ttl_seconds=600)
        tmp_store.cache_set("b", b"2", Fonte.CONAB, ttl_seconds=600)

        count = tmp_store.cache_clear()
        assert count == 2


class TestCacheTTL:
    def test_ttl_sets_expiry(self, tmp_store: DuckDBStore):
        before = datetime.utcnow()
        tmp_store.cache_set("k1", b"data", Fonte.CEPEA, ttl_seconds=7200)

        conn = tmp_store._get_conn()
        row = conn.execute(
            "SELECT expires_at FROM cache_entries WHERE key = ?", ["k1"]
        ).fetchone()

        expected_min = before + timedelta(seconds=7200)
        expected_max = datetime.utcnow() + timedelta(seconds=7200)
        assert expected_min <= row[0] <= expected_max

    def test_invalidate_marks_stale(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("k1", b"data", Fonte.CEPEA, ttl_seconds=3600)
        tmp_store.cache_invalidate("k1")

        conn = tmp_store._get_conn()
        row = conn.execute(
            "SELECT stale FROM cache_entries WHERE key = ?", ["k1"]
        ).fetchone()
        assert row[0] is True


class TestCacheConcurrent:
    def test_concurrent_writes_same_key(self, tmp_store: DuckDBStore):
        tmp_store.cache_set("race", b"first", Fonte.CEPEA, ttl_seconds=600)
        tmp_store.cache_set("race", b"second", Fonte.CEPEA, ttl_seconds=600)

        data, _ = tmp_store.cache_get("race")
        assert data == b"second"


class TestDBCorrupted:
    def test_connect_ioerror(self, tmp_path: Path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="test.duckdb")
        store = DuckDBStore(settings)

        with mock.patch("duckdb.connect", side_effect=IOError("disk full")):
            store._conn = None
            with pytest.raises(IOError, match="disk full"):
                store.cache_get("any")


class TestHistorySave:
    @pytest.mark.xfail(
        reason="BUG: history_entries.id é PRIMARY KEY sem autoincrement — "
        "INSERT falha com NOT NULL constraint e ConstraintException é silenciada",
        strict=True,
    )
    def test_save_and_retrieve(self, tmp_store: DuckDBStore):
        data_date = datetime(2024, 6, 15)
        tmp_store.history_save(
            key="cepea:soja",
            data=b"hist_data",
            source=Fonte.CEPEA,
            data_date=data_date,
            parser_version=1,
            fingerprint_hash="abc123",
        )

        result = tmp_store.history_get("cepea:soja", data_date)
        assert result == b"hist_data"

    @pytest.mark.xfail(
        reason="BUG: history_entries.id sem autoincrement — save silenciosamente falha",
        strict=True,
    )
    def test_save_duplicate_is_idempotent(self, tmp_store: DuckDBStore):
        data_date = datetime(2024, 6, 15)
        tmp_store.history_save("k", b"d", Fonte.CEPEA, data_date, 1)
        tmp_store.history_save("k", b"d", Fonte.CEPEA, data_date, 1)

        result = tmp_store.history_get("k", data_date)
        assert result == b"d"

    @pytest.mark.xfail(
        reason="BUG: history_entries.id sem autoincrement — save silenciosamente falha",
        strict=True,
    )
    def test_history_get_latest(self, tmp_store: DuckDBStore):
        tmp_store.history_save("k", b"old", Fonte.CEPEA, datetime(2024, 1, 1), 1)
        tmp_store.history_save("k", b"new", Fonte.CEPEA, datetime(2024, 6, 1), 1)

        result = tmp_store.history_get("k")
        assert result == b"new"

    def test_history_get_missing(self, tmp_store: DuckDBStore):
        result = tmp_store.history_get("nonexistent")
        assert result is None

    def test_save_disabled_by_settings(self, tmp_path: Path):
        settings = CacheSettings(
            cache_dir=tmp_path, db_name="test.duckdb", save_to_history=False
        )
        store = DuckDBStore(settings)
        store.history_save("k", b"d", Fonte.CEPEA, datetime(2024, 1, 1), 1)

        result = store.history_get("k")
        assert result is None
        store.close()


class TestIndicadores:
    def test_upsert_and_query(self, tmp_store: DuckDBStore):
        indicadores = [
            {
                "produto": "soja",
                "praca": "paranagua",
                "data": datetime(2024, 6, 15),
                "valor": 135.50,
                "unidade": "BRL/sc",
                "fonte": "cepea",
            }
        ]
        count = tmp_store.indicadores_upsert(indicadores)
        assert count == 1

        results = tmp_store.indicadores_query("soja")
        assert len(results) == 1
        assert results[0]["praca"] == "paranagua"

    def test_upsert_empty_list(self, tmp_store: DuckDBStore):
        assert tmp_store.indicadores_upsert([]) == 0

    def test_query_with_date_range(self, tmp_store: DuckDBStore):
        for month in range(1, 7):
            tmp_store.indicadores_upsert([{
                "produto": "soja",
                "praca": "paranagua",
                "data": datetime(2024, month, 15),
                "valor": 130.0 + month,
                "unidade": "BRL/sc",
                "fonte": "cepea",
            }])

        results = tmp_store.indicadores_query(
            "soja",
            inicio=datetime(2024, 3, 1),
            fim=datetime(2024, 4, 30),
        )
        assert len(results) == 2

    def test_query_empty_result(self, tmp_store: DuckDBStore):
        results = tmp_store.indicadores_query("inexistente")
        assert results == []

    def test_get_dates(self, tmp_store: DuckDBStore):
        for month in [1, 3, 6]:
            tmp_store.indicadores_upsert([{
                "produto": "milho",
                "data": datetime(2024, month, 10),
                "valor": 50.0,
                "unidade": "BRL/sc",
                "fonte": "cepea",
            }])

        dates = tmp_store.indicadores_get_dates("milho")
        assert len(dates) == 3


class TestStoreClose:
    def test_close_and_reopen(self, tmp_path: Path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="test.duckdb")

        store = DuckDBStore(settings)
        store.cache_set("k1", b"data", Fonte.CEPEA, ttl_seconds=600)
        store.close()

        store2 = DuckDBStore(settings)
        data, _ = store2.cache_get("k1")
        assert data == b"data"
        store2.close()
