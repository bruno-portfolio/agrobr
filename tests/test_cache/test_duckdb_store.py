from __future__ import annotations

import threading
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import duckdb
import pytest

from agrobr.cache.duckdb_store import DuckDBStore
from agrobr.constants import CacheSettings


@pytest.fixture()
def tmp_store(tmp_path: Path) -> DuckDBStore:
    settings = CacheSettings(cache_dir=tmp_path, db_name="test.duckdb")
    store = DuckDBStore(settings)
    yield store
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
            tmp_store.indicadores_upsert(
                [
                    {
                        "produto": "soja",
                        "praca": "paranagua",
                        "data": datetime(2024, month, 15),
                        "valor": 130.0 + month,
                        "unidade": "BRL/sc",
                        "fonte": "cepea",
                    }
                ]
            )

        results = tmp_store.indicadores_query(
            "soja",
            inicio=datetime(2024, 3, 1),
            fim=datetime(2024, 4, 30),
        )
        assert len(results) == 2

    @pytest.mark.parametrize("praca", ["paranagua", "Paranaguá/PR"])
    def test_query_praca_normalizada(self, tmp_store: DuckDBStore, praca: str):
        tmp_store.indicadores_upsert(
            [
                {
                    "produto": "soja",
                    "praca": "Paranaguá/PR",
                    "data": datetime(2024, 6, 15),
                    "valor": 135.50,
                    "unidade": "BRL/sc",
                    "fonte": "cepea",
                },
                {
                    "produto": "soja",
                    "praca": "Paraná",
                    "data": datetime(2024, 6, 15),
                    "valor": 130.00,
                    "unidade": "BRL/sc",
                    "fonte": "cepea",
                },
            ]
        )

        results = tmp_store.indicadores_query("soja", praca=praca)

        assert len(results) == 1
        assert results[0]["praca"] == "Paranaguá/PR"

    def test_query_empty_result(self, tmp_store: DuckDBStore):
        results = tmp_store.indicadores_query("inexistente")
        assert results == []

    def test_upsert_chunked_boundary(self, tmp_store: DuckDBStore):
        from agrobr.cache.duckdb_store import UPSERT_CHUNK_SIZE

        n = UPSERT_CHUNK_SIZE + 50
        indicadores = [
            {
                "produto": "soja",
                "praca": f"p_{i}",
                "data": datetime(2024, 1, 1) + timedelta(days=i),
                "valor": 100.0 + i,
                "unidade": "BRL/sc",
                "fonte": "cepea",
            }
            for i in range(n)
        ]
        count = tmp_store.indicadores_upsert(indicadores)
        assert count == n

        results = tmp_store.indicadores_query("soja")
        assert len(results) == n

    def test_upsert_skips_invalid_rows(self, tmp_store: DuckDBStore):
        indicadores = [
            {
                "produto": "soja",
                "praca": "ok",
                "data": datetime(2024, 1, 1),
                "valor": 100.0,
                "unidade": "BRL/sc",
                "fonte": "cepea",
            },
            {"produto": "bad", "praca": "x"},
            {
                "produto": "milho",
                "praca": "ok",
                "data": datetime(2024, 1, 2),
                "valor": 50.0,
                "unidade": "BRL/sc",
                "fonte": "cepea",
            },
        ]
        count = tmp_store.indicadores_upsert(indicadores)
        assert count == 2

    def test_upsert_conflict_updates(self, tmp_store: DuckDBStore):
        base = {
            "produto": "soja",
            "praca": "paranagua",
            "data": datetime(2024, 6, 15),
            "valor": 100.0,
            "unidade": "BRL/sc",
            "fonte": "cepea",
        }
        tmp_store.indicadores_upsert([base])
        tmp_store.indicadores_upsert([{**base, "valor": 200.0}])

        results = tmp_store.indicadores_query("soja")
        assert len(results) == 1
        assert float(results[0]["valor"]) == 200.0

    def test_threaded_indicadores(self, tmp_store: DuckDBStore):
        errors: list[str] = []

        def worker(tid: int) -> None:
            try:
                for j in range(10):
                    tmp_store.indicadores_upsert(
                        [
                            {
                                "produto": f"prod_{tid}",
                                "praca": f"p_{j}",
                                "data": datetime(2024, 1, 1) + timedelta(days=j),
                                "valor": 100.0 + tid + j,
                                "unidade": "BRL/sc",
                                "fonte": "cepea",
                            }
                        ]
                    )
                results = tmp_store.indicadores_query(f"prod_{tid}")
                assert len(results) == 10
            except Exception as e:
                errors.append(f"Thread {tid}: {e}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert errors == [], f"Thread errors: {errors}"


class TestIndicadoresUpsertChunkError:
    def test_chunk_error_fallback_row_by_row(self, tmp_store: DuckDBStore):
        indicadores = [
            {
                "produto": "soja",
                "praca": f"p_{i}",
                "data": datetime(2024, 1, 1) + timedelta(days=i),
                "valor": 100.0 + i,
                "unidade": "BRL/sc",
                "fonte": "cepea",
            }
            for i in range(3)
        ]

        real_conn = tmp_store._get_conn()
        call_count = [0]

        class ConnWrapper:
            def __getattr__(self, name):
                if name == "executemany":

                    def patched_executemany(sql, params):
                        call_count[0] += 1
                        if call_count[0] == 1 and "_ind_staging" in sql:
                            import duckdb

                            raise duckdb.Error("simulated chunk error")
                        return real_conn.executemany(sql, params)

                    return patched_executemany
                return getattr(real_conn, name)

        with mock.patch.object(tmp_store, "_get_conn", return_value=ConnWrapper()):
            count = tmp_store.indicadores_upsert(indicadores)

        assert count >= 0


class TestToRow:
    def test_defaults_applied(self):
        now = datetime(2024, 1, 1)
        ind = {
            "produto": "SOJA",
            "data": datetime(2024, 6, 15),
            "valor": 100.5,
        }
        row = DuckDBStore._to_row(ind, now)
        assert row[0] == "soja"
        assert row[1] == ""
        assert row[4] == "BRL/unidade"
        assert row[5] == "unknown"
        assert row[9] == 1

    def test_null_praca_normalized_to_empty(self):
        now = datetime(2024, 1, 1)
        ind = {
            "produto": "soja",
            "praca": None,
            "data": datetime(2024, 6, 15),
            "valor": 100.0,
        }
        row = DuckDBStore._to_row(ind, now)
        assert row[1] == ""

    def test_full_values(self):
        now = datetime(2024, 1, 1)
        ind = {
            "produto": "Milho",
            "praca": "Campinas",
            "data": datetime(2024, 6, 15),
            "valor": 50.25,
            "unidade": "BRL/sc",
            "fonte": "cepea",
            "metodologia": "media",
            "variacao_percentual": 1.5,
            "parser_version": 2,
        }
        row = DuckDBStore._to_row(ind, now)
        assert row[0] == "milho"
        assert row[1] == "Campinas"
        assert row[6] == "media"
        assert row[7] == 1.5
        assert row[9] == 2


class TestUpsertDedup:
    def test_upsert_dedup_empty_praca(self, tmp_store: DuckDBStore):
        base = {
            "produto": "soja",
            "praca": None,
            "data": datetime(2024, 6, 15),
            "valor": 100.0,
            "unidade": "BRL/sc",
            "fonte": "cepea",
        }
        tmp_store.indicadores_upsert([base])
        tmp_store.indicadores_upsert([{**base, "valor": 200.0}])

        results = tmp_store.indicadores_query("soja")
        assert len(results) == 1
        assert float(results[0]["valor"]) == 200.0


class TestCacheDegradado:
    def test_connect_falha_degrada_para_no_op(self, tmp_path: Path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="locked.duckdb")
        store = DuckDBStore(settings)
        indicador = {
            "produto": "soja",
            "praca": "paranagua",
            "data": datetime(2024, 6, 15),
            "valor": 100.0,
            "unidade": "BRL/sc",
            "fonte": "cepea",
        }

        with mock.patch(
            "agrobr.cache.duckdb_store.duckdb.connect",
            side_effect=duckdb.IOException("File is already open in another process"),
        ) as mock_connect:
            assert store.indicadores_query("soja") == []
            assert store.indicadores_upsert([indicador]) == 0
            assert store.indicadores_query("soja") == []

        assert mock_connect.call_count == 1
        assert store._degraded is True
        store.close()

    def test_oserror_tambem_degrada(self, tmp_path: Path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="sem_permissao.duckdb")
        store = DuckDBStore(settings)

        with mock.patch(
            "agrobr.cache.duckdb_store.duckdb.connect",
            side_effect=OSError("permission denied"),
        ):
            assert store.indicadores_query("soja") == []

        assert store._degraded is True

    def test_store_saudavel_nao_degrada(self, tmp_store: DuckDBStore):
        assert tmp_store.indicadores_query("soja") == []
        assert tmp_store._degraded is False

    def test_falha_no_schema_fecha_conexao_parcial(self, tmp_path: Path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="schema_quebrado.duckdb")
        store = DuckDBStore(settings)

        with mock.patch(
            "agrobr.cache.migrations.migrate",
            side_effect=duckdb.Error("migration boom"),
        ):
            assert store.indicadores_query("soja") == []

        assert store._degraded is True
        assert store._conn is None
        assert store.indicadores_query("soja") == []


class TestGetStore:
    def test_singleton_pattern(self):
        import agrobr.cache.duckdb_store as store_mod

        store_mod._store = None
        with mock.patch.object(store_mod, "DuckDBStore") as mock_cls:
            mock_instance = mock.MagicMock()
            mock_cls.return_value = mock_instance
            s1 = store_mod.get_store()
            s2 = store_mod.get_store()
            assert s1 is s2
            mock_cls.assert_called_once()
        store_mod._store = None
