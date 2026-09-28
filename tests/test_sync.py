from __future__ import annotations

import asyncio
import threading
import warnings
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from agrobr import constants
from agrobr.cache import duckdb_store
from agrobr.datasets.deterministic import deterministic, get_snapshot
from agrobr.sync import _SyncModule, run_sync, sync_wrapper
from tests.helpers import levanta_exatamente, sem_excecao


class TestRunSync:
    def test_propagates_typed_exception(self):
        async def failing():
            raise RuntimeError("typed")

        with pytest.raises(RuntimeError, match="typed"):
            run_sync(failing())


class TestSyncWrapper:
    def test_preserves_doc_with_sync_prefix(self):
        async def documented():
            """Original doc."""
            pass

        wrapped = sync_wrapper(documented)
        assert wrapped.__doc__ is not None
        assert "SYNC" in wrapped.__doc__
        assert "Original doc" in wrapped.__doc__

    def test_no_doc_no_error(self):
        async def no_doc():
            pass

        no_doc.__doc__ = None
        wrapped = sync_wrapper(no_doc)
        assert wrapped.__doc__ is None


class TestSyncModule:
    def test_passes_non_coroutine_through(self):
        mock_module = mock.MagicMock()
        mock_module.CONSTANT = 42

        sync_mod = _SyncModule(mock_module)
        assert sync_mod.CONSTANT == 42


class TestModuleLazyLoading:
    def test_valid_module_loads(self):
        import agrobr.sync as sync_module

        with mock.patch("importlib.import_module") as mock_import:
            mock_async_mod = mock.MagicMock()
            mock_import.return_value = mock_async_mod

            sync_module._modules["cepea"] = None

            result = sync_module.__getattr__("cepea")

            mock_import.assert_called_with("agrobr.cepea")
            assert result is not None

            sync_module._modules["cepea"] = None

    def test_cached_module_not_reimported(self):
        import agrobr.sync as sync_module

        sentinel = mock.MagicMock()
        original = sync_module._modules.get("cepea")
        sync_module._modules["cepea"] = sentinel

        try:
            with mock.patch("importlib.import_module") as mock_import:
                result = sync_module.__getattr__("cepea")

                mock_import.assert_not_called()
                assert result is sentinel
        finally:
            sync_module._modules["cepea"] = original

    def test_anec_embarques_is_synchronous_callable(self):
        import agrobr.sync as sync_module

        async_module = mock.MagicMock()
        async_module.embarques = mock.AsyncMock(return_value="ok")
        original = sync_module._modules["anec"]
        sync_module._modules["anec"] = None

        try:
            with mock.patch("importlib.import_module", return_value=async_module):
                embarques = sync_module.__getattr__("anec").embarques
                assert callable(embarques)
                assert embarques() == "ok"
                async_module.embarques.assert_awaited_once_with()
        finally:
            sync_module._modules["anec"] = original

    def test_modules_match_top_level_api_packages(self):
        import agrobr.sync as sync_module

        package_root = Path(sync_module.__file__).parent
        expected = {path.parent.name for path in package_root.glob("*/api.py")}
        expected.update({"datasets", "noticias_agricolas", "sicar"})
        assert set(sync_module._modules) == expected


class TestRunningLoop:
    def test_dentro_de_loop_roda_em_thread_duas_vezes_e_o_2o_asyncio_run_segue(self):
        async def identificar(valor):
            return valor, threading.current_thread().name

        async def cenario():
            chamador = threading.current_thread().name
            return chamador, [run_sync(identificar(valor)) for valor in (1, 2)]

        with warnings.catch_warnings(record=True) as avisos, sem_excecao():
            warnings.simplefilter("always")
            primeiro = asyncio.run(cenario())
            segundo = asyncio.run(cenario())

        for chamador, resultados in (primeiro, segundo):
            assert [valor for valor, _ in resultados] == [1, 2]
            assert all(thread not in (chamador, "") for _, thread in resultados)
        mensagens = [str(aviso.message) for aviso in avisos if "agrobr.sync" in str(aviso.message)]
        assert len(mensagens) == 1
        assert "await" in mensagens[0]

    def test_dentro_de_loop_leva_o_modo_deterministico_para_a_thread(self):
        async def snapshot_na_thread():
            return get_snapshot()

        async def cenario():
            async with deterministic("2025-03-10"):
                return run_sync(snapshot_na_thread())

        with sem_excecao():
            assert asyncio.run(cenario()) == "2025-03-10"

    def test_dentro_de_loop_preserva_a_excecao_da_corrotina(self):
        async def falha():
            raise ImportError("dependência interna ausente")

        async def cenario():
            run_sync(falha())

        with levanta_exatamente(ImportError, "dependência interna ausente"):
            asyncio.run(cenario())

    def test_dentro_de_loop_o_cache_duckdb_aceita_a_conexao_da_thread(self, tmp_path):
        store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path))
        linha = {
            "produto": "soja",
            "praca": "paranagua",
            "data": datetime(2026, 9, 25),
            "valor": 140.5,
            "unidade": "BRL/sc",
            "fonte": "cepea",
        }

        async def gravar_e_ler():
            return store.indicadores_upsert([linha]), store.indicadores_query("soja")

        async def cenario():
            return run_sync(gravar_e_ler())

        with sem_excecao():
            gravadas, lidas = asyncio.run(cenario())
        assert gravadas == 1
        assert [(item["praca"], float(item["valor"])) for item in lidas] == [("paranagua", 140.5)]
