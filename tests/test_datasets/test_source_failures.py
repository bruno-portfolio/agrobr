from __future__ import annotations

import inspect
import warnings
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from agrobr import abiove, comexstat, constants, datasets
from agrobr.cache import duckdb_store
from agrobr.cepea import api
from agrobr.datasets.preco_diario import PrecoDiarioDataset
from agrobr.exceptions import (
    CacheMigrationError,
    ResourceLimitError,
    SourceFallbackWarning,
    SourceUnavailableError,
)
from agrobr.models import Indicador
from agrobr.utils import time as time_utils
from tests.helpers import collect_failures, fixture_instance, isolated_dataset_case


async def test_exportacao_limite_local_nao_aciona_fallback(monkeypatch):
    error = ResourceLimitError("comexstat", "max_memoria_bytes excedido")
    source = AsyncMock(side_effect=error)
    fallback = AsyncMock()
    monkeypatch.setattr(comexstat, "exportacao", source)
    monkeypatch.setattr(abiove, "exportacao", fallback)
    with pytest.raises(ResourceLimitError) as caught:
        await datasets.exportacao("soja", ano=2025)
    assert caught.value is error
    fallback.assert_not_awaited()


@pytest.fixture
def isolated_store(tmp_path, monkeypatch):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path))
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: store)
    yield store
    store.close()


_case_fixture_isolated_store = inspect.unwrap(isolated_store)


async def test_source_failures_casos_1(tmp_path_factory):
    with collect_failures() as check:
        case = "test_nested_fallback_warning_can_be_error"
        with check(case), isolated_dataset_case(case) as monkeypatch:
            tmp_path = tmp_path_factory.mktemp("dataset_case")
            with fixture_instance(
                _case_fixture_isolated_store, tmp_path=tmp_path, monkeypatch=monkeypatch
            ) as isolated_store:
                indicador = Indicador(
                    produto="soja",
                    praca="Paranaguá",
                    data=time_utils.hoje(),
                    valor=135,
                    unidade="BRL/sc60kg",
                    fonte=constants.Fonte.NOTICIAS_AGRICOLAS,
                    parser_version=1,
                )
                result = api._FetchResult(
                    [indicador], "noticias_agricolas", "https://example.com", 1, "test", 1, 0
                )
                monkeypatch.setattr(api, "_fetch_and_parse", AsyncMock(return_value=result))
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    warnings.filterwarnings("error", category=SourceFallbackWarning)
                    with pytest.raises(
                        SourceFallbackWarning,
                        match=r"'cepea' indisponível \(a página do CEPEA não respondeu ou veio "
                        r"sem dados reconhecidos; os dados são da Notícias Agrícolas\); usando "
                        r"fallback 'noticias_agricolas'",
                    ):
                        await datasets.preco_diario(
                            "soja",
                            inicio=time_utils.hoje(),
                            fim=time_utils.hoje(),
                            force_refresh=True,
                        )
                assert len(isolated_store.indicadores_query("soja")) == 1
        case = "test_rejected_network_fallback_then_offline_reports_cached_origin"
        with check(case), isolated_dataset_case(case) as monkeypatch:
            tmp_path = tmp_path_factory.mktemp("dataset_case")
            with fixture_instance(
                _case_fixture_isolated_store, tmp_path=tmp_path, monkeypatch=monkeypatch
            ) as isolated_store:
                indicator = Indicador(
                    produto="soja",
                    praca="Paranaguá/PR",
                    data=time_utils.hoje(),
                    valor=135,
                    unidade="BRL/sc60kg",
                    fonte=constants.Fonte.NOTICIAS_AGRICOLAS,
                )
                fetch = AsyncMock(
                    return_value=api._FetchResult(
                        [indicator], "noticias_agricolas", "https://example.org", 3, "test", 1, 0
                    )
                )
                monkeypatch.setattr(api, "_fetch_and_parse", fetch)
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    warnings.filterwarnings("error", category=SourceFallbackWarning)
                    with pytest.raises(SourceFallbackWarning):
                        await datasets.preco_diario("soja", force_refresh=True)
                    _, meta = await datasets.preco_diario("soja", offline=True, return_meta=True)
                assert meta.selected_source == "cache"
                assert meta.data_sources == ["noticias_agricolas"]
                assert len(isolated_store.indicadores_query("soja")) == 1
                fetch.assert_awaited_once()


@pytest.mark.parametrize("source", ["cepea", "noticias_agricolas"])
@pytest.mark.parametrize("force_refresh", [True, False])
async def test_internal_cache_fallback_obeys_warning_policy(
    isolated_store, monkeypatch, source, force_refresh
):
    isolated_store.indicadores_upsert(
        [
            {
                "produto": "soja",
                "praca": "Paranaguá/PR",
                "data": time_utils.hoje() - timedelta(days=3),
                "valor": 135,
                "unidade": "BRL/sc60kg",
                "fonte": source,
            }
        ]
    )
    fetch = AsyncMock(side_effect=SourceUnavailableError("cepea", last_error="timeout"))
    monkeypatch.setattr(api, "_fetch_and_parse", fetch)
    monkeypatch.setattr(api, "_vencido", lambda _ultima_coleta: True)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        warnings.filterwarnings("error", category=SourceFallbackWarning)
        with pytest.raises(
            SourceFallbackWarning,
            match=r"indisponível \(cepea unavailable: timeout\); usando fallback 'cache'",
        ):
            await datasets.preco_diario(
                "soja",
                inicio=time_utils.hoje() - timedelta(days=8),
                fim=time_utils.hoje(),
                force_refresh=force_refresh,
                return_meta=True,
            )
    fetch.assert_awaited_once()


async def test_fallback_do_cache_publica_a_coleta_original(isolated_store, monkeypatch):
    coleta = datetime(2026, 9, 20, 12, 30)
    with monkeypatch.context() as relogio:
        relogio.setattr(duckdb_store, "utcnow", lambda: coleta)
        isolated_store.indicadores_upsert(
            [
                {
                    "produto": "soja",
                    "praca": "Paranaguá/PR",
                    "data": time_utils.hoje() - timedelta(days=3),
                    "valor": 135,
                    "unidade": "BRL/sc60kg",
                    "fonte": "cepea",
                }
            ]
        )
    cepea = PrecoDiarioDataset.info.sources[0]
    monkeypatch.setattr(
        cepea,
        "fetch_fn",
        AsyncMock(side_effect=SourceUnavailableError("cepea", last_error="timeout")),
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await datasets.preco_diario(
            "soja",
            inicio=time_utils.hoje() - timedelta(days=8),
            fim=time_utils.hoje(),
            force_refresh=True,
            return_meta=True,
        )
    assert (meta.selected_source, meta.from_cache) == ("cache", True)
    assert meta.fetched_at == coleta.replace(tzinfo=UTC)
    assert meta.fetch_timestamp is None


async def test_migration_failure_propagates_without_dataset_fallback(isolated_store, monkeypatch):
    def fail(**_kwargs):
        raise CacheMigrationError(8, "falha simulada")

    monkeypatch.setattr(isolated_store, "indicadores_query", fail)
    with pytest.raises(CacheMigrationError):
        await datasets.preco_diario("soja", offline=True)


async def test_data_sources_matches_rows_after_dataset_normalization(isolated_store):
    isolated_store.indicadores_upsert(
        [
            {
                "produto": "soja",
                "praca": "Paranaguá/PR",
                "data": time_utils.hoje(),
                "valor": 135,
                "unidade": "BRL/sc60kg",
                "fonte": source,
            }
            for source in ("cepea", "noticias_agricolas")
        ]
    )
    raw, source_meta = await api.indicador("soja", offline=True, return_meta=True)
    frame, meta = await datasets.preco_diario("soja", offline=True, return_meta=True)
    assert len(isolated_store.indicadores_query("soja")) == 2
    assert len(raw) == 1
    assert source_meta.data_sources == ["cepea"]
    assert source_meta.fetch_timestamp is None and meta.fetch_timestamp is None
    assert raw["fonte"].tolist() == ["cepea"]
    assert len(frame) == 1
    assert meta.data_sources == sorted(frame["fonte"].unique().tolist())


async def test_unavailability_not_empty_success(isolated_store, monkeypatch):
    monkeypatch.setattr(
        api,
        "_fetch_and_parse",
        AsyncMock(side_effect=SourceUnavailableError("cepea", last_error="timeout")),
    )
    with pytest.raises(SourceUnavailableError):
        await datasets.preco_diario(
            "soja", inicio=time_utils.hoje(), fim=time_utils.hoje(), force_refresh=True
        )
    assert not isolated_store.indicadores_query("soja")
