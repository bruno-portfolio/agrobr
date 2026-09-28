from __future__ import annotations

import copy
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import requests

from agrobr import constants
from agrobr.cache import duckdb_store, policies
from agrobr.cepea import api
from agrobr.datasets.preco_diario import PrecoDiarioDataset
from agrobr.exceptions import SourceUnavailableError
from agrobr.models import Indicador
from tests.helpers import levanta_exatamente

FERIADO = date(2026, 9, 7)
TERCA = date(2026, 9, 8)
COLETA_NO_FERIADO = datetime(2026, 9, 7, 22)


def _indicador(dia: date) -> Indicador:
    return Indicador(
        fonte=constants.Fonte.CEPEA,
        produto="soja",
        praca="Paranaguá/PR",
        data=dia,
        valor=Decimal("150"),
        unidade="BRL/sc60kg",
        parser_version=2,
    )


@pytest.fixture
def rede_recusada(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = httpx.AsyncClient

    def recusar(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("rede recusada", request=request)

    def recusar_requests(*_args: object, **_kwargs: object) -> requests.Response:
        raise requests.ConnectionError("rede recusada")

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(recusar), **kwargs),
    )
    monkeypatch.setattr(requests.Session, "request", recusar_requests)
    monkeypatch.setenv("AGROBR_HTTP_MAX_RETRIES", "1")


@pytest.fixture
def cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[duckdb_store.DuckDBStore]:
    database = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(api, "get_store", lambda: database)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: database)
    try:
        yield database
    finally:
        database.close()


@pytest.mark.usefixtures("rede_recusada", "cache")
async def test_indicador_sem_rede_e_sem_cache_levanta():
    with levanta_exatamente(SourceUnavailableError) as erro:
        await api.indicador("soja")
    assert erro.value.attempted_sources == ["cepea", "noticias_agricolas", "cache"]


@pytest.mark.usefixtures("rede_recusada", "cache")
async def test_ultimo_sem_rede_e_sem_cache_levanta():
    with levanta_exatamente(SourceUnavailableError) as erro:
        await api.ultimo("soja")
    assert erro.value.attempted_sources == ["cepea", "noticias_agricolas", "cache"]


@pytest.mark.usefixtures("cache")
async def test_ultimo_offline_com_cache_vazio_levanta_indisponibilidade():
    with levanta_exatamente(SourceUnavailableError, "offline sem dado no cache") as erro:
        await api.ultimo("soja", offline=True)
    assert erro.value.attempted_sources == ["cache"]


@pytest.mark.usefixtures("cache")
async def test_offline_com_cache_vazio_nao_carimba_o_cache():
    frame, meta = await api.indicador("soja", offline=True, return_meta=True)
    assert frame.empty
    assert (meta.from_cache, meta.cache_expires_at) == (False, None)


@pytest.mark.parametrize(
    ("ultima_coleta", "agora", "cache_ate", "consulta_a_fonte"),
    [
        (COLETA_NO_FERIADO, datetime(2026, 9, 8, 13), FERIADO, False),
        (None, datetime(2026, 9, 8, 13), FERIADO, True),
        (COLETA_NO_FERIADO, datetime(2026, 9, 8, 22), FERIADO, True),
        (None, datetime(2026, 9, 8, 22), TERCA + timedelta(days=1), False),
    ],
    ids=["dentro_da_validade", "sem_coleta", "venceu", "sem_coleta_cache_completo"],
)
async def test_feriado_e_hoje_sem_publicacao_nao_forcam_coleta_dentro_da_validade(
    monkeypatch: pytest.MonkeyPatch,
    ultima_coleta: datetime | None,
    agora: datetime,
    cache_ate: date,
    consulta_a_fonte: bool,
):
    dias = [
        date(2026, 8, 14) + timedelta(days=n)
        for n in range((cache_ate - date(2026, 8, 14)).days)
        if (date(2026, 8, 14) + timedelta(days=n)).weekday() < 5
    ]
    store = MagicMock()
    store.indicadores_query.return_value = [
        {**api._indicadores_to_dicts([_indicador(dia)])[0], "collected_at": COLETA_NO_FERIADO}
        for dia in dias
    ]
    store.indicadores_ultima_coleta.return_value = ultima_coleta
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(api, "_today", lambda: TERCA)
    monkeypatch.setattr(api, "utcnow", lambda: agora)
    monkeypatch.setattr(policies, "utcnow", lambda: agora)
    fonte = AsyncMock(
        return_value=api._FetchResult([], "cepea", "https://example.invalid", 2, "", 0, 0)
    )
    monkeypatch.setattr(api, "_fetch_and_parse", fonte)

    frame = await api.indicador("soja", inicio="2026-08-14", fim="2026-09-08")

    assert len(frame) == len(dias)
    assert fonte.await_count == int(consulta_a_fonte)


async def test_preco_diario_pelo_cache_sem_periodo_usa_a_janela_padrao(
    cache: duckdb_store.DuckDBStore, monkeypatch: pytest.MonkeyPatch
):
    cache.indicadores_upsert(api._indicadores_to_dicts([_indicador(date(2026, 9, 25))]))
    monkeypatch.setattr(api, "_today", lambda: date(2027, 10, 30))
    dataset = PrecoDiarioDataset()
    dataset.info = copy.deepcopy(dataset.info)
    dataset.info.sources[0].enabled = False

    with levanta_exatamente(SourceUnavailableError, "cache"):
        await dataset.fetch("soja")
    explicito = await dataset.fetch("soja", inicio="2026-09-25", fim="2026-09-25")

    assert explicito["valor"].tolist() == [150]
