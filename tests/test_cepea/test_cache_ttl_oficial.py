from __future__ import annotations

import json
import warnings
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr import constants, datasets
from agrobr.cache import duckdb_store
from agrobr.cepea import api, client
from agrobr.exceptions import SourceUnavailableError, StaleDataWarning
from tests import helpers

GOLDEN = Path(__file__).parents[1] / "golden_data/cepea/cache_ttl_20260923"
COLETA = datetime(2026, 9, 22, 22, 56, 5, 645394)
ANTES_DA_VIRADA = datetime(2026, 9, 23, 20, 0)
DEPOIS_DA_VIRADA = datetime(2026, 9, 23, 23, 6, 52)
VIRADA_23 = datetime(2026, 9, 23, 21, 0, tzinfo=UTC)
VIRADA_24 = datetime(2026, 9, 24, 21, 0, tzinfo=UTC)


@pytest.fixture
def cache_real(tmp_path, monkeypatch):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path))
    conn = store._get_conn()
    assert conn is not None
    for linha in json.loads((GOLDEN / "cache_soja_20260922.json").read_text(encoding="utf-8")):
        conn.execute(
            f"INSERT INTO indicadores ({', '.join(linha)}) VALUES ({', '.join('?' * len(linha))})",
            list(linha.values()),
        )
    monkeypatch.setattr(api, "get_store", lambda: store)
    yield store
    store.close()


def fixar_relogio(monkeypatch: pytest.MonkeyPatch, agora: datetime) -> None:
    monkeypatch.setattr(api, "utcnow", lambda: agora)
    monkeypatch.setattr(duckdb_store, "utcnow", lambda: agora)
    monkeypatch.setattr(api, "_today", lambda: agora.date())


def servir_fonte(monkeypatch: pytest.MonkeyPatch, *, fora: bool = False) -> AsyncMock:
    if fora:
        busca = AsyncMock(side_effect=SourceUnavailableError("cepea", last_error="HTTP 503"))
    else:
        pagina = (GOLDEN / "soja_20260923.html").read_bytes().decode("utf-8")
        busca = AsyncMock(return_value=client.FetchResult(html=pagina, source="cepea"))
    monkeypatch.setattr(api.client, "fetch_indicador_page", busca)
    return busca


@pytest.mark.usefixtures("cache_real")
async def test_cache_valido_antes_da_virada_publica_a_coleta_real(monkeypatch):
    fixar_relogio(monkeypatch, ANTES_DA_VIRADA)
    busca = servir_fonte(monkeypatch)
    with helpers.sem_excecao():
        df, meta = await api.indicador(
            "soja", inicio="2026-09-22", fim="2026-09-22", return_meta=True
        )
    assert busca.await_count == 0
    assert df["valor"].tolist() == [161.93]
    assert (meta.from_cache, meta.source) == (True, "cache")
    assert (meta.raw_content_hash, meta.raw_content_size, meta.fetch_timestamp) == (None, 0, None)
    assert meta.fetched_at == COLETA.replace(tzinfo=UTC)
    assert meta.cache_expires_at == VIRADA_23


async def test_cache_vencido_busca_de_novo_e_publica_a_nova_coleta(cache_real, monkeypatch):
    fixar_relogio(monkeypatch, DEPOIS_DA_VIRADA)
    busca = servir_fonte(monkeypatch)
    with helpers.sem_excecao():
        df, meta = await api.indicador(
            "soja", inicio="2026-09-22", fim="2026-09-22", return_meta=True
        )
        novo, meta_novo = await api.indicador(
            "soja", inicio="2026-09-23", fim="2026-09-23", return_meta=True
        )
    assert busca.await_count == 1
    assert df["valor"].tolist() == [161.93]
    assert (meta.from_cache, meta.source) == (False, "cepea")
    assert meta.fetched_at == DEPOIS_DA_VIRADA.replace(tzinfo=UTC)
    assert meta.cache_expires_at == VIRADA_24
    assert cache_real.indicadores_ultima_coleta("soja") == DEPOIS_DA_VIRADA
    assert novo["valor"].tolist() == [161.65]
    assert (meta_novo.from_cache, meta_novo.fetched_at, meta_novo.cache_expires_at) == (
        True,
        DEPOIS_DA_VIRADA.replace(tzinfo=UTC),
        VIRADA_24,
    )


@pytest.mark.usefixtures("cache_real")
async def test_cache_vencido_com_fonte_fora_publica_a_coleta_antiga_com_aviso(monkeypatch):
    fixar_relogio(monkeypatch, DEPOIS_DA_VIRADA)
    busca = servir_fonte(monkeypatch, fora=True)
    with warnings.catch_warnings(record=True) as avisos, helpers.sem_excecao():
        warnings.simplefilter("always")
        df, meta = await api.indicador(
            "soja", inicio="2026-09-22", fim="2026-09-22", return_meta=True
        )
    assert busca.await_count == 1
    assert df["valor"].tolist() == [161.93]
    assert (meta.from_cache, meta.source) == (True, "cache_fallback")
    assert meta.fetched_at == COLETA.replace(tzinfo=UTC)
    assert meta.cache_expires_at == VIRADA_23
    assert [a for a in avisos if issubclass(a.category, StaleDataWarning)]


@pytest.mark.usefixtures("cache_real")
async def test_coleta_sem_dado_publica_o_cache_sem_a_proveniencia_da_coleta(monkeypatch):
    fixar_relogio(monkeypatch, DEPOIS_DA_VIRADA)
    sem_dado = "<html><body><p>sem tabela</p></body></html>"

    async def pagina(_produto: str, force_alternative: bool = False) -> client.FetchResult:
        return client.FetchResult(
            html=sem_dado, source="noticias_agricolas" if force_alternative else "cepea"
        )

    busca = AsyncMock(side_effect=pagina)
    monkeypatch.setattr(api.client, "fetch_indicador_page", busca)
    monkeypatch.setattr(api.client, "can_use_alternative_source", lambda _produto: True)
    with warnings.catch_warnings(record=True) as avisos, helpers.sem_excecao():
        warnings.simplefilter("always")
        df, meta = await api.indicador(
            "soja", inicio="2026-09-22", fim="2026-09-22", return_meta=True
        )
    assert [c.kwargs.get("force_alternative", False) for c in busca.await_args_list] == [
        False,
        True,
    ]
    assert df["valor"].tolist() == [161.93]
    assert (meta.from_cache, meta.source, meta.source_url) == (True, "cache_fallback", "")
    assert (meta.raw_content_hash, meta.raw_content_size, meta.fetch_timestamp) == (None, 0, None)
    assert meta.fetched_at == COLETA.replace(tzinfo=UTC)
    assert [a for a in avisos if issubclass(a.category, StaleDataWarning)]


@pytest.mark.parametrize(
    ("agora", "buscas", "data", "valor"),
    [
        (ANTES_DA_VIRADA, 0, date(2026, 9, 22), 161.93),
        (DEPOIS_DA_VIRADA, 1, date(2026, 9, 23), 161.65),
    ],
    ids=["antes_da_virada", "depois_da_virada"],
)
@pytest.mark.usefixtures("cache_real")
async def test_ultimo_respeita_a_virada_das_18h(monkeypatch, agora, buscas, data, valor):
    fixar_relogio(monkeypatch, agora)
    busca = servir_fonte(monkeypatch)
    with helpers.sem_excecao():
        ultimo = await api.ultimo("soja")
    assert busca.await_count == buscas
    assert (ultimo.data, float(ultimo.valor)) == (data, valor)


@pytest.mark.usefixtures("cache_real")
@pytest.mark.parametrize(
    "consulta",
    [
        lambda: api.indicador("soja", inicio="2026-09-22", fim="2026-09-22", return_meta=True),
        lambda: datasets.preco_diario(
            "soja", inicio="2026-09-22", fim="2026-09-22", return_meta=True
        ),
    ],
    ids=["fonte", "preco_diario"],
)
async def test_periodo_fechado_sai_do_cache_sem_validade(monkeypatch, consulta):
    fixar_relogio(monkeypatch, datetime(2026, 11, 3, 13))
    busca = servir_fonte(monkeypatch)
    with helpers.sem_excecao():
        df, meta = await consulta()
    assert busca.await_count == 0
    assert df["valor"].tolist() == [161.93]
    assert (meta.from_cache, meta.fetched_at) == (True, COLETA.replace(tzinfo=UTC))
    assert meta.cache_expires_at is None
