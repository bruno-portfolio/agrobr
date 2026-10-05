from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from agrobr import constants
from agrobr.cache import duckdb_store
from agrobr.cepea import api
from agrobr.datasets.preco_diario import PrecoDiarioDataset
from agrobr.exceptions import ParseError
from agrobr.models import Indicador
from tests.helpers import levanta_exatamente, sem_excecao

R6 = Path(__file__).parents[1] / "golden_data" / "reconciliacao_r6_20260918"
CASO = next(
    caso
    for caso in json.loads((R6 / "manifest.json").read_text(encoding="utf-8"))["cases"]
    if caso["id"] == "noticias_agricolas_soja"
)
JANELA = {"inicio": CASO["period"]["oldest"], "fim": CASO["period"]["newest"]}
CEPEA_HOSTS = {httpx.URL(endpoint).host for endpoint in constants._CEPEA_ENDPOINTS}
NA_HOST = httpx.URL(constants.URLS[constants.Fonte.NOTICIAS_AGRICOLAS]["cotacoes"]).host
NA = constants.NOTICIAS_AGRICOLAS_PARSER_VERSION
CEPEA = constants.CEPEA_PARSER_VERSION
_CLIENTE_REAL = httpx.AsyncClient


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[duckdb_store.DuckDBStore]:
    database = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(api, "get_store", lambda: database)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: database)
    try:
        yield database
    finally:
        database.close()


def cepea_fora_do_ar(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    corpo = (R6 / CASO["file"]).resolve().read_bytes()
    hosts: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        hosts.append(request.url.host)
        if request.url.host == NA_HOST:
            cabecalhos = {"content-type": "text/html; charset=utf-8"}
            return httpx.Response(200, content=corpo, headers=cabecalhos)
        return httpx.Response(503, content=b"unavailable")

    class Cliente(_CLIENTE_REAL):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(responder)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Cliente)
    monkeypatch.setattr(api, "_today", lambda: date(2026, 9, 18))
    return hosts


async def test_cache_preenchido_pelo_noticias_agricolas_informa_a_versao_gravada(
    store: duckdb_store.DuckDBStore, monkeypatch: pytest.MonkeyPatch
):
    hosts = cepea_fora_do_ar(monkeypatch)
    _, coleta = await api.indicador("soja", **JANELA, return_meta=True)
    assert (coleta.selected_source, coleta.parser_version) == ("noticias_agricolas", NA)
    assert CEPEA_HOSTS & set(hosts) and NA_HOST in hosts
    assert {registro["parser_version"] for registro in store.indicadores_query("soja")} == {NA}

    with sem_excecao():
        frame, cache = await api.indicador("soja", **JANELA, offline=True, return_meta=True)
    assert (cache.from_cache, cache.selected_source, cache.parser_version) == (True, "cache", NA)
    assert "parser_versions" not in cache.source_details

    ultimo_dia = frame["data"].max().date()
    cepea = Indicador(
        fonte=constants.Fonte.CEPEA,
        produto="soja",
        praca="Paranaguá/PR",
        data=ultimo_dia,
        valor=Decimal("150"),
        unidade="BRL/sc60kg",
        parser_version=CEPEA,
    )
    store.indicadores_upsert(api._indicadores_to_dicts([cepea]))
    versoes = {"cepea": [CEPEA], "noticias_agricolas": [NA]}

    with sem_excecao():
        _, misto = await api.indicador("soja", **JANELA, offline=True, return_meta=True)
        _, direto = await PrecoDiarioDataset().fetch(
            "soja", **JANELA, offline=True, return_meta=True
        )
        _, dia = await PrecoDiarioDataset().fetch(
            "soja", inicio=ultimo_dia, fim=ultimo_dia, offline=True, return_meta=True
        )
    assert (misto.parser_version, misto.source_details.get("parser_versions")) == (NA, versoes)
    assert (direto.selected_source, direto.parser_version) == ("cache", NA)
    assert direto.source_details.get("parser_versions") == versoes
    assert (dia.parser_version, dia.data_sources) == (CEPEA, ["cepea"])
    assert "parser_versions" not in dia.source_details


@pytest.mark.usefixtures("store")
async def test_ultimo_sem_dado_informa_a_versao_do_parser_do_cepea(
    monkeypatch: pytest.MonkeyPatch,
):
    sem_indicador = AsyncMock(
        return_value=api._FetchResult([], "cepea", "https://example.invalid", CEPEA, "", 0, 0)
    )
    monkeypatch.setattr(api, "_fetch_and_parse", sem_indicador)
    with levanta_exatamente(ParseError, match="No indicators found for soja") as erro:
        await api.ultimo("soja")
    assert isinstance(erro.value, ParseError) and erro.value.parser_version == CEPEA
