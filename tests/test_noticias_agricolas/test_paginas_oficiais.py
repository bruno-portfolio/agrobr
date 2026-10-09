from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import constants
from agrobr.cache import duckdb_store
from agrobr.cepea import api as cepea_api
from agrobr.noticias_agricolas import parser
from tests.helpers import collect_failures, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data" / "noticias_agricolas" / "paginas_20260925"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
CASOS = {caso["produto"]: caso for caso in MANIFESTO["cases"]}
COTACOES = constants.URLS[constants.Fonte.NOTICIAS_AGRICOLAS]["cotacoes"]
NA = constants.NOTICIAS_AGRICOLAS_PARSER_VERSION
_CLIENTE_REAL = httpx.AsyncClient


def bruto(caso: dict[str, Any]) -> bytes:
    conteudo = (GOLDEN / caso["file"]).resolve().read_bytes()
    assert hashlib.sha256(conteudo).hexdigest() == caso["sha256"]
    return conteudo


def linhas_do_oraculo(caso: dict[str, Any]) -> list[tuple[Any, ...]]:
    return [
        (
            linha["data"],
            linha["praca"],
            Decimal(linha["valor"]),
            caso["unidade"]["esperada"],
            None if linha["variacao"] is None else Decimal(linha["variacao"]),
            ("media_semanal",) if linha["semanal"] else (),
            linha["periodo"],
        )
        for linha in caso["linhas"]
    ]


def test_parser_entrega_todas_as_linhas_das_paginas_oficiais():
    assert sorted(CASOS) == sorted(constants.NOTICIAS_AGRICOLAS_PRODUTOS)
    with collect_failures() as check:
        for produto, caso in CASOS.items():
            with check(produto):
                with sem_excecao():
                    indicadores = parser.parse_indicador(bruto(caso).decode("utf-8"), produto)
                assert [
                    (
                        ind.data.isoformat(),
                        ind.praca,
                        ind.valor,
                        ind.unidade,
                        None
                        if "variacao_percentual" not in ind.meta
                        else Decimal(str(ind.meta["variacao_percentual"])),
                        tuple(ind.anomalies),
                        ind.meta.get("periodo"),
                    )
                    for ind in indicadores
                ] == linhas_do_oraculo(caso)
                assert {
                    (
                        ind.produto,
                        ind.fonte,
                        ind.parser_version,
                        ind.meta.get("tipo"),
                        ind.meta.get("fonte_original"),
                        ind.meta.get("via"),
                        ind.metodologia,
                    )
                    for ind in indicadores
                } == {
                    (
                        produto,
                        constants.Fonte.NOTICIAS_AGRICOLAS,
                        NA,
                        "media_semanal" if linha["semanal"] else None,
                        "CEPEA/ESALQ",
                        "Notícias Agrícolas",
                        "CEPEA/ESALQ via Notícias Agrícolas",
                    )
                    for linha in caso["linhas"]
                }


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[duckdb_store.DuckDBStore]:
    database = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(cepea_api, "get_store", lambda: database)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: database)
    try:
        yield database
    finally:
        database.close()


def cepea_fora_do_ar(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    paginas = {
        httpx.URL(f"{COTACOES}/{constants.NOTICIAS_AGRICOLAS_PRODUTOS[produto]}").path: bruto(caso)
        for produto, caso in CASOS.items()
    }
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        if request.url.path in paginas and request.url.host == httpx.URL(COTACOES).host:
            cabecalhos = {"content-type": "text/html; charset=utf-8"}
            return httpx.Response(200, content=paginas[request.url.path], headers=cabecalhos)
        return httpx.Response(503, content=b"unavailable")

    class Cliente(_CLIENTE_REAL):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(responder)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Cliente)
    monkeypatch.setattr(cepea_api, "_today", lambda: date(2026, 9, 25))
    return pedidos


@pytest.mark.usefixtures("store")
async def test_fallback_do_cepea_entrega_as_linhas_de_cada_pagina(
    monkeypatch: pytest.MonkeyPatch,
):
    pedidos = cepea_fora_do_ar(monkeypatch)
    series_no_cache: set[str] = set()
    reusos = 0
    with collect_failures() as check:
        for produto, caso in CASOS.items():
            if not caso["fallback_cepea"]:
                continue
            serie = constants.CEPEA_SERIE_CANONICA.get(produto, produto)
            do_cache = serie in series_no_cache
            series_no_cache.add(serie)
            reusos += do_cache
            with check(produto):
                datas = [linha["data"] for linha in caso["linhas"]]
                with sem_excecao():
                    frame, meta = await cepea_api.indicador(
                        produto, inicio=min(datas), fim=max(datas), return_meta=True
                    )
                assert sorted(
                    zip(
                        frame["data"].dt.strftime("%Y-%m-%d"),
                        frame["praca"],
                        frame["valor"],
                        frame["unidade"],
                        frame["fonte"],
                        strict=True,
                    )
                ) == sorted(
                    (data, praca, float(valor), unidade, "noticias_agricolas")
                    for data, praca, valor, unidade, *_ in linhas_do_oraculo(caso)
                )
                assert (meta.selected_source, meta.attempted_sources, meta.parser_version) == (
                    ("cache", ["cache"], NA)
                    if do_cache
                    else ("noticias_agricolas", ["cepea", "noticias_agricolas"], NA)
                )
    assert reusos >= 1
    assert {url for url in pedidos if url.startswith(COTACOES)} == {
        f"{COTACOES}/{constants.NOTICIAS_AGRICOLAS_PRODUTOS[produto]}"
        for produto, caso in CASOS.items()
        if caso["fallback_cepea"]
    }
