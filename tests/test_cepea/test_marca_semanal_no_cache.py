from __future__ import annotations

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
from agrobr.cepea import api
from agrobr.models import Indicador
from tests.helpers import sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data" / "noticias_agricolas" / "paginas_20260925"
CASO = next(
    caso
    for caso in json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))["cases"]
    if caso["produto"] == "etanol_hidratado"
)
DATAS = [linha["data"] for linha in CASO["linhas"]]
JANELA = {"inicio": min(DATAS), "fim": max(DATAS)}
NA_HOST = httpx.URL(constants.URLS[constants.Fonte.NOTICIAS_AGRICOLAS]["cotacoes"]).host
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
    corpo = (GOLDEN / CASO["file"]).read_bytes()
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
    return hosts


@pytest.mark.usefixtures("store")
async def test_marca_semanal_do_na_volta_do_cache_offline_e_morno(monkeypatch: pytest.MonkeyPatch):
    hosts = cepea_fora_do_ar(monkeypatch)
    monkeypatch.setattr(api, "_today", lambda: date(2026, 9, 25))
    with sem_excecao():
        coleta, meta_coleta = await api.indicador("etanol_hidratado", **JANELA, return_meta=True)
    semanas = ['["media_semanal"]'] * len(DATAS)
    assert (meta_coleta.selected_source, coleta["anomalies"].tolist()) == (
        "noticias_agricolas",
        semanas,
    )
    coletas = len(hosts)

    with sem_excecao():
        offline, meta_offline = await api.indicador(
            "etanol_hidratado", **JANELA, offline=True, return_meta=True
        )
    monkeypatch.setattr(api, "_today", lambda: date(2026, 11, 30))
    with sem_excecao():
        morno, meta_morno = await api.indicador("etanol_hidratado", **JANELA, return_meta=True)

    assert len(hosts) == coletas
    for rotulo, frame, meta in (("offline", offline, meta_offline), ("morno", morno, meta_morno)):
        assert (rotulo, meta.from_cache, frame["anomalies"].tolist()) == (rotulo, True, semanas)
        assert frame[["data", "valor"]].to_dict("list") == coleta[["data", "valor"]].to_dict("list")


def test_recoleta_atualiza_a_marca_da_mesma_linha(store: duckdb_store.DuckDBStore):
    semana = Indicador(
        fonte=constants.Fonte.NOTICIAS_AGRICOLAS,
        produto="etanol_hidratado",
        praca="São Paulo/SP",
        data=date(2026, 9, 25),
        valor=Decimal("2.5820"),
        unidade="BRL/L",
        parser_version=constants.NOTICIAS_AGRICOLAS_PARSER_VERSION,
    )
    store.indicadores_upsert(api._indicadores_to_dicts([semana]))
    marcada = semana.model_copy(update={"anomalies": ["media_semanal"]})
    store.indicadores_upsert(api._indicadores_to_dicts([marcada]))
    assert [linha["anomalies"] for linha in store.indicadores_query("etanol_hidratado")] == [
        ["media_semanal"]
    ]
