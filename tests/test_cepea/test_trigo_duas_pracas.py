from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import constants, datasets
from agrobr.cache import duckdb_store
from agrobr.cepea import api
from tests.helpers import conferir_corpo, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data" / "cepea" / "trigo_duas_pracas_20260925"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
LINHAS = MANIFESTO["rows"]
DATAS = [linha["data"] for linha in LINHAS]
JANELA = {"inicio": min(DATAS), "fim": max(DATAS)}
CEPEA_HOSTS = {httpx.URL(endpoint).host for endpoint in constants._CEPEA_ENDPOINTS}
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


def cepea_no_ar(monkeypatch: pytest.MonkeyPatch) -> None:
    corpo = (GOLDEN / MANIFESTO["file"]).read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == MANIFESTO["sha256"]

    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.host in CEPEA_HOSTS:
            cabecalhos = {"content-type": "text/html; charset=utf-8"}
            return httpx.Response(200, content=corpo, headers=cabecalhos)
        return httpx.Response(503, content=b"unavailable")

    class Cliente(_CLIENTE_REAL):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(responder)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Cliente)
    monkeypatch.setattr(api, "_today", lambda: date(2026, 9, 25))


@pytest.mark.usefixtures("store")
async def test_pagina_traz_o_sha256_completo_e_o_horario_da_coleta(
    monkeypatch: pytest.MonkeyPatch,
):
    cepea_no_ar(monkeypatch)
    with sem_excecao():
        _, meta = await api.indicador("trigo", **JANELA, force_refresh=True, return_meta=True)

    assert meta.source == "cepea"
    conferir_corpo(meta, (GOLDEN / MANIFESTO["file"]).read_bytes())


@pytest.mark.usefixtures("store")
async def test_trigo_sai_nas_duas_pracas_da_pagina(monkeypatch: pytest.MonkeyPatch):
    cepea_no_ar(monkeypatch)
    with sem_excecao():
        frame = await api.indicador("trigo", **JANELA)
        rio_grande = await api.indicador("trigo", **JANELA, praca="rio_grande_do_sul")
        pracas = await api.pracas("trigo")
        dataset = await datasets.preco_diario("trigo", **JANELA)

    assert sorted(
        zip(
            frame["data"].dt.strftime("%Y-%m-%d"),
            frame["praca"],
            frame["valor"],
            frame["valor_usd"],
            frame["unidade"],
            frame["fonte"],
            strict=True,
        )
    ) == sorted(
        (
            linha["data"],
            linha["praca"],
            float(linha["valor"]),
            float(linha["valor_usd"]),
            "BRL/ton",
            "cepea",
        )
        for linha in LINHAS
    )
    assert pracas == ["parana", "rio_grande_do_sul"]
    assert sorted(zip(rio_grande["data"].dt.strftime("%Y-%m-%d"), rio_grande["valor"])) == sorted(
        (linha["data"], float(linha["valor"]))
        for linha in LINHAS
        if linha["praca"] == "Rio Grande do Sul"
    )
    assert (set(dataset["praca"]), len(dataset)) == ({"Paraná"}, len(set(DATAS)))
