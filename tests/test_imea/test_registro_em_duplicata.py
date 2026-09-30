from __future__ import annotations

import hashlib
import json
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import imea
from agrobr.imea import client
from tests import helpers
from tests.test_imea import oficial

GOLDEN = Path(__file__).parents[1] / "golden_data" / "imea" / "duplicatas_20260926"
COTACOES = f"{oficial.BASE}/4/cotacoes"
INDICADORES = f"{oficial.BASE}/4/indicadores"
INDICADOR = "708192508838936580"
AVISO_DUPLICATAS = (
    "IMEA: {n} registro(s) publicados mais de uma vez pela fonte, iguais em todas as colunas, "
    f"saem uma vez só (indicadores {INDICADOR})"
)
AVISO_CHAVE = (
    "IMEA: 2 linha(s) repetem a chave (indicador, localidade, data, safra e unidade) com valores "
    f"diferentes e saem todas (indicadores {INDICADOR})"
)


def _manifest() -> dict[str, Any]:
    dados: dict[str, Any] = json.loads((GOLDEN / "manifest.json").read_bytes())
    for item in dados["arquivos"]:
        corpo = (GOLDEN / item["arquivo"]).read_bytes()
        assert hashlib.sha256(corpo).hexdigest() == item["sha256"], item["arquivo"]
    for nome, item in dados["recortes"].items():
        assert hashlib.sha256((GOLDEN / nome).read_bytes()).hexdigest() == item["sha256"], nome
    return dados


def _registros() -> list[dict[str, Any]]:
    dados: list[dict[str, Any]] = json.loads((GOLDEN / "cotacoes_4.json").read_bytes())
    return dados


def _servir(monkeypatch: pytest.MonkeyPatch, cotacoes: bytes) -> None:
    servidos = {COTACOES: cotacoes, INDICADORES: (GOLDEN / "indicadores_4.json").read_bytes()}

    def responder(request: httpx.Request) -> httpx.Response:
        corpo = servidos.get(str(request.url))
        if corpo is None:
            return httpx.Response(404, request=request)
        return httpx.Response(
            200,
            content=corpo,
            headers={"content-type": "application/json; charset=utf-8"},
            request=request,
        )

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(client, "httpx", namespace)


def _linha(registro: dict[str, Any], nomes: dict[str, str]) -> dict[str, Any]:
    return {
        "cadeia": "soja",
        "indicador_id": registro["IndicadorFinalId"],
        "indicador": nomes.get(registro["IndicadorFinalId"]),
        "localidade": registro["Localidade"],
        "valor": None if registro["Valor"] is None else float(registro["Valor"]),
        "variacao": None if registro["Variacao"] is None else float(registro["Variacao"]),
        "safra": registro["Safra"],
        "unidade": registro["UnidadeSigla"],
        "unidade_descricao": registro["UnidadeDescricao"],
        "data_publicacao": None
        if registro["DataPublicacao"] is None
        else pd.Timestamp(registro["DataPublicacao"]),
    }


def _unicas(registros: list[dict[str, Any]]) -> list[dict[str, Any]]:
    catalogo = json.loads((GOLDEN / "indicadores_4.json").read_bytes())
    nomes = {item["Id"]: item["Nome"] for item in catalogo}
    linhas = [_linha(registro, nomes) for registro in registros]
    return _ordem(
        list({json.dumps(linha, sort_keys=True, default=str): linha for linha in linhas}.values())
    )


def _ordem(linhas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(linhas, key=lambda linha: json.dumps(linha, sort_keys=True, default=str))


async def _cotacoes(**filtro: str) -> tuple[Any, Any, list[str]]:
    with warnings.catch_warnings(record=True) as avisos, helpers.sem_excecao():
        warnings.simplefilter("always")
        df, meta = await imea.cotacoes("soja", return_meta=True, **filtro)
    return (
        df,
        meta,
        [
            str(aviso.message)
            for aviso in avisos
            if "mais de uma vez" in str(aviso.message) or "repetem a chave" in str(aviso.message)
        ],
    )


async def test_registro_publicado_em_duplicata_sai_uma_vez(monkeypatch):
    manifest = _manifest()
    catalogo = next(a for a in manifest["arquivos"] if a["arquivo"] == "indicadores_4.json")
    _servir(monkeypatch, (GOLDEN / "cotacoes_4.json").read_bytes())
    registros = _registros()

    with helpers.collect_failures() as check:
        for filtro, aviso in (({}, [AVISO_DUPLICATAS.format(n=69)]), ({"unidade": "R$/sc"}, [])):
            with check(filtro):
                df, meta, avisos = await _cotacoes(**filtro)
                selecionados = [
                    r
                    for r in registros
                    if r["UnidadeSigla"] == filtro.get("unidade", r["UnidadeSigla"])
                ]
                esperado = _unicas(selecionados)
                assert _ordem(oficial.publicado(df)) == esperado
                assert len(df) == meta.records_count == len(esperado)
                assert meta.source_details["duplicatas_colapsadas"] == {
                    "linhas": manifest["duplicatas"]["linhas_a_mais"],
                    "indicadores": manifest["duplicatas"]["indicador_id"],
                }
                assert meta.source_details["chaves_repetidas"] == {"linhas": 0, "indicadores": []}
                assert (
                    meta.source_details.get("indicadores_url"),
                    meta.source_details.get("indicadores_sha256"),
                    meta.source_details.get("indicadores_bytes"),
                ) == (catalogo["url"], catalogo["sha256"], int(catalogo["bytes"]))
                assert avisos == aviso
    assert manifest["duplicatas"]["linhas_a_mais"] == 69
    assert manifest["duplicatas"]["indicador_id"] == [INDICADOR]
    assert len(_unicas(registros)) == manifest["duplicatas"]["registros_unicos"] == 67


async def test_chave_repetida_com_valor_diferente_sai_toda_com_aviso(monkeypatch):
    registros = _registros()
    copias = [
        i
        for i, r in enumerate(registros)
        if r["IndicadorFinalId"] == INDICADOR and r["Localidade"] == "Sorriso"
    ]
    assert len(copias) == 4
    registros[copias[-1]] = {**registros[copias[-1]], "Valor": registros[copias[-1]]["Valor"] + 1}
    _servir(monkeypatch, json.dumps(registros).encode("utf-8"))

    df, meta, avisos = await _cotacoes()

    esperado = _unicas(registros)
    assert _ordem(oficial.publicado(df)) == esperado
    assert len(df) == 68
    sorriso = df[(df["indicador_id"] == INDICADOR) & (df["localidade"] == "Sorriso")]
    assert sorted(sorriso["valor"]) == sorted({registros[i]["Valor"] for i in copias})
    assert meta.source_details["duplicatas_colapsadas"] == {
        "linhas": 68,
        "indicadores": [INDICADOR],
    }
    assert meta.source_details["chaves_repetidas"] == {"linhas": 2, "indicadores": [INDICADOR]}
    assert avisos == [AVISO_DUPLICATAS.format(n=68), AVISO_CHAVE]
