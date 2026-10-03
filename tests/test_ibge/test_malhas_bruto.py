from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import bruto
from agrobr.bruto import registry, validation
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.ibge import bruto as ibge_bruto
from agrobr.ibge import malhas
from tests.test_bruto.conftest import manifesto, resposta

GOLDEN = Path(__file__).parents[1] / "golden_data" / "ibge"
ALVOS = {
    "malha_municipal": "bruto_malha_4674_20261003",
    "areas_urbanizadas": "bruto_urbana_4674_20261003",
}


class Wfs:
    """Servidor falso que devolve os corpos oficiais do golden; ``trocar`` altera um corpo só em memória."""

    def __init__(self, pasta: Path) -> None:
        self.metadata = json.loads((pasta / "metadata.json").read_text("utf-8"))
        self.esperado = json.loads((pasta / "expected.json").read_text("utf-8"))
        self.hits = [(pasta / "hits.xml").read_bytes(), (pasta / "hits_depois.xml").read_bytes()]
        self.paginas = {
            info["start_index"]: (pasta / nome).read_bytes()
            for nome, info in self.metadata["files"].items()
            if info["papel"] == "pagina"
        }
        self.pedidos: list[dict[str, str]] = []

    def responder(self, request: httpx.Request) -> httpx.Response:
        parametros = dict(request.url.params)
        self.pedidos.append(parametros)
        if parametros.get("resultType") == "hits":
            corpo = self.hits[0] if len(self.hits) == 1 else self.hits.pop(0)
            return resposta(200, corpo, {"Content-Type": "text/xml; charset=UTF-8"})
        corpo = self.paginas[int(parametros["startIndex"])]
        return resposta(200, corpo, {"Content-Type": "application/json;charset=UTF-8"})


def _selecao(wfs: Wfs) -> dict[str, Any]:
    selecao = wfs.metadata["selecao"]
    kwargs = {
        "bbox": tuple(selecao["bbox"]),
        "bbox_crs": selecao["bbox_crs"],
        "nome": selecao["nome"],
    }
    return kwargs | ({"uf": selecao["uf"]} if selecao["uf"] else {})


@pytest.fixture
def servir(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], Wfs]:
    original = httpx.AsyncClient

    def instalar(recurso: str) -> Wfs:
        wfs = Wfs(GOLDEN / ALVOS[recurso])

        def fabrica(**kwargs: Any) -> httpx.AsyncClient:
            return original(transport=httpx.MockTransport(wfs.responder), **kwargs)

        monkeypatch.setattr(ibge_bruto.httpx, "AsyncClient", fabrica)
        chave = ("ibge", recurso)
        entrada = dataclasses.replace(registry.RECURSOS[chave], habilitado=True)
        monkeypatch.setitem(registry.RECURSOS, chave, entrada)
        return wfs

    return instalar


async def _coletar(recurso: str, wfs: Wfs, destino: Path) -> Any:
    return await bruto.coletar(
        "ibge", recurso, destino=destino, tamanho_pagina=2, compactar=False, **_selecao(wfs)
    )


@pytest.mark.parametrize("recurso", sorted(ALVOS))
async def test_corpos_oficiais_guardados_com_contagem_ordem_e_crs_conferidos(
    servir, tmp_path, recurso
):
    wfs = servir(recurso)
    camada = ibge_bruto.CAMADAS[recurso]

    entrada = (await _coletar(recurso, wfs, tmp_path)).entrada

    esperado = wfs.esperado
    assert (entrada.status, entrada.crs, entrada.formato) == ("ok", "EPSG:4674", "geojson")
    assert entrada.cobertura.estado == "conferida" and entrada.cobertura.campo_id == camada.ordem
    assert entrada.cobertura.recebidas == esperado["total_antes"] == len(esperado["ids"])
    assert (entrada.selecao.camada, entrada.selecao.edicao) == (camada.typename, camada.edicao)
    assert entrada.parametros == wfs.metadata["parametros"]
    assert entrada.crs_evidencia is not None
    assert entrada.crs_evidencia.valor == "urn:ogc:def:crs:EPSG::4674"
    assert [p.paginacao.inicio for p in entrada.paginas] == [
        p["start_index"] for p in esperado["paginas"]
    ]
    for pagina, (nome, info) in zip(
        entrada.paginas,
        [(n, i) for n, i in wfs.metadata["files"].items() if i["papel"] == "pagina"],
        strict=True,
    ):
        assert (pagina.sha256, pagina.bytes) == (info["sha256"], info["bytes"])
        assert (tmp_path / pagina.arquivo).read_bytes() == (
            GOLDEN / ALVOS[recurso] / nome
        ).read_bytes()
    pedidos = [p for p in wfs.pedidos if "resultType" not in p]
    assert all("propertyName" not in p and "srsName" not in p for p in wfs.pedidos)
    assert [p["sortBy"] for p in pedidos] == [camada.ordem] * len(pedidos)
    assert [p.get("resultType") for p in wfs.pedidos].count("hits") == 2


@pytest.mark.parametrize("crs", ["EPSG:4674", "EPSG:4326"])
def test_filtros_da_malha_uf_e_bbox_no_crs_pedido(crs):
    bbox = (-35.8, -9.7, -35.65, -9.55)
    so_uf = ibge_bruto.adaptador.planejar(_pedido("malha_municipal", uf="AL"))
    com_bbox = ibge_bruto.adaptador.planejar(
        _pedido("malha_municipal", uf="AL", bbox=bbox, nome="x", bbox_crs=crs)
    )
    brasil = ibge_bruto.adaptador.planejar(_pedido("malha_municipal"))

    assert so_uf.parametros["CQL_FILTER"] == "sigla_uf='AL'"
    assert com_bbox.parametros["CQL_FILTER"] == (
        f"sigla_uf='AL' AND BBOX(geom,-35.8,-9.7,-35.65,-9.55,'{crs}')"
    )
    assert com_bbox.selecao.bbox_crs == crs
    assert "CQL_FILTER" not in brasil.parametros


def test_api_tabular_e_geo_continuam_em_4326_com_os_mesmos_tetos():
    consulta = malhas.Consulta(malhas.MALHA_MUNICIPAL, uf="AL", bbox=(-35.8, -9.7, -35.65, -9.55))

    assert consulta.filtro_cql().endswith("'EPSG:4326')")
    assert "srsName=EPSG:4326" in consulta.url_feicoes(geo=True)
    assert (malhas.MALHA_MUNICIPAL.max_tabular, malhas.MALHA_MUNICIPAL.max_geo) == (10_000, 900)
    assert (malhas.AREAS_URBANIZADAS.max_tabular, malhas.AREAS_URBANIZADAS.max_geo) == (
        50_000,
        10_000,
    )


async def test_uf_em_areas_urbanizadas_recusada_antes_da_rede(servir, tmp_path):
    wfs = servir("areas_urbanizadas")

    with pytest.raises(InvalidParameterError, match="não aceita uf"):
        await bruto.coletar("ibge", "areas_urbanizadas", uf="AL", destino=tmp_path)

    assert wfs.pedidos == []


def _pedido(recurso: str, **kwargs: Any) -> Any:
    padrao = {"nome": None, "uf": None, "bbox": None, "bbox_crs": "EPSG:4674"}
    argumentos = padrao | kwargs
    return validation.pedido(
        registry.RECURSOS[("ibge", recurso)],
        tamanho_pagina=None,
        compactar=True,
        retomar=False,
        limites=None,
        **argumentos,
    )


def _feicoes(corpo: bytes, mudar: Callable[[dict[str, Any]], None]) -> bytes:
    colecao = json.loads(corpo)
    mudar(colecao)
    return json.dumps(colecao).encode()


def _curta(colecao: dict[str, Any]) -> None:
    colecao["features"] = colecao["features"][:1]
    colecao["numberReturned"] = 1


def _sem_chave(colecao: dict[str, Any]) -> None:
    colecao["features"][1]["properties"].pop("cd_mun")


def _crs_4326(colecao: dict[str, Any]) -> None:
    colecao["crs"]["properties"]["name"] = "urn:ogc:def:crs:EPSG::4326"


def _devolvidas_erradas(colecao: dict[str, Any]) -> None:
    colecao["numberReturned"] = 3


NAO_COMPROVADA: set[str] = {"contagem_unknown", "number_returned_errado", "excecao_ogc_em_200"}

MUTANTES: dict[str, Callable[[Wfs], None]] = {
    "pagina_curta_antes_do_fim": lambda w: w.paginas.update({2: _feicoes(w.paginas[2], _curta)}),
    "pagina_repetida": lambda w: w.paginas.update({2: w.paginas[0]}),
    "ordem_regressiva": lambda w: w.paginas.update({0: w.paginas[2], 2: w.paginas[0]}),
    "id_faltando": lambda w: w.paginas.update({2: _feicoes(w.paginas[2], _sem_chave)}),
    "total_alterado": lambda w: w.hits.__setitem__(
        1, w.hits[1].replace(b'numberMatched="6"', b'numberMatched="7"')
    ),
    "crs_incompativel": lambda w: w.paginas.update({2: _feicoes(w.paginas[2], _crs_4326)}),
    "contagem_unknown": lambda w: w.hits.__setitem__(
        0, w.hits[0].replace(b'numberMatched="6"', b'numberMatched="unknown"')
    ),
    "number_returned_errado": lambda w: w.paginas.update(
        {0: _feicoes(w.paginas[0], _devolvidas_erradas)}
    ),
    "excecao_ogc_em_200": lambda w: w.paginas.update(
        {
            2: b'<?xml version="1.0"?><ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1"/>'
        }
    ),
}


@pytest.mark.parametrize("mutante", sorted(MUTANTES))
async def test_transporte_simulado_que_quebra_a_cobertura_vira_erro(servir, tmp_path, mutante):
    wfs = servir("malha_municipal")
    MUTANTES[mutante](wfs)

    with pytest.raises(ParseError):
        await _coletar("malha_municipal", wfs, tmp_path)

    (registro,) = manifesto(tmp_path)
    assert registro["status"] == "erro" and registro["cobertura"]["completa"] is False
    esperado = "nao_comprovada" if mutante in NAO_COMPROVADA else "divergente"
    assert registro["cobertura"]["estado"] == esperado
