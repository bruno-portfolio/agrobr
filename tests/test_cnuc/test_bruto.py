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
from agrobr.cnuc import bruto as cnuc_bruto
from agrobr.cnuc import client
from agrobr.exceptions import ParseError
from tests.test_bruto.conftest import manifesto, resposta

PASTA = Path(__file__).parents[1] / "golden_data" / "cnuc" / "bruto_ucs_4674_20261003"
METADATA = json.loads((PASTA / "metadata.json").read_text("utf-8"))
ESPERADO = json.loads((PASTA / "expected.json").read_text("utf-8"))
BBOX = tuple(METADATA["selecao"]["bbox"])


class Mapserver:
    """Servidor falso que devolve os corpos oficiais do golden; os mutantes alteram um corpo só em memória."""

    def __init__(self) -> None:
        self.hits = [(PASTA / "hits.xml").read_bytes(), (PASTA / "hits_depois.xml").read_bytes()]
        self.paginas = {
            info["start_index"]: (PASTA / nome).read_bytes()
            for nome, info in METADATA["files"].items()
            if info["papel"] == "pagina"
        }
        self.pedidos: list[dict[str, str]] = []

    def responder(self, request: httpx.Request) -> httpx.Response:
        parametros = dict(request.url.params)
        self.pedidos.append(parametros)
        cabecalhos = {"Content-Type": 'text/xml; subtype="gml/3.2.1"; charset=UTF-8'}
        if parametros.get("RESULTTYPE") == "hits":
            corpo = self.hits[0] if len(self.hits) == 1 else self.hits.pop(0)
            return resposta(200, corpo, cabecalhos)
        return resposta(200, self.paginas[int(parametros["STARTINDEX"])], cabecalhos)


@pytest.fixture
def mapserver(monkeypatch: pytest.MonkeyPatch) -> Mapserver:
    servidor = Mapserver()
    original = httpx.AsyncClient

    def fabrica(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(servidor.responder), **kwargs)

    monkeypatch.setattr(cnuc_bruto.httpx, "AsyncClient", fabrica)
    chave = ("cnuc", "ucs")
    monkeypatch.setitem(
        registry.RECURSOS, chave, dataclasses.replace(registry.RECURSOS[chave], habilitado=True)
    )
    return servidor


async def _coletar(destino: Path) -> Any:
    return await bruto.coletar(
        "cnuc",
        "ucs",
        uf="AL",
        bbox=BBOX,
        nome=METADATA["selecao"]["nome"],
        destino=destino,
        tamanho_pagina=2,
        compactar=False,
    )


def _plano(**kwargs: Any) -> Any:
    argumentos = {"nome": None, "uf": None, "bbox": None, "bbox_crs": "EPSG:4674"} | kwargs
    pedido = validation.pedido(
        registry.RECURSOS[("cnuc", "ucs")],
        tamanho_pagina=None,
        compactar=True,
        retomar=False,
        limites=None,
        **argumentos,
    )
    return cnuc_bruto.adaptador.planejar(pedido)


async def test_gml_oficial_guardado_com_hits_numericos_e_cd_cnuc_ordenado(mapserver, tmp_path):
    entrada = (await _coletar(tmp_path)).entrada

    assert (entrada.status, entrada.crs, entrada.formato) == ("ok", "EPSG:4674", "gml")
    assert entrada.cobertura.estado == "conferida" and entrada.cobertura.campo_id == "cd_cnuc"
    assert (entrada.cobertura.total_antes, entrada.cobertura.recebidas) == (
        ESPERADO["total_antes"],
        len(ESPERADO["ids"]),
    )
    assert [p.total_declarado for p in entrada.paginas] == ["unknown", "unknown"]
    assert entrada.parametros == METADATA["parametros"]
    assert entrada.selecao.camada == "ms:ucs_selected"
    assert entrada.crs_evidencia is not None
    assert entrada.crs_evidencia.valor == "urn:ogc:def:crs:EPSG::4674"
    paginas = [(n, i) for n, i in METADATA["files"].items() if i["papel"] == "pagina"]
    for pagina, (nome, info) in zip(entrada.paginas, paginas, strict=True):
        assert (pagina.sha256, pagina.bytes) == (info["sha256"], info["bytes"])
        assert (tmp_path / pagina.arquivo).read_bytes() == (PASTA / nome).read_bytes()
    assert all("PROPERTYNAME" not in p and "SRSNAME" not in p for p in mapserver.pedidos)
    assert [p.get("STARTINDEX") for p in mapserver.pedidos] == [None, "0", "2", None]


@pytest.mark.parametrize("crs", ["EPSG:4674", "EPSG:4326"])
def test_filtro_com_limite_uc_uf_e_bbox_em_latitude_longitude(crs):
    plano = _plano(uf="AL", bbox=BBOX, nome="x", bbox_crs=crs)

    filtro = client.FiltroServidor(uf="AL", bbox=BBOX, bbox_srs=f"urn:ogc:def:crs:EPSG::{crs[5:]}")
    assert plano.parametros["FILTER"] == client.build_filter(filtro)
    assert "<fes:Literal>uc</fes:Literal>" in plano.parametros["FILTER"]
    assert f"<gml:Envelope srsName='urn:ogc:def:crs:EPSG::{crs[5:]}'>" in plano.parametros["FILTER"]
    assert "<gml:lowerCorner>-9.7 -35.8</gml:lowerCorner>" in plano.parametros["FILTER"]
    assert "PROPERTYNAME" not in plano.parametros and "SRSNAME" not in plano.parametros


def test_uf_composta_segue_a_semantica_da_api_e_brasil_so_limite_uc():
    assert _plano(uf="MT").parametros["FILTER"] == client.build_filter(
        client.FiltroServidor(uf="MT")
    )
    assert _plano().parametros["FILTER"] == client.build_filter(client.FiltroServidor())


def _trocar(indice: int, antigo: bytes, novo: bytes) -> Callable[[Mapserver], None]:
    def aplicar(servidor: Mapserver) -> None:
        assert antigo in servidor.paginas[indice]
        servidor.paginas[indice] = servidor.paginas[indice].replace(antigo, novo)

    return aplicar


def _hits(indice: int, novo: bytes) -> Callable[[Mapserver], None]:
    def aplicar(servidor: Mapserver) -> None:
        servidor.hits[indice] = servidor.hits[indice].replace(b'numberMatched="4"', novo)

    return aplicar


def _com_gml_id_sem_cd_cnuc(servidor: Mapserver) -> None:
    _trocar(0, b"<ms:ucs_selected>", b'<ms:ucs_selected gml:id="ucs_selected.1">')(servidor)
    _trocar(0, b"<ms:cd_cnuc>0000.27.0920</ms:cd_cnuc>", b"")(servidor)


NAO_COMPROVADA: set[str] = {"hits_unknown", "number_returned_errado", "excecao_ogc_em_200"}

MUTANTES: dict[str, Callable[[Mapserver], None]] = {
    "cd_cnuc_vazio": _trocar(0, b">0000.27.0920<", b"><"),
    "cd_cnuc_ausente_com_gml_id": _com_gml_id_sem_cd_cnuc,
    "cd_cnuc_repetido_entre_paginas": _trocar(2, b">0000.27.1607<", b">0000.27.0887<"),
    "ordem_regressiva": lambda s: s.paginas.update({0: s.paginas[2], 2: s.paginas[0]}),
    "crs_incompativel": _trocar(2, b"urn:ogc:def:crs:EPSG::4674", b"urn:ogc:def:crs:EPSG::4326"),
    "hits_unknown": _hits(0, b'numberMatched="unknown"'),
    "total_alterado": _hits(1, b'numberMatched="5"'),
    "number_returned_errado": _trocar(0, b'numberReturned="2"', b'numberReturned="3"'),
    "excecao_ogc_em_200": lambda s: s.paginas.update(
        {
            2: b'<?xml version="1.0"?><ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1"/>'
        }
    ),
}


@pytest.mark.parametrize("mutante", sorted(MUTANTES))
async def test_transporte_simulado_que_quebra_a_cobertura_vira_erro(mapserver, tmp_path, mutante):
    MUTANTES[mutante](mapserver)

    with pytest.raises(ParseError):
        await _coletar(tmp_path)

    (registro,) = manifesto(tmp_path)
    assert registro["status"] == "erro" and registro["cobertura"]["completa"] is False
    esperado = "nao_comprovada" if mutante in NAO_COMPROVADA else "divergente"
    assert registro["cobertura"]["estado"] == esperado
