from __future__ import annotations

import gzip
import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from agrobr import bruto
from agrobr.bruto import models
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.incra import bruto as incra_bruto
from tests.test_bruto.conftest import manifesto, resposta

CRS_4674 = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4674"}}


def _hits(total: int | str) -> bytes:
    return (
        b'<?xml version="1.0" encoding="UTF-8"?><wfs:FeatureCollection '
        b'xmlns:wfs="http://www.opengis.net/wfs/2.0" numberMatched="%s" numberReturned="0" '
        b'timeStamp="2026-10-04T22:30:20.444Z"/>' % str(total).encode()
    )


def _feicao(numero: int) -> dict[str, Any]:
    return {
        "type": "Feature",
        "id": f"lim_quilombolas_a.{numero}",
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [[[[-48.71, -1.53], [-48.70, -1.52], [-48.71, -1.53]]]],
        },
        "geometry_name": "geom",
        "properties": {
            "cd_quilomb": None,
            "no_comunidade": "Comunidade São José\r\n",
            "sg_uf": "PA",
        },
        "bbox": [-48.71, -1.53, -48.70, -1.52],
    }


class Cmr:
    """GeoServer falso do CMR: ``resultType=hits`` em XML e a página GeoJSON sem ordem."""

    def __init__(self, total: int) -> None:
        self.feicoes = [_feicao(n) for n in range(1, total + 1)]
        self.hits = [_hits(total)]
        self.mudar: Callable[[dict[str, Any]], None] | None = None
        self.trocar: httpx.Response | None = None
        self.pedidos: list[dict[str, str]] = []
        self.enviados: list[bytes] = []

    def responder(self, request: httpx.Request) -> httpx.Response:
        parametros = dict(request.url.params)
        self.pedidos.append(parametros)
        if parametros.get("resultType") == "hits":
            corpo = self.hits[0] if len(self.hits) == 1 else self.hits.pop(0)
            self.enviados.append(corpo)
            return resposta(200, corpo, {"Content-Type": "text/xml"})
        if self.trocar is not None:
            return self.trocar
        fatia = self.feicoes[: int(parametros["count"])]
        colecao = {
            "type": "FeatureCollection",
            "features": fatia,
            "totalFeatures": len(self.feicoes),
            "numberMatched": len(self.feicoes),
            "numberReturned": len(fatia),
            "timeStamp": "2026-10-04T22:30:30.000Z",
            "crs": CRS_4674,
        }
        if self.mudar is not None:
            self.mudar(colecao)
        corpo = json.dumps(colecao, ensure_ascii=False, separators=(",", ":")).encode()
        self.enviados.append(corpo)
        return resposta(200, corpo, {"Content-Type": "application/json;charset=UTF-8"})

    def paginas(self) -> list[dict[str, str]]:
        return [p for p in self.pedidos if p.get("resultType") != "hits"]


@pytest.fixture
def servir(monkeypatch: pytest.MonkeyPatch) -> Callable[[int], Cmr]:
    original = httpx.AsyncClient

    def instalar(total: int) -> Cmr:
        servidor = Cmr(total)

        def fabrica(**kwargs: Any) -> httpx.AsyncClient:
            return original(transport=httpx.MockTransport(servidor.responder), **kwargs)

        monkeypatch.setattr(incra_bruto.httpx, "AsyncClient", fabrica)
        return servidor

    return instalar


def _linha_valida(destino: Any) -> dict[str, Any]:
    (registro,) = manifesto(destino)
    linha = json.dumps(registro, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert models.RecursoBruto.model_validate_json(linha).linha() == linha
    return registro


async def test_pagina_unica_sem_ordem_conferida_por_contagem_e_feature_id(servir, tmp_path):
    servidor = servir(7)

    entrada = (await bruto.coletar("incra", "quilombolas", destino=tmp_path)).entrada

    assert (entrada.status, entrada.crs, entrada.formato) == ("ok", "EPSG:4674", "geojson")
    assert (entrada.opcoes.tamanho_pagina, entrada.nome) == (1000, "brasil")
    assert (entrada.cobertura.estado, entrada.cobertura.campo_id) == ("conferida", "feature.id")
    assert entrada.cobertura.ids_distintos == entrada.feicoes == 7
    assert [p.paginacao.model_dump() for p in entrada.paginas] == [
        {"tipo": "offset", "inicio": 0, "quantidade": 1000}
    ]
    assert entrada.selecao.camada == "CMR-PUBLICO:lim_quilombolas_a"
    assert entrada.parametros == {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": "CMR-PUBLICO:lim_quilombolas_a",
        "outputFormat": "application/json",
    }
    (pagina,) = servidor.paginas()
    assert (pagina["count"], pagina["startIndex"]) == ("1000", "0")
    assert all("sortBy" not in p and "srsName" not in p for p in servidor.pedidos)
    assert [p.get("resultType") for p in servidor.pedidos] == ["hits", None, "hits"]
    assert [c.papel for c in entrada.controles] == ["contagem_antes", "contagem_depois"]
    guardados = [
        gzip.decompress((tmp_path / a.arquivo).read_bytes())
        for a in sorted([*entrada.paginas, *entrada.controles], key=lambda a: a.inicio)
    ]
    assert guardados == servidor.enviados
    _linha_valida(tmp_path)


async def test_zero_feicoes_confirmado_fecha_sem_pagina(servir, tmp_path):
    servidor = servir(0)

    entrada = (await bruto.coletar("incra", "quilombolas", destino=tmp_path)).entrada

    assert (entrada.status, entrada.feicoes, entrada.paginas) == ("ok", 0, [])
    assert servidor.paginas() == []
    _linha_valida(tmp_path)


async def test_total_acima_da_pagina_unica_vira_erro_sem_pedir_pagina(servir, tmp_path):
    servidor = servir(0)
    servidor.hits = [_hits(1001)]

    with pytest.raises(ParseError, match="acima da página única de 1000"):
        await bruto.coletar("incra", "quilombolas", destino=tmp_path)

    assert servidor.paginas() == []
    registro = _linha_valida(tmp_path)
    assert (registro["status"], registro["cobertura"]["estado"]) == ("erro", "nao_comprovada")


@pytest.mark.parametrize("selecao", [{"uf": "PA"}, {"bbox": (-49, -2, -48, -1), "nome": "x"}])
async def test_uf_e_bbox_recusadas_antes_da_rede(servir, tmp_path, selecao):
    servidor = servir(3)

    with pytest.raises(InvalidParameterError, match="não aceita"):
        await bruto.coletar("incra", "quilombolas", destino=tmp_path, **selecao)

    assert servidor.pedidos == []


def _anuncia_mais(colecao: dict[str, Any]) -> None:
    colecao["features"] = colecao["features"][:-1]
    colecao["numberReturned"] = len(colecao["features"])


def _id_repetido(colecao: dict[str, Any]) -> None:
    colecao["features"][2]["id"] = colecao["features"][0]["id"]


def _sem_id(colecao: dict[str, Any]) -> None:
    del colecao["features"][2]["id"]


def _crs(colecao: dict[str, Any]) -> None:
    colecao["crs"] = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}}


def _total_pagina(colecao: dict[str, Any]) -> None:
    colecao["numberMatched"] += 1


def _devolvidas(colecao: dict[str, Any]) -> None:
    colecao["numberReturned"] += 1


MUTANTES: dict[str, Callable[[Cmr], None]] = {
    "fonte_anuncia_mais_do_que_veio": lambda s: setattr(s, "mudar", _anuncia_mais),
    "feature_id_repetido": lambda s: setattr(s, "mudar", _id_repetido),
    "feature_id_faltando": lambda s: setattr(s, "mudar", _sem_id),
    "crs_4326": lambda s: setattr(s, "mudar", _crs),
    "total_da_pagina_difere": lambda s: setattr(s, "mudar", _total_pagina),
    "total_alterado_depois": lambda s: setattr(s, "hits", [_hits(7), _hits(8)]),
    "number_returned_errado": lambda s: setattr(s, "mudar", _devolvidas),
    "contagem_unknown": lambda s: setattr(s, "hits", [_hits("unknown")]),
    "html_em_200": lambda s: setattr(
        s, "trocar", resposta(200, b"<html>manutencao</html>", {"Content-Type": "text/html"})
    ),
}
NAO_COMPROVADA = {"number_returned_errado", "contagem_unknown", "html_em_200"}


@pytest.mark.parametrize("mutante", sorted(MUTANTES))
async def test_pagina_que_nao_fecha_contagem_ids_ou_formato_vira_erro(servir, tmp_path, mutante):
    servidor = servir(7)
    MUTANTES[mutante](servidor)

    with pytest.raises(ParseError):
        await bruto.coletar("incra", "quilombolas", destino=tmp_path)

    registro = _linha_valida(tmp_path)
    assert registro["status"] == "erro" and registro["erro"]["tipo"] == "ParseError"
    esperado = "nao_comprovada" if mutante in NAO_COMPROVADA else "divergente"
    assert registro["cobertura"]["estado"] == esperado


@pytest.mark.parametrize("valor", [None, "unknown", "8", "ausente", True, -1, 7.0])
async def test_pagina_unica_sem_number_matched_inteiro_nao_fecha(servir, tmp_path, valor):
    servidor = servir(7)

    def mudar(colecao: dict[str, Any]) -> None:
        if valor == "ausente":
            del colecao["numberMatched"]
        else:
            colecao["numberMatched"] = valor

    servidor.mudar = mudar

    with pytest.raises(ParseError, match="numberMatched"):
        await bruto.coletar("incra", "quilombolas", destino=tmp_path)

    registro = _linha_valida(tmp_path)
    assert (registro["status"], registro["cobertura"]["estado"]) == ("erro", "nao_comprovada")
    assert registro["paginas"] == []


async def test_pagina_acima_do_teto_vira_erro_de_limite(servir, tmp_path):
    servir(7)

    with pytest.raises(ResourceLimitError):
        await bruto.coletar(
            "incra",
            "quilombolas",
            destino=tmp_path,
            limites=bruto.LimitesBrutos(max_bytes_pagina=1000),
        )

    registro = _linha_valida(tmp_path)
    assert (registro["status"], registro["erro"]["tipo"], registro["paginas"]) == (
        "erro",
        "ResourceLimitError",
        [],
    )


async def test_404_na_pagina_vira_fonte_indisponivel(servir, tmp_path):
    servidor = servir(7)
    servidor.trocar = resposta(404, b"<html>404</html>", {"Content-Type": "text/html"})

    with pytest.raises(SourceUnavailableError):
        await bruto.coletar("incra", "quilombolas", destino=tmp_path)

    registro = _linha_valida(tmp_path)
    assert (registro["status"], registro["erro"]["http_status"]) == ("erro", 404)
