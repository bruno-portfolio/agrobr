from __future__ import annotations

import gzip
import json
import ssl
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
from agrobr.funai import bruto as funai_bruto
from tests.test_bruto.conftest import manifesto, resposta

CRS_4674 = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4674"}}
HTML_403 = b"<html>\r\n<head><title>403 Forbidden</title></head>\r\n<body></body>\r\n</html>\r\n"


def _feicao(camada: str, gid: int) -> dict[str, Any]:
    return {
        "type": "Feature",
        "id": f"{camada}.fid-49aa9d51_1a106f512ac_{gid:x}",
        "geometry": {"type": "Point", "coordinates": [-52.1234567, -14.7654321]},
        "geometry_name": "the_geom",
        "properties": {"gid": gid, "terrai_codigo": 1000 + gid, "terrai_nome": "Ãrara\r\n"},
        "bbox": [-52.1234567, -14.7654321, -52.1234567, -14.7654321],
    }


class Geoserver:
    """GeoServer falso da FUNAI: ``resultType=hits`` dá 403; contagem é ``count=1`` sem ``startIndex``; páginas por ``startIndex``."""

    def __init__(self, gids: list[int], camada: str = "tis_poligonais") -> None:
        self.gids = gids
        self.camada = camada
        self.total_depois: int | None = None
        self.mudar: dict[int, Callable[[dict[str, Any]], None]] = {}
        self.trocar: dict[int, httpx.Response] = {}
        self.contagem: Callable[[dict[str, Any]], None] | None = None
        self.pedidos: list[dict[str, str]] = []
        self.enviados: list[bytes] = []
        self.verify: list[Any] = []

    def _colecao(self, fatia: list[int], total: int) -> dict[str, Any]:
        return {
            "type": "FeatureCollection",
            "features": [_feicao(self.camada, gid) for gid in fatia],
            "totalFeatures": total,
            "numberMatched": total,
            "numberReturned": len(fatia),
            "timeStamp": "2026-10-04T22:33:32.172Z",
            "crs": CRS_4674,
        }

    def responder(self, request: httpx.Request) -> httpx.Response:
        parametros = dict(request.url.params)
        self.pedidos.append(parametros)
        if parametros.get("resultType") == "hits":
            return resposta(403, HTML_403, {"Content-Type": "text/html"})
        if "startIndex" not in parametros:
            contagens = sum("startIndex" not in p for p in self.pedidos)
            total = len(self.gids)
            if contagens > 1 and self.total_depois is not None:
                total = self.total_depois
            colecao = self._colecao(self.gids[:1] if total else [], total)
            if self.contagem is not None:
                self.contagem(colecao)
            return self._json(colecao)
        inicio = int(parametros["startIndex"])
        if inicio in self.trocar:
            return self.trocar.pop(inicio)
        colecao = self._colecao(
            self.gids[inicio : inicio + int(parametros["count"])], len(self.gids)
        )
        if inicio in self.mudar:
            self.mudar[inicio](colecao)
        return self._json(colecao)

    def _json(self, colecao: dict[str, Any]) -> httpx.Response:
        corpo = json.dumps(colecao, ensure_ascii=False, separators=(",", ":")).encode()
        self.enviados.append(corpo)
        return resposta(200, corpo, {"Content-Type": "application/json;charset=UTF-8"})

    def paginas(self) -> list[dict[str, str]]:
        return [p for p in self.pedidos if "startIndex" in p]


@pytest.fixture
def servir(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Geoserver]:
    original = httpx.AsyncClient

    def instalar(gids: list[int], camada: str = "tis_poligonais") -> Geoserver:
        servidor = Geoserver(gids, camada)

        def fabrica(**kwargs: Any) -> httpx.AsyncClient:
            servidor.verify.append(kwargs.get("verify"))
            return original(transport=httpx.MockTransport(servidor.responder), **kwargs)

        monkeypatch.setattr(funai_bruto.httpx, "AsyncClient", fabrica)
        return servidor

    return instalar


def _linha_valida(destino: Any) -> dict[str, Any]:
    (registro,) = manifesto(destino)
    linha = json.dumps(registro, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert models.RecursoBruto.model_validate_json(linha).linha() == linha
    return registro


async def test_poligonos_paginados_por_gid_no_padrao_20_com_corpos_preservados(servir, tmp_path):
    servidor = servir(list(range(1, 46)))

    entrada = (await bruto.coletar("funai", "terras_indigenas", destino=tmp_path)).entrada

    assert (entrada.status, entrada.crs, entrada.formato) == ("ok", "EPSG:4674", "geojson")
    assert entrada.opcoes.tamanho_pagina == 20 and entrada.nome == "brasil"
    assert [p.feicoes_recebidas for p in entrada.paginas] == [20, 20, 5]
    assert [p.paginacao.inicio for p in entrada.paginas] == [0, 20, 40]
    assert (entrada.cobertura.estado, entrada.cobertura.campo_id) == ("conferida", "gid")
    assert entrada.cobertura.ids_distintos == entrada.feicoes == 45
    assert (entrada.selecao.camada, entrada.selecao.edicao) == ("Funai:tis_poligonais", None)
    assert entrada.parametros == {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": "Funai:tis_poligonais",
        "outputFormat": "application/json",
        "sortBy": "gid",
    }
    assert [(c.papel, c.formato, c.valor_declarado) for c in entrada.controles] == [
        ("contagem_antes", "json", 45),
        ("contagem_depois", "json", 45),
    ]
    assert all(c.parametros["count"] == "1" for c in entrada.controles)
    assert all(p.get("resultType") is None for p in servidor.pedidos)
    assert all("srsName" not in p and "propertyName" not in p for p in servidor.pedidos)
    assert [p["sortBy"] for p in servidor.pedidos] == ["gid"] * 5
    guardados = [
        (tmp_path / a.arquivo).read_bytes()
        for a in sorted([*entrada.paginas, *entrada.controles], key=lambda a: a.inicio)
    ]
    assert [gzip.decompress(corpo) for corpo in guardados] == servidor.enviados
    assert entrada.crs_evidencia is not None
    assert entrada.crs_evidencia.valor == "urn:ogc:def:crs:EPSG::4674"
    assert isinstance(servidor.verify[0], ssl.SSLContext)
    _linha_valida(tmp_path)


async def test_pontos_na_camada_propria_com_padrao_100(servir, tmp_path):
    servidor = servir(list(range(1, 164)), camada="tis_pontos")

    entrada = (
        await bruto.coletar("funai", "terras_indigenas_pontos", destino=tmp_path, compactar=False)
    ).entrada

    assert entrada.status == "ok" and entrada.opcoes.tamanho_pagina == 100
    assert [p.feicoes_recebidas for p in entrada.paginas] == [100, 63]
    assert {p["typeNames"] for p in servidor.pedidos} == {"Funai:tis_pontos"}
    _linha_valida(tmp_path)


async def test_zero_feicoes_confirmado_fecha_sem_paginas(servir, tmp_path):
    servidor = servir([])

    entrada = (await bruto.coletar("funai", "terras_indigenas", destino=tmp_path)).entrada

    assert (entrada.status, entrada.feicoes, entrada.paginas) == ("ok", 0, [])
    assert servidor.paginas() == []
    _linha_valida(tmp_path)


@pytest.mark.parametrize("selecao", [{"uf": "AM"}, {"bbox": (-60, -10, -59, -9), "nome": "x"}])
async def test_uf_e_bbox_recusadas_antes_da_rede(servir, tmp_path, selecao):
    servidor = servir([1])

    with pytest.raises(InvalidParameterError, match="não aceita"):
        await bruto.coletar("funai", "terras_indigenas", destino=tmp_path, **selecao)

    assert servidor.pedidos == []


def _gids(colecao: dict[str, Any], gids: list[Any]) -> None:
    for feicao, gid in zip(colecao["features"], gids, strict=True):
        feicao["properties"]["gid"] = gid


def _sem_gid(colecao: dict[str, Any]) -> None:
    del colecao["features"][3]["properties"]["gid"]


def _curta(colecao: dict[str, Any]) -> None:
    colecao["features"] = colecao["features"][:-1]
    colecao["numberReturned"] = len(colecao["features"])


def _crs(colecao: dict[str, Any]) -> None:
    colecao["crs"] = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}}


def _devolvidas(colecao: dict[str, Any]) -> None:
    colecao["numberReturned"] += 1


def _total_pagina(colecao: dict[str, Any]) -> None:
    colecao["numberMatched"] += 1


def _sem_total(colecao: dict[str, Any]) -> None:
    del colecao["numberMatched"]


MUTANTES: dict[str, Callable[[Geoserver], None]] = {
    "ordem_regressiva": lambda s: s.mudar.update({5: lambda c: _gids(c, [10, 9, 8, 7, 6])}),
    "gid_repetido_entre_paginas": lambda s: s.mudar.update(
        {5: lambda c: _gids(c, [5, 7, 8, 9, 10])}
    ),
    "gid_em_texto": lambda s: s.mudar.update({5: lambda c: _gids(c, ["6", "7", "8", "9", "10"])}),
    "gid_faltando": lambda s: s.mudar.update({5: _sem_gid}),
    "pagina_curta_antes_do_fim": lambda s: s.mudar.update({0: _curta}),
    "total_alterado_depois": lambda s: setattr(s, "total_depois", 13),
    "crs_4326": lambda s: s.mudar.update({5: _crs}),
    "total_da_pagina_difere": lambda s: s.mudar.update({5: _total_pagina}),
    "number_returned_errado": lambda s: s.mudar.update({0: _devolvidas}),
    "contagem_sem_number_matched": lambda s: setattr(s, "contagem", _sem_total),
    "html_em_200": lambda s: s.trocar.update(
        {5: resposta(200, b"<html><body>manutencao</body></html>", {"Content-Type": "text/html"})}
    ),
}
NAO_COMPROVADA = {"number_returned_errado", "contagem_sem_number_matched", "html_em_200"}


@pytest.mark.parametrize("mutante", sorted(MUTANTES))
async def test_resposta_que_quebra_ordem_contagem_ou_formato_vira_erro(servir, tmp_path, mutante):
    servidor = servir(list(range(1, 13)))
    MUTANTES[mutante](servidor)

    with pytest.raises(ParseError):
        await bruto.coletar("funai", "terras_indigenas", destino=tmp_path, tamanho_pagina=5)

    registro = _linha_valida(tmp_path)
    assert registro["status"] == "erro" and registro["erro"]["tipo"] == "ParseError"
    esperado = "nao_comprovada" if mutante in NAO_COMPROVADA else "divergente"
    assert registro["cobertura"]["estado"] == esperado


async def test_pagina_acima_do_teto_vira_erro_de_limite(servir, tmp_path):
    servir(list(range(1, 13)))

    with pytest.raises(ResourceLimitError):
        await bruto.coletar(
            "funai",
            "terras_indigenas",
            destino=tmp_path,
            tamanho_pagina=5,
            limites=bruto.LimitesBrutos(max_bytes_pagina=1000),
        )

    registro = _linha_valida(tmp_path)
    assert (registro["status"], registro["erro"]["tipo"]) == ("erro", "ResourceLimitError")
    assert registro["paginas"] == []


@pytest.mark.parametrize("status", [404, 403])
async def test_status_de_erro_em_pagina_vira_fonte_indisponivel(servir, tmp_path, status):
    servidor = servir(list(range(1, 13)))
    servidor.trocar[5] = resposta(status, b"<html>erro</html>", {"Content-Type": "text/html"})

    with pytest.raises(SourceUnavailableError):
        await bruto.coletar("funai", "terras_indigenas", destino=tmp_path, tamanho_pagina=5)

    registro = _linha_valida(tmp_path)
    assert (registro["status"], registro["erro"]["http_status"]) == ("erro", status)
    assert [p["feicoes_recebidas"] for p in registro["paginas"]] == [5]
