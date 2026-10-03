from __future__ import annotations

import dataclasses
import gzip
import json
import sys
import types
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from agrobr.bruto import models, registry

URL_ARQUIVO = "https://fonte.test/arquivos/AL.zip"
URL_WFS = "https://fonte.test/wfs"
CRS_URN = "urn:ogc:def:crs:EPSG::4674"


def resposta(status: int, corpo: bytes, cabecalhos: dict[str, str] | None = None) -> httpx.Response:
    """Resposta em stream, como a da rede: ``content=`` faria o httpx ler o corpo na criação."""
    return httpx.Response(status, headers=cabecalhos, stream=httpx.ByteStream(corpo))


ZIP_SINTETICO = b"PK\x03\x04" + bytes(range(256)) * 40 + b"PK\x05\x06" + b"\x00" * 18


class FonteFalsa:
    """Servidor falso: arquivo ZIP e um WFS com contagem (hits) e páginas GeoJSON por ``startIndex``."""

    def __init__(self, ids: list[Any] | None = None) -> None:
        self.ids: list[Any] = list(range(1, 6)) if ids is None else ids
        self.total_depois: int | None = None
        self.total_pagina: int | str | None = None
        self.crs = CRS_URN
        self.zip = ZIP_SINTETICO
        self.status_arquivo = 200
        self.respostas: dict[str, list[httpx.Response]] = {}
        self.pedidos: list[httpx.Request] = []
        self.contagens = 0
        self.gzip_http = False

    def responder(self, request: httpx.Request) -> httpx.Response:
        self.pedidos.append(request)
        fila = self.respostas.get(request.url.path)
        if fila:
            return fila.pop(0)
        if request.url.path.endswith(".zip"):
            cabecalhos = {
                "ETag": 'W/"abc"',
                "Last-Modified": "Fri, 02 Oct 2026 20:00:00 GMT",
                "Set-Cookie": "s=1",
            }
            return resposta(self.status_arquivo, self.zip, cabecalhos)
        params = request.url.params
        if params.get("resultType") == "hits":
            self.contagens += 1
            total = len(self.ids)
            if self.contagens > 1 and self.total_depois is not None:
                total = self.total_depois
            corpo = f'<wfs:FeatureCollection numberMatched="{total}" numberReturned="0"/>'.encode()
            return resposta(200, corpo, {"Content-Type": "text/xml"})
        inicio, quantidade = int(params["startIndex"]), int(params["count"])
        feicoes = [
            {
                "type": "Feature",
                "properties": {"cd_mun": i, "nome": "São José\r\n"},
                "geometry": None,
            }
            for i in self.ids[inicio : inicio + quantidade]
        ]
        envelope = {
            "type": "FeatureCollection",
            "totalFeatures": len(self.ids) if self.total_pagina is None else self.total_pagina,
            "features": feicoes,
            "crs": {"type": "name", "properties": {"name": self.crs}},
        }
        corpo = json.dumps(envelope, ensure_ascii=False, indent=1).encode("utf-8")
        cabecalhos = {"Content-Type": "application/json"}
        if self.gzip_http:
            corpo = gzip.compress(corpo)
            cabecalhos["Content-Encoding"] = "gzip"
        return resposta(200, corpo, cabecalhos)

    def cliente(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.responder))


class AdaptadorArquivo:
    def __init__(self, fonte: FonteFalsa) -> None:
        self.fonte = fonte

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=URL_ARQUIVO,
            parametros={},
            selecao=models.Selecao(
                uf=pedido.uf, bbox=None, bbox_crs=None, camada=None, edicao=None, natureza="publico"
            ),
            modo="arquivo",
            formato="zip",
            opcoes=models.Opcoes(tamanho_pagina=None, compactar=False, limites=pedido.limites),
            campo_id=None,
            crs_esperado=None,
        )

    async def adquirir(self, plano: models.PlanoBruto, *, contexto: Any) -> models.ConclusaoBruta:
        async with self.fonte.cliente() as http:
            pedido = models.PedidoHTTP(
                url=plano.url_solicitada, parametros={}, papel="arquivo", numero=1, formato="zip"
            )
            resposta = await contexto.obter(http, pedido)
        if resposta.http_status == 404:
            return models.ConclusaoBruta(status="ausente_na_fonte")
        contexto.registrar_arquivo(resposta)
        return models.ConclusaoBruta(status="ok")


class AdaptadorPaginas:
    """Adaptador WFS mínimo: contagem antes, páginas por offset até o total e contagem depois."""

    def __init__(self, fonte: FonteFalsa, *, crs_lido: str | None = "EPSG:4674") -> None:
        self.fonte = fonte
        self.crs_lido = crs_lido

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=URL_WFS,
            parametros={"typeNames": "camada", "outputFormat": "application/json"},
            selecao=models.Selecao(
                uf=pedido.uf,
                bbox=pedido.bbox,
                bbox_crs=pedido.bbox_crs,
                camada="camada",
                edicao=2025,
                natureza=None,
            ),
            modo="paginado",
            formato="geojson",
            opcoes=models.Opcoes(
                tamanho_pagina=pedido.tamanho_pagina,
                compactar=pedido.compactar,
                limites=pedido.limites,
            ),
            campo_id="cd_mun",
            crs_esperado="EPSG:4674",
        )

    async def _contagem(
        self, contexto: Any, http: httpx.AsyncClient, plano: Any, papel: Any
    ) -> int:
        numero = len(contexto.controles) + 1
        pedido = models.PedidoHTTP(
            url=URL_WFS,
            parametros={**plano.parametros, "resultType": "hits"},
            papel=papel,
            numero=numero,
            formato="xml",
        )
        resposta = await contexto.obter(http, pedido)
        total = int(resposta.corpo.split(b'numberMatched="')[1].split(b'"')[0])
        contexto.registrar_controle(
            resposta, models.LeituraControle(papel=papel, valor_declarado=total)
        )
        return total

    async def adquirir(self, plano: models.PlanoBruto, *, contexto: Any) -> models.ConclusaoBruta:
        tamanho = plano.opcoes.tamanho_pagina
        async with self.fonte.cliente() as http:
            total = await self._contagem(contexto, http, plano, "contagem_antes")
            inicio, numero = 0, 1
            while inicio < total:
                parametros = {**plano.parametros, "count": str(tamanho), "startIndex": str(inicio)}
                pedido = models.PedidoHTTP(
                    url=URL_WFS,
                    parametros=parametros,
                    papel="pagina",
                    numero=numero,
                    formato="geojson",
                    paginacao=models.PaginacaoOffset(
                        tipo="offset", inicio=inicio, quantidade=tamanho
                    ),
                )
                resposta = await contexto.obter(http, pedido)
                envelope = json.loads(resposta.corpo)
                feicoes = envelope["features"]
                valor = envelope["crs"]["properties"]["name"]
                contexto.registrar_pagina(
                    resposta,
                    models.LeituraPagina(
                        feicoes_recebidas=len(feicoes),
                        ids=tuple(f["properties"]["cd_mun"] for f in feicoes),
                        total_declarado=envelope["totalFeatures"],
                        crs=self.crs_lido if valor == CRS_URN else valor,
                        crs_localizador="/crs/properties/name",
                        crs_valor=valor,
                    ),
                )
                if not feicoes:
                    break
                inicio += len(feicoes)
                numero += 1
            await self._contagem(contexto, http, plano, "contagem_depois")
        return models.ConclusaoBruta(status="ok")


ARQUIVO = ("acervo_fundiario", "snci_publico")
PAGINADO = ("ibge", "malha_municipal")


@pytest.fixture
def fonte() -> FonteFalsa:
    return FonteFalsa()


@pytest.fixture
def habilitar(monkeypatch: pytest.MonkeyPatch) -> Callable[[tuple[str, str], object], None]:
    def instalar(chave: tuple[str, str], adaptador: object) -> None:
        nome = f"agrobr_bruto_teste_{chave[0]}_{chave[1]}"
        modulo = types.ModuleType(nome)
        modulo.adaptador = adaptador  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, nome, modulo)
        entrada = dataclasses.replace(registry.RECURSOS[chave], modulo=nome, habilitado=True)
        monkeypatch.setitem(registry.RECURSOS, chave, entrada)

    return instalar


@pytest.fixture
def arquivo(fonte: FonteFalsa, habilitar: Callable[..., None]) -> FonteFalsa:
    habilitar(ARQUIVO, AdaptadorArquivo(fonte))
    return fonte


@pytest.fixture
def paginado(fonte: FonteFalsa, habilitar: Callable[..., None]) -> FonteFalsa:
    habilitar(PAGINADO, AdaptadorPaginas(fonte))
    return fonte


def manifesto(destino: Any) -> list[dict[str, Any]]:
    texto = (destino / "manifesto.jsonl").read_bytes().decode("utf-8")
    return [json.loads(linha) for linha in texto.splitlines()]
