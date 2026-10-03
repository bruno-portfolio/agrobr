from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from typing import Any, Literal

from lxml import etree

from agrobr.bruto import models, protocols
from agrobr.exceptions import ParseError

from . import client
from .models import PARSER_VERSION, SICAR_GEOM_COLUMN, WFS_BASE, WFS_VERSION, layer_name

CRS_NATIVO = "EPSG:4674"
ORDEM = "cod_imovel A,dat_criacao A"
_SEM_CONTAGEM = ("outputFormat", "sortBy")
_INSTANTE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z")
_EPSG = re.compile(
    r"(?:urn:ogc:def:crs:EPSG::|EPSG:|http://www\.opengis\.net/def/crs/EPSG/0/)(\d+)"
)


def _erro(mensagem: str) -> ParseError:
    return ParseError("sicar", PARSER_VERSION, mensagem)


def ler_contagem(corpo: bytes) -> int | Literal["unknown"]:
    parser = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True)
    try:
        raiz = etree.fromstring(corpo, parser=parser)
    except etree.XMLSyntaxError as exc:
        raise _erro(f"contagem WFS ilegível: {exc}") from exc
    if raiz.getroottree().docinfo.doctype:
        raise _erro("contagem WFS com DOCTYPE")
    valor = raiz.get("numberMatched")
    if etree.QName(raiz).localname != "FeatureCollection" or valor is None:
        texto = " ".join(raiz.itertext()).strip()[:300]
        raise _erro(f"contagem WFS sem numberMatched ({etree.QName(raiz).localname}): {texto}")
    if valor.isdigit():
        return int(valor)
    if valor == "unknown":
        return "unknown"
    raise _erro(f"numberMatched inválido na contagem: {valor!r}")


def ler_pagina(corpo: bytes) -> tuple[models.LeituraPagina, list[tuple[Any, Any]]]:
    """A chave da cobertura é o ``feature.id`` (cada versão conta); a ordem é a do par (``cod_imovel``, ``dat_criacao``)."""
    try:
        colecao = json.loads(corpo)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _erro(f"página WFS não é GeoJSON: {corpo[:200]!r}") from exc
    if (
        not isinstance(colecao, dict)
        or colecao.get("type") != "FeatureCollection"
        or not isinstance(colecao.get("features"), list)
    ):
        raise _erro("página WFS sem FeatureCollection com lista de features")
    feicoes = colecao["features"]
    if colecao.get("numberReturned") != len(feicoes):
        raise _erro(
            f"numberReturned={colecao.get('numberReturned')!r} e {len(feicoes)} feições na página"
        )
    if not all(isinstance(f, dict) and isinstance(f.get("properties"), dict) for f in feicoes):
        raise _erro("feição sem objeto properties")
    total = colecao.get("numberMatched")
    crs = colecao.get("crs")
    valor = crs.get("properties", {}).get("name") if isinstance(crs, dict) else None
    epsg = _EPSG.fullmatch(valor) if isinstance(valor, str) else None
    leitura = models.LeituraPagina(
        feicoes_recebidas=len(feicoes),
        ids=tuple(f.get("id") for f in feicoes),
        total_declarado=(
            total
            if (isinstance(total, int) and not isinstance(total, bool)) or total == "unknown"
            else None
        ),
        crs=f"EPSG:{epsg[1]}" if epsg else None,
        crs_localizador="/crs/properties/name" if isinstance(valor, str) else None,
        crs_valor=valor if isinstance(valor, str) else None,
    )
    ordem = [
        (f["properties"].get("cod_imovel"), f["properties"].get("dat_criacao")) for f in feicoes
    ]
    return leitura, ordem


def conferir_avanco(
    contexto: protocols.ContextoBruto,
    ordem: list[tuple[Any, Any]],
    anterior: tuple[str, datetime] | None,
    *,
    recebidas: int,
    pedidas: int,
    faltam: int,
) -> tuple[str, datetime] | None:
    """O par (``cod_imovel``, ``dat_criacao``) cresce estritamente: empate entre versões não tem ordem comprovada."""
    for cod_imovel, criacao in ordem:
        if (
            not isinstance(cod_imovel, str)
            or not isinstance(criacao, str)
            or not _INSTANTE.fullmatch(criacao)
        ):
            raise contexto.divergencia(f"par de ordenação inválido: ({cod_imovel!r}, {criacao!r})")
        try:
            par = (cod_imovel, datetime.fromisoformat(criacao.replace("Z", "+00:00")))
        except ValueError as exc:
            raise contexto.divergencia(
                f"dat_criacao fora do calendário: ({cod_imovel!r}, {criacao!r})"
            ) from exc
        if anterior is not None and par <= anterior:
            raise contexto.divergencia(
                f"({cod_imovel}, {criacao}) depois de ({anterior[0]}, {anterior[1].isoformat()}): "
                "ordem quebrada ou versões empatadas"
            )
        anterior = par
    if recebidas > pedidas or recebidas < min(pedidas, faltam):
        raise contexto.divergencia(
            f"página com {recebidas} feições: pedidas {pedidas} e faltavam {faltam} da contagem"
        )
    return anterior


class AdaptadorSicar:
    """WFS do SICAR por UF: contagem, páginas GeoJSON por ``startIndex`` na ordem (``cod_imovel``, ``dat_criacao``) e contagem.

    Guarda todas as versões de um imóvel e conta o ``feature.id``; pede todos os atributos no CRS nativo, com a sessão TLS e as
    pausas do client.
    """

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        assert pedido.uf is not None
        camada = f"sicar:{layer_name(pedido.uf)}"
        parametros = {
            "service": "WFS",
            "version": WFS_VERSION,
            "request": "GetFeature",
            "typeNames": camada,
            "outputFormat": "application/json",
            "sortBy": ORDEM,
        }
        if pedido.bbox is not None:
            minx, miny, maxx, maxy = pedido.bbox
            parametros["CQL_FILTER"] = (
                f"BBOX({SICAR_GEOM_COLUMN},{minx!r},{miny!r},{maxx!r},{maxy!r},'{pedido.bbox_crs}')"
            )
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=WFS_BASE,
            parametros=parametros,
            selecao=models.Selecao(
                uf=pedido.uf,
                bbox=pedido.bbox,
                bbox_crs=pedido.bbox_crs,
                camada=camada,
                edicao=None,
                natureza=None,
            ),
            modo="paginado",
            formato="geojson",
            opcoes=models.Opcoes(
                tamanho_pagina=pedido.tamanho_pagina,
                compactar=pedido.compactar,
                limites=pedido.limites,
            ),
            campo_id="feature.id",
            crs_esperado=CRS_NATIVO,
        )

    async def adquirir(
        self, plano: models.PlanoBruto, *, contexto: protocols.ContextoBruto
    ) -> models.ConclusaoBruta:
        tamanho = plano.opcoes.tamanho_pagina
        assert tamanho is not None
        contagem = {k: v for k, v in plano.parametros.items() if k not in _SEM_CONTAGEM}
        contagem["resultType"] = "hits"
        async with client.make_session() as http:

            async def contar(papel: Any, numero: int) -> int:
                pedido = models.PedidoHTTP(
                    url=plano.url_solicitada,
                    parametros=contagem,
                    papel=papel,
                    numero=numero,
                    formato="xml",
                )
                resposta = await contexto.obter(http, pedido)
                valor = ler_contagem(resposta.corpo or b"")
                contexto.registrar_controle(
                    resposta, models.LeituraControle(papel=papel, valor_declarado=valor)
                )
                if not isinstance(valor, int):
                    raise _erro(f"{papel}: numberMatched={valor!r} não permite paginar nem fechar")
                return valor

            total = await contar("contagem_antes", 1)
            inicio, numero = 0, 1
            anterior: tuple[str, datetime] | None = None
            while inicio < total:
                if numero > client.THROTTLE_AFTER_PAGE + 1:
                    await asyncio.sleep(client.THROTTLE_DELAY)
                    contexto.conferir_prazo()
                pedido = models.PedidoHTTP(
                    url=plano.url_solicitada,
                    parametros={
                        **plano.parametros,
                        "count": str(tamanho),
                        "startIndex": str(inicio),
                    },
                    papel="pagina",
                    numero=numero,
                    formato="geojson",
                    paginacao=models.PaginacaoOffset(
                        tipo="offset", inicio=inicio, quantidade=tamanho
                    ),
                )
                resposta = await contexto.obter(http, pedido)
                leitura, ordem = ler_pagina(resposta.corpo or b"")
                contexto.registrar_pagina(resposta, leitura)
                anterior = conferir_avanco(
                    contexto,
                    ordem,
                    anterior,
                    recebidas=leitura.feicoes_recebidas,
                    pedidas=tamanho,
                    faltam=total - inicio,
                )
                inicio += leitura.feicoes_recebidas
                numero += 1
            await contar("contagem_depois", 2)
        return models.ConclusaoBruta(status="ok")


adaptador = AdaptadorSicar()
