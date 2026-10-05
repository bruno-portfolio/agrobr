from __future__ import annotations

import json
from typing import Any, Literal

import httpx
from lxml import etree

from agrobr import constants
from agrobr.bruto import models, protocols
from agrobr.exceptions import ParseError
from agrobr.http.user_agents import UserAgentRotator

from . import _transport, parser

CRS_NATIVO = "EPSG:4674"
CAMPO = "feature.id"
URL = constants.URLS[constants.Fonte.INCRA]["geoserver"]
CAMADA = f"{constants.INCRA_NAMESPACE}:{constants.INCRA_LAYER}"
_URN = "urn:ogc:def:crs:EPSG::"


def _erro(mensagem: str) -> ParseError:
    return ParseError("incra", parser.PARSER_VERSION, mensagem)


def ler_contagem(corpo: bytes) -> int | Literal["unknown"]:
    leitor = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True)
    try:
        raiz = etree.fromstring(corpo, parser=leitor)
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


def ler_pagina(corpo: bytes) -> models.LeituraPagina:
    """A chave é o ``feature.id`` (a camada não publica atributo único nem ordenável); o ``numberMatched`` tem de ser inteiro."""
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
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise _erro(
            f"numberMatched={total!r} na página única: sem total inteiro, a página não prova que cobre a camada"
        )
    crs = colecao.get("crs")
    valor = crs.get("properties", {}).get("name") if isinstance(crs, dict) else None
    codigo = valor.removeprefix(_URN) if isinstance(valor, str) else ""
    return models.LeituraPagina(
        feicoes_recebidas=len(feicoes),
        ids=tuple(f.get("id") for f in feicoes),
        total_declarado=total,
        crs=f"EPSG:{codigo}" if codigo.isdigit() else None,
        crs_localizador="/crs/properties/name" if isinstance(valor, str) else None,
        crs_valor=valor if isinstance(valor, str) else None,
    )


class AdaptadorQuilombolas:
    """WFS do CMR: contagem, uma página GeoJSON nacional sem ordem e nova contagem.

    Sem campo ordenável único, paginar por ``startIndex`` não é demonstrável: a coleta é uma página de até
    ``tamanho_pagina`` (o máximo do contrato), fechada só com todas as feições anunciadas na página e ``feature.id``
    distintos. Pede todos os atributos no CRS nativo e não passa pelo parser tabular.
    """

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=URL,
            parametros={
                "service": "WFS",
                "version": constants.INCRA_WFS_VERSION,
                "request": "GetFeature",
                "typeNames": CAMADA,
                "outputFormat": "application/json",
            },
            selecao=models.Selecao(
                uf=pedido.uf,
                bbox=pedido.bbox,
                bbox_crs=pedido.bbox_crs,
                camada=CAMADA,
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
            campo_id=CAMPO,
            crs_esperado=CRS_NATIVO,
        )

    async def adquirir(
        self, plano: models.PlanoBruto, *, contexto: protocols.ContextoBruto
    ) -> models.ConclusaoBruta:
        tamanho = plano.opcoes.tamanho_pagina
        assert tamanho is not None
        contagem = {k: v for k, v in plano.parametros.items() if k != "outputFormat"}
        contagem["resultType"] = "hits"
        async with httpx.AsyncClient(
            headers=UserAgentRotator.get_bot_headers(),
            follow_redirects=True,
            timeout=_transport.TIMEOUT,
        ) as http:

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
                    raise _erro(f"{papel}: numberMatched={valor!r} não permite fechar a coleta")
                return valor

            total = await contar("contagem_antes", 1)
            if total > tamanho:
                raise _erro(
                    f"a fonte declara {total} feições, acima da página única de {tamanho}: sem campo "
                    "ordenável, a coleta não pode ser paginada; atualize o agrobr"
                )
            if total:
                pedido = models.PedidoHTTP(
                    url=plano.url_solicitada,
                    parametros={**plano.parametros, "count": str(tamanho), "startIndex": "0"},
                    papel="pagina",
                    numero=1,
                    formato="geojson",
                    paginacao=models.PaginacaoOffset(tipo="offset", inicio=0, quantidade=tamanho),
                )
                resposta = await contexto.obter(http, pedido)
                contexto.registrar_pagina(resposta, ler_pagina(resposta.corpo or b""))
            await contar("contagem_depois", 2)
        return models.ConclusaoBruta(status="ok")


adaptador = AdaptadorQuilombolas()
