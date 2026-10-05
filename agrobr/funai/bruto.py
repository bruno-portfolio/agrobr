from __future__ import annotations

import json
from typing import Any, Literal

import httpx

from agrobr import constants
from agrobr.bruto import models, protocols
from agrobr.exceptions import ParseError
from agrobr.http.user_agents import UserAgentRotator

from . import _tls, _transport, parser

CRS_NATIVO = "EPSG:4674"
CAMPO = "gid"
URL = constants.URLS[constants.Fonte.FUNAI]["geoserver"]
CAMADAS = {
    "terras_indigenas": f"{constants.FUNAI_NAMESPACE}:tis_poligonais",
    "terras_indigenas_pontos": f"{constants.FUNAI_NAMESPACE}:tis_pontos",
}
_URN = "urn:ogc:def:crs:EPSG::"


def _erro(mensagem: str) -> ParseError:
    return ParseError("funai", parser.PARSER_VERSION, mensagem)


def _colecao(corpo: bytes) -> dict[str, Any]:
    try:
        colecao = json.loads(corpo)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _erro(f"resposta WFS não é GeoJSON: {corpo[:200]!r}") from exc
    if (
        not isinstance(colecao, dict)
        or colecao.get("type") != "FeatureCollection"
        or not isinstance(colecao.get("features"), list)
    ):
        raise _erro("resposta WFS sem FeatureCollection com lista de features")
    feicoes = colecao["features"]
    if colecao.get("numberReturned") != len(feicoes):
        raise _erro(
            f"numberReturned={colecao.get('numberReturned')!r} e {len(feicoes)} feições na resposta"
        )
    if not all(isinstance(f, dict) and isinstance(f.get("properties"), dict) for f in feicoes):
        raise _erro("feição sem objeto properties")
    return colecao


def _total(colecao: dict[str, Any]) -> int | Literal["unknown"] | None:
    total = colecao.get("numberMatched")
    if (
        isinstance(total, int) and not isinstance(total, bool) and total >= 0
    ) or total == "unknown":
        return total
    return None


def ler_contagem(corpo: bytes) -> int | Literal["unknown"] | None:
    """O ``numberMatched`` de uma página de 1 feição: o GeoServer da FUNAI responde 403 ao ``resultType=hits``."""
    return _total(_colecao(corpo))


def ler_pagina(corpo: bytes) -> models.LeituraPagina:
    colecao = _colecao(corpo)
    feicoes = colecao["features"]
    crs = colecao.get("crs")
    valor = crs.get("properties", {}).get("name") if isinstance(crs, dict) else None
    codigo = valor.removeprefix(_URN) if isinstance(valor, str) else ""
    return models.LeituraPagina(
        feicoes_recebidas=len(feicoes),
        ids=tuple(f["properties"].get(CAMPO) for f in feicoes),
        total_declarado=_total(colecao),
        crs=f"EPSG:{codigo}" if codigo.isdigit() else None,
        crs_localizador="/crs/properties/name" if isinstance(valor, str) else None,
        crs_valor=valor if isinstance(valor, str) else None,
    )


def conferir_avanco(
    contexto: protocols.ContextoBruto,
    ids: tuple[Any, ...],
    anterior: int | None,
    *,
    recebidas: int,
    pedidas: int,
    faltam: int,
) -> int | None:
    """``gid`` é inteiro e cresce estritamente (a ordem do ``sortBy``); página curta só na última."""
    for valor in ids:
        if isinstance(valor, bool) or not isinstance(valor, int):
            raise contexto.divergencia(
                f"{CAMPO} {valor!r} não é inteiro: a ordem do servidor não pode ser conferida"
            )
        if anterior is not None and valor <= anterior:
            raise contexto.divergencia(
                f"{CAMPO} {valor} depois de {anterior}: ordem ou avanço da paginação quebrado"
            )
        anterior = valor
    if recebidas > pedidas or recebidas < min(pedidas, faltam):
        raise contexto.divergencia(
            f"página com {recebidas} feições: pedidas {pedidas} e faltavam {faltam} da contagem"
        )
    return anterior


class AdaptadorFunai:
    """WFS da FUNAI: contagem, páginas GeoJSON por ``startIndex`` na ordem de ``gid`` e nova contagem.

    A contagem é o ``numberMatched`` de uma página de 1 feição na mesma ordem (o ``resultType=hits`` é bloqueado). Pede todos
    os atributos no CRS nativo (sem ``propertyName`` nem ``srsName``) e não passa pelo parser tabular.
    """

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        camada = CAMADAS[pedido.recurso]
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=URL,
            parametros={
                "service": "WFS",
                "version": constants.FUNAI_WFS_VERSION,
                "request": "GetFeature",
                "typeNames": camada,
                "outputFormat": "application/json",
                "sortBy": CAMPO,
            },
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
            campo_id=CAMPO,
            crs_esperado=CRS_NATIVO,
        )

    async def adquirir(
        self, plano: models.PlanoBruto, *, contexto: protocols.ContextoBruto
    ) -> models.ConclusaoBruta:
        tamanho = plano.opcoes.tamanho_pagina
        assert tamanho is not None
        async with httpx.AsyncClient(
            headers=UserAgentRotator.get_bot_headers(),
            follow_redirects=True,
            timeout=_transport.TIMEOUT,
            verify=_tls.build_context(),
        ) as http:

            async def contar(papel: Any, numero: int) -> int:
                pedido = models.PedidoHTTP(
                    url=plano.url_solicitada,
                    parametros={**plano.parametros, "count": "1"},
                    papel=papel,
                    numero=numero,
                    formato="json",
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
            inicio, numero, anterior = 0, 1, None
            while inicio < total:
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
                leitura = ler_pagina(resposta.corpo or b"")
                contexto.registrar_pagina(resposta, leitura)
                anterior = conferir_avanco(
                    contexto,
                    leitura.ids,
                    anterior,
                    recebidas=leitura.feicoes_recebidas,
                    pedidas=tamanho,
                    faltam=total - inicio,
                )
                inicio += leitura.feicoes_recebidas
                numero += 1
            await contar("contagem_depois", 2)
        return models.ConclusaoBruta(status="ok")


adaptador = AdaptadorFunai()
