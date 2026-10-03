from __future__ import annotations

import json
from typing import Any, Literal

import httpx
from lxml import etree

from agrobr.bruto import models, protocols
from agrobr.exceptions import ParseError
from agrobr.http.user_agents import UserAgentRotator

from . import malhas

CRS_NATIVO = "EPSG:4674"
CAMADAS = {camada.nome: camada for camada in (malhas.MALHA_MUNICIPAL, malhas.AREAS_URBANIZADAS)}
_SEM_CONTAGEM = ("outputFormat", "sortBy")


def _erro(mensagem: str) -> ParseError:
    return ParseError("ibge", malhas.PARSER_VERSION, mensagem)


def epsg(valor: object) -> str | None:
    """``urn:ogc:def:crs:EPSG::4674``, ``EPSG:4674`` ou a URI OGC viram ``EPSG:4674``; o resto, ``None``."""
    if not isinstance(valor, str):
        return None
    for prefixo in ("urn:ogc:def:crs:EPSG::", "EPSG:", "http://www.opengis.net/def/crs/EPSG/0/"):
        if valor.startswith(prefixo) and valor[len(prefixo) :].isdigit():
            return f"EPSG:{valor[len(prefixo) :]}"
    return None


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


def ler_pagina(corpo: bytes, campo: str) -> models.LeituraPagina:
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
    valor = malhas._crs_declarado(colecao)
    return models.LeituraPagina(
        feicoes_recebidas=len(feicoes),
        ids=tuple(f["properties"].get(campo) for f in feicoes),
        total_declarado=(
            total
            if (isinstance(total, int) and not isinstance(total, bool)) or total == "unknown"
            else None
        ),
        crs=epsg(valor),
        crs_localizador="/crs/properties/name" if isinstance(valor, str) else None,
        crs_valor=valor if isinstance(valor, str) else None,
    )


def conferir_avanco(
    contexto: protocols.ContextoBruto,
    ids: tuple[Any, ...],
    anterior: str | None,
    *,
    recebidas: int,
    pedidas: int,
    faltam: int,
) -> str | None:
    """A chave é texto e cresce estritamente (a ordem do ``sortBy``); página curta só na última."""
    for valor in ids:
        if not isinstance(valor, str):
            raise contexto.divergencia(
                f"chave {valor!r} não é texto: a ordem do servidor não pode ser conferida"
            )
        if anterior is not None and valor <= anterior:
            raise contexto.divergencia(
                f"chave {valor!r} depois de {anterior!r}: ordem ou avanço da paginação quebrado"
            )
        anterior = valor
    if recebidas > pedidas or recebidas < min(pedidas, faltam):
        raise contexto.divergencia(
            f"página com {recebidas} feições: pedidas {pedidas} e faltavam {faltam} da contagem"
        )
    return anterior


class AdaptadorMalhas:
    """WFS do IBGE: contagem, páginas GeoJSON por ``startIndex`` na ordem da chave e nova contagem.

    Pede todos os atributos e o CRS nativo (sem ``propertyName`` nem ``srsName``) e não passa pelo
    ``parse_geojson``: o corpo vai como a fonte publica.
    """

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        camada = CAMADAS[pedido.recurso]
        consulta = malhas.Consulta(
            camada, uf=pedido.uf, bbox=pedido.bbox, bbox_crs=pedido.bbox_crs or CRS_NATIVO
        )
        parametros = {
            "service": "WFS",
            "version": malhas.WFS_VERSION,
            "request": "GetFeature",
            "typeNames": camada.typename,
            "outputFormat": "application/json",
            "sortBy": camada.ordem,
        }
        filtro = consulta.filtro_cql()
        if filtro is not None:
            parametros["CQL_FILTER"] = filtro
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=camada.url,
            parametros=parametros,
            selecao=models.Selecao(
                uf=pedido.uf,
                bbox=pedido.bbox,
                bbox_crs=pedido.bbox_crs,
                camada=camada.typename,
                edicao=camada.edicao,
                natureza=None,
            ),
            modo="paginado",
            formato="geojson",
            opcoes=models.Opcoes(
                tamanho_pagina=pedido.tamanho_pagina,
                compactar=pedido.compactar,
                limites=pedido.limites,
            ),
            campo_id=camada.ordem,
            crs_esperado=CRS_NATIVO,
        )

    async def adquirir(
        self, plano: models.PlanoBruto, *, contexto: protocols.ContextoBruto
    ) -> models.ConclusaoBruta:
        tamanho = plano.opcoes.tamanho_pagina
        campo = CAMADAS[plano.recurso].ordem
        assert tamanho is not None
        contagem = {k: v for k, v in plano.parametros.items() if k not in _SEM_CONTAGEM}
        contagem["resultType"] = "hits"
        async with httpx.AsyncClient(
            headers=UserAgentRotator.get_bot_headers(),
            follow_redirects=True,
            timeout=malhas.TIMEOUT,
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
                leitura = ler_pagina(resposta.corpo or b"", campo)
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


adaptador = AdaptadorMalhas()
