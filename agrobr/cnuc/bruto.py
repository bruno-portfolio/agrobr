from __future__ import annotations

import json
from typing import Any, Literal

import httpx
from lxml import etree

from agrobr.bruto import arquivo, models, protocols
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import ParseError
from agrobr.http.user_agents import UserAgentRotator

from . import client, parser
from .models import GEOM_COLUMN, MAPSERVER, NS_MS, NS_WFS, TYPENAME

CRS_NATIVO = "EPSG:4674"
CAMPO = "cd_cnuc"
EDICAO_CADASTRO = 202607
URL_CADASTRO = URLS[Fonte.CNUC]["cadastro_csv"]
_URN = "urn:ogc:def:crs:EPSG::"


def _erro(mensagem: str) -> ParseError:
    return ParseError("cnuc", parser.PARSER_VERSION, mensagem)


def ler_pagina(corpo: bytes) -> models.LeituraPagina:
    """Lê o GML como veio: a chave é ``ms:cd_cnuc`` (``gml:id`` não a substitui) e o CRS, o ``srsName`` das geometrias."""
    raiz = parser._colecao(corpo)
    feicoes = []
    for membro in raiz.iterchildren(f"{{{NS_WFS}}}member"):
        filhos = list(membro.iterchildren(etree.Element))
        if len(filhos) != 1:
            raise _erro(f"wfs:member com {len(filhos)} elementos; esperado 1 feição")
        feicoes.append(filhos[0])
    devolvidas = raiz.get("numberReturned") or ""
    if not devolvidas.isdigit() or int(devolvidas) != len(feicoes):
        raise _erro(f"numberReturned={devolvidas!r} e {len(feicoes)} feições na página")
    geometrias = [
        elemento
        for feicao in feicoes
        for geometria in feicao.iterchildren(f"{{{NS_MS}}}{GEOM_COLUMN}")
        for elemento in geometria.iter(etree.Element)
        if elemento.get("srsName")
    ]
    declarados = sorted({elemento.get("srsName") for elemento in geometrias})
    valor = declarados[0] if len(declarados) == 1 else None
    total = raiz.get("numberMatched")
    total_declarado: int | Literal["unknown"] | None = (
        int(total)
        if total is not None and total.isdigit()
        else "unknown"
        if total == "unknown"
        else None
    )
    return models.LeituraPagina(
        feicoes_recebidas=len(feicoes),
        ids=tuple(feicao.findtext(f"{{{NS_MS}}}{CAMPO}") for feicao in feicoes),
        total_declarado=total_declarado,
        crs=(
            f"EPSG:{valor.removeprefix(_URN)}"
            if valor and valor.removeprefix(_URN).isdigit()
            else (" | ".join(declarados) or None)
        ),
        crs_localizador=(
            f"//*[local-name()='{etree.QName(geometrias[0]).localname}']/@srsName"
            if valor
            else None
        ),
        crs_valor=valor,
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
    """``cd_cnuc`` é texto e cresce estritamente (a ordem do ``SORTBY``); página curta só na última."""
    for valor in ids:
        if not isinstance(valor, str):
            raise contexto.divergencia(
                f"{CAMPO} {valor!r} não é texto: a ordem do servidor não pode ser conferida"
            )
        if anterior is not None and valor <= anterior:
            raise contexto.divergencia(
                f"{CAMPO} {valor!r} depois de {anterior!r}: ordem ou avanço da paginação quebrado"
            )
        anterior = valor
    if recebidas > pedidas or recebidas < min(pedidas, faltam):
        raise contexto.divergencia(
            f"página com {recebidas} feições: pedidas {pedidas} e faltavam {faltam} da contagem"
        )
    return anterior


class AdaptadorCnuc:
    """WFS do CNUC: contagem, páginas GML por ``STARTINDEX`` na ordem de ``cd_cnuc`` e nova contagem.

    Sempre com ``limite=uc`` e o filtro de UF da API (estados compostos); sem ``PROPERTYNAME`` nem
    ``SRSNAME``, para vir com todos os atributos e no CRS nativo, e sem o parser tabular.
    """

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        crs = pedido.bbox_crs or CRS_NATIVO
        filtro = client.FiltroServidor(
            uf=pedido.uf, bbox=pedido.bbox, bbox_srs=_URN + crs.removeprefix("EPSG:")
        )
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=MAPSERVER,
            parametros=client.parametros({"FILTER": client.build_filter(filtro), "SORTBY": CAMPO}),
            selecao=models.Selecao(
                uf=pedido.uf,
                bbox=pedido.bbox,
                bbox_crs=pedido.bbox_crs,
                camada=TYPENAME,
                edicao=None,
                natureza=None,
            ),
            modo="paginado",
            formato="gml",
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
        contagem = {k: v for k, v in plano.parametros.items() if k != "SORTBY"}
        contagem["RESULTTYPE"] = "hits"
        async with httpx.AsyncClient(
            headers=UserAgentRotator.get_bot_headers(),
            follow_redirects=True,
            timeout=client.TIMEOUT,
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
                valor = parser.parse_feature_count(resposta.corpo or b"")
                contexto.registrar_controle(
                    resposta, models.LeituraControle(papel=papel, valor_declarado=valor)
                )
                return valor

            total = await contar("contagem_antes", 1)
            inicio, numero, anterior = 0, 1, None
            while inicio < total:
                pedido = models.PedidoHTTP(
                    url=plano.url_solicitada,
                    parametros={
                        **plano.parametros,
                        "COUNT": str(tamanho),
                        "STARTINDEX": str(inicio),
                    },
                    papel="pagina",
                    numero=numero,
                    formato="gml",
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


async def conferir_catalogo(contexto: protocols.ContextoBruto, http: httpx.AsyncClient) -> None:
    """O CKAN do MMA tem de publicar a edição (``CNUC_AAAA_MM``, CSV) na URL fixada no agrobr, e só nela."""
    nome = f"CNUC_{EDICAO_CADASTRO // 100}_{EDICAO_CADASTRO % 100:02d}"
    catalogo = URLS[Fonte.CNUC]["ckan_package"]
    corpo = await contexto.consultar(http, catalogo)
    try:
        urls = [
            recurso["url"]
            for recurso in json.loads(corpo)["result"]["resources"]
            if recurso["name"] == nome and str(recurso["format"]).upper() == "CSV"
        ]
    except (ValueError, KeyError, TypeError) as exc:
        raise _erro(f"catálogo {catalogo} ilegível ({type(exc).__name__}: {exc})") from exc
    if urls != [URL_CADASTRO]:
        raise _erro(
            f"o catálogo {catalogo} publica {nome} (CSV) em {urls}, não em {URL_CADASTRO}: "
            "edição republicada ou retirada; atualize o agrobr"
        )


adaptador = AdaptadorCnuc()
cadastro = arquivo.AdaptadorArquivo(
    URL_CADASTRO, EDICAO_CADASTRO, arquivo.conferir_csv("Código UC"), conferir_catalogo
)
