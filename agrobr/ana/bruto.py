from __future__ import annotations

import asyncio
import json
from typing import Annotated, Any, Literal, TypeVar

import httpx
import pydantic

from agrobr import exceptions
from agrobr.bruto import models, protocols
from agrobr.http import user_agents
from agrobr.utils import geo

from . import client
from . import models as fonte

_Modelo = TypeVar("_Modelo", bound=pydantic.BaseModel)
_Contador = Annotated[pydantic.StrictInt, pydantic.Field(ge=0)]


class _Contagem(pydantic.BaseModel):
    count: _Contador


class _Ids(pydantic.BaseModel):
    objectIdFieldName: Literal["FID"]
    objectIds: list[pydantic.StrictInt] | None


class _Atributos(pydantic.BaseModel):
    FID: pydantic.StrictInt


class _Feicao(pydantic.BaseModel):
    attributes: _Atributos
    geometry: dict[str, Any] | None


class _Referencia(pydantic.BaseModel):
    wkid: pydantic.StrictInt
    latestWkid: pydantic.StrictInt | None = None


class _Pagina(pydantic.BaseModel):
    spatialReference: _Referencia
    features: list[_Feicao]
    exceededTransferLimit: pydantic.StrictBool = False
    count: _Contador | None = None


def _erro(mensagem: str) -> exceptions.ParseError:
    return exceptions.ParseError(source="ana", parser_version=1, reason=mensagem)


def _ler(resposta: models.RespostaCapturada, modelo: type[_Modelo]) -> _Modelo:
    try:
        dado = json.loads(resposta.corpo or b"")
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _erro("Resposta ArcGIS sem JSON legível") from exc
    if isinstance(dado, dict) and "error" in dado:
        raise _erro("Resposta ArcGIS de erro em HTTP 200")
    try:
        return modelo.model_validate(dado)
    except pydantic.ValidationError as exc:
        raise _erro(f"Envelope ArcGIS inválido: {exc}") from exc


async def _contagem(
    http: httpx.AsyncClient,
    contexto: protocols.ContextoBruto,
    plano: models.PlanoBruto,
    papel: Literal["contagem_antes", "contagem_depois"],
    numero: int,
) -> int:
    resposta = await contexto.obter(
        http,
        models.PedidoHTTP(
            url=plano.url_solicitada,
            parametros={**plano.parametros, "returnCountOnly": "true"},
            papel=papel,
            numero=numero,
            formato="json",
        ),
    )
    total = _ler(resposta, _Contagem).count
    contexto.registrar_controle(
        resposta, models.LeituraControle(papel=papel, valor_declarado=total)
    )
    return total


async def _ids(
    http: httpx.AsyncClient,
    contexto: protocols.ContextoBruto,
    plano: models.PlanoBruto,
    total: int,
) -> tuple[int, ...]:
    resposta = await contexto.obter(
        http,
        models.PedidoHTTP(
            url=plano.url_solicitada,
            parametros={**plano.parametros, "returnIdsOnly": "true"},
            papel="ids",
            numero=2,
            formato="json",
        ),
    )
    lista = _ler(resposta, _Ids).objectIds
    if lista is None and total != 0:
        raise contexto.divergencia("Lista oficial de FIDs nula para contagem positiva")
    ids = tuple(sorted(lista or []))
    contexto.registrar_controle(resposta, models.LeituraControle(papel="ids", valor_declarado=None))
    contexto.conferir_ids(ids, papel="esperados")
    if len(ids) != total:
        raise contexto.divergencia("Lista oficial de FIDs difere da contagem anterior")
    return ids


async def _pagina(
    http: httpx.AsyncClient,
    contexto: protocols.ContextoBruto,
    plano: models.PlanoBruto,
    faixa: tuple[int, ...],
    numero: int,
    total: int,
) -> None:
    where = plano.parametros["where"]
    resposta = await contexto.obter(
        http,
        models.PedidoHTTP(
            url=plano.url_solicitada,
            parametros={
                **plano.parametros,
                "where": f"({where}) AND FID >= {faixa[0]} AND FID <= {faixa[-1]}",
            },
            papel="pagina",
            numero=numero,
            formato="esri_json",
            paginacao=models.PaginacaoFID(
                tipo="fid", min=faixa[0], max=faixa[-1], quantidade=len(faixa)
            ),
        ),
    )
    pagina = _ler(resposta, _Pagina)
    if pagina.exceededTransferLimit:
        raise _erro(f"Página {numero} incompleta: limite de transferência ArcGIS excedido")
    recebidos = tuple(feicao.attributes.FID for feicao in pagina.features)
    if tuple(sorted(recebidos)) != faixa:
        raise contexto.divergencia(
            f"Página {numero} não confirma a faixa de FIDs {faixa[0]}–{faixa[-1]}"
        )
    if pagina.count is not None and pagina.count != total:
        raise contexto.divergencia("Total declarado na página difere da contagem de controle")
    referencia = pagina.spatialReference
    if referencia.wkid != 4674 or referencia.latestWkid not in (None, 4674):
        raise contexto.divergencia("Página ArcGIS não declara o CRS esperado EPSG:4674")
    contexto.registrar_pagina(
        resposta,
        models.LeituraPagina(
            feicoes_recebidas=len(recebidos),
            ids=recebidos,
            total_declarado=pagina.count,
            crs="EPSG:4674",
            crs_localizador="/spatialReference/wkid",
            crs_valor=str(referencia.wkid),
        ),
    )


class _Adaptador:
    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        camada = fonte.MASSAS_DAGUA["service_path"]
        consulta = httpx.URL(
            geo.build_arcgis_query_url(
                f"{fonte.ANA_SPR}/{camada}",
                where=client.massas_where(pedido.uf),
                bbox=pedido.bbox,
                in_sr=4326 if pedido.bbox_crs == "EPSG:4326" else 4674,
                out_sr=4674,
                f="json",
                return_geometry=True,
            )
        )
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=str(consulta.copy_with(query=None)),
            parametros=dict(consulta.params),
            selecao=models.Selecao(
                uf=pedido.uf,
                bbox=pedido.bbox,
                bbox_crs=pedido.bbox_crs,
                camada=camada,
                edicao=None,
                natureza=None,
            ),
            modo="paginado",
            formato="esri_json",
            opcoes=models.Opcoes(
                tamanho_pagina=pedido.tamanho_pagina,
                compactar=pedido.compactar,
                limites=pedido.limites,
            ),
            campo_id="FID",
            crs_esperado="EPSG:4674",
        )

    async def adquirir(
        self, plano: models.PlanoBruto, *, contexto: protocols.ContextoBruto
    ) -> models.ConclusaoBruta:
        tamanho = plano.opcoes.tamanho_pagina
        assert tamanho is not None
        async with httpx.AsyncClient(
            timeout=client.TIMEOUT,
            headers=user_agents.UserAgentRotator.get_bot_headers(),
            follow_redirects=True,
        ) as http:
            total = await _contagem(http, contexto, plano, "contagem_antes", 1)
            ids = await _ids(http, contexto, plano, total)
            for numero, inicio in enumerate(range(0, len(ids), tamanho), 1):
                contexto.conferir_prazo()
                await _pagina(http, contexto, plano, ids[inicio : inicio + tamanho], numero, total)
                if numero > client._PAUSA_APOS_PAGINA:
                    await asyncio.sleep(client._PAUSA_SEGUNDOS)
            depois = await _contagem(http, contexto, plano, "contagem_depois", 3)
            if depois != total:
                raise contexto.divergencia("Contagem mudou durante a coleta")
        return models.ConclusaoBruta(status="ok")


adaptador: protocols.AdaptadorBruto = _Adaptador()
