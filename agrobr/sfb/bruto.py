from __future__ import annotations

import asyncio
import json
from typing import Annotated, Any, Literal, TypeVar

import httpx
import pydantic

from agrobr import exceptions
from agrobr.bruto import models, protocols
from agrobr.http import user_agents

from . import client
from . import models as fonte

CAMADA = fonte.LAYERS["cnfp"]["service_path"]
EDICAO = 20250717
CRS_NATIVO = "EPSG:3857"
_PAUSA_APOS_PAGINA = 5
_PAUSA_SEGUNDOS = 1.0

_Modelo = TypeVar("_Modelo", bound=pydantic.BaseModel)
_Contador = Annotated[pydantic.StrictInt, pydantic.Field(ge=0)]


class _Contagem(pydantic.BaseModel):
    count: _Contador


class _Ids(pydantic.BaseModel):
    objectIdFieldName: Literal["fid"]
    objectIds: list[pydantic.StrictInt] | None
    exceededTransferLimit: pydantic.StrictBool = False


class _Atributos(pydantic.BaseModel):
    fid: pydantic.StrictInt


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


def _erro(mensagem: str) -> exceptions.ParseError:
    return exceptions.ParseError(source="sfb", parser_version=1, reason=mensagem)


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
    leitura = _ler(resposta, _Ids)
    if leitura.exceededTransferLimit:
        raise _erro("Lista oficial de fids truncada: limite de transferência ArcGIS excedido")
    if leitura.objectIds is None and total != 0:
        raise contexto.divergencia("Lista oficial de fids nula para contagem positiva")
    ids = tuple(sorted(leitura.objectIds or []))
    contexto.registrar_controle(resposta, models.LeituraControle(papel="ids", valor_declarado=None))
    contexto.conferir_ids(ids, papel="esperados")
    if len(ids) != total:
        raise contexto.divergencia("Lista oficial de fids difere da contagem anterior")
    return ids


async def _pagina(
    http: httpx.AsyncClient,
    contexto: protocols.ContextoBruto,
    plano: models.PlanoBruto,
    faixa: tuple[int, ...],
    numero: int,
) -> None:
    where = plano.parametros["where"]
    pedido = models.PedidoHTTP(
        url=plano.url_solicitada,
        parametros={
            **plano.parametros,
            "where": f"({where}) AND fid >= {faixa[0]} AND fid <= {faixa[-1]}",
            "orderByFields": "fid",
        },
        papel="pagina",
        numero=numero,
        formato="esri_json",
        paginacao=models.PaginacaoFID(
            tipo="fid", min=faixa[0], max=faixa[-1], quantidade=len(faixa)
        ),
    )
    try:
        resposta = await contexto.obter(http, pedido)
    except exceptions.ResourceLimitError as exc:
        limite = plano.opcoes.limites.max_bytes_pagina
        raise exceptions.ResourceLimitError(
            "sfb",
            f"página {numero} (fid {faixa[0]}–{faixa[-1]}, max_bytes_pagina={limite}): {exc.reason}",
            url=exc.url,
        ) from exc
    pagina = _ler(resposta, _Pagina)
    if pagina.exceededTransferLimit:
        raise _erro(f"Página {numero} incompleta: limite de transferência ArcGIS excedido")
    recebidos = tuple(feicao.attributes.fid for feicao in pagina.features)
    if recebidos != faixa:
        raise contexto.divergencia(
            f"Página {numero} não traz os fids {faixa[0]}–{faixa[-1]} em ordem crescente"
        )
    referencia = pagina.spatialReference
    if (referencia.wkid, referencia.latestWkid) != (102100, 3857):
        raise contexto.divergencia("Página ArcGIS não declara o CRS nativo EPSG:3857")
    contexto.registrar_pagina(
        resposta,
        models.LeituraPagina(
            feicoes_recebidas=len(recebidos),
            ids=recebidos,
            total_declarado=None,
            crs=CRS_NATIVO,
            crs_localizador="/spatialReference/latestWkid",
            crs_valor="3857",
        ),
    )


class _Adaptador:
    """CNFP no CRS nativo, sem ``outSR``: contagem, lista oficial de fids, páginas por faixa de fid e nova contagem."""

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=f"{fonte.SFB_BASE}/{CAMADA}/query",
            parametros={"where": "1=1", "outFields": "*", "returnGeometry": "true", "f": "json"},
            selecao=models.Selecao(
                uf=None, bbox=None, bbox_crs=None, camada=CAMADA, edicao=EDICAO, natureza=None
            ),
            modo="paginado",
            formato="esri_json",
            opcoes=models.Opcoes(
                tamanho_pagina=pedido.tamanho_pagina,
                compactar=pedido.compactar,
                limites=pedido.limites,
            ),
            campo_id="fid",
            crs_esperado=CRS_NATIVO,
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
                await _pagina(http, contexto, plano, ids[inicio : inicio + tamanho], numero)
                if numero > _PAUSA_APOS_PAGINA:
                    await asyncio.sleep(_PAUSA_SEGUNDOS)
            await _contagem(http, contexto, plano, "contagem_depois", 3)
        return models.ConclusaoBruta(status="ok")


adaptador: protocols.AdaptadorBruto = _Adaptador()
