from __future__ import annotations

import math
import re
from collections.abc import Sequence
from typing import get_args

from agrobr import constants
from agrobr.bruto import models
from agrobr.bruto.registry import RecursoRegistrado
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from agrobr.utils.validation import validate_uf

_NOME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
_DISPOSITIVOS = frozenset(
    {"CON", "PRN", "AUX", "NUL"} | {f"{p}{n}" for p in ("COM", "LPT") for n in range(1, 10)}
)
_PADRAO_BBOX_CRS = "EPSG:4674"


def pedido(
    registrado: RecursoRegistrado,
    *,
    nome: object,
    uf: object,
    bbox: object,
    bbox_crs: object,
    tamanho_pagina: object,
    compactar: object,
    retomar: object,
    limites: object,
) -> models.PedidoBruto:
    """Confere e normaliza os argumentos de ``coletar`` antes de qualquer I/O (``InvalidParameterError``)."""
    for parametro, valor in (("compactar", compactar), ("retomar", retomar)):
        if not isinstance(valor, bool):
            raise InvalidParameterError(
                f"bruto: {parametro} deve ser True ou False, recebeu {valor!r}"
            )
    if limites is not None and not isinstance(limites, models.LimitesBrutos):
        raise InvalidParameterError(
            f"bruto: limites deve ser bruto.LimitesBrutos ou None, recebeu {type(limites).__name__}"
        )
    alvo = f"{registrado.fonte}/{registrado.recurso}"
    uf_norm = _uf(uf, registrado, alvo)
    bbox_norm = _bbox(bbox, registrado, alvo)
    if bbox_crs not in get_args(models.CrsBbox):
        raise InvalidParameterError(
            f"bruto: bbox_crs deve ser 'EPSG:4674' ou 'EPSG:4326', recebeu {bbox_crs!r}"
        )
    if bbox_norm is None and bbox_crs != _PADRAO_BBOX_CRS:
        raise InvalidParameterError("bruto: bbox_crs sem bbox não tem efeito")
    if registrado.exige_recorte and uf_norm is None and bbox_norm is None:
        raise InvalidParameterError(f"bruto: {alvo} exige uf ou bbox")
    arquivo = registrado.modo == "arquivo"
    if arquivo and tamanho_pagina is not None:
        raise InvalidParameterError(f"bruto: tamanho_pagina não se aplica a {alvo} (arquivo)")
    return models.PedidoBruto(
        fonte=registrado.fonte,
        recurso=registrado.recurso,
        nome=_nome(nome, uf_norm, bbox_norm),
        modo=registrado.modo,
        formato=registrado.formato,
        uf=uf_norm,
        bbox=bbox_norm,
        bbox_crs=None if bbox_norm is None else bbox_crs,  # type: ignore[arg-type]
        tamanho_pagina=None if arquivo else _tamanho_pagina(tamanho_pagina),
        compactar=False if arquivo else bool(compactar),
        limites=limites if limites is not None else models.LimitesBrutos(),
    )


def _uf(uf: object, registrado: RecursoRegistrado, alvo: str) -> str | None:
    if uf is None:
        if registrado.uf == "obrigatoria":
            raise InvalidParameterError(f"bruto: {alvo} exige uf")
        return None
    if registrado.uf == "recusada":
        raise InvalidParameterError(f"bruto: {alvo} não aceita uf; use bbox ou nenhum recorte")
    if not isinstance(uf, str):
        raise InvalidParameterError(f"bruto: uf deve ser sigla em texto, recebeu {uf!r}")
    return validate_uf(uf)


def _bbox(
    bbox: object, registrado: RecursoRegistrado, alvo: str
) -> tuple[float, float, float, float] | None:
    if bbox is None:
        return None
    if registrado.bbox == "recusada":
        raise InvalidParameterError(f"bruto: {alvo} não aceita bbox")
    if (
        not isinstance(bbox, Sequence)
        or isinstance(bbox, str | bytes)
        or len(bbox) != 4
        or any(isinstance(v, bool) or not isinstance(v, int | float) for v in bbox)
    ):
        raise InvalidParameterError(
            f"bruto: bbox deve ser (minx, miny, maxx, maxy) com 4 números, recebeu {bbox!r}"
        )
    minx, miny, maxx, maxy = (float(v) + 0.0 for v in bbox)
    if not all(math.isfinite(v) for v in (minx, miny, maxx, maxy)):
        raise InvalidParameterError(f"bruto: bbox com número não finito: {bbox!r}")
    if not (-180 <= minx < maxx <= 180 and -90 <= miny < maxy <= 90):
        raise InvalidParameterError(
            "bruto: bbox fora de -180 <= minx < maxx <= 180 e -90 <= miny < maxy <= 90 "
            f"(não cruza o antimeridiano): {bbox!r}"
        )
    return minx, miny, maxx, maxy


def _nome(nome: object, uf: str | None, bbox: tuple[float, float, float, float] | None) -> str:
    if nome is None:
        if bbox is not None:
            raise InvalidParameterError("bruto: com bbox, nome é obrigatório")
        return uf if uf is not None else "brasil"
    if not isinstance(nome, str) or not _NOME.fullmatch(nome):
        raise InvalidParameterError(
            f"bruto: nome deve casar [A-Za-z0-9][A-Za-z0-9_-]{{0,63}}, recebeu {nome!r}"
        )
    if nome.upper() in _DISPOSITIVOS:
        raise InvalidParameterError(f"bruto: nome {nome!r} é nome de dispositivo do Windows")
    return nome


def _tamanho_pagina(tamanho: object) -> int:
    if tamanho is None:
        return constants.BRUTO_TAMANHO_PAGINA_PADRAO
    maximo = constants.BRUTO_TAMANHO_PAGINA_MAX
    if isinstance(tamanho, bool) or not isinstance(tamanho, int) or not 1 <= tamanho <= maximo:
        raise InvalidParameterError(
            f"bruto: tamanho_pagina deve ser inteiro de 1 a {maximo}, recebeu {tamanho!r}"
        )
    return tamanho


def conferir_plano(
    plano: models.PlanoBruto, pedido: models.PedidoBruto, registrado: RecursoRegistrado
) -> None:
    """O plano do adaptador tem de repetir o pedido normalizado e a regra do registro (``ContractViolationError``)."""
    esperado = {
        "fonte": pedido.fonte,
        "recurso": pedido.recurso,
        "nome": pedido.nome,
        "modo": registrado.modo,
        "formato": registrado.formato,
        "campo_id": registrado.campo_id,
        "selecao.uf": pedido.uf,
        "selecao.bbox": pedido.bbox,
        "selecao.bbox_crs": pedido.bbox_crs,
        "opcoes.tamanho_pagina": pedido.tamanho_pagina,
        "opcoes.compactar": pedido.compactar,
        "opcoes.limites": pedido.limites.model_dump(),
    }
    obtido = {
        "fonte": plano.fonte,
        "recurso": plano.recurso,
        "nome": plano.nome,
        "modo": plano.modo,
        "formato": plano.formato,
        "campo_id": plano.campo_id,
        "selecao.uf": plano.selecao.uf,
        "selecao.bbox": plano.selecao.bbox,
        "selecao.bbox_crs": plano.selecao.bbox_crs,
        "opcoes.tamanho_pagina": plano.opcoes.tamanho_pagina,
        "opcoes.compactar": plano.opcoes.compactar,
        "opcoes.limites": plano.opcoes.limites.model_dump(),
    }
    divergentes = [campo for campo in esperado if esperado[campo] != obtido[campo]]
    if plano.modo == "arquivo" and plano.parametros:
        divergentes.append("parametros (arquivo não tem parâmetros)")
    if plano.modo == "paginado":
        if "?" in plano.url_solicitada:
            divergentes.append("url_solicitada (paginado: endpoint sem query string)")
        if plano.crs_esperado is None:
            divergentes.append("crs_esperado (paginado confere o CRS)")
    if divergentes:
        raise ContractViolationError(
            "bruto", f"plano de {pedido.fonte}/{pedido.recurso} difere do pedido em {divergentes}"
        )
