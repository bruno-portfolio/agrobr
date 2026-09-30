from __future__ import annotations

import dataclasses
import re
from typing import Any, Literal, cast

from agrobr import constants
from agrobr.comexstat import models
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import time as time_utils
from agrobr.utils import validation

Fluxo = Literal["exportacao", "importacao"]
Agregacao = Literal["mensal", "detalhado"]
TabelaDicionario = Literal["unidades", "paises", "vias", "urfs"]


@dataclasses.dataclass(frozen=True)
class ComexQuery:
    fluxo: Fluxo
    produto: str
    ncm: models.SelecaoNcm
    ano: int
    uf: str | None
    pais: str | None
    via: str | None
    urf: str | None
    agregacao: Agregacao
    max_linhas: int | None
    max_memoria_bytes: int
    requested: tuple[tuple[str, str | int | None], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "fluxo": self.fluxo,
            "produto": self.produto,
            "ncm_prefixos": list(self.ncm.prefixos),
            "ncm_excluidos": list(self.ncm.excluidos),
            "ano": self.ano,
            "filtros": {"uf": self.uf, "pais": self.pais, "via": self.via, "urf": self.urf},
            "agregacao": self.agregacao,
            "limites": {"max_linhas": self.max_linhas, "max_memoria_bytes": self.max_memoria_bytes},
            "pedido": dict(self.requested),
        }


def validate_flags(*, as_polars: bool, return_meta: bool) -> None:
    if type(as_polars) is not bool or type(return_meta) is not bool:
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")


def _positive_limit(value: int | None, name: str, *, allow_none: bool = False) -> None:
    if allow_none and value is None:
        return
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        suffix = " ou None" if allow_none else ""
        raise InvalidParameterError(f"{name} deve ser inteiro positivo{suffix}")


def code_filter(value: str | int | None, name: str, raw_column: str) -> str | None:
    if value is None:
        return None
    width = constants.COMEXSTAT_CODE_WIDTHS[raw_column]
    if isinstance(value, int) and not isinstance(value, bool):
        token = str(value)
    elif isinstance(value, str):
        token = value.strip()
    else:
        raise InvalidParameterError(f"{name} deve ser código inteiro ou texto com dígitos ASCII")
    if not re.fullmatch(rf"[0-9]{{1,{width}}}", token):
        raise InvalidParameterError(f"{name} deve conter de 1 a {width} dígitos ASCII")
    return token.zfill(width)


def _uf_filter(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidParameterError("uf deve ser texto")
    normalized = value.strip().upper()
    if normalized in constants.COMEXSTAT_EXTRA_UFS:
        return normalized
    return validation.validate_uf(value)


def dictionary_table(value: str) -> TabelaDicionario:
    if not isinstance(value, str):
        raise InvalidParameterError("tabela deve ser texto")
    normalized = value.strip().lower()
    if normalized not in constants.COMEXSTAT_DICTIONARY_URLS:
        raise InvalidParameterError("tabela deve ser unidades, paises, vias ou urfs")
    return cast(TabelaDicionario, normalized)


def build_query(
    *,
    fluxo: Fluxo,
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    pais: str | int | None = None,
    via: str | int | None = None,
    urf: str | int | None = None,
    agregacao: str = "mensal",
    max_linhas: int | None = constants.COMEXSTAT_DEFAULT_MAX_ROWS,
    max_memoria_bytes: int = constants.COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES,
) -> ComexQuery:
    if fluxo not in ("exportacao", "importacao"):
        raise InvalidParameterError("fluxo deve ser exportacao ou importacao")
    ncm = models.resolve_ncm(produto)
    current_year = time_utils.hoje().year
    selected_year = current_year - 1 if ano is None else ano
    if not isinstance(selected_year, int) or isinstance(selected_year, bool):
        raise InvalidParameterError("ano deve ser inteiro")
    if not 1997 <= selected_year <= current_year:
        raise InvalidParameterError(f"ano deve estar entre 1997 e {current_year}")
    if selected_year < ncm.primeiro_ano:
        raise InvalidParameterError(
            f"Produto '{produto}' só tem código NCM equivalente a partir de {ncm.primeiro_ano}: "
            f"{ncm.motivo_primeiro_ano}"
        )
    if not isinstance(agregacao, str) or agregacao not in ("mensal", "detalhado"):
        raise InvalidParameterError("agregacao deve ser 'mensal' ou 'detalhado'")
    _positive_limit(max_linhas, "max_linhas", allow_none=True)
    _positive_limit(max_memoria_bytes, "max_memoria_bytes")
    return ComexQuery(
        fluxo=fluxo,
        produto=produto.strip().lower(),
        ncm=ncm,
        ano=selected_year,
        uf=_uf_filter(uf),
        pais=code_filter(pais, "pais", "CO_PAIS"),
        via=code_filter(via, "via", "CO_VIA"),
        urf=code_filter(urf, "urf", "CO_URF"),
        agregacao=cast(Agregacao, agregacao),
        max_linhas=max_linhas,
        max_memoria_bytes=max_memoria_bytes,
        requested=tuple(
            {
                "produto": produto,
                "ano": ano,
                "uf": uf,
                "pais": pais,
                "via": via,
                "urf": urf,
                "agregacao": agregacao,
                "max_linhas": max_linhas,
                "max_memoria_bytes": max_memoria_bytes,
            }.items()
        ),
    )
