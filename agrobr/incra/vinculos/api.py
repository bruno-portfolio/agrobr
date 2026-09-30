from __future__ import annotations

import copy
import importlib
import time
from datetime import UTC, date, datetime
from typing import Any, Literal, cast, overload

import httpx
import pandas as pd

from agrobr import _log, constants, contracts
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import AgrobrError, ContractViolationError, InvalidParameterError
from agrobr.incra import api as geographical_api
from agrobr.incra import query
from agrobr.incra.andamento import api as administrative_api
from agrobr.incra.andamento import parser as administrative_parser
from agrobr.models import MetaInfo
from agrobr.utils import result

from . import budget, metadata, relation

logger = _log.get_logger(__name__)


def _validate_contract(frame: pd.DataFrame, name: str) -> None:
    contract = contracts.get_contract(name)
    valid, errors = contract.validate(frame)
    if not valid:
        raise ContractViolationError(contract.name, "; ".join(errors))


def _guards(
    edicao: date | str | None,
    tamanho_pagina: int | None,
    max_vinculos: int | None,
    as_polars: bool,
    return_meta: bool,
    kwargs: dict[str, Any],
) -> None:
    if kwargs:
        raise TypeError(f"Argumentos desconhecidos em vinculos_quilombolas: {sorted(kwargs)}")
    if type(as_polars) is not bool or type(return_meta) is not bool:
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if max_vinculos is not None and (type(max_vinculos) is not int or max_vinculos <= 0):
        raise InvalidParameterError("max_vinculos deve ser inteiro positivo ou None")
    administrative_api.validate_edition(edicao)
    query.build_query(include_geometry=False, max_registros=None, tamanho_pagina=tamanho_pagina)
    if get_snapshot() is not None:
        raise InvalidParameterError(
            "vinculos_quilombolas não suporta deterministic: fontes sem snapshot conjunto"
        )
    administrative_parser.check_pdf()
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError(
                "polars é necessário. Instale com: pip install agrobr[polars]"
            ) from None


async def _acquire_parents(
    edicao: date | str | None,
    tamanho_pagina: int | None,
    parents: dict[str, dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    geographical, first_meta = await geographical_api.quilombolas(
        max_registros=None, tamanho_pagina=tamanho_pagina, as_polars=False, return_meta=True
    )
    _validate_contract(geographical, "incra_quilombolas")
    parents["geographical"] = metadata.parent_payload(first_meta, geographical, geographical=True)
    budget.check(budget.retained_size([geographical, parents]))
    administrative, second_meta = await administrative_api.andamento_quilombola(
        edicao=edicao, as_polars=False, return_meta=True
    )
    _validate_contract(administrative, "incra_andamento_quilombola")
    parents["administrative"] = metadata.parent_payload(
        second_meta, administrative, geographical=False
    )
    return geographical, administrative


def _finalize(
    frame: pd.DataFrame,
    meta: MetaInfo,
    *,
    as_polars: bool,
    return_meta: bool,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    retained_base = meta.source_details["budgets"][
        "retained_bytes_estimate"
    ] + budget.retained_size(meta.to_dict())
    retained = budget.check(retained_base + (budget.retained_size(frame) if as_polars else 0))
    meta.source_details["budgets"]["retained_bytes_estimate"] = retained
    finalized = result.finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=relation.string_columns(),
    )
    if as_polars:
        output = cast(Any, finalized[0] if return_meta else finalized)
        estimated = budget.check(retained_base + int(output.estimated_size()))
        meta.source_details["budgets"]["retained_bytes_estimate"] = max(retained, estimated)
        meta.source_details["output_dtypes"] = {
            name: str(dtype) for name, dtype in output.schema.items()
        }
    return finalized


@overload
async def vinculos_quilombolas(
    *,
    edicao: date | str | None = None,
    tamanho_pagina: int | None = None,
    max_vinculos: int | None = constants.INCRA_VINCULOS_MAX_ROWS,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def vinculos_quilombolas(
    *,
    edicao: date | str | None = None,
    tamanho_pagina: int | None = None,
    max_vinculos: int | None = constants.INCRA_VINCULOS_MAX_ROWS,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def vinculos_quilombolas(
    *,
    edicao: date | str | None = None,
    tamanho_pagina: int | None = None,
    max_vinculos: int | None = constants.INCRA_VINCULOS_MAX_ROWS,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def vinculos_quilombolas(
    *,
    edicao: date | str | None = None,
    tamanho_pagina: int | None = None,
    max_vinculos: int | None = constants.INCRA_VINCULOS_MAX_ROWS,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    _guards(edicao, tamanho_pagina, max_vinculos, as_polars, return_meta, kwargs)
    started = time.monotonic()
    parents: dict[str, dict[str, Any]] = {}
    try:
        geographical, administrative = await _acquire_parents(edicao, tamanho_pagina, parents)
        relation_started = time.monotonic()
        parsed = relation.build_relation(
            geographical,
            administrative,
            max_rows=max_vinculos,
            retained_base_bytes=budget.retained_size(parents),
        )
        _validate_contract(parsed.frame, "incra_vinculos_quilombolas")
        elapsed_ms = int((time.monotonic() - started) * 1000)
        relation_ms = int((time.monotonic() - relation_started) * 1000)
        meta = metadata.build_meta(
            parsed,
            parents,
            {
                "edicao": edicao.isoformat() if type(edicao) is date else edicao,
                "tamanho_pagina": tamanho_pagina,
                "max_vinculos": max_vinculos,
            },
            elapsed_ms,
            relation_ms,
        )
        finalized = _finalize(parsed.frame, meta, as_polars=as_polars, return_meta=return_meta)
        meta.source_details["composition_duration_ms"] = int((time.monotonic() - started) * 1000)
        meta.timestamp = datetime.now(UTC)
    except (AgrobrError, httpx.HTTPError, ValueError) as error:
        cast(Any, error).vinculos_completed_sources = copy.deepcopy(parents)
        raise
    logger.info(
        "incra_vinculos_quilombolas", rows=len(parsed.frame), states=parsed.counts["states"]
    )
    return finalized
