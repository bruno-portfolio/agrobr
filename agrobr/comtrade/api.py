from __future__ import annotations

import time
import warnings as python_warnings
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.contracts import comtrade as source_contracts
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result, warnings
from agrobr.utils import time as time_utils
from agrobr.utils.result import DataFrame, DataFrameResult

from . import acquisition, client, metadata, models, parser, query

logger = _log.get_logger(__name__)


def prepare_query(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> acquisition.TradeQuery:
    if kwargs:
        raise InvalidParameterError(f"Parâmetros Comtrade desconhecidos: {sorted(kwargs)}")
    query.validate_access_options(api_key, require_complete)
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    reporter_code = models.resolve_pais(reporter)
    partner_code = _resolve_partner(partner)
    selection = query.build_query(
        reporter=reporter_code,
        partner=partner_code,
        hs_codes=models.resolve_hs(produto),
        flow=fluxo,
        period=periodo if periodo is not None else str(time_utils.hoje().year - 1),
        freq=freq,
    )
    models.validate_hs_periods(produto, selection.periods, selection.reporter)
    return selection


def _resolve_partner(partner: str | None) -> int | None:
    if partner is None:
        return 0
    if isinstance(partner, str) and partner.strip().lower() in {"all", "todos"}:
        return None
    return models.resolve_pais(partner)


def _warn_license() -> None:
    warnings.warn_once(
        "comtrade_license",
        (
            "UN Comtrade: classificação restrito; redistribuição sujeita a "
            "autorização/licenciamento, com dispensas expressas na política da fonte. Preserve "
            "atribuição e confira as condições, inclusive assinatura quando exigida. Política: "
            "https://uncomtrade.org/docs/policy-on-use-and-re-dissemination/. Veja "
            "https://www.agrobr.dev/docs/licenses/."
        ),
    )


async def _acquire(
    selection: acquisition.TradeQuery,
    *,
    api_key: str | None,
    require_complete: bool,
) -> tuple[pd.DataFrame, MetaInfo]:
    _warn_license()
    logger.info("comtrade_comercio", query=selection.model_dump(mode="json"))
    started = time.monotonic()
    acquired = await client.fetch_trade_acquisition(
        selection, api_key=api_key, require_complete=require_complete
    )
    for message in acquired.warnings:
        python_warnings.warn(message, UserWarning, stacklevel=3)
    fetch_ms = int((time.monotonic() - started) * 1000)
    started = time.monotonic()
    frame = parser.parse_trade_data(acquired.records)
    contracts.validate_dataset(frame, source_contracts.COMERCIO_BILATERAL_V2)
    parse_ms = int((time.monotonic() - started) * 1000)
    return frame, metadata.trade_meta(acquired, frame, fetch_ms=fetch_ms, parse_ms=parse_ms)


@overload
async def comercio(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def comercio(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def comercio(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> DataFrameResult: ...


async def comercio(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> DataFrameResult:
    selection = prepare_query(
        produto,
        reporter=reporter,
        partner=partner,
        fluxo=fluxo,
        periodo=periodo,
        freq=freq,
        api_key=api_key,
        require_complete=require_complete,
        as_polars=as_polars,
        return_meta=return_meta,
        **kwargs,
    )
    frame, meta = await _acquire(selection, api_key=api_key, require_complete=require_complete)
    return result.finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(
            c.name
            for c in source_contracts.COMERCIO_BILATERAL_V2.columns
            if c.type == contracts.ColumnType.STRING
        ),
    )


@overload
async def trade_mirror(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str = "CN",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def trade_mirror(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str = "CN",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def trade_mirror(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str = "CN",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> DataFrameResult: ...


async def trade_mirror(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str = "CN",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    require_complete: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> DataFrameResult:
    if kwargs:
        raise InvalidParameterError(f"Parâmetros trade_mirror desconhecidos: {sorted(kwargs)}")
    selection = prepare_query(
        produto,
        reporter=reporter,
        partner=partner,
        periodo=periodo,
        freq=freq,
        api_key=api_key,
        require_complete=require_complete,
        as_polars=as_polars,
        return_meta=return_meta,
        **kwargs,
    )
    if not selection.partner or selection.partner == selection.reporter:
        raise InvalidParameterError("Espelho requer dois países positivos, explícitos e distintos")
    inverse = query.build_query(
        reporter=selection.partner,
        partner=selection.reporter,
        hs_codes=selection.hs_codes,
        flow="M",
        period=",".join(selection.periods),
        freq=selection.freq,
    )
    models.validate_hs_periods(produto, inverse.periods, inverse.reporter)
    started = time.monotonic()
    exports, export_meta = await _acquire(
        selection, api_key=api_key, require_complete=require_complete
    )
    imports, import_meta = await _acquire(
        inverse, api_key=api_key, require_complete=require_complete
    )
    fetch_ms = int((time.monotonic() - started) * 1000)
    started = time.monotonic()
    frame = parser.parse_mirror(
        exports,
        imports,
        None,
        None,
        reporter_code=selection.reporter,
        partner_code=selection.partner,
    )
    contracts.validate_dataset(frame, source_contracts.TRADE_MIRROR_V2)
    parse_ms = int((time.monotonic() - started) * 1000)
    meta = metadata.mirror_meta(
        export_meta, import_meta, frame, fetch_ms=fetch_ms, parse_ms=parse_ms
    )
    return result.finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(
            c.name
            for c in source_contracts.TRADE_MIRROR_V2.columns
            if c.type == contracts.ColumnType.STRING
        ),
    )


def paises() -> list[str]:
    return sorted(set(models.COMTRADE_PAISES_INV.values()))


def produtos() -> dict[str, list[str]]:
    return {name: list(codes) for name, codes in models.HS_PRODUTOS_AGRO.items()}
