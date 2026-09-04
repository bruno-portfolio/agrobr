from __future__ import annotations

import time
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.normalize.crops import _remover_acentos
from agrobr.normalize.regions import UFS_VALIDAS
from agrobr.utils.result import build_source_meta, finalize_result

from . import client, parser
from .models import CULTURAS_ZARC, extract_safras

logger = structlog.get_logger()

_MAX_CACHED_SAFRAS = 2
_cache: dict[str, tuple[pd.DataFrame, str]] = {}


@overload
async def zoneamento(
    *,
    cultura: str | None = None,
    uf: str | None = None,
    municipio: int | str | None = None,
    safra: str | None = None,
    solo: int | None = None,
    ciclo: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def zoneamento(
    *,
    cultura: str | None = ...,
    uf: str | None = ...,
    municipio: int | str | None = ...,
    safra: str | None = ...,
    solo: int | None = ...,
    ciclo: int | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def zoneamento(
    *,
    cultura: str | None = None,
    uf: str | None = None,
    municipio: int | str | None = None,
    safra: str | None = None,
    solo: int | None = None,
    ciclo: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Janelas de plantio ZARC por municipio/cultura/solo/ciclo.

    Args:
        cultura: nome da cultura (ex: "soja", "milho_1", "trigo")
        uf: sigla UF (ex: "MT", "SP")
        municipio: codigo IBGE 7 digitos (int) ou nome parcial (str)
        safra: "2025/2026" ou "perene" (default: safra mais recente)
        solo: codigo tipo de solo (1-3 antigo, 11-16 novo 6-AD)
        ciclo: codigo ciclo cultivar (20, 21, 22, 24)
    """
    if uf is not None:
        uf_upper = uf.upper()
        if uf_upper not in UFS_VALIDAS:
            raise InvalidParameterError(f"UF invalida: '{uf}'. Validas: {sorted(UFS_VALIDAS)}")
    else:
        uf_upper = None

    resources: list[dict[str, str]] | None = None
    if safra is None:
        resources = await client.discover_resources()
        safras = extract_safras(resources)
        safra = next((s for s in reversed(safras) if s != "perene"), None)
        if safra is None:
            raise SourceUnavailableError(
                source="zarc",
                last_error="CKAN sem recursos de safra",
            )

    if safra in _cache:
        df, source_url = _cache[safra]
        df = df.copy()
        fetch_ms = 0
    else:
        t0 = time.monotonic()
        csv_bytes, source_url = await client.fetch_tabua_risco(safra, resources=resources)
        fetch_ms = int((time.monotonic() - t0) * 1000)

        df = parser.parse_tabua_risco(csv_bytes)
        if len(_cache) >= _MAX_CACHED_SAFRAS:
            _cache.pop(next(iter(_cache)))
        _cache[safra] = (df, source_url)
        df = df.copy()

    t1 = time.monotonic()

    if cultura is not None:
        cultura_key = _remover_acentos(cultura.lower().replace(" ", "_"))
        culturas_tabua = df["cultura"].dropna().unique().tolist()
        matches = [c for c in culturas_tabua if _remover_acentos(str(c)) == cultura_key]
        if not matches:
            raise InvalidParameterError(
                f"Cultura '{cultura}' nao encontrada na tabua {safra}. "
                f"Disponiveis: {sorted(culturas_tabua)}"
            )
        df = df[df["cultura"].isin(matches)]
    if uf_upper is not None:
        df = df[df["uf"] == uf_upper]
    if municipio is not None:
        if isinstance(municipio, int) or (isinstance(municipio, str) and municipio.isdigit()):
            df = df[df["geocodigo"] == str(municipio)]
        else:
            df = df[df["municipio"].str.contains(str(municipio), case=False, na=False, regex=False)]
    if solo is not None:
        df = df[df["solo_codigo"] == solo]
    if ciclo is not None:
        df = df[df["ciclo_codigo"] == ciclo]

    df = df.reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "zarc",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def culturas() -> list[str]:
    return sorted(set(CULTURAS_ZARC.values()))


async def safras_disponiveis() -> list[str]:
    resources = await client.discover_resources()
    return extract_safras(resources)
