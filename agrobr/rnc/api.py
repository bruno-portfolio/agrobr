from __future__ import annotations

import time
from datetime import timedelta
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, constants, contracts
from agrobr.models import MetaInfo
from agrobr.normalize.regions import remover_acentos
from agrobr.utils import result

from . import acquisition, loading, parser, query

logger = _log.get_logger(__name__)


def _sem_acento(texto: str) -> str:
    return remover_acentos(texto).casefold()


def _select(frame: pd.DataFrame, filters: dict[str, str]) -> pd.DataFrame:
    selected = frame
    for name, value in filters.items():
        series = selected["nome_comum" if name == "especie" else name]
        mask = (
            series.eq(value)
            if name.startswith("nr_")
            else series.map(_sem_acento, na_action="ignore").str.contains(
                _sem_acento(value), na=False, regex=False
            )
        )
        selected = selected[mask]
    return selected.copy().reset_index(drop=True)


def _metadata(
    table: loading.RncTable,
    frame: pd.DataFrame,
    filters: dict[str, str],
    filter_ms: int,
) -> MetaInfo:
    captured = table.acquisition
    route = f"rnc_{captured.kind}"
    expected = captured.search.reported_total
    details = {
        "acquisition": captured.provenance(),
        "parser": table.parsed.details,
        "selection": {"filters": filters, "output_rows": len(frame)},
        "filter_duration_ms": filter_ms,
        "cache_status": table.cache_status,
        "coverage": {
            "scope": "exported_search_population",
            "status": "count_matched" if expected is not None else "unknown",
            "reported_total": expected,
            "source_rows": len(table.parsed.frame),
            "output_rows": len(frame),
            "transactional_snapshot": False,
            "reason": (
                "CSV count equals the total reported by the preceding public search"
                if expected is not None
                else "The public search did not expose a verified total"
            ),
        },
    }
    meta = result.build_source_meta(
        "rnc",
        captured.resource.url,
        "cache" if table.from_cache else "httpx+csv",
        table.fetch_ms,
        table.parse_ms,
        frame,
        parser.PARSER_VERSION,
        schema_version="1.0",
        attempted_sources=[route],
        selected_source=route,
        raw_content_hash=captured.resource.sha256,
        source_details=details,
    )
    meta.fetched_at = captured.resource.received_at
    meta.fetch_timestamp = captured.resource.received_at
    meta.raw_content_size = captured.resource.size_bytes
    meta.from_cache = table.from_cache
    if table.cache_status in {"hit", "stored"}:
        meta.cache_key = f"rnc/{captured.kind}/parser{parser.PARSER_VERSION}"
        meta.cache_expires_at = captured.resource.received_at + timedelta(
            seconds=constants.RNC_CACHE_TTL_SECONDS
        )
    return meta


async def _fetch(
    kind: acquisition.Family,
    filters: dict[str, str | None],
    extras: dict[str, Any],
    *,
    use_cache: bool,
    as_polars: bool,
    return_meta: bool,
) -> result.DataFrameResult:
    selected = query.validate_filters(
        filters, extras, use_cache=use_cache, as_polars=as_polars, return_meta=return_meta
    )
    logger.info("rnc_fetch", kind=kind, filter_names=list(selected))
    table = await loading.load(kind, use_cache)
    started = time.monotonic()
    frame = _select(table.parsed.frame, selected)
    filter_ms = int((time.monotonic() - started) * 1000)
    contracts.validate_dataset(frame, f"rnc_{kind}")
    meta = _metadata(table, frame, selected, filter_ms) if return_meta else None
    return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    nr_registro: str | None = None,
    nr_formulario: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def registradas(
    *,
    cultivar: str | None = ...,
    especie: str | None = ...,
    grupo: str | None = ...,
    situacao: str | None = ...,
    mantenedor: str | None = ...,
    nr_registro: str | None = ...,
    nr_formulario: str | None = ...,
    use_cache: bool = ...,
    as_polars: Literal[False] = ...,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def registradas(
    *,
    cultivar: str | None = ...,
    especie: str | None = ...,
    grupo: str | None = ...,
    situacao: str | None = ...,
    mantenedor: str | None = ...,
    nr_registro: str | None = ...,
    nr_formulario: str | None = ...,
    use_cache: bool = ...,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult: ...


async def registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    nr_registro: str | None = None,
    nr_formulario: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    return await _fetch(
        "registradas",
        {
            "cultivar": cultivar,
            "especie": especie,
            "grupo": grupo,
            "situacao": situacao,
            "mantenedor": mantenedor,
            "nr_registro": nr_registro,
            "nr_formulario": nr_formulario,
        },
        kwargs,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )


@overload
async def protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    nr_processo: str | None = None,
    nr_certificado: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def protegidas(
    *,
    cultivar: str | None = ...,
    especie: str | None = ...,
    situacao: str | None = ...,
    titular: str | None = ...,
    nr_processo: str | None = ...,
    nr_certificado: str | None = ...,
    use_cache: bool = ...,
    as_polars: Literal[False] = ...,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def protegidas(
    *,
    cultivar: str | None = ...,
    especie: str | None = ...,
    situacao: str | None = ...,
    titular: str | None = ...,
    nr_processo: str | None = ...,
    nr_certificado: str | None = ...,
    use_cache: bool = ...,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult: ...


async def protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    nr_processo: str | None = None,
    nr_certificado: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    return await _fetch(
        "protegidas",
        {
            "cultivar": cultivar,
            "especie": especie,
            "situacao": situacao,
            "titular": titular,
            "nr_processo": nr_processo,
            "nr_certificado": nr_certificado,
        },
        kwargs,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
    )
