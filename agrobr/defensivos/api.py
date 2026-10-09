from __future__ import annotations

import hashlib
import time
from datetime import UTC, timedelta
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, constants, contracts
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result as result_utils
from agrobr.utils import tasks
from agrobr.utils.time import utcnow

from . import client, parser, snapshot

logger = _log.get_logger(__name__)


def _validate_query(
    kind: str,
    filters: dict[str, str | None],
    extras: dict[str, Any],
    flags: dict[str, bool],
) -> dict[str, str]:
    if kind not in ("formulados", "tecnicos"):
        raise InvalidParameterError("tipo deve ser formulados ou tecnicos")
    if extras:
        raise InvalidParameterError(f"Parâmetros não suportados: {', '.join(sorted(extras))}")
    for name, flag in flags.items():
        if not isinstance(flag, bool):
            raise InvalidParameterError(f"{name} deve ser booleano")
    selected = {}
    for name, value in filters.items():
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip():
            raise InvalidParameterError(f"{name} deve ser uma string não vazia")
        selected[name] = value.strip()
    return selected


def _validate_tables(tables: dict[str, pd.DataFrame]) -> None:
    for name, frame in tables.items():
        contracts.validate_dataset(frame, f"agrofit_{name}")


async def _load_snapshot(kind: str, use_cache: bool) -> snapshot.Snapshot:
    async with snapshot.acquisition_lock(kind):
        if use_cache:
            started = time.monotonic()
            cached = await tasks.to_thread_ate_o_fim(snapshot.read_snapshot, kind)
            if cached is not None:
                try:
                    _validate_tables(cached.tables)
                except ContractViolationError:
                    logger.warning("defensivos_cached_contract_invalid", kind=kind)
                else:
                    cached.meta.from_cache = True
                    cached.meta.source_method = "cache"
                    cached.meta.fetch_duration_ms = 0
                    cached.meta.parse_duration_ms = int((time.monotonic() - started) * 1000)
                    return cached
        started = time.monotonic()
        if kind == "formulados":
            raw = await client.download_formulados()
            url = client.FORMULADOS_URL
        else:
            raw = await client.download_tecnicos()
            url = client.TECNICOS_URL
        fetched_at = utcnow().replace(tzinfo=UTC)
        fetch_ms = int((time.monotonic() - started) * 1000)
        started = time.monotonic()
        parse_bundle = (
            parser.parse_formulados_bundle if kind == "formulados" else parser.parse_tecnicos_bundle
        )
        tables, details = await tasks.to_thread_ate_o_fim(parse_bundle, raw)
        _validate_tables(tables)
        parse_ms = int((time.monotonic() - started) * 1000)
        raw_hash = hashlib.sha256(raw).hexdigest()
        details["resource"] = {
            "url": url,
            "sha256": raw_hash,
            "bytes": len(raw),
            "fetched_at": fetched_at.isoformat(),
        }
        details["kind"] = kind
        details["revision_semantics"] = "current_export_identified_by_content_hash"
        meta = result_utils.build_source_meta(
            "defensivos",
            url,
            "httpx+csv",
            fetch_ms,
            parse_ms,
            tables[kind],
            parser.PARSER_VERSION,
            schema_version=contracts.get_contract(f"agrofit_{kind}").version,
            raw_content_hash=raw_hash,
            source_details=details,
        )
        meta.fetched_at = fetched_at
        meta.contract_version = meta.schema_version
        meta.raw_content_size = len(raw)
        meta.validation_warnings = list(details.get("warnings", []))
        meta.cache_key = f"defensivos/{kind}/parser{parser.PARSER_VERSION}" if use_cache else None
        meta.cache_expires_at = (
            fetched_at + timedelta(seconds=constants.DEFENSIVOS_CACHE_TTL_SECONDS)
            if use_cache
            else None
        )
        if use_cache:
            try:
                await tasks.to_thread_ate_o_fim(snapshot.write_snapshot, kind, tables, meta)
            except OSError as error:
                logger.warning(
                    "defensivos_cache_write_failed", kind=kind, error=type(error).__name__
                )
                meta.cache_key = None
                meta.cache_expires_at = None
        return snapshot.Snapshot(tables=tables, meta=meta)


def _filter(frame: pd.DataFrame, filters: dict[str, str]) -> pd.DataFrame:
    result = frame.copy()
    for name, value in filters.items():
        column = "marca_comercial" if name == "marca" else name
        if name in ("nr_registro", "organicos"):
            mask = result[column].eq(value)
        elif name == "situacao":
            mask = result[column].str.strip().str.casefold().eq(value.casefold())
        else:
            mask = result[column].str.contains(value, case=False, na=False, regex=False)
        result = result.loc[mask.fillna(False)]
    return result.reset_index(drop=True)


async def _query(
    kind: str,
    table: str,
    *,
    filters: dict[str, str | None],
    extras: dict[str, Any],
    as_polars: bool,
    return_meta: bool,
    use_cache: bool,
) -> result_utils.DataFrameResult:
    selected = _validate_query(
        kind,
        filters,
        extras,
        {"use_cache": use_cache, "as_polars": as_polars, "return_meta": return_meta},
    )
    result_utils.check_polars(as_polars)
    acquired = await _load_snapshot(kind, use_cache)
    started = time.monotonic()
    frame = _filter(acquired.tables[table], selected)
    contract = contracts.get_contract(f"agrofit_{table}")
    contracts.validate_dataset(frame, contract)
    meta = acquired.meta
    now = utcnow()
    meta.timestamp = now
    meta.fetch_timestamp = meta.fetched_at
    meta.records_count = len(frame)
    meta.columns = frame.columns.tolist()
    meta.schema_version = contract.version
    meta.contract_version = contract.version
    meta.parse_duration_ms += int((time.monotonic() - started) * 1000)
    meta.source_details["query"] = {"table": table, "filters": selected}
    return result_utils.finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(
            column.name for column in contract.columns if column.type == contracts.ColumnType.STRING
        ),
    )


@overload
async def formulados(
    *,
    ingrediente_ativo: str | None = None,
    classe_toxicologica: str | None = None,
    classe_ambiental: str | None = None,
    titular: str | None = None,
    organicos: str | None = None,
    marca: str | None = None,
    formulacao: str | None = None,
    classe: str | None = None,
    nr_registro: str | None = None,
    situacao: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    use_cache: bool = True,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def formulados(
    *,
    ingrediente_ativo: str | None = ...,
    classe_toxicologica: str | None = ...,
    classe_ambiental: str | None = ...,
    titular: str | None = ...,
    organicos: str | None = ...,
    marca: str | None = ...,
    formulacao: str | None = ...,
    classe: str | None = ...,
    nr_registro: str | None = ...,
    situacao: str | None = ...,
    as_polars: Literal[False] = ...,
    return_meta: Literal[True],
    use_cache: bool = ...,
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def formulados(
    *,
    ingrediente_ativo: str | None = ...,
    classe_toxicologica: str | None = ...,
    classe_ambiental: str | None = ...,
    titular: str | None = ...,
    organicos: str | None = ...,
    marca: str | None = ...,
    formulacao: str | None = ...,
    classe: str | None = ...,
    nr_registro: str | None = ...,
    situacao: str | None = ...,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = ...,
    **kwargs: Any,
) -> result_utils.DataFrameResult: ...


async def formulados(
    *,
    ingrediente_ativo: str | None = None,
    classe_toxicologica: str | None = None,
    classe_ambiental: str | None = None,
    titular: str | None = None,
    organicos: str | None = None,
    marca: str | None = None,
    formulacao: str | None = None,
    classe: str | None = None,
    nr_registro: str | None = None,
    situacao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = True,
    **kwargs: Any,
) -> result_utils.DataFrameResult:
    return await _query(
        "formulados",
        "formulados",
        filters={
            "ingrediente_ativo": ingrediente_ativo,
            "classe_toxicologica": classe_toxicologica,
            "classe_ambiental": classe_ambiental,
            "titular": titular,
            "organicos": organicos,
            "marca": marca,
            "formulacao": formulacao,
            "classe": classe,
            "nr_registro": nr_registro,
            "situacao": situacao,
        },
        extras=kwargs,
        as_polars=as_polars,
        return_meta=return_meta,
        use_cache=use_cache,
    )


@overload
async def autorizacoes(
    *,
    nr_registro: str | None = None,
    cultura: str | None = None,
    ingrediente_ativo: str | None = None,
    classe: str | None = None,
    situacao: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    use_cache: bool = True,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def autorizacoes(
    *,
    nr_registro: str | None = ...,
    cultura: str | None = ...,
    ingrediente_ativo: str | None = ...,
    classe: str | None = ...,
    situacao: str | None = ...,
    as_polars: Literal[False] = ...,
    return_meta: Literal[True],
    use_cache: bool = ...,
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def autorizacoes(
    *,
    nr_registro: str | None = ...,
    cultura: str | None = ...,
    ingrediente_ativo: str | None = ...,
    classe: str | None = ...,
    situacao: str | None = ...,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = ...,
    **kwargs: Any,
) -> result_utils.DataFrameResult: ...


async def autorizacoes(
    *,
    nr_registro: str | None = None,
    cultura: str | None = None,
    ingrediente_ativo: str | None = None,
    classe: str | None = None,
    situacao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = True,
    **kwargs: Any,
) -> result_utils.DataFrameResult:
    return await _query(
        "formulados",
        "autorizacoes",
        filters={
            "nr_registro": nr_registro,
            "cultura": cultura,
            "ingrediente_ativo": ingrediente_ativo,
            "classe": classe,
            "situacao": situacao,
        },
        extras=kwargs,
        as_polars=as_polars,
        return_meta=return_meta,
        use_cache=use_cache,
    )


@overload
async def tecnicos(
    *,
    ingrediente_ativo: str | None = None,
    titular: str | None = None,
    classe: str | None = None,
    marca: str | None = None,
    nr_registro: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    use_cache: bool = True,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def tecnicos(
    *,
    ingrediente_ativo: str | None = ...,
    titular: str | None = ...,
    classe: str | None = ...,
    marca: str | None = ...,
    nr_registro: str | None = ...,
    as_polars: Literal[False] = ...,
    return_meta: Literal[True],
    use_cache: bool = ...,
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def tecnicos(
    *,
    ingrediente_ativo: str | None = ...,
    titular: str | None = ...,
    classe: str | None = ...,
    marca: str | None = ...,
    nr_registro: str | None = ...,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = ...,
    **kwargs: Any,
) -> result_utils.DataFrameResult: ...


async def tecnicos(
    *,
    ingrediente_ativo: str | None = None,
    titular: str | None = None,
    classe: str | None = None,
    marca: str | None = None,
    nr_registro: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = True,
    **kwargs: Any,
) -> result_utils.DataFrameResult:
    return await _query(
        "tecnicos",
        "tecnicos",
        filters={
            "ingrediente_ativo": ingrediente_ativo,
            "titular": titular,
            "classe": classe,
            "marca": marca,
            "nr_registro": nr_registro,
        },
        extras=kwargs,
        as_polars=as_polars,
        return_meta=return_meta,
        use_cache=use_cache,
    )


@overload
async def composicao(
    *,
    tipo: str = "formulados",
    nr_registro: str | None = None,
    ingrediente_ativo: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    use_cache: bool = True,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def composicao(
    *,
    tipo: str = ...,
    nr_registro: str | None = ...,
    ingrediente_ativo: str | None = ...,
    as_polars: Literal[False] = ...,
    return_meta: Literal[True],
    use_cache: bool = ...,
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def composicao(
    *,
    tipo: str = ...,
    nr_registro: str | None = ...,
    ingrediente_ativo: str | None = ...,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = ...,
    **kwargs: Any,
) -> result_utils.DataFrameResult: ...


async def composicao(
    *,
    tipo: str = "formulados",
    nr_registro: str | None = None,
    ingrediente_ativo: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = True,
    **kwargs: Any,
) -> result_utils.DataFrameResult:
    return await _query(
        tipo,
        "composicao",
        filters={"nr_registro": nr_registro, "ingrediente_ativo": ingrediente_ativo},
        extras=kwargs,
        as_polars=as_polars,
        return_meta=return_meta,
        use_cache=use_cache,
    )
