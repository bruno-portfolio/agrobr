from __future__ import annotations

import copy
import time
import warnings
from dataclasses import dataclass
from typing import Any, Literal, overload

import duckdb
import pandas as pd

from agrobr import _log, constants, contracts
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.utils import result

from . import acquisition, cache, catalog, client, models, parser, query, store

logger = _log.get_logger(__name__)


@dataclass(frozen=True)
class _Selection:
    frame: pd.DataFrame
    resource: acquisition.HTTPResource
    details: dict[str, Any]
    cache_status: str
    fetch_ms: int
    parse_ms: int


def _guard(as_polars: bool, return_meta: bool, use_cache: bool, kwargs: dict[str, Any]) -> None:
    if kwargs:
        raise InvalidParameterError(f"Argumentos desconhecidos: {sorted(kwargs)}")
    if any(type(flag) is not bool for flag in (as_polars, return_meta, use_cache)):
        raise InvalidParameterError("as_polars, return_meta e use_cache devem ser booleanos")
    if get_snapshot() is not None:
        raise InvalidParameterError(
            "ZARC não suporta deterministic: safra não seleciona revisão histórica"
        )


async def _catalog(use_cache: bool) -> tuple[catalog.Catalog, bool]:
    cached = cache.get_catalog() if use_cache else None
    if cached is not None:
        return cached, True
    captured = await client.discover_catalog()
    if use_cache:
        cache.put_catalog(captured)
    return captured, False


def _culture_available(details: dict[str, Any], selected: query.ZarcQuery, safra: str) -> None:
    if selected.cultura is not None and selected.cultura not in details["cultures"]:
        raise parser._missing_culture(selected.cultura, safra, details["cultures"])


def _filtered(frame: pd.DataFrame, selected: query.ZarcQuery) -> pd.DataFrame:
    mask = pd.Series(True, index=frame.index)
    for name, value in [
        ("cultura", selected.cultura),
        ("uf", selected.uf),
        ("solo_codigo", selected.solo),
        ("ciclo_codigo", selected.ciclo),
    ]:
        if value is not None:
            mask &= frame[name].eq(value)
    if selected.municipio is not None:
        municipality = str(selected.municipio)
        if isinstance(selected.municipio, int) or municipality.isascii() and municipality.isdigit():
            mask &= frame["geocodigo"].eq(municipality)
        else:
            mask &= (
                frame["municipio"]
                .map(models.normalize_municipio, na_action="ignore")
                .str.contains(models.normalize_municipio(municipality), regex=False)
            )
    return frame.loc[mask].reset_index(drop=True)


def _conferido(resource: acquisition.HTTPResource, details: dict[str, Any]) -> bool:
    """O pacote só vai para o store, e só é reusado, com o tamanho conferido e com registros."""
    return resource.published_size_bytes == resource.size_bytes and details["source_rows"] > 0


def _cached(
    key: str, selected: query.ZarcQuery, safra: str
) -> tuple[pd.DataFrame, store.Entry] | None:
    try:
        entry = store.lookup(key)
        if entry is None:
            return None
        if not _conferido(entry.details.resource, entry.details.parser):
            raise ValueError("Pacote ZARC do store sem tamanho conferido ou sem registros")
        if (
            entry.safra_recurso != safra
            or entry.details.parser["parser_version"] != parser.PARSER_VERSION
        ):
            raise ValueError("Versão ou safra do cache ZARC incompatível")
        if not isinstance(entry.details.parser["cultures"], dict):
            raise ValueError("Culturas do cache ZARC inválidas")
        frame = store.query(key, selected)
        frame = frame.assign(cod_municipio=regions.cod_municipio(frame["geocodigo"]))
        contracts.validate_dataset(frame, "zoneamento_agricola")
    except (
        duckdb.Error,
        OSError,
        ValueError,
        TypeError,
        KeyError,
        OverflowError,
        ContractViolationError,
    ) as exc:
        logger.warning("zarc_store_read_failed", error=str(exc), error_type=type(exc).__name__)
        return None
    _culture_available(entry.details.parser, selected, safra)
    return frame, entry


def _save(key: str, safra: str, frame: pd.DataFrame, details: store.StoredDetails) -> str:
    try:
        store.store(key, details.resource.sha256, safra, frame, details)
    except (duckdb.Error, OSError, ValueError, TypeError, KeyError, OverflowError) as exc:
        logger.warning("zarc_store_write_failed", error=str(exc), error_type=type(exc).__name__)
        return "store_error"
    return "store_miss"


async def _load(
    selected: catalog.CatalogResource,
    selected_query: query.ZarcQuery,
    safra: str,
    started: float,
    use_cache: bool,
) -> _Selection:
    key = selected.revision_key()
    cached = _cached(key, selected_query, safra) if use_cache else None
    if cached is not None:
        frame, entry = cached
        details = copy.deepcopy(entry.details.parser)
        details["selected_rows"] = len(frame)
        return _Selection(
            frame,
            entry.details.resource,
            details,
            "store_hit",
            int((time.monotonic() - started) * 1000),
            0,
        )
    captured = await client.download_acquisition(selected.url)
    fetch_ms = int((time.monotonic() - started) * 1000)
    parse_started = time.monotonic()
    bundle = parser.parse_tabua_risco_bundle(
        captured.content, query=None if use_cache else selected_query, expected_safra=safra
    )
    resource = captured.resource
    del captured
    frame = bundle.frame.assign(cod_municipio=regions.cod_municipio(bundle.frame["geocodigo"]))
    contracts.validate_dataset(frame, "zoneamento_agricola")
    if resource.published_size_bytes is None:
        aviso = "zarc: tamanho do arquivo não conferido (o servidor não informou o total)"
        bundle.details["warnings"].append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=3)
    status = "bypass"
    if use_cache:
        _culture_available(bundle.details, selected_query, safra)
        status = (
            _save(
                key,
                safra,
                bundle.frame,
                store.StoredDetails(resource=resource, parser=bundle.details),
            )
            if _conferido(resource, bundle.details)
            else "store_skipped"
        )
        frame = _filtered(frame, selected_query)
    details = copy.deepcopy(bundle.details)
    details["selected_rows"] = len(frame)
    return _Selection(
        frame,
        resource,
        details,
        status,
        fetch_ms,
        int((time.monotonic() - parse_started) * 1000),
    )


def _metadata(
    frame: pd.DataFrame,
    selected_query: query.ZarcQuery,
    effective_safra: str,
    listing: catalog.Catalog,
    selected: catalog.CatalogResource,
    resource: acquisition.HTTPResource,
    parser_details: dict[str, Any],
    *,
    from_cache: bool,
    catalog_cached: bool,
    cache_status: str,
    fetch_ms: int,
    parse_ms: int,
    use_cache: bool,
) -> MetaInfo:
    details = {
        "resource": resource.model_dump(mode="json"),
        "catalog": {
            "resource": listing.acquisition.model_dump(mode="json"),
            "publication": listing.publication.model_dump(mode="json"),
            "from_cache": catalog_cached,
            "expires_at": cache.expires_at(
                listing.acquisition.received_at, is_catalog=True
            ).isoformat()
            if use_cache
            else None,
        },
        "selected_resource": selected.model_dump(mode="json"),
        "query": selected_query.model_dump(mode="json"),
        "effective_safra": effective_safra,
        "parser": parser_details,
        "cache": {
            "status": cache_status,
            "from_cache": from_cache,
            "enabled": use_cache,
            "ttl_seconds": constants.ZARC_CACHE_TTL_SECONDS,
            "max_revisions": constants.ZARC_STORE_MAX_REVISIONS,
            "storage": constants.ZARC_STORE_FILENAME,
        },
        "coverage": {
            "status": "unknown" if resource.published_size_bytes is None else "size_checked",
            "reason": "no_source_total"
            if resource.published_size_bytes is None
            else "published_size_matches_received",
            "published_size_bytes": resource.published_size_bytes,
            "transactional_snapshot": False,
        },
    }
    meta = result.build_source_meta(
        "zarc",
        resource.final_url,
        "cache" if from_cache else "httpx+csv",
        fetch_ms,
        parse_ms,
        frame,
        parser.PARSER_VERSION,
        schema_version=contracts.get_contract("zoneamento_agricola").version,
        raw_content_hash=resource.sha256,
        source_details=details,
    )
    meta.contract_version = meta.schema_version
    meta.fetched_at = resource.received_at
    meta.fetch_timestamp = resource.received_at
    meta.raw_content_size = resource.size_bytes
    meta.from_cache = from_cache
    meta.validation_warnings = list(parser_details.get("warnings", []))
    if use_cache and cache_status in {"store_hit", "store_miss"}:
        meta.cache_key = selected.revision_key()
        meta.cache_expires_at = cache.expires_at(resource.received_at)
    return meta


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
    use_cache: bool = True,
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
    use_cache: bool = ...,
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
    use_cache: bool = True,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    _guard(as_polars, return_meta, use_cache, kwargs)
    selected_query = query.build_query(
        cultura=cultura, uf=uf, municipio=municipio, safra=safra, solo=solo, ciclo=ciclo
    )
    started = time.monotonic()
    async with cache.acquisition_lock():
        listing, catalog_cached = await _catalog(use_cache)
        effective_safra, selected = listing.select(selected_query.safra)
        loaded = await _load(selected, selected_query, effective_safra, started, use_cache)
        meta = _metadata(
            loaded.frame,
            selected_query,
            effective_safra,
            listing,
            selected,
            loaded.resource,
            loaded.details,
            from_cache=loaded.cache_status == "store_hit",
            catalog_cached=catalog_cached,
            cache_status=loaded.cache_status,
            fetch_ms=loaded.fetch_ms,
            parse_ms=loaded.parse_ms,
            use_cache=use_cache,
        )
    return result.finalize_result(
        loaded.frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(constants.ZARC_STRING_COLUMNS),
    )


def culturas() -> list[str]:
    return sorted(set(models.CULTURAS_ZARC.values()))


async def safras_disponiveis(*, use_cache: bool = True) -> list[str]:
    _guard(False, False, use_cache, {})
    async with cache.acquisition_lock():
        listing, _ = await _catalog(use_cache)
        return models.extract_safras(listing.resources())
