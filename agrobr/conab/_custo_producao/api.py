from __future__ import annotations

import hashlib
import importlib
import json
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast, overload

import httpx
import pandas as pd
from pydantic import ValidationError

from agrobr import constants, contracts
from agrobr.contracts.conab_custos import CONAB_CUSTOS_V3, TEXTO
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    SourceUnavailableError,
)
from agrobr.models import MetaInfo
from agrobr.utils.time import utcnow_aware
from agrobr.utils.validation import validate_uf
from agrobr.utils.warnings import warn_once

from . import models
from ._acquisition import Acquisition
from ._context import candidatos, context, inventory, key, lista_curta, select
from ._parse import frame, parse_selected, totals
from ._workbook import Workbook

if TYPE_CHECKING:
    import polars as pl

    Frame: TypeAlias = pd.DataFrame | pl.DataFrame


def prepare_query(
    as_polars: bool, return_meta: bool, *, use_cache: bool = True, **kwargs: Any
) -> models.ConsultaCusto:
    if any(type(value) is not bool for value in (as_polars, return_meta, use_cache)):
        raise InvalidParameterError("as_polars, return_meta e use_cache devem ser bool")
    try:
        query = models.ConsultaCusto(**kwargs)
    except ValidationError as error:
        raise InvalidParameterError(str(error)) from error
    if query.uf is not None:
        query = query.model_copy(update={"uf": validate_uf(query.uf)})
    if get_snapshot() is not None:
        raise InvalidParameterError("Custos CONAB não oferece snapshot imutável para deterministic")
    if as_polars:
        importlib.import_module("polars")
    return query


def _resource(resources: list[models.RecursoCusto], requested: str | None) -> models.RecursoCusto:
    if requested is not None and all(r.planilha != requested for r in resources):
        raise InvalidParameterError(
            f"Planilha não está no catálogo atual da CONAB: {requested}. "
            f"Planilhas no catálogo ({len(resources)}): "
            f"{lista_curta([r.planilha for r in resources]) or 'nenhuma'}. "
            "Use catalogo_custos(produto) para ver o nome atual da planilha."
        )
    selected = [r for r in resources if requested is None or r.planilha == requested]
    if len(selected) != 1:
        raise InvalidParameterError(
            f"Selecione planilha= explicitamente; {len(selected)} candidatas: "
            f"{[r.planilha for r in selected[:15]]}. "
            "Use catalogo_custos(produto, planilha=...) para selecionar a aba e o contexto."
        )
    return selected[0]


def _meta(
    acquired: Acquisition,
    query: models.ConsultaCusto,
    df: pd.DataFrame,
    details: dict[str, Any],
    fetch_ms: int,
    parse_ms: int,
) -> MetaInfo:
    manifest = {
        "query": query.model_dump(mode="json"),
        "acquisition": acquired.details(),
        "selection": details.get("selection"),
    }
    encoded = json.dumps(
        manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    last = acquired.last_receipt()
    now = utcnow_aware()
    fetched = datetime.fromisoformat(last["finished_at"])
    return MetaInfo(
        source="conab_custo",
        source_url=last["url"],
        source_method="httpx",
        fetched_at=fetched,
        timestamp=now,
        fetch_timestamp=fetched,
        fetch_duration_ms=fetch_ms,
        parse_duration_ms=parse_ms,
        raw_content_hash=hashlib.sha256(encoded).hexdigest(),
        raw_content_size=len(encoded),
        records_count=len(df),
        columns=list(df.columns),
        parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
        schema_version="3.0",
        contract_version="3.0",
        attempted_sources=["conab_custo"],
        selected_source="conab_custo",
        data_sources=["conab_custo"],
        from_cache=acquired.catalog_cache == "hit" and not acquired.receipts,
        source_details={
            **details,
            "catalog_cache": acquired.catalog_cache,
            "manifest": manifest,
            "hash_kind": "sha256_canonical_utf8_query_acquisition_selection_manifest",
            "limits": {
                name: getattr(constants, name)
                for name in (
                    "CONAB_CUSTOS_MAX_BODY_BYTES",
                    "CONAB_CUSTOS_MAX_TOTAL_BYTES",
                    "CONAB_CUSTOS_MAX_REQUESTS",
                    "CONAB_CUSTOS_MAX_EXPANDED_BYTES",
                    "CONAB_CUSTOS_MAX_SHEETS",
                    "CONAB_CUSTOS_MAX_SHEET_ROWS",
                    "CONAB_CUSTOS_MAX_SHEET_CELLS",
                )
            },
            "raw_content_size_basis": "canonical UTF-8 manifest bytes, the same object as raw_content_hash",
            "received_bytes": acquired.received_bytes,
            "received_bytes_basis": "all HTTP attempts including metadata and failures",
            "data_file_bytes": sum(
                receipt["bytes"]
                for receipt in acquired.receipts
                if receipt["role"] == "workbook" and receipt.get("eof")
            ),
            "output_dtypes": {str(c): str(df[c].dtype) for c in df.columns},
        },
    )


def _subtotal_warnings(result: models.ResultadoCusto) -> list[str]:
    messages = []
    for check in result.detalhes.get("subtotal_checks", []):
        if check["fecha"]:
            continue
        parts = "dos itens" if check["tipo"] == "subtotal" else "das parcelas"
        message = (
            f"CONAB custos: na aba {result.contexto.aba} ({result.contexto.planilha}), "
            f"{check['item'].strip()} publicado {check['publicado']:.2f} e a soma {parts} "
            f"{check['soma_itens']:.2f} (diferença de "
            f"{check['publicado'] - check['soma_itens']:.2f}); o agrobr repassa os números publicados"
        )
        warn_once(
            f"conab_custos_subtotal:{result.contexto.planilha}:{result.contexto.aba}:{check['linha']}",
            message,
        )
        messages.append(message)
    return messages


def finalize_output(
    df: pd.DataFrame, meta: MetaInfo, as_polars: bool, return_meta: bool
) -> Frame | tuple[Frame, MetaInfo]:
    output: Any = df
    if as_polars:
        pl = importlib.import_module("polars")
        series = {}
        for name in df.columns:
            source = df[name]
            if pd.api.types.is_datetime64_any_dtype(source.dtype):
                values = [None if pd.isna(v) else v.value for v in source]
                series[name] = pl.Series(name, values, dtype=pl.Int64, strict=True).cast(
                    pl.Datetime("ns")
                )
            else:
                dtype = (
                    pl.Int64
                    if str(source.dtype) == "Int64"
                    else pl.Float64
                    if str(source.dtype) == "float64"
                    else pl.Utf8
                )
                values = [
                    None
                    if pd.isna(v)
                    else int(v)
                    if dtype == pl.Int64
                    else float(v)
                    if dtype == pl.Float64
                    else str(v)
                    for v in source
                ]
                series[name] = pl.Series(name, values, dtype=dtype, strict=True)
            del values
        output = pl.DataFrame(series)
        meta.source_details["output_dtypes"] = {str(c): str(t) for c, t in output.schema.items()}
    typed_output = cast("Frame", output)
    return (typed_output, meta) if return_meta else typed_output


def _sem_planilha(cultura: str | None, acquired: Acquisition) -> InvalidParameterError:
    prefix = key(models.normalize_cultura(cultura or ""))
    matches = [c for c in acquired.culturas_catalogo if key(c).startswith(prefix)]
    label = "culturas no catálogo com esse prefixo" if matches else "culturas disponíveis"
    return InvalidParameterError(
        f"Nenhuma planilha para '{cultura}'; {label}: {matches or acquired.culturas_catalogo}"
    )


def _sem_unicidade(
    unresolved: list[dict[str, Any]], identificadas: list[models.ContextoCusto]
) -> InvalidParameterError:
    abas = [str(r["aba"]) for r in unresolved]
    rotulos = [
        f"{c.aba} ({c.local}/{c.uf}, ano {c.ano_referencia}"
        + (f", safra {c.safra})" if c.safra else ")")
        for c in identificadas
    ]
    return InvalidParameterError(
        f"Catálogo contém {len(abas)} aba(s) com contexto não identificado, e a consulta não escolhe sem "
        f"provar unicidade: {lista_curta(abas)}. Abas identificadas que casam com o filtro ({len(rotulos)}): "
        f"{lista_curta(rotulos) or 'nenhuma'}. Selecione a aba exata com aba= (catalogo_custos lista todas)."
    )


async def _load(
    query: models.ConsultaCusto, acquired: Acquisition, *, use_cache: bool = True
) -> tuple[models.ResultadoCusto, dict[str, Any], int, int]:
    start = time.perf_counter()
    resources = await acquired.catalog(query.cultura, use_cache=use_cache)
    if not resources:
        raise _sem_planilha(query.cultura, acquired)
    resource = _resource(resources, query.planilha)
    raw = await acquired.workbook(resource)
    fetch_ms = round((time.perf_counter() - start) * 1000)
    start = time.perf_counter()
    book = Workbook(raw)
    try:
        if query.aba is not None:
            if query.aba not in book.names:
                raise InvalidParameterError(f"Aba não disponível: {query.aba}")
            contexts = [
                context(book.read(query.aba, head=True), resource, book.names.index(query.aba))
            ]
            unresolved: list[dict[str, Any]] = []
        else:
            contexts, unresolved = inventory(book, resource)
        if unresolved and query.aba is None:
            raise _sem_unicidade(unresolved, candidatos(contexts, query))
        selected = select(contexts, query)
        result = parse_selected(book.read(selected.aba), selected)
    finally:
        book.close()
    details = {
        "selection": selected.model_dump(mode="json"),
        "resource": resource.model_dump(),
        "workbook_sha256": hashlib.sha256(raw).hexdigest(),
        "catalog_resources": len(resources),
        "recognized_contexts": len(contexts),
        "unresolved_contexts": unresolved,
        "context_coverage": "selected_sheet_only" if query.aba is not None else "all_data_sheets",
        "parser": result.detalhes,
        "rows_include": "items_subtotals_and_totals_as_published",
        "layout_validation": "selected_sheet_context_headers_and_measure_cells",
    }
    return result, details, fetch_ms, round((time.perf_counter() - start) * 1000)


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> pd.DataFrame: ...


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> pl.DataFrame: ...


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[True],
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = True,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> Frame | tuple[Frame, MetaInfo]: ...


async def custo_producao(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    use_cache: bool = True,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> Frame | tuple[Frame, MetaInfo]:
    query = prepare_query(
        as_polars,
        return_meta,
        use_cache=use_cache,
        cultura=produto,
        uf=uf,
        safra=safra,
        local=local,
        ano=ano,
        planilha=planilha,
        aba=aba,
    )
    if query.cultura is None:
        raise InvalidParameterError("produto é obrigatório")
    acquired = Acquisition()
    try:
        result, details, fetch_ms, parse_ms = await _load(query, acquired, use_cache=use_cache)
        df = frame(result.observacoes)
        contracts.validate_dataset(df, CONAB_CUSTOS_V3)
        meta = _meta(acquired, query, df, details, fetch_ms, parse_ms)
        meta.validation_warnings.extend(_subtotal_warnings(result))
        return finalize_output(df, meta, as_polars, return_meta)
    except (
        httpx.HTTPError,
        OSError,
        InvalidParameterError,
        ParseError,
        ContractViolationError,
        SourceUnavailableError,
    ) as error:
        error.__dict__["conab_custos_acquisition"] = acquired.details()
        raise


@overload
async def custo_producao_total(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    return_meta: Literal[False] = False,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> dict[str, Any]: ...


@overload
async def custo_producao_total(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    return_meta: Literal[True],
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> tuple[dict[str, Any], MetaInfo]: ...


@overload
async def custo_producao_total(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    return_meta: bool = False,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> dict[str, Any] | tuple[dict[str, Any], MetaInfo]: ...


async def custo_producao_total(
    produto: str,
    uf: str | None = None,
    safra: str | None = None,
    *,
    return_meta: bool = False,
    local: str | None = None,
    ano: int | None = None,
    planilha: str | None = None,
    aba: str | None = None,
) -> dict[str, Any] | tuple[dict[str, Any], MetaInfo]:
    query = prepare_query(
        False,
        return_meta,
        cultura=produto,
        uf=uf,
        safra=safra,
        local=local,
        ano=ano,
        planilha=planilha,
        aba=aba,
    )
    if query.cultura is None:
        raise InvalidParameterError("produto é obrigatório")
    acquired = Acquisition()
    try:
        result, details, fetch_ms, parse_ms = await _load(query, acquired)
        df = frame(result.observacoes)
        contracts.validate_dataset(df, CONAB_CUSTOS_V3)
        output = totals(result)
        meta = _meta(acquired, query, df, details, fetch_ms, parse_ms)
        meta.validation_warnings.extend(_subtotal_warnings(result))
        meta.records_count = 1
        meta.columns = list(output)
        meta.source_details["output_dtypes"] = {
            name: type(value).__name__ for name, value in output.items()
        }
        return (output, meta) if return_meta else output
    except (
        httpx.HTTPError,
        OSError,
        InvalidParameterError,
        ParseError,
        ContractViolationError,
        SourceUnavailableError,
    ) as error:
        error.__dict__["conab_custos_acquisition"] = acquired.details()
        raise


@overload
async def catalogo_custos(
    produto: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    planilha: str | None = None,
) -> pd.DataFrame: ...


@overload
async def catalogo_custos(
    produto: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    planilha: str | None = None,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def catalogo_custos(
    produto: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
    planilha: str | None = None,
) -> pl.DataFrame: ...


@overload
async def catalogo_custos(
    produto: str | None = None,
    *,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[True],
    planilha: str | None = None,
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def catalogo_custos(
    produto: str | None = None,
    *,
    use_cache: bool = True,
    planilha: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> Frame | tuple[Frame, MetaInfo]: ...


async def catalogo_custos(
    produto: str | None = None,
    *,
    use_cache: bool = True,
    planilha: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> Frame | tuple[Frame, MetaInfo]:
    query = prepare_query(
        as_polars, return_meta, use_cache=use_cache, cultura=produto, planilha=planilha
    )
    acquired = Acquisition()
    start = time.perf_counter()
    try:
        resources = await acquired.catalog(produto, use_cache=use_cache)
        if produto is not None and not resources:
            raise _sem_planilha(produto, acquired)
        details: dict[str, Any] = {
            "catalog_level": "resources",
            "catalog_resources": len(resources),
        }
        rows = [r.model_dump() for r in resources]
        names = list(models.RecursoCusto.model_fields)
        if planilha is not None:
            resource = _resource(resources, planilha)
            raw = await acquired.workbook(resource)
            book = Workbook(raw)
            try:
                contexts, rejected = inventory(book, resource)
            finally:
                book.close()
            names = [*models.ContextoCusto.model_fields, "status", "error"]
            rows = [
                {**c.model_dump(mode="json"), "status": "identified", "error": None}
                for c in contexts
            ]
            rows.extend(
                {
                    **r,
                    "planilha": resource.planilha,
                    "cultura": resource.cultura,
                    "status": "unresolved",
                }
                for r in rejected
            )
            details.update(
                catalog_level="contexts",
                unresolved_contexts=rejected,
                workbook_sha256=hashlib.sha256(raw).hexdigest(),
            )
        df = pd.DataFrame.from_records(rows, columns=names)
        for name in df:
            if name in {"ano_referencia", "indice_aba"}:
                df[name] = df[name].astype("Int64")
            elif name == "data_referencia":
                df[name] = pd.to_datetime(df[name], format="%Y-%m-%d").astype("datetime64[ns]")
            else:
                df[name] = (
                    df[name]
                    .map(
                        lambda value: (
                            json.dumps(value, ensure_ascii=False, sort_keys=True)
                            if isinstance(value, dict)
                            else value
                        )
                    )
                    .astype(TEXTO)
                )
        meta = _meta(acquired, query, df, details, round((time.perf_counter() - start) * 1000), 0)
        return finalize_output(df, meta, as_polars, return_meta)
    except (
        httpx.HTTPError,
        OSError,
        InvalidParameterError,
        ParseError,
        ContractViolationError,
        SourceUnavailableError,
    ) as error:
        error.__dict__["conab_custos_acquisition"] = acquired.details()
        raise
