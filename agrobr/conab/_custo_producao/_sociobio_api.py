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

from agrobr import _log, constants, contracts
from agrobr.contracts import conab_custos
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    SourceUnavailableError,
)
from agrobr.models import MetaInfo
from agrobr.utils import time as time_utils
from agrobr.utils import validation

from . import _acquisition, _sociobio_context, _sociobio_parse, api, models
from ._context import key
from ._sociobio_workbook import WorkbookSociobio, fail

if TYPE_CHECKING:
    import polars as pl

    Frame: TypeAlias = pd.DataFrame | pl.DataFrame

logger = _log.get_logger(__name__)


def prepare_query(
    as_polars: bool, return_meta: bool, use_cache: bool, **selectors: Any
) -> models.ConsultaSociobio:
    if any(type(value) is not bool for value in (as_polars, return_meta, use_cache)):
        raise InvalidParameterError("as_polars, return_meta e use_cache devem ser bool")
    try:
        query = models.ConsultaSociobio(**selectors)
    except ValidationError as error:
        raise InvalidParameterError(str(error)) from error
    if query.produto is not None:
        product = models.normalize_produto_sociobio(query.produto)
        if product not in models.SOCIOBIODIVERSIDADE_PRODUTOS:
            raise InvalidParameterError(
                f"Produto de sociobiodiversidade desconhecido: {query.produto!r}"
            )
        query = query.model_copy(update={"produto": product})
    if query.ano is not None and query.ano > time_utils.utcnow().year:
        raise InvalidParameterError(
            f"ano {query.ano} posterior ao corrente ({time_utils.utcnow().year})"
        )
    if query.uf is not None:
        query = query.model_copy(update={"uf": validation.validate_uf(query.uf)})
    if get_snapshot() is not None:
        raise InvalidParameterError("Custos de sociobiodiversidade não oferecem snapshot imutável")
    if as_polars:
        importlib.import_module("polars")
    return query


def _resource(resources: list[models.RecursoCusto], requested: str | None) -> models.RecursoCusto:
    selected = (
        [resource for resource in resources if resource.planilha == requested]
        if requested is not None
        else [
            resource
            for resource in resources
            if isinstance(resource, models.RecursoSociobio) and resource.ativo
        ]
    )
    if len(selected) != 1:
        raise InvalidParameterError(
            f"Seleção de planilha de sociobiodiversidade ambígua ou ausente: {len(selected)} candidatas. Use catalogo_sociobiodiversidade() e planilha= exata; candidatas: {[resource.planilha for resource in selected]}"
        )
    return selected[0]


def finalize_output(
    frame: pd.DataFrame, meta: MetaInfo, as_polars: bool, return_meta: bool
) -> Frame | tuple[Frame, MetaInfo]:
    output: Any = api.finalize_output(frame, meta, as_polars, False)
    if as_polars:
        pl = importlib.import_module("polars")
        for name in frame.select_dtypes(include="boolean"):
            output = output.with_columns(
                pl.Series(
                    name,
                    [None if pd.isna(value) else bool(value) for value in frame[name]],
                    dtype=pl.Boolean,
                )
            )
        meta.source_details["output_dtypes"] = {str(c): str(t) for c, t in output.schema.items()}
    typed = cast("Frame", output)
    return (typed, meta) if return_meta else typed


def _unresolved_matches(row: dict[str, Any], query: models.ConsultaSociobio) -> bool:
    if query.aba is not None and query.aba != row["aba"]:
        return False
    if query.uf is not None and row.get("uf") is not None and key(query.uf) != row["uf"]:
        return False
    if (
        query.local is not None
        and row.get("local") is not None
        and key(query.local) != key(row["local"])
    ):
        return False
    years = row.get("anos_publicados", [])
    return not (query.ano is not None and years and query.ano not in years)


def _selected_contexts(
    book: WorkbookSociobio, resource: models.RecursoCusto, query: models.ConsultaSociobio
) -> tuple[list[models.ContextoSociobio], list[dict[str, Any]]]:
    if query.aba is not None:
        if query.aba not in book.names:
            raise InvalidParameterError(f"Aba não disponível: {query.aba}")
        selected = [
            _sociobio_context.context(
                book.read(query.aba, head=True), resource, book.names.index(query.aba)
            )
        ]
        return _sociobio_context.select(selected, query), []
    contexts, unresolved = _sociobio_context.inventory(book, resource)
    ambiguous = [row for row in unresolved if _unresolved_matches(row, query)]
    if ambiguous:
        raise fail(
            resource.planilha,
            f"Abas não identificadas podem corresponder aos filtros: {[row['aba'] for row in ambiguous]}; use catalogo_sociobiodiversidade(produto) e aba= exata",
        )
    return _sociobio_context.select(contexts, query), unresolved


def _meta(
    acquired: _acquisition.Acquisition,
    query: models.ConsultaSociobio,
    frame: pd.DataFrame,
    details: dict[str, Any],
    duration_ms: int,
    raw: bytes | None = None,
    *,
    parse_ms: int = 0,
) -> MetaInfo:
    manifest = {
        "query": query.model_dump(mode="json"),
        "acquisition": acquired.details(),
        "selection": details.get("selection"),
    }
    content = (
        raw
        if raw is not None
        else json.dumps(
            manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    )
    receipt = acquired.last_receipt()
    fetched = datetime.fromisoformat(receipt["finished_at"])
    return MetaInfo(
        source="conab_sociobio",
        source_url=receipt["url"],
        source_method="httpx",
        fetched_at=fetched,
        timestamp=time_utils.utcnow_aware(),
        fetch_timestamp=fetched,
        fetch_duration_ms=duration_ms,
        parse_duration_ms=parse_ms,
        raw_content_hash=hashlib.sha256(content).hexdigest(),
        raw_content_size=len(content),
        records_count=len(frame),
        columns=list(frame.columns),
        parser_version=constants.CONAB_SOCIOBIO_PARSER_VERSION,
        schema_version="1.0",
        contract_version="1.0",
        attempted_sources=["conab_sociobio"],
        selected_source="conab_sociobio",
        data_sources=["conab_sociobio"],
        from_cache=acquired.catalog_cache == "hit" and not acquired.receipts,
        source_details={
            **details,
            "catalog_cache": acquired.catalog_cache,
            "manifest": manifest,
            "hash_kind": "sha256_workbook_bytes" if raw is not None else "sha256_catalog_manifest",
            "output_dtypes": {str(name): str(frame[name].dtype) for name in frame.columns},
        },
    )


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> Frame | tuple[Frame, MetaInfo]: ...


async def custo_sociobiodiversidade(
    produto: str,
    uf: str | None = None,
    ano: int | None = None,
    *,
    local: str | None = None,
    planilha: str | None = None,
    aba: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> Frame | tuple[Frame, MetaInfo]:
    query = prepare_query(
        as_polars,
        return_meta,
        use_cache,
        produto=produto,
        uf=uf,
        ano=ano,
        local=local,
        planilha=planilha,
        aba=aba,
    )
    if query.produto is None:
        raise InvalidParameterError("produto é obrigatório")
    acquired = _acquisition.Acquisition(family="sociobiodiversidade")
    started = time.perf_counter()
    logger.info("conab_custo_sociobiodiversidade", produto=query.produto, uf=uf, ano=ano)
    try:
        resources = await acquired.catalog(query.produto, use_cache=use_cache)
        resource = _resource(resources, query.planilha)
        raw = await acquired.workbook(resource)
        fetch_ms = round((time.perf_counter() - started) * 1000)
        parse_started = time.perf_counter()
        book = WorkbookSociobio(raw)
        try:
            selected, unresolved = _selected_contexts(book, resource, query)
            results = [
                _sociobio_parse.parse_selected(book.read(context.aba), context)
                for context in selected
            ]
        finally:
            book.close()
        frame = _sociobio_parse.frame([row for result in results for row in result.observacoes])
        contracts.validate_dataset(frame, conab_custos.CONAB_SOCIOBIO_V1)
        details = {
            "selection": [context.model_dump(mode="json") for context in selected],
            "resource": resource.model_dump(),
            "catalog_resources": len(resources),
            "unresolved_contexts": unresolved,
            "context_coverage": "selected_sheet_only"
            if query.aba is not None
            else "all_data_sheets",
            "parser": [{"aba": result.contexto.aba, **result.detalhes} for result in results],
            "rows_include": "items_sections_and_totals_as_published",
        }
        meta = _meta(
            acquired,
            query,
            frame,
            details,
            fetch_ms,
            raw,
            parse_ms=round((time.perf_counter() - parse_started) * 1000),
        )
        return finalize_output(frame, meta, as_polars, return_meta)
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


def _catalog_frame(rows: list[dict[str, Any]], names: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame.from_records(rows, columns=names)
    for name in frame:
        if name in {"ano", "indice_aba"}:
            frame[name] = frame[name].astype("Int64")
        elif name == "produtividade":
            frame[name] = frame[name].astype("float64")
        elif name == "data_precos":
            frame[name] = pd.to_datetime(frame[name]).astype("datetime64[ns]")
        elif name == "ativo":
            frame[name] = frame[name].astype("boolean")
        else:
            frame[name] = (
                frame[name]
                .map(
                    lambda value: (
                        json.dumps(value, ensure_ascii=False, sort_keys=True)
                        if isinstance(value, (dict, list))
                        else value
                    )
                )
                .astype(conab_custos.TEXTO)
            )
    return frame


@overload
async def catalogo_sociobiodiversidade(
    produto: str | None = None,
    *,
    planilha: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def catalogo_sociobiodiversidade(
    produto: str | None = None,
    *,
    planilha: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def catalogo_sociobiodiversidade(
    produto: str | None = None,
    *,
    planilha: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> Frame | tuple[Frame, MetaInfo]:
    query = prepare_query(as_polars, return_meta, use_cache, produto=produto, planilha=planilha)
    acquired = _acquisition.Acquisition(family="sociobiodiversidade")
    started = time.perf_counter()
    try:
        resources = await acquired.catalog(query.produto, use_cache=use_cache)
        rows = [
            {**resource.model_dump(exclude={"cultura"}), "produto": resource.cultura}
            for resource in resources
        ]
        names = ["produto", "planilha", "titulo", "pagina_url", "ativo"]
        details: dict[str, Any] = {
            "catalog_level": "resources",
            "catalog_resources": len(resources),
        }
        raw = None
        fetch_ms = round((time.perf_counter() - started) * 1000)
        parse_started = time.perf_counter()
        if query.produto is not None or planilha is not None:
            resource = _resource(resources, planilha)
            raw = await acquired.workbook(resource)
            fetch_ms = round((time.perf_counter() - started) * 1000)
            parse_started = time.perf_counter()
            book = WorkbookSociobio(raw)
            try:
                contexts, unresolved = _sociobio_context.inventory(book, resource)
            finally:
                book.close()
            rows = [
                {**context.model_dump(), "status": "identified", "error": None}
                for context in contexts
            ]
            rows.extend(
                {
                    **row,
                    "produto": resource.cultura,
                    "planilha": resource.planilha,
                    "status": "unresolved",
                }
                for row in unresolved
            )
            names = [*models.ContextoSociobio.model_fields, "status", "error", "anos_publicados"]
            details.update(
                catalog_level="contexts",
                resource=resource.model_dump(),
                unresolved_contexts=unresolved,
            )
        frame = _catalog_frame(rows, names)
        meta = _meta(
            acquired,
            query,
            frame,
            details,
            fetch_ms,
            raw,
            parse_ms=round((time.perf_counter() - parse_started) * 1000),
        )
        return finalize_output(frame, meta, as_polars, return_meta)
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
