from __future__ import annotations

import time
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.contracts import lista_suja as source_contracts
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult, build_source_meta, finalize_result
from agrobr.utils.validation import validate_uf
from agrobr.utils.warnings import warn_once

from . import client, models, parser

logger = _log.get_logger(__name__)


def _validate_query(
    uf: str | None, id_registro: str | None, formato: str, kwargs: dict[str, Any]
) -> str | None:
    if kwargs:
        raise InvalidParameterError(f"Argumentos desconhecidos: {', '.join(sorted(kwargs))}")
    if not isinstance(formato, str) or formato not in ("auto", "csv", "pdf"):
        raise InvalidParameterError("formato deve ser 'auto', 'csv' ou 'pdf'")
    if uf is not None and not isinstance(uf, str):
        raise InvalidParameterError("uf deve ser uma sigla textual de estado")
    if id_registro is not None and (not isinstance(id_registro, str) or not id_registro.strip()):
        raise InvalidParameterError("id_registro deve ser texto não vazio")
    return validate_uf(uf)


def _source_details(
    acquisition: models.Acquisition,
    parsed: dict[str, Any],
    query: dict[str, Any],
    df: pd.DataFrame,
) -> dict[str, Any]:
    return {
        **parsed,
        "collection": "cadastro_de_empregadores",
        "format": acquisition.formato,
        "discovery": acquisition.discovery.model_dump(mode="json", exclude={"content"}),
        "resource": acquisition.resource.model_dump(mode="json", exclude={"content"}),
        "companion": (
            acquisition.companion.model_dump(mode="json", exclude={"content"})
            if acquisition.companion is not None
            else None
        ),
        "discovered_publication": acquisition.publication.model_dump(mode="json"),
        "revision_semantics": "current_publication_identified_by_content_hash",
        "fallback": acquisition.fallback,
        "warnings": [*acquisition.warnings, *parsed.get("warnings", [])],
        "query": query,
        "null_counts_scope": "complete_source_before_filters",
        "output_rows": len(df),
    }


@overload
async def empregadores(
    *,
    uf: str | None = None,
    id_registro: str | None = None,
    formato: str = "auto",
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def empregadores(
    *,
    uf: str | None = None,
    id_registro: str | None = None,
    formato: str = "auto",
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def empregadores(
    *,
    uf: str | None = None,
    id_registro: str | None = None,
    formato: str = "auto",
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def empregadores(
    *,
    uf: str | None = None,
    id_registro: str | None = None,
    formato: str = "auto",
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> DataFrameResult:
    uf = _validate_query(uf, id_registro, formato, kwargs)
    warn_once(
        "lista_suja_pii",
        "Lista Suja contém CPF/CNPJ — dados públicos pela Lei de Acesso à Informação.",
    )
    logger.info("lista_suja_empregadores", uf=uf, id_registro=id_registro, formato=formato)

    started = time.monotonic()
    acquisition = await client.fetch_empregadores(formato=formato)
    fetch_ms = int((time.monotonic() - started) * 1000)

    started = time.monotonic()
    df, parsed = parser.parse_empregadores_bundle(
        acquisition.resource.content,
        formato=acquisition.formato,
        companion=acquisition.companion.content if acquisition.companion is not None else None,
    )
    contract = source_contracts.LISTA_SUJA_EMPREGADORES_V2
    contracts.validate_dataset(df, contract)
    if uf is not None:
        df = df.loc[df["uf"] == uf].copy()
    if id_registro is not None:
        df = df.loc[df["id_registro"] == id_registro].copy()
    df = df.reset_index(drop=True)
    parse_ms = int((time.monotonic() - started) * 1000)

    meta = build_source_meta(
        "lista_suja",
        acquisition.resource.url,
        f"httpx+{acquisition.formato}",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        schema_version=contract.version,
        attempted_sources=acquisition.attempted_sources,
        selected_source=acquisition.selected_source,
        raw_content_hash=acquisition.resource.sha256,
        source_details=_source_details(
            acquisition, parsed, {"uf": uf, "id_registro": id_registro, "formato": formato}, df
        ),
    )
    meta.fetched_at = acquisition.resource.fetched_at
    meta.fetch_timestamp = acquisition.resource.fetched_at
    meta.raw_content_size = acquisition.resource.size_bytes
    meta.contract_version = contract.version
    meta.validation_warnings = list(meta.source_details["warnings"])
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
