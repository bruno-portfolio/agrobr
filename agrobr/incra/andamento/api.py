from __future__ import annotations

import hashlib
import importlib
import json
import re
import time
from datetime import UTC, date, datetime
from typing import Any, Literal, cast, overload

import pandas as pd

from agrobr import _log, constants, contracts
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result
from agrobr.utils.warnings import warn_once

from . import client, models, parser

logger = _log.get_logger(__name__)


def validate_edition(value: date | str | None) -> date | None:
    if value is None or type(value) is date:
        return value
    if type(value) is str and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    raise InvalidParameterError("edicao deve ser date, YYYY-MM-DD ou None")


def _meta(
    acquired: models.Acquisition,
    parsed: models.ParsedPublication,
    requested: date | str | None,
    parse_ms: int,
) -> MetaInfo:
    details: dict[str, Any] = {
        "query": {
            "requested_edition": requested.isoformat()
            if isinstance(requested, date)
            else requested,
            "resolved_edition": parsed.edition.isoformat(),
        },
        "resources": [resource.model_dump(mode="json") for resource in acquired.resources],
        "publication": {
            "page_url": acquired.page_url,
            **acquired.linked_edition.model_dump(mode="json"),
            "pdf_sha256": parsed.source_sha256,
            "internal_edition": parsed.edition.isoformat(),
        },
        "layout": parsed.model_dump(mode="json"),
    }
    manifest_fields = ["query", "resources", "publication", "layout"]
    manifest = json.dumps(
        details, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    details.update(
        manifest_fields=manifest_fields,
        manifest_encoding="UTF-8 JSON sorted keys compact separators no ASCII escaping",
        raw_content_hash_kind="resource_and_publication_manifest_sha256",
        raw_content_size_kind="serialized_manifest_bytes",
        resource_bytes_basis="decoded_response_bytes_including_errors_partials_and_retries",
        resource_bytes=sum(resource.size_bytes for resource in acquired.resources),
        revision_snapshot=False,
        edition_semantics="publisher_date_and_PDF_bytes_not_immutable_URL",
        position_semantics="numero_publicado_scoped_to_PDF_sha256_not_semantic_primary_key",
        semantic_primary_key=[],
        coverage={
            "status": "reconciled_publication",
            "declared_rows": parsed.declared_total,
            "validated_rows": len(parsed.frame),
            "returned_rows": len(parsed.frame),
            "all_logical_requests_succeeded": True,
            "local_filters": [],
        },
        budgets={
            "body_bytes": constants.INCRA_ANDAMENTO_MAX_BODY_BYTES,
            "total_bytes": constants.INCRA_ANDAMENTO_MAX_TOTAL_BODY_BYTES,
            "physical_requests": constants.INCRA_ANDAMENTO_MAX_ATTEMPTS,
            "pdf_pages": constants.INCRA_ANDAMENTO_MAX_PAGES,
            "basis": "SDK_local_limits_not_source_limits",
        },
        output_dtypes={name: str(dtype) for name, dtype in parsed.frame.dtypes.items()},
        output_null_counts={name: int(count) for name, count in parsed.frame.isna().sum().items()},
        reproduction_terms="PDF permits reproduction with source attribution; no geographic-layer license inferred",
    )
    meta = result.build_source_meta(
        "incra",
        acquired.linked_edition.url,
        "httpx+pdf+pdfminer",
        acquired.duration_ms,
        parse_ms,
        parsed.frame,
        constants.INCRA_ANDAMENTO_PARSER_VERSION,
        attempted_sources=["incra_andamento_pdf"],
        selected_source="incra_andamento_pdf",
        raw_content_hash=hashlib.sha256(manifest).hexdigest(),
        source_details=details,
    )
    meta.raw_content_size = len(manifest)
    meta.contract_version = "1.0"
    meta.fetched_at = max(
        resource.finished_at or resource.requested_at for resource in acquired.resources
    )
    meta.fetch_timestamp = meta.fetched_at
    meta.timestamp = datetime.now(UTC)
    meta.validation_warnings = [
        "Texto subjacente parcialmente cortado preservado; regional atribuída por grupos gráficos, sem inferência de UF"
    ]
    return meta


@overload
async def andamento_quilombola(
    *,
    edicao: date | str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def andamento_quilombola(
    *,
    edicao: date | str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def andamento_quilombola(
    *,
    edicao: date | str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def andamento_quilombola(
    *,
    edicao: date | str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    from agrobr.datasets.deterministic import get_snapshot

    if kwargs:
        raise TypeError(f"Argumentos desconhecidos em andamento_quilombola: {sorted(kwargs)}")
    if type(as_polars) is not bool or type(return_meta) is not bool:
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    edition = validate_edition(edicao)
    if get_snapshot() is not None:
        raise InvalidParameterError(
            "andamento_quilombola não suporta deterministic: URL de edição mutável"
        )
    parser.check_pdf()
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError(
                "polars é necessário. Instale com: pip install agrobr[polars]"
            ) from None
    acquired = await client.fetch_publication()
    started = time.monotonic()
    parsed = parser.parse_publication(acquired.content)
    parse_ms = int((time.monotonic() - started) * 1000)
    file_date = acquired.linked_edition.file_date
    if edition is not None and edition != parsed.edition:
        raise InvalidParameterError(
            f"Edição {edition.isoformat()} não publicada: a publicação atual do INCRA é de "
            f"{parsed.edition.isoformat()} (arquivo nomeado {file_date:%d/%m/%Y})"
        )
    contract = contracts.get_contract("incra_andamento_quilombola")
    valid, errors = contract.validate(parsed.frame)
    if not valid:
        raise ContractViolationError(contract.name, "; ".join(errors))
    meta = _meta(acquired, parsed, edicao, parse_ms)
    if file_date != parsed.edition:
        message = (
            f"INCRA: arquivo nomeado {file_date:%d/%m/%Y}, conteúdo de "
            f"{parsed.edition:%d/%m/%Y}; usada a data interna"
        )
        warn_once(f"incra_andamento_{file_date}_{parsed.edition}", message)
        meta.validation_warnings.append(message)
    finalized = result.finalize_result(
        parsed.frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(name for name in models.columns() if name != "numero_publicado"),
    )
    if as_polars:
        output = cast(Any, finalized[0] if return_meta else finalized)
        meta.source_details["output_dtypes"] = {
            name: str(dtype) for name, dtype in output.schema.items()
        }
    logger.info(
        "incra_andamento_quilombola", edition=parsed.edition.isoformat(), rows=len(parsed.frame)
    )
    return finalized
