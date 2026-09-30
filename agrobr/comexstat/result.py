from __future__ import annotations

import copy
import hashlib
import importlib
import json
import sys
import time
import warnings
from datetime import UTC, datetime
from typing import Any, cast

import pandas as pd

from agrobr import contracts
from agrobr.comexstat import models, transport_models
from agrobr.contracts import comexstat as source_contracts
from agrobr.exceptions import ContractViolationError, ResourceLimitError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrame, DataFrameResult


def _memory_check(estimated: int, limit: int, stage: str) -> None:
    if estimated > limit:
        raise ResourceLimitError(
            "comexstat", f"max_memoria_bytes excedido em {stage}: {estimated}>{limit}"
        )


def _polars_estimate(
    frame: pd.DataFrame, contract: source_contracts.ComexstatContract, resident: int, limit: int
) -> dict[str, int]:
    payload = 0
    largest_column = 0
    scratch = 0
    peak = 0
    for column in contract.columns:
        size = len(frame) * 8 + (len(frame) + 7) // 8 + 4096
        if column.type == contracts.ColumnType.STRING:
            size += 8
            for value in frame[column.name]:
                if not pd.isna(value):
                    peak = max(
                        peak,
                        resident
                        + payload * 2
                        + size * 3
                        + sys.getsizeof(value) * 8
                        + len(frame) * 64
                        + 65536,
                    )
                    _memory_check(peak, limit, "estimativa UTF-8 da ponte Polars")
                    encoded_size = len(value.encode("utf-8"))
                    size += encoded_size
                    scratch = max(scratch, encoded_size * 2)
        payload += size
        largest_column = max(largest_column, size)
        peak = max(
            peak,
            resident + payload * 2 + largest_column + len(frame) * 64 + scratch + 65536,
        )
        _memory_check(peak, limit, "ponte Polars")
    return {
        "polars_payload_estimated_bytes": payload,
        "polars_bridge_peak_estimated_bytes": peak,
    }


def _metadata_estimate(details: dict[str, Any], output_bytes: int, limit: int) -> tuple[int, int]:
    pending: list[Any] = [details]
    seen: set[int] = set()
    graph_bytes = 0
    peak = 0
    while pending:
        value = pending.pop()
        identity = id(value)
        if identity in seen:
            continue
        graph_bytes += sys.getsizeof(value)
        estimate = (
            graph_bytes * 16
            + sys.getsizeof(seen) * 4
            + (len(seen) + 1) * 64
            + sys.getsizeof(pending) * 2
            + 16384
        )
        peak = max(peak, estimate)
        _memory_check(output_bytes + estimate, limit, "metadados completos e deepcopy")
        seen.add(identity)
        if isinstance(value, dict):
            pending.extend(value.keys())
            pending.extend(value.values())
        elif isinstance(value, (list, tuple)):
            pending.extend(value)
    return graph_bytes, peak


def _manifest_hash(manifest: dict[str, Any]) -> tuple[str, int]:
    encoder = json.JSONEncoder(
        sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    digest = hashlib.sha256()
    size = 0
    for chunk in encoder.iterencode(manifest):
        encoded = chunk.encode("utf-8")
        digest.update(encoded)
        size += len(encoded)
    return digest.hexdigest(), size


def _polars(frame: pd.DataFrame, contract: source_contracts.ComexstatContract) -> Any:
    module = importlib.import_module("polars")
    dtypes = {
        contracts.ColumnType.INTEGER: module.Int64,
        contracts.ColumnType.FLOAT: module.Float64,
        contracts.ColumnType.STRING: module.Utf8,
    }
    columns = []
    for column in contract.columns:
        values = [None if pd.isna(value) else value for value in frame[column.name]]
        columns.append(module.Series(column.name, values, dtype=dtypes[column.type], strict=True))
        del values
    return module.DataFrame(columns)


def _metadata(
    frame: pd.DataFrame,
    parsed: models.ParsedResource,
    acquired: transport_models.DownloadedResource,
    contract: source_contracts.ComexstatContract,
    dataset: str,
    request: dict[str, Any],
    memory: dict[str, Any],
    *,
    fetch_ms: int,
    parse_ms: int,
    output_dtypes: dict[str, str],
) -> MetaInfo:
    if (
        not acquired.complete
        or acquired.fetched_at is None
        or acquired.fetched_at.utcoffset() is None
        or not acquired.client_closed
        or not acquired.spool_closed
        or not parsed.details["eof_reached"]
        or parsed.details["source_rows"] != parsed.details["validated_rows"]
    ):
        raise ContractViolationError("comexstat", "Aquisição ou validação integral incompleta")
    manifest = {"query": request, "acquisition": acquired.details()}
    details = {
        "query": request,
        "acquisition": manifest["acquisition"],
        "parsing": parsed.details,
        "coverage": {
            "status": "complete_acquired_resource",
            "source_rows": parsed.details["source_rows"],
            "validated_rows": parsed.details["validated_rows"],
            "selected_rows": parsed.details["selected_rows"],
            "returned_rows": len(frame),
            "resource_eof": True,
            "published_control_totals_checked": False,
            "transactional_snapshot": False,
        },
        "semantics": {
            "urf": "Unidade da Receita Federal; não identifica necessariamente um porto físico",
            "country": "Código MDIC; origem na importação e destino na exportação",
            "statistical_quantity": "Disponível no detalhe com a unidade publicada; não somada no mensal",
            "missing_measures": "Ausência não é zero e se propaga na soma da medida",
        },
        "raw_content_hash_kind": "raw_identity_sha256",
        "raw_content_size_basis": "complete_selected_body_bytes",
        "manifest_sha256": "0" * 64,
        "manifest_hash_kind": "canonical_utf8_query_and_acquisition_sha256",
        "output_dtypes": output_dtypes,
        "memory": memory,
    }
    graph, metadata_peak = _metadata_estimate(
        details, memory["output_estimated_bytes"], memory["limit_bytes"]
    )
    memory.update(
        metadata_graph_bytes=graph,
        metadata_peak_estimated_bytes=metadata_peak,
        final_peak_estimated_bytes=memory["output_estimated_bytes"] + metadata_peak,
        metadata_basis="Full details graph including parsing; object/visited-stack reserve, deepcopy and JSON escaping/UTF-8 scratch. Manifest digest streamed without a full JSON buffer.",
    )
    details["manifest_sha256"], memory["manifest_bytes"] = _manifest_hash(manifest)
    avisos = (
        []
        if acquired.size_check
        else ["comexstat: tamanho do arquivo não conferido (sem Content-Length no GET nem no HEAD)"]
    )
    for aviso in avisos:
        warnings.warn(aviso, UserWarning, stacklevel=2)
    return MetaInfo(
        source="comexstat",
        source_url=acquired.resource.url,
        source_method="httpx",
        fetched_at=acquired.fetched_at,
        fetch_timestamp=acquired.fetched_at,
        timestamp=datetime.now(UTC),
        fetch_duration_ms=fetch_ms,
        parse_duration_ms=parse_ms,
        raw_content_hash=acquired.sha256,
        raw_content_size=acquired.size_bytes,
        records_count=len(frame),
        columns=list(frame.columns),
        schema_version=contract.version,
        contract_version=contract.version,
        parser_version=parsed.details["parser_version"],
        dataset=dataset,
        attempted_sources=["comexstat"],
        selected_source="comexstat",
        data_sources=["comexstat"],
        source_details=copy.deepcopy(details),
        validation_warnings=avisos,
    )


def _finish(
    parsed: models.ParsedResource,
    acquired: transport_models.DownloadedResource,
    request: dict[str, Any],
    dataset: str,
    *,
    as_polars: bool,
    return_meta: bool,
    max_memoria_bytes: int,
    fetch_ms: int,
    parse_started: float,
) -> DataFrameResult:
    frame = parsed.frame
    resident = int(parsed.details["frame_resident_bytes"])
    estimate = resident * 4 + len(frame) * 64 + 65536
    _memory_check(estimate, max_memoria_bytes, "contrato e conversão")
    contract = cast(source_contracts.ComexstatContract, contracts.get_contract(dataset))
    contracts.validate_dataset(frame, contract)
    bridge = _polars_estimate(frame, contract, resident, max_memoria_bytes) if as_polars else {}
    estimate = max(estimate, bridge.get("polars_bridge_peak_estimated_bytes", 0))
    _memory_check(estimate, max_memoria_bytes, "ponte Polars")
    converted = _polars(frame, contract) if as_polars else frame
    output_dtypes: dict[str, str] = {
        str(name): str(dtype)
        for name, dtype in (converted.schema.items() if as_polars else frame.dtypes.items())
    }
    memory = {
        "limit_bytes": max_memoria_bytes,
        "frame_resident_bytes": resident,
        "output_estimated_bytes": estimate,
        "basis": "Conservative retained/transient estimate, not process RSS; disk spool excluded",
        "output_basis": "Maximum of four frame resident estimates plus row reserve and the Polars per-occurrence UTF-8/column bridge; full metadata accounted separately",
        "polars_bridge": "Typed Series with one temporary Python list at a time; no PyArrow",
        **bridge,
    }
    meta = _metadata(
        frame,
        parsed,
        acquired,
        contract,
        dataset,
        request,
        memory,
        fetch_ms=fetch_ms,
        parse_ms=int((time.monotonic() - parse_started) * 1000),
        output_dtypes=output_dtypes,
    )
    output: DataFrame = converted
    return (output, meta) if return_meta else output


def finish(
    parsed: models.ParsedResource,
    acquired: transport_models.DownloadedResource,
    request: dict[str, Any],
    dataset: str,
    *,
    as_polars: bool,
    return_meta: bool,
    max_memoria_bytes: int,
    fetch_ms: int,
    parse_started: float,
) -> DataFrameResult:
    try:
        return _finish(
            parsed,
            acquired,
            request,
            dataset,
            as_polars=as_polars,
            return_meta=return_meta,
            max_memoria_bytes=max_memoria_bytes,
            fetch_ms=fetch_ms,
            parse_started=parse_started,
        )
    except Exception as exc:
        try:
            exc.__dict__["comexstat_acquisition"] = acquired.details()
            exc.__dict__["comexstat_parsing"] = copy.deepcopy(parsed.details)
            exc.__dict__["comexstat_query"] = copy.deepcopy(request)
        except Exception as attachment_error:
            exc.add_note(
                f"Falha secundária ao anexar proveniência: {type(attachment_error).__name__}"
            )
        raise
