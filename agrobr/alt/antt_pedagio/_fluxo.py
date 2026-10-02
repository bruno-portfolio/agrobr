from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import json
import sys
import time
import warnings
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
import pandas as pd

from agrobr import constants, contracts
from agrobr.exceptions import (
    ContractViolationError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult
from agrobr.utils.warnings import warn_once

from . import _memory, acquisition, client, models, parser, query

Enrichment = dict[tuple[str, str], tuple[str | None, str | None, str | None]]


def _unavailable(message: str) -> SourceUnavailableError:
    return SourceUnavailableError(source="antt_pedagio", last_error=message)


def _mapping_bytes(mapping: Enrichment) -> int:
    return sys.getsizeof(mapping) + sum(
        sys.getsizeof(key)
        + sys.getsizeof(values)
        + sum(sys.getsizeof(value) for value in (*key, *values))
        for key, values in mapping.items()
    )


def _tree_bytes(value: Any) -> int:
    if isinstance(value, dict):
        return sys.getsizeof(value) + sum(
            _tree_bytes(key) + _tree_bytes(item) for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return sys.getsizeof(value) + sum(_tree_bytes(item) for item in value)
    return sys.getsizeof(value)


@dataclass
class Pipeline:
    validated: query.FluxoQuery
    acquisitions: list[dict[str, Any]] = field(default_factory=list)
    pending_acquisition: dict[str, Any] | None = None
    parsing: list[dict[str, Any]] = field(default_factory=list)
    enrichment: dict[str, Any] = field(default_factory=lambda: {"status": "disabled"})
    mapping: Enrichment = field(default_factory=dict)
    mapping_bytes: int = 0
    acquisition_bytes: int = 0
    memory_checks: list[dict[str, Any]] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)
    heavy_unknown_rows: int = 0
    heavy_unknown_volume: int = 0
    geographic_unknown_rows: int = 0
    geographic_unknown_volume: int = 0
    geographic_unknown_pairs: set[tuple[str, str]] = field(default_factory=set)
    fetch_seconds: float = 0
    started: float = field(default_factory=time.monotonic)

    def check(self, estimated: int, stage: str) -> int:
        estimated += self.acquisition_bytes
        self.memory_checks.append({"stage": stage, "estimated_bytes": estimated})
        if estimated > self.validated.max_memoria_bytes:
            raise ResourceLimitError(
                "antt_pedagio",
                f"Limite de retenção estimada em {stage}: "
                f"{estimated}>{self.validated.max_memoria_bytes} bytes",
            )
        return estimated

    def remember(self, role: str, details: dict[str, Any]) -> None:
        self.pending_acquisition = {"role": role, "acquisition": details}
        estimated = _tree_bytes(details)
        self.check(self.mapping_bytes + estimated * 2, "acquisition_details_preallocation")
        self.acquisitions.append({"role": role, "acquisition": copy.deepcopy(details)})
        self.acquisition_bytes += estimated
        self.pending_acquisition = None

    def measure(self, frames: list[pd.DataFrame], stage: str, extra: int = 0) -> _memory.FrameUsage:
        occupied = self.mapping_bytes + extra
        self.check(occupied + 2048, stage)
        usage = _memory.frame_usage(
            frames,
            max_bytes=self.validated.max_memoria_bytes - occupied - self.acquisition_bytes,
        )
        self.check(occupied + usage.accounting_peak_bytes, stage + "_accounting")
        return usage

    def completed_sources(self) -> list[dict[str, Any]]:
        observed = list(self.acquisitions)
        if self.pending_acquisition is not None:
            observed.append(self.pending_acquisition)
        return copy.deepcopy([item for item in observed if item["acquisition"].get("complete")])


def _basic_match(record: models.TrafegoRecord, selected: query.FluxoQuery) -> bool:
    if selected.inicio is not None and record.data < selected.inicio:
        return False
    if selected.fim is not None and record.data > selected.fim:
        return False
    for name in ("concessionaria", "praca"):
        value = getattr(selected, name)
        if value is not None and value.casefold() not in getattr(record, name).casefold():
            return False
    if selected.tipo_veiculo is not None and record.tipo_veiculo != selected.tipo_veiculo:
        return False
    return selected.tipo_cobranca is None or (
        record.tipo_cobranca is not None
        and query.chave_texto(record.tipo_cobranca) == query.chave_texto(selected.tipo_cobranca)
    )


def _mesmo_valor(nome: str, publicado: str | None, pedido: str) -> bool:
    normalizar = query._rodovia if nome == "rodovia" else str.upper
    return publicado is not None and normalizar(publicado) == normalizar(pedido)


def _avisar_filtro_sem_praca(state: Pipeline, cadastro: pd.DataFrame) -> None:
    filtros = {
        nome: getattr(state.validated, nome)
        for nome in ("uf", "rodovia")
        if getattr(state.validated, nome) is not None
    }
    if not filtros:
        return
    for linha in cadastro.reindex(columns=["rodovia", "uf"]).to_dict("records"):
        publicados = {nome: parser._nullable_text(valor) for nome, valor in linha.items()}
        if all(_mesmo_valor(nome, publicados[nome], valor) for nome, valor in filtros.items()):
            return
    descricao = ", ".join(f"{nome}={valor!r}" for nome, valor in filtros.items())
    message = (
        f"Nenhuma praça do cadastro ANTT casa {descricao}; o resultado sai vazio, sem falha "
        "de transporte"
    )
    state.messages.append(message)
    warnings.warn(message, UserWarning, stacklevel=4)


def _geographic_match(record: models.TrafegoRecord, state: Pipeline) -> bool:
    selected = state.validated
    if selected.uf is None and selected.rodovia is None:
        return True
    values = state.mapping.get((record.concessionaria, record.praca), (None, None, None))
    unknown = False
    for index, name in ((0, "rodovia"), (1, "uf")):
        expected = getattr(selected, name)
        actual = values[index]
        if expected is None:
            continue
        if actual is None or not actual.strip():
            unknown = True
        else:
            normalizar = query._rodovia if name == "rodovia" else str.upper
            if normalizar(actual) != normalizar(expected):
                return False
    if unknown:
        state.geographic_unknown_rows += 1
        state.geographic_unknown_volume += record.volume
        state.geographic_unknown_pairs.add((record.concessionaria, record.praca))
    return not unknown


def _keep(record: models.TrafegoRecord, state: Pipeline) -> bool:
    if not _basic_match(record, state.validated) or not _geographic_match(record, state):
        return False
    if not state.validated.apenas_pesados:
        return True
    status = parser.heavy_vehicle_status(record)
    if status is None:
        state.heavy_unknown_rows += 1
        state.heavy_unknown_volume += record.volume
        return False
    return status


async def _load_enrichment(state: Pipeline) -> None:
    if not state.validated.enriquecer:
        return
    started = time.monotonic()
    try:
        async with client.open_pracas() as bundle:
            state.fetch_seconds += time.monotonic() - started
            state.remember("pracas", bundle.details())
            item = bundle.files[0]
            state.check(item.size_bytes * 64 + 16384, "cadastro_parse_mapping_preallocation")
            frame = parser.parse_pracas_file(item.file)
            size = state.measure([frame], "cadastro_frame").resident_bytes
            state.check(size * 8 + 16384, "cadastro_mapping_preallocation")
            mapping, diagnostics = parser.build_pracas_enrichment(frame)
            state.mapping_bytes = _mapping_bytes(mapping)
            state.check(size + state.mapping_bytes, "cadastro_mapping_retained")
            state.mapping = mapping
            state.enrichment = {
                "status": "available",
                "parsing": copy.deepcopy(frame.attrs.get("parsing", {})),
                **copy.deepcopy(diagnostics),
            }
            _avisar_filtro_sem_praca(state, frame)
    except (httpx.HTTPError, SourceUnavailableError) as exc:
        state.fetch_seconds += time.monotonic() - started
        details = getattr(exc, "antt_acquisition", None)
        if details is not None:
            state.remember("pracas_failed", details)
        if state.validated.uf is not None or state.validated.rodovia is not None:
            raise
        message = f"Cadastro ANTT indisponível; enriquecimento ausente: {type(exc).__name__}: {exc}"
        state.messages.append(message)
        state.enrichment = {"status": "unavailable", "reason": str(exc)}
        warnings.warn(message, UserWarning, stacklevel=3)


def _examples(items: list[dict[str, Any]]) -> str:
    return "; ".join(
        f"linha {item['line']} '{item['value']}' {item['concessionaria']}" for item in items[:3]
    )


def _source_warnings(
    state: Pipeline, item: acquisition.DownloadedCSV, diagnostics: dict[str, Any]
) -> None:
    label = f"ANTT {item.frequencia} {item.ano}"
    notes = []
    if count := diagnostics["normalized_references"]:
        notes.append(
            (
                "referencias",
                f"{label}: {count} registros com referência fora do dia 1 normalizados para o mês "
                f"({_examples(diagnostics['normalized_references_examples'])})",
            )
        )
    if count := diagnostics["excluded_volumes"]:
        notes.append(
            (
                "volumes",
                f"{label}: {count} registros com volume que não é contagem (fracionário ou "
                f"negativo) excluídos ({_examples(diagnostics['excluded_volumes_examples'])})",
            )
        )
    if blocks := diagnostics["duplicated_blocks"]:
        names = "; ".join(
            f"{block['concessionaria']} {block['mes']}, linhas {block['first_line']}–{block['last_line']}"
            for block in blocks
        )
        notes.append(
            (
                "blocos",
                f"{label}: {len(blocks)} bloco(s) concessionária × mês publicado em duplicata com "
                f"linhas idênticas; mantida uma cópia ({names})",
            )
        )
    for kind, message in notes:
        state.messages.append(message)
        warn_once(f"antt_pedagio_{kind}_{item.sha256}", message, stacklevel=4)


async def _traffic_frames(state: Pipeline) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    retained, rows = 0, 0
    started = time.monotonic()
    async with client.open_trafego_anos(
        list(state.validated.anos), frequencia=state.validated.frequencia
    ) as bundle:
        state.fetch_seconds += time.monotonic() - started
        state.remember("trafego", bundle.details())
        for item in bundle.files:
            if item.frequencia != state.validated.frequencia or item.ano is None:
                raise _unavailable("Recurso adquirido com ano/frequência divergente")
            occupied = retained + state.mapping_bytes
            state.check(occupied + 2048, "parser_preallocation")
            parsed = parser.parse_trafego_file(
                item.file,
                ano=item.ano,
                frequencia=state.validated.frequencia,
                keep=lambda record: _keep(record, state),
                max_rows=state.validated.max_linhas - rows,
                max_memory_bytes=state.validated.max_memoria_bytes
                - occupied
                - state.acquisition_bytes,
            )
            state.parsing.append(copy.deepcopy(parsed.diagnostics))
            _source_warnings(state, item, parsed.diagnostics)
            state.check(
                occupied + parsed.diagnostics["retained_bytes_estimate"], "parser_with_previous"
            )
            rows += len(parsed.frame)
            frames.append(parsed.frame)
            retained = state.measure(frames, "traffic_frames_retained").resident_bytes
    if state.heavy_unknown_rows:
        message = (
            f"Classificação de pesado indeterminada em {state.heavy_unknown_rows} registros "
            f"(volume={state.heavy_unknown_volume}); excluídos pelo filtro apenas_pesados. "
            "O resultado inclui apenas veículos comprovadamente pesados."
        )
        state.messages.append(message)
        warnings.warn(message, UserWarning, stacklevel=3)
    if state.geographic_unknown_rows:
        pairs = sorted(state.geographic_unknown_pairs)
        message = (
            f"Filtro geográfico: {state.geographic_unknown_rows} registros "
            f"(volume={state.geographic_unknown_volume}) de {len(pairs)} pares "
            f"concessionária/praça sem vínculo único no cadastro excluídos "
            f"({'; '.join(f'{concession}/{plaza}' for concession, plaza in pairs[:3])}). "
            "O resultado inclui apenas praças comprovadamente na UF/rodovia pelo cadastro."
        )
        state.messages.append(message)
        warnings.warn(message, UserWarning, stacklevel=3)
    filtros = {
        name: getattr(state.validated, name)
        for name in ("concessionaria", "praca", "tipo_cobranca")
        if getattr(state.validated, name) is not None
    }
    if filtros and not rows:
        message = (
            f"Nenhum registro da ANTT casou {filtros} nos anos {list(state.validated.anos)}; "
            "concessionaria e praca comparam por trecho do nome, sem caixa, e tipo_cobranca "
            "pelo nome inteiro, sem caixa e acento"
        )
        state.messages.append(message)
        warnings.warn(message, UserWarning, stacklevel=3)
    return frames


def _assemble(frames: list[pd.DataFrame], state: Pipeline) -> pd.DataFrame:
    keys = contracts.get_contract("antt_pedagio_fluxo").primary_key
    usage = state.measure(frames, "concat_sort_preallocation")
    rows = sum(len(frame) for frame in frames)
    scratch = rows * (8 * (len(keys) + 6) + 80) + 16384
    state.check(
        state.mapping_bytes + usage.resident_bytes + usage.buffer_bytes * 2 + scratch,
        "concat_sort_preallocation",
    )
    if len(frames) == 1:
        frame = frames.pop()
    elif frames:
        frame = pd.concat(frames, ignore_index=True)
        frames.clear()
    else:
        frame = contracts.get_contract("antt_pedagio_fluxo").empty_frame()
    frame = frame.sort_values(keys, kind="stable", na_position="last", ignore_index=True)
    state.measure([frame], "concat_sort_retained")
    return frame


def _enrich(frame: pd.DataFrame, state: Pipeline) -> None:
    rows = len(frame)
    size = state.measure([frame], "enrichment_frame").resident_bytes
    if not state.mapping:
        state.enrichment.update(output_rows=rows, linked_rows=0, unlinked_rows=rows)
        return
    predicted = state.mapping_bytes + size + rows * 32 + 16384
    state.check(predicted, "enrichment_preallocation")
    matched = sum(
        (concession, plaza) in state.mapping
        for concession, plaza in zip(frame["concessionaria"], frame["praca"], strict=True)
    )
    for index, name in enumerate(("rodovia", "uf", "municipio")):
        frame[name] = pd.Series(
            [
                state.mapping.get((concession, plaza), (None, None, None))[index]
                for concession, plaza in zip(frame["concessionaria"], frame["praca"], strict=True)
            ],
            index=frame.index,
            dtype=pd.StringDtype(storage="python"),
        )
    state.enrichment.update(output_rows=rows, linked_rows=matched, unlinked_rows=rows - matched)
    state.measure([frame], "enriched_pandas_retained")


def _validate(frame: pd.DataFrame, state: Pipeline) -> None:
    size = state.measure([frame], "contract_frame").resident_bytes
    text_scratch = max(
        sum(sys.getsizeof(value) for value in frame[name] if isinstance(value, str))
        for name in ("categoria_eixo", "concessionaria", "praca")
    )
    state.check(
        state.mapping_bytes + size + text_scratch * 2 + len(frame) * 192 + 16384,
        "contract_validation_preallocation",
    )
    contract = contracts.get_contract("antt_pedagio_fluxo")
    if contract.version != "3.0":
        raise ContractViolationError("antt_pedagio.fluxo", "Contrato ANTT 3.0 não disponível")
    valid, errors = contract.validate(frame)
    if not valid:
        raise ContractViolationError("antt_pedagio.fluxo", "; ".join(errors))


def _column_bridge_bytes(column: pd.Series[Any], name: str) -> tuple[int, int]:
    rows = len(column)
    pointers = rows * 10 + 64
    if name in ("data", "volume", "n_eixos"):
        return pointers + rows * 36 + 4096, rows * 18 + 4096
    payload = sum(len(value) * 4 for value in column if isinstance(value, str))
    return pointers + 4096, payload + (rows + 1) * 8 + (rows + 7) // 8 + 4096


def _polars_column(module: Any, column: pd.Series[Any], name: str, dtype: Any) -> Any:
    values = [
        None
        if pd.isna(value)
        else int(value.value)
        if name == "data"
        else int(value)
        if name in ("volume", "n_eixos")
        else value
        for value in column
    ]
    result = module.Series(
        name, values, dtype=module.Int64 if name == "data" else dtype, strict=True
    )
    del values
    return result.cast(module.Datetime("ns"), strict=True) if name == "data" else result


def _polars(frame: pd.DataFrame, state: Pipeline) -> Any:
    module = importlib.import_module("polars")
    retained = state.measure([frame], "polars_bridge_preallocation").resident_bytes
    schema = {
        name: module.Datetime("ns")
        if name == "data"
        else module.Int64
        if name in ("volume", "n_eixos")
        else module.Utf8
        for name in constants.ANTT_FLUXO_COLUMNS
    }
    series = {}
    native = 0
    for name, dtype in schema.items():
        bridge, output = _column_bridge_bytes(frame[name], name)
        state.check(
            state.mapping_bytes + retained + native + bridge + output * 2 + 16384,
            f"polars_column_preallocation:{name}",
        )
        series[name] = _polars_column(module, frame[name], name, dtype)
        native += series[name].estimated_size() + 4096
        state.check(state.mapping_bytes + retained + native, f"polars_column_retained:{name}")
    state.check(
        state.mapping_bytes + retained + native * 2 + 16384, "polars_dataframe_preallocation"
    )
    result = module.DataFrame(series)
    if result.schema != schema:
        raise ContractViolationError("antt_pedagio.fluxo", "Dtypes Polars divergentes")
    state.check(
        state.mapping_bytes + retained + native + result.estimated_size(), "polars_bridge_retained"
    )
    return result


def _manifest(state: Pipeline) -> tuple[bytes, int, datetime, list[str]]:
    value = {"query": state.validated.details(), "sources": state.acquisitions}
    content = json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    received: list[datetime] = []
    urls: list[str] = []
    size = 0
    for entry in state.acquisitions:
        acquired = entry["acquisition"]
        size += acquired["transfer_bytes"]
        for attempt in acquired["attempts"]:
            if attempt["url"] not in urls:
                urls.append(attempt["url"])
            if attempt["finished_at"] and (attempt["size_bytes"] or attempt["complete_body"]):
                timestamp = datetime.fromisoformat(attempt["finished_at"])
                if timestamp.utcoffset() is None:
                    raise ContractViolationError("antt_pedagio.fluxo", "Recibo de coleta sem fuso")
                received.append(timestamp.astimezone(UTC))
    if not received:
        raise ContractViolationError("antt_pedagio.fluxo", "Aquisição sem relógio de recebimento")
    return content, size, max(received), urls


def _meta(frame: pd.DataFrame, result: Any, state: Pipeline) -> MetaInfo:
    output_bytes = result.estimated_size() if hasattr(result, "estimated_size") else 0
    retained = state.measure([frame], "metadata_frame", extra=output_bytes).resident_bytes
    state.check(
        state.mapping_bytes + retained + output_bytes + state.acquisition_bytes * 4,
        "manifest_preallocation",
    )
    manifest, size, fetched, urls = _manifest(state)
    state.check(
        state.mapping_bytes + retained + output_bytes + len(manifest) * 8,
        "metadata_preallocation",
    )
    elapsed = time.monotonic() - state.started
    details = {
        "query": state.validated.details(),
        "source_urls": urls,
        "acquisitions": state.acquisitions,
        "parsing": state.parsing,
        "enrichment": state.enrichment,
        "heavy_filter": {
            "enabled": state.validated.apenas_pesados,
            "unknown_rows_excluded": state.heavy_unknown_rows,
            "unknown_volume_excluded": state.heavy_unknown_volume,
            "basis": "commercial_with_at_least_three_explicit_axles",
        },
        "geographic_filter": {
            "enabled": state.validated.uf is not None or state.validated.rodovia is not None,
            "unlinked_rows_excluded": state.geographic_unknown_rows,
            "unlinked_volume_excluded": state.geographic_unknown_volume,
            "unlinked_pairs": [list(pair) for pair in sorted(state.geographic_unknown_pairs)[:20]],
            "unlinked_pairs_count": len(state.geographic_unknown_pairs),
            "basis": "unique literal concessionaria/praca link with non-empty requested field in the current plaza registry",
        },
        "coverage": {
            "status": "complete_selected_resources",
            "requested_years": list(state.validated.anos),
            "source_validated_rows": sum(item["validated_rows"] for item in state.parsing),
            "selected_occurrences": sum(item["selected_rows"] for item in state.parsing),
            "returned_groups": len(frame),
            "all_resources_eof": all(item["eof_reached"] for item in state.parsing),
            "transactional_snapshot": False,
            "population_total_known": False,
            "aggregation": "sum exact Int64 per period and literal dimensions; occurrences not deduplicated",
        },
        "memory": {
            "basis": "conservative retained/transient estimates; not measured process RSS; excludes disk spool",
            "limit_bytes": state.validated.max_memoria_bytes,
            "peak_estimated_bytes": max(item["estimated_bytes"] for item in state.memory_checks),
            "checks": state.memory_checks,
            "cadastro_preallocation_basis": "64 times received CSV bytes plus overhead; then frame/mapping estimates",
            "frame_retention_basis": "arrays/masks/index plus text objects counted once by identity, including estimator set/IDs and container margin",
            "concat_sort_basis": "shared text payload once, up to three array sets, factorization/index buffers and 80 bytes per row extra workspace",
            "contract_basis": "resident frame plus duplicated/hash/mask buffers, two maximum temporary text columns and 192 bytes per row workspace",
            "polars_bridge_basis": "one Python list at a time; existing Series estimated_size plus current UTF8 upper bound/offsets/bitmap or exact Int64 buffers; explicit temporary margins; no Arrow",
        },
        "hash_kind": "sha256_canonical_utf8_query_and_acquisition_manifest",
        "manifest": json.loads(manifest),
        "raw_content_size_basis": "canonical UTF-8 manifest bytes, the same object as raw_content_hash",
        "received_bytes": size,
        "received_bytes_basis": "all decoded bytes received across attempts, catalogs and CSVs, including retries/errors",
        "data_file_bytes": sum(
            item["size_bytes"]
            for entry in state.acquisitions
            for item in entry["acquisition"]["files"]
        ),
        "timing_basis": "fetch excludes parsing; parse includes filtering, assembly, validation and conversion",
        "output_dtypes": {
            name: str(dtype)
            for name, dtype in (
                result.schema.items() if hasattr(result, "schema") else result.dtypes.items()
            )
        },
    }
    return MetaInfo(
        source="antt_pedagio",
        source_url=next(
            entry["acquisition"]["files"][0]["resource"]["url"]
            for entry in state.acquisitions
            if entry["role"] == "trafego"
        ),
        source_method="httpx",
        fetched_at=fetched,
        timestamp=datetime.now(UTC),
        fetch_timestamp=fetched,
        fetch_duration_ms=int(state.fetch_seconds * 1000),
        parse_duration_ms=int(max(0, elapsed - state.fetch_seconds) * 1000),
        raw_content_hash=hashlib.sha256(manifest).hexdigest(),
        raw_content_size=len(manifest),
        records_count=len(frame),
        columns=list(constants.ANTT_FLUXO_COLUMNS),
        schema_version="3.0",
        dataset="antt_pedagio_fluxo",
        contract_version="3.0",
        parser_version=parser.PARSER_VERSION,
        attempted_sources=["antt_pedagio"],
        selected_source="antt_pedagio",
        data_sources=["antt_pedagio"],
        validation_passed=True,
        validation_warnings=list(state.messages),
        source_details=details,
    )


async def fetch(
    validated: query.FluxoQuery, *, as_polars: bool, return_meta: bool
) -> DataFrameResult:
    state = Pipeline(validated)
    try:
        await _load_enrichment(state)
        frames = await _traffic_frames(state)
        frame = _assemble(frames, state)
        del frames
        _enrich(frame, state)
        _validate(frame, state)
        result = _polars(frame, state) if as_polars else frame
        meta = _meta(frame, result, state)
        return (result, meta) if return_meta else result
    except (
        ParseError,
        ContractViolationError,
        ResourceLimitError,
        SourceUnavailableError,
        httpx.HTTPError,
        OSError,
        ValueError,
        TypeError,
        ImportError,
        MemoryError,
        asyncio.CancelledError,
    ) as exc:
        exc.__dict__["antt_completed_sources"] = state.completed_sources()
        exc.__dict__["antt_pipeline"] = {
            "query": validated.details(),
            "parsing": copy.deepcopy(state.parsing),
            "enrichment": copy.deepcopy(state.enrichment),
            "memory": copy.deepcopy(state.memory_checks),
        }
        raise
