from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Callable
from datetime import date
from typing import Any, Literal

import pandas as pd
import pydantic

from agrobr import constants
from agrobr.exceptions import ResourceLimitError

from . import _csv, _memory, models

_KEY_FIELDS = (
    "data",
    "concessionaria",
    "praca",
    "sentido",
    "n_eixos",
    "tipo_veiculo",
    "categoria_eixo",
    "tipo_cobranca",
    "frequencia",
)


def check_memory(estimated: int, maximum: int) -> int:
    if estimated > maximum:
        raise ResourceLimitError(
            source="antt_pedagio",
            reason=f"Limite de memória estimada excedido: {estimated}>{maximum}",
        )
    return estimated


def frame_from_groups(
    groups: dict[tuple[Any, ...], int], retained: int, maximum: int
) -> tuple[pd.DataFrame, int]:
    length = len(groups)
    buffers = 0
    peak = retained
    columns: dict[str, pd.Series] = {}
    for name in constants.ANTT_FLUXO_COLUMNS:
        estimate = retained + buffers + length * 64 + 1024
        peak = max(peak, check_memory(estimate, maximum))
        if name in _KEY_FIELDS:
            index = _KEY_FIELDS.index(name)
            values = [key[index] for key in groups]
        elif name == "volume":
            values = list(groups.values())
        else:
            values = [None] * len(groups)
        dtype = (
            "datetime64[ns]"
            if name == "data"
            else "Int64"
            if name in ("volume", "n_eixos")
            else pd.StringDtype(storage="python")
        )
        columns[name] = pd.Series(values, dtype=dtype)
        buffers += int(columns[name].memory_usage(index=False, deep=False))
        del values
    peak = max(peak, check_memory(retained + buffers * 2 + 16384, maximum))
    frame = pd.DataFrame(columns, copy=False)
    return frame, peak


class TrafficScan:
    def __init__(self, max_rows: int, max_memory_bytes: int) -> None:
        self.groups: dict[tuple[Any, ...], int] = {}
        self.literal_pool: dict[Any, Any] = {}
        self.blocks: dict[tuple[str, date], list[int]] = {}
        self.block_bytes = 0
        self.max_rows, self.max_memory_bytes = max_rows, max_memory_bytes
        self.group_payload_bytes = 0
        self.literal_bytes = 0
        self.transient_bytes = 0
        self.peak_bytes = 0
        self.diagnostics: dict[str, Any] = {
            "csv_records_read": 0,
            "blank_records": 0,
            "validated_rows": 0,
            "selected_rows": 0,
            "source_volume_sum": 0,
            "selected_volume_sum": 0,
            "source_zero_volume_rows": 0,
            "selected_zero_volume_rows": 0,
            "source_missing_eixos": 0,
            "selected_missing_eixos": 0,
            "source_date_min": None,
            "source_date_max": None,
            "selected_date_min": None,
            "selected_date_max": None,
            "normalized_references": 0,
            "normalized_references_examples": [],
            "excluded_volumes": 0,
            "excluded_volumes_examples": [],
        }

    def retained_bytes(self) -> int:
        return (
            16384
            + sys.getsizeof(self.groups)
            + sys.getsizeof(self.literal_pool)
            + sys.getsizeof(self.blocks)
            + self.group_payload_bytes
            + self.literal_bytes
            + self.block_bytes
        )

    def check(self, estimate: int) -> int:
        self.peak_bytes = max(self.peak_bytes, check_memory(estimate, self.max_memory_bytes))
        return estimate

    def account_record(
        self, record: models.TrafegoRecord, row: list[str], values: dict[str, str]
    ) -> None:
        temporary = (
            constants.ANTT_CSV_READ_CHUNK * 5
            + sys.getsizeof(record)
            + sys.getsizeof(record.__dict__)
            + sys.getsizeof(row)
            + sum(sys.getsizeof(value) for value in row)
            + sys.getsizeof(values) * 2
            + 4096
        )
        self.transient_bytes = max(self.transient_bytes, temporary)
        self.check(self.retained_bytes() + temporary)

    def note(self, kind: str, index: int, line: int, value: str, concession: str) -> None:
        self.diagnostics[kind] += 1
        if len(self.diagnostics[f"{kind}_examples"]) < 10:
            self.diagnostics[f"{kind}_examples"].append(
                {"record": index, "line": line, "value": value, "concessionaria": concession}
            )

    def track(self, record: models.TrafegoRecord, row: list[str], line: int) -> None:
        key = (record.concessionaria, record.data.replace(day=1))
        block = self.blocks.get(key)
        if block is None:
            block = [0, 0, line, line, 0]
            added = (
                sys.getsizeof(key)
                + sys.getsizeof(block)
                + sys.getsizeof(record.concessionaria)
                + 4 * sys.getsizeof(constants.ANTT_INT64_MAX)
            )
            self.check(self.retained_bytes() + self.transient_bytes + added)
            self.block_bytes += added
            self.blocks[key] = block
        block[0] ^= hash(tuple(row))
        block[1] += 1
        block[3] = line
        block[4] += record.volume

    def pooled_key(self, raw_key: tuple[Any, ...]) -> tuple[Any, ...]:
        added = {value for value in raw_key if value not in self.literal_pool}
        scalar_bytes = sum(sys.getsizeof(value) for value in added)
        predicted = (
            self.retained_bytes()
            + scalar_bytes
            + sys.getsizeof(self.literal_pool) * 2
            + sys.getsizeof(self.groups) * 2
            + sys.getsizeof(raw_key) * 3
            + sys.getsizeof(added)
            + 4096
            + self.transient_bytes
        )
        self.check(predicted)
        for value in added:
            self.literal_pool[value] = value
        self.literal_bytes += scalar_bytes
        return tuple(self.literal_pool[value] for value in raw_key)

    def observe(self, record: models.TrafegoRecord, *, selected: bool) -> None:
        prefix = "selected" if selected else "source"
        self.diagnostics[f"{prefix}_volume_sum"] += record.volume
        self.diagnostics[f"{prefix}_zero_volume_rows"] += record.volume == 0
        self.diagnostics[f"{prefix}_missing_eixos"] += record.n_eixos is None
        for suffix, operator in (("min", min), ("max", max)):
            key = f"{prefix}_date_{suffix}"
            previous = self.diagnostics[key]
            self.diagnostics[key] = (
                record.data if previous is None else operator(previous, record.data)
            )

    def append(self, record: models.TrafegoRecord) -> None:
        key = tuple(getattr(record, name) for name in _KEY_FIELDS)
        if key not in self.groups:
            if len(self.groups) >= self.max_rows:
                raise ResourceLimitError(
                    source="antt_pedagio",
                    reason=f"Limite de grupos de saída excedido: {self.max_rows}",
                )
            key = self.pooled_key(key)
            self.group_payload_bytes += sys.getsizeof(key) + sys.getsizeof(0)
            self.groups[key] = 0
        previous = self.groups[key]
        total = previous + record.volume
        if total > constants.ANTT_INT64_MAX:
            raise _csv.fail(f"Registro {record.source_record}, volume agregado fora de Int64")
        self.group_payload_bytes += sys.getsizeof(total) - sys.getsizeof(previous)
        self.check(self.retained_bytes() + self.transient_bytes + sys.getsizeof(total))
        self.groups[key] = total
        self.diagnostics["selected_rows"] += 1
        self.observe(record, selected=True)

    def finish(
        self, header: list[str], encoding: str, line: int, frequency: str, year: int
    ) -> models.ParsedTraffic:
        duplicated = {key: block for key, block in self.blocks.items() if block[0] == 0}
        for group in self.groups:
            if (group[1], group[0].replace(day=1)) in duplicated:
                self.groups[group] //= 2
        pool_count = len(self.literal_pool)
        pool_bytes = self.literal_bytes + sys.getsizeof(self.literal_pool)
        grouping_bytes = self.retained_bytes()
        frame, construction_peak = frame_from_groups(
            self.groups, grouping_bytes + self.transient_bytes, self.max_memory_bytes
        )
        self.peak_bytes = max(self.peak_bytes, construction_peak)
        self.groups.clear()
        self.literal_pool.clear()
        usage = _memory.frame_usage(
            [frame], max_bytes=self.max_memory_bytes - self.transient_bytes - 16384
        )
        self.check(usage.accounting_peak_bytes + self.transient_bytes + 16384)
        estimate = self.peak_bytes
        layout = {"columns": header, "delimiter": ";", "parser_version": 3, "frequencia": frequency}
        fingerprint = hashlib.sha256(
            json.dumps(layout, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        self.diagnostics.update(
            encoding=encoding,
            eof_reached=True,
            physical_lines=line,
            output_rows=len(frame),
            retained_bytes_estimate=estimate,
            frame_resident_bytes=usage.resident_bytes,
            memory_details={
                "literal_pool_values": pool_count,
                "literal_pool_bytes": pool_bytes,
                "grouping_resident_bytes": grouping_bytes,
                "scan_transient_reserve_bytes": self.transient_bytes,
                "frame_buffer_bytes": usage.buffer_bytes,
                "construction_peak_bytes": construction_peak,
                "basis": "Literal scalar objects shared within the scan; measured pool/dict/tuple sizes; integer volume storage charged conservatively per group. Reserve covers dict growth, current CSV/Pydantic record, one column list/conversion and frame buffers. Pool and groups released before final frame accounting.",
            },
            max_rows=self.max_rows,
            max_memory_bytes=self.max_memory_bytes,
            ano=year,
            frequencia=frequency,
            layout_fingerprint={**layout, "sha256": fingerprint},
            missing_columns=[
                name for name in _csv._ALIASES if name not in _csv.header_mapping(header)
            ],
            multiplicity="all validated occurrences contribute, except one copy of a concessionaria/month block published twice with identical rows",
            duplicated_blocks=[
                {
                    "concessionaria": concession,
                    "mes": reference.strftime("%Y-%m"),
                    "rows": block[1],
                    "first_line": block[2],
                    "last_line": block[3],
                }
                for (concession, reference), block in duplicated.items()
            ],
            duplicate_rows_removed=sum(block[1] // 2 for block in duplicated.values()),
            duplicate_volume_removed=sum(block[4] // 2 for block in duplicated.values()),
            parser_version=3,
        )
        for name, value in self.diagnostics.items():
            if isinstance(value, date):
                self.diagnostics[name] = value.isoformat()
        return models.ParsedTraffic(frame=frame, diagnostics=self.diagnostics)


def scan(
    reader: Any,
    *,
    ano: int,
    frequencia: Literal["mensal", "diaria"],
    encoding: str,
    keep: Callable[[models.TrafegoRecord], bool] | None,
    max_rows: int,
    max_memory_bytes: int,
) -> models.ParsedTraffic:
    header = _csv.read_header(reader)
    mapping = _csv.header_mapping(header)
    state = TrafficScan(max_rows, max_memory_bytes)
    for index, row in enumerate(reader, 1):
        state.diagnostics["csv_records_read"] = index
        if not row:
            state.diagnostics["blank_records"] += 1
            continue
        if len(row) != len(header):
            raise _csv.fail(
                f"Registro {index}, linha {reader.line_num}: largura {len(row)}, esperada {len(header)}"
            )
        if row == header:
            raise _csv.fail(f"Registro {index}: cabeçalho repetido no corpo")
        values = {name: row[position] for name, position in mapping.items()}
        try:
            record = models.TrafegoRecord.model_validate(
                {**values, "frequencia": frequencia, "source_record": index}, context={"ano": ano}
            )
        except pydantic.ValidationError as exc:
            failures = exc.errors(include_input=False, include_url=False)
            if all(error["type"] == models.UNCOUNTABLE_VOLUME for error in failures):
                state.note(
                    "excluded_volumes",
                    index,
                    reader.line_num,
                    values["volume"],
                    values["concessionaria"],
                )
                continue
            errors = [f"{'.'.join(map(str, error['loc']))}: {error['msg']}" for error in failures]
            raise _csv.fail(
                f"Registro {index}, linha {reader.line_num}: {'; '.join(errors)}"
            ) from exc
        reference = values["data"]
        if frequencia == "mensal" and reference.count("/") == 2 and reference[:3] != "01/":
            state.note(
                "normalized_references", index, reader.line_num, reference, record.concessionaria
            )
        state.diagnostics["validated_rows"] += 1
        state.account_record(record, row, values)
        state.track(record, row, reader.line_num)
        state.observe(record, selected=False)
        selected = True if keep is None else keep(record)
        if type(selected) is not bool:
            raise TypeError("keep deve retornar bool")
        if selected:
            state.append(record)
    return state.finish(header, encoding, reader.line_num, frequencia, ano)
