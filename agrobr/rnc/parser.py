from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

import pandas as pd
import pydantic
from bs4 import BeautifulSoup

from agrobr import _log, constants
from agrobr.exceptions import ParseError
from agrobr.normalize import encoding

from . import models

logger = _log.get_logger(__name__)

PARSER_VERSION = 2


@dataclass(frozen=True)
class RncParsedTable:
    frame: pd.DataFrame
    details: dict[str, Any]


def _fail(reason: str) -> ParseError:
    return ParseError(source="rnc", parser_version=PARSER_VERSION, reason=reason)


def _header(reader: Any, rename: dict[str, str], family: str) -> tuple[list[str], list[str]]:
    try:
        raw = next(reader)
    except StopIteration as exc:
        raise _fail(f"CSV {family} vazio") from exc
    normalized = [value.strip() for value in raw]
    expected = {name.strip(): value for name, value in rename.items()}
    if len(normalized) != len(set(normalized)):
        raise _fail(f"Cabeçalho {family} com colunas duplicadas")
    missing = set(expected) - set(normalized)
    unexpected = set(normalized) - set(expected)
    if missing or unexpected:
        raise _fail(
            f"Cabeçalho {family}: colunas ausentes {sorted(missing)}; "
            f"colunas não reconhecidas {sorted(unexpected)}"
        )
    return raw, [expected[name] for name in normalized]


def _validated(
    model: type[models.RncRecord], values: dict[str, str], index: int, line: int
) -> dict[str, Any]:
    try:
        return model.model_validate(values).model_dump()
    except pydantic.ValidationError as exc:
        fields = sorted({str(error["loc"][0]) for error in exc.errors() if error["loc"]})
        raise _fail(f"Registro {index}, linha física {line}: campos inválidos {fields}") from exc


def _fingerprint(headers: list[str], family: str) -> dict[str, Any]:
    structure = {"family": family, "delimiter": ",", "headers": headers, "version": 1}
    raw = json.dumps(structure, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "algorithm": "sha256",
        "version": 1,
        "parser_version": PARSER_VERSION,
        "sha256": hashlib.sha256(raw.encode()).hexdigest(),
    }


def _statistics(frame: pd.DataFrame, dates: list[str]) -> dict[str, Any]:
    fields = {}
    date_stats = {}
    identifiers = {}
    for name in (str(column_name) for column_name in frame):
        column = frame[name]
        if name in dates:
            valid = column.dropna()
            date_stats[name] = {
                "null_count": int(column.isna().sum()),
                "minimum": valid.min().date().isoformat() if len(valid) else None,
                "maximum": valid.max().date().isoformat() if len(valid) else None,
            }
        else:
            fields[name] = {"blank_count": int(column.eq("").sum())}
            if name.startswith("nr_"):
                counts = column[column.ne("")].value_counts()
                repeated = counts[counts.gt(1)]
                identifiers[name] = {
                    "unique_nonempty": len(counts),
                    "blank_count": fields[name]["blank_count"],
                    "repeated_groups": len(repeated),
                    "rows_in_repeated_groups": int(repeated.sum()),
                }
    return {
        "field_statistics": fields,
        "date_statistics": date_stats,
        "identifier_statistics": identifiers,
    }


def _details(
    frame: pd.DataFrame,
    *,
    family: str,
    headers: list[str],
    actual_encoding: str,
    blank_lines: int,
    dates: list[str],
    primary_key: str,
    empty_dates: Counter[str],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "family": family,
        "parser_version": PARSER_VERSION,
        "encoding": actual_encoding,
        "source_headers": headers,
        "layout_fingerprint": _fingerprint(headers, family),
        "source_rows": len(frame),
        "validated_rows": len(frame),
        "output_rows": len(frame),
        "blank_physical_rows": blank_lines,
        "eof_reached": True,
        "primary_key": [primary_key],
        "primary_key_scope": "one_resource_content_hash",
        "situacoes": dict(sorted(Counter(frame["situacao"]).items())),
        "coverage": {
            "scope": "received_csv",
            "status": "unknown",
            "reason": "no_verified_source_total",
        },
        "warnings": [],
        **_statistics(frame, dates),
    }
    for name in dates:
        result["date_statistics"][name]["empty_count"] = empty_dates[name]
    if family == "protegidas":
        text = frame["termino_protecao_texto"]
        result["termination_counts"] = {
            "dated": int(frame["termino_protecao"].notna().sum()),
            "conditional": int(text.eq(constants.SNPC_CONDITIONAL_END).sum()),
            "blank": int(text.eq("").sum()),
        }
    return result


def _parse_csv(
    data: bytes,
    *,
    rename: dict[str, str],
    dates: list[str],
    columns: list[str],
    family: str,
    model: type[models.RncRecord],
    primary_key: str,
) -> RncParsedTable:
    if not data or data.isspace():
        raise _fail(f"CSV {family} vazio")
    actual_encoding = encoding.detect_encoding_chain(data)
    buffers: dict[str, list[Any]] = {name: [] for name in columns}
    seen: set[str] = set()
    blank_lines = 0
    empty_dates: Counter[str] = Counter()
    try:
        with io.TextIOWrapper(io.BytesIO(data), encoding=actual_encoding, newline="") as stream:
            reader = csv.reader(stream, delimiter=",", quotechar='"', strict=True)
            headers, mapped = _header(reader, rename, family)
            for row in reader:
                if not row:
                    blank_lines += 1
                    continue
                index = len(seen) + 1
                if len(row) != len(headers):
                    raise _fail(
                        f"Registro {index}, linha física {reader.line_num}: largura CSV incompatível"
                    )
                values = dict(zip(mapped, row, strict=True))
                if family == "protegidas":
                    values["termino_protecao_texto"] = values["termino_protecao"]
                record = _validated(model, values, index, reader.line_num)
                identity = record[primary_key]
                if identity in seen:
                    raise _fail(
                        f"Registro {index}, linha física {reader.line_num}: {primary_key} duplicado"
                    )
                seen.add(identity)
                empty_dates.update(name for name in dates if not values[name].strip())
                for name in columns:
                    buffers[name].append(record[name])
    except (csv.Error, UnicodeError) as exc:
        raise _fail(f"CSV {family} inválido ou encoding incompatível") from exc
    if not seen:
        raise _fail(f"CSV {family} sem registros")
    frame = pd.DataFrame(
        {
            name: pd.Series(values, dtype="datetime64[ns]" if name in dates else "object")
            for name, values in buffers.items()
        }
    )
    details = _details(
        frame,
        family=family,
        headers=headers,
        actual_encoding=actual_encoding,
        blank_lines=blank_lines,
        dates=dates,
        primary_key=primary_key,
        empty_dates=empty_dates,
    )
    logger.info("rnc_parse_ok", label=family, records=len(frame), parser_version=PARSER_VERSION)
    return RncParsedTable(frame=frame, details=details)


def parse_registradas_bundle(data: bytes) -> RncParsedTable:
    return _parse_csv(
        data,
        rename=models.REGISTRADAS_RENAME,
        dates=models.DATE_COLS_REG,
        columns=models.REGISTRADAS_COLS,
        family="registradas",
        model=models.RncRegistrada,
        primary_key="nr_registro",
    )


def parse_protegidas_bundle(data: bytes) -> RncParsedTable:
    return _parse_csv(
        data,
        rename=models.PROTEGIDAS_RENAME,
        dates=models.DATE_COLS_PROT,
        columns=models.PROTEGIDAS_COLS,
        family="protegidas",
        model=models.SnpcProtegida,
        primary_key="nr_processo",
    )


def parse_reported_total(data: bytes) -> int | None:
    soup = BeautifulSoup(data, "lxml")
    text = " ".join(soup.stripped_strings)
    text = " ".join(text.split())
    starts = list(re.finditer(r"Sua pesquisa retornou\b", text))
    if not starts:
        return None
    totals = []
    for start in starts:
        match = re.match(
            r"Sua pesquisa retornou ([0-9]+|[0-9]{1,3}(?:\.[0-9]{3})+) registros\b",
            text[start.start() :],
        )
        if match is None:
            raise _fail("Mensagem de total da pesquisa com número ou estrutura inválidos")
        try:
            totals.append(int(match[1].replace(".", "")))
        except ValueError as exc:
            raise _fail("Número de registros publicado incompatível com a contagem") from exc
    if len(set(totals)) != 1:
        raise _fail("Mensagens conflitantes de total da pesquisa")
    return totals[0]
