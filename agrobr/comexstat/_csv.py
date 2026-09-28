from __future__ import annotations

import codecs
import csv
import hashlib
import io
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import BinaryIO

from agrobr import constants
from agrobr.exceptions import ParseError


def error(reason: str) -> ParseError:
    return ParseError(source="comexstat", parser_version=2, reason=reason)


def detect_encoding(file: BinaryIO) -> tuple[str, list[dict[str, str]]]:
    attempts: list[dict[str, str]] = []
    for encoding in ("utf-8-sig", "cp1252", "iso-8859-1"):
        file.seek(0)
        decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
        try:
            while block := file.read(constants.COMEXSTAT_CSV_CHUNK_BYTES):
                decoder.decode(block, final=False)
            decoder.decode(b"", final=True)
        except UnicodeDecodeError as exc:
            attempts.append({"encoding": encoding, "status": "rejected", "reason": str(exc)})
            continue
        attempts.append({"encoding": encoding, "status": "accepted"})
        file.seek(0)
        return encoding, attempts
    raise error("Nenhum encoding aceito")


@dataclass
class CSVRows:
    reader: Iterator[list[str]]
    header: tuple[str, ...]
    fingerprint: str
    records: int = 0
    eof: bool = False

    def __iter__(self) -> Iterator[tuple[int, dict[str, str]]]:
        try:
            for ordinal, row in enumerate(self.reader, 1):
                self.records = ordinal
                if len(row) != len(self.header):
                    raise error(
                        f"Registro {ordinal}: largura {len(row)}, esperada {len(self.header)}"
                    )
                yield ordinal, dict(zip(self.header, row, strict=True))
            self.eof = True
        except (csv.Error, UnicodeDecodeError) as exc:
            raise error(f"CSV inválido após registro {self.records}: {exc}") from exc


@contextmanager
def open_rows(file: BinaryIO, encoding: str, expected: tuple[str, ...]) -> Iterator[CSVRows]:
    file.seek(0)
    text = io.TextIOWrapper(file, encoding=encoding, errors="strict", newline="")
    try:
        reader = csv.reader(text, delimiter=";", strict=True)
        try:
            header = tuple(next(reader))
        except (StopIteration, csv.Error, UnicodeDecodeError) as exc:
            raise error("CSV sem cabeçalho válido") from exc
        if (
            len(header) != len(expected)
            or len(set(header)) != len(header)
            or set(header) != set(expected)
        ):
            raise error(f"Projeção divergente: esperado {expected!r}; recebido {header!r}")
        fingerprint = hashlib.sha256(
            json.dumps(
                {"properties": header, "delimiter": ";", "parser": 2}, separators=(",", ":")
            ).encode()
        ).hexdigest()
        yield CSVRows(reader, header, fingerprint)
    finally:
        text.detach()
