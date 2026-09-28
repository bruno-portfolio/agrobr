from __future__ import annotations

import codecs
import csv
import io
import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, BinaryIO

from agrobr import constants
from agrobr.exceptions import ParseError

_ALIASES = {
    "data": ("mes_ano", "data"),
    "concessionaria": ("concessionaria",),
    "praca": ("praca",),
    "sentido": ("sentido",),
    "categoria_eixo": ("categoria_eixo", "categoria", "eixo", "n_eixos"),
    "tipo_veiculo": ("tipo_de_veiculo", "tipo_veiculo"),
    "tipo_cobranca": ("tipo_cobranca",),
    "volume": ("volume_total", "quantidade", "volume", "qtd"),
}


def fail(reason: str) -> ParseError:
    return ParseError(source="antt_pedagio", parser_version=3, reason=reason)


def detect_file_encoding(handle: BinaryIO) -> str:
    handle.seek(0)
    bom = handle.read(3) == codecs.BOM_UTF8
    for encoding in ("utf-8-sig",) if bom else ("utf-8", "windows-1252", "iso-8859-1"):
        handle.seek(0)
        decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
        try:
            while block := handle.read(constants.ANTT_CSV_READ_CHUNK):
                decoder.decode(block)
            decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            if bom:
                raise
            continue
        handle.seek(0)
        return encoding
    raise UnicodeError("CSV sem encoding suportado")


def header_mapping(header: list[str]) -> dict[str, int]:
    if len(header) != len(set(header)):
        raise fail("Cabeçalho duplicado")
    known = {alias for names in _ALIASES.values() for alias in names}
    if extra := set(header) - known:
        raise fail(f"Cabeçalho com colunas desconhecidas: {sorted(extra)}")
    mapping = {}
    for field, aliases in _ALIASES.items():
        found = [header.index(alias) for alias in aliases if alias in header]
        if len(found) > 1:
            raise fail(f"Cabeçalho ambíguo para {field}")
        if found:
            mapping[field] = found[0]
        elif field in ("data", "concessionaria", "praca", "volume"):
            raise fail(f"Cabeçalho sem campo obrigatório {field}")
    return mapping


def read_header(reader: Any) -> list[str]:
    try:
        first: list[str] = next(reader)
    except StopIteration as exc:
        raise fail("CSV vazio, sem cabeçalho") from exc
    if any(re.fullmatch(r"(?:[0-9]{2}/)?[0-9]{2}/[0-9]{4}", cell) for cell in first):
        raise fail("CSV sem cabeçalho; layout não suportado")
    header_mapping(first)
    return first


@contextmanager
def open_reader(handle: BinaryIO, encoding: str, delimiter: str = ";") -> Iterator[Any]:
    text = io.TextIOWrapper(handle, encoding=encoding, newline="")
    try:
        yield csv.reader(text, delimiter=delimiter, strict=True)
    finally:
        text.detach()
