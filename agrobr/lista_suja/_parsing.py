from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from datetime import datetime
from typing import Any

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.normalize import dates, encoding

from . import models

_DATE = constants.LISTA_SUJA_DATE_PATTERN
_INCLUSION = re.compile(rf"{_DATE}(?: a {_DATE})?(?:, {_DATE}(?: a {_DATE})?)*")
_TITLE = constants.LISTA_SUJA_PUBLICATION_TITLE


def fail(reason: str) -> ParseError:
    return ParseError(source="lista_suja", parser_version=models.PARSER_VERSION, reason=reason)


def compact(value: str | None) -> str:
    return " ".join((value or "").split())


def date_value(value: str, field: str, row_id: str) -> datetime | None:
    if not value:
        return None
    if not re.fullmatch(_DATE, value):
        raise fail(f"ID {row_id}: formato de data inválido em {field}")
    try:
        return datetime.strptime(value, "%d/%m/%Y")
    except ValueError as exc:
        raise fail(f"ID {row_id}: data inválida em {field}") from exc


def inclusion(value: str, row_id: str) -> tuple[datetime | None, bool]:
    normalized = compact(value)
    if not normalized:
        raise fail(f"ID {row_id}: inclusão no cadastro ausente")
    if not _INCLUSION.fullmatch(normalized):
        raise fail(f"ID {row_id}: estrutura de inclusão inválida")
    dates = [date_value(token, "inclusão", row_id) for token in re.findall(_DATE, normalized)]
    return (dates[0], False) if len(dates) == 1 else (None, True)


def header(values: list[str]) -> list[str]:
    normalized = [compact(value) for value in values]
    if normalized != models.SOURCE_COLUMNS:
        raise fail("Cabeçalho ausente, ambíguo ou incompatível com a Lista Suja")
    return normalized


def fingerprint(values: list[str]) -> dict[str, Any]:
    digest = hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "algorithm": "sha256",
        "version": 1,
        "parser_version": models.PARSER_VERSION,
        "sha256": digest,
    }


def read_csv(data: bytes) -> list[list[str]]:
    try:
        text = data.decode(encoding.detect_encoding_chain(data))
        rows = list(csv.reader(io.StringIO(text, newline=""), delimiter=";", strict=True))
    except (UnicodeError, csv.Error) as exc:
        raise fail("CSV inválido ou encoding incompatível") from exc
    if not rows:
        raise fail("CSV vazio")
    header(rows[0])
    records = rows[1:]
    if not records:
        raise fail("CSV sem registros")
    for position, row in enumerate(records, 2):
        if len(row) != len(models.SOURCE_COLUMNS):
            raise fail(f"Linha {position}: quantidade de campos incompatível")
    return records


def known_decoration(value: str) -> bool:
    normalized = compact(value)
    return normalized.startswith(
        (
            "Cadastro de Empregadores",
            "(Portaria Interministerial",
            "Atualização periódica",
            "I- PUBLICAÇÃO DO CADASTRO DE EMPREGADORES PREVISTA NO ARTIGO 2º",
            "(*",
        )
    ) or normalized in {
        "DENÚNCIAS DE TRABALHO ANÁLOGO À ESCRAVIDÃO",
        "https://ipe.sit.trabalho.gov.br",
        "xrLabelNumeracaoPagina",
    }


def _notes(text: str) -> list[str]:
    result = []
    current: list[str] = []
    for raw_line in text.splitlines():
        line = compact(raw_line)
        if re.match(r"\(\*[0-9]+\)", line):
            if current:
                result.append(" ".join(current))
            current = [line]
        elif current:
            if not line or known_decoration(line) or re.fullmatch(r"[0-9]+(?:/[0-9]+)?", line):
                result.append(" ".join(current))
                current = []
            else:
                current.append(line)
    if current:
        result.append(" ".join(current))
    return list(dict.fromkeys(result))


def publication(text: str) -> dict[str, Any]:
    normalized = compact(text)
    if _TITLE.casefold() not in normalized.casefold():
        raise fail("Publicação não identifica o cadastro principal da Lista Suja")
    if "cadastro de empregadores em ajustamento de conduta" in normalized.casefold():
        raise fail("Publicação CEAC não corresponde à Lista Suja")
    periodic = set(
        re.findall(
            r"Atualização periódica de ([0-9]{1,2}) de ([a-zç]+) de ([0-9]{4})",
            normalized,
            flags=re.I,
        )
    )
    updates = set(
        re.findall(r"Cadastro atualizado em ([0-9]{2}/[0-9]{2}/[0-9]{4})", normalized, flags=re.I)
    )
    if len(periodic) != 1 or len(updates) > 1:
        raise fail("Edição ausente ou conflitante na publicação")
    day, month, year = next(iter(periodic))
    try:
        periodic_date = datetime(int(year), dates.MESES_PT[month.casefold()], int(day))
    except (ValueError, KeyError) as exc:
        raise fail("Data de atualização periódica inválida") from exc
    update = date_value(next(iter(updates)), "atualização", "publicação") if updates else None
    if "cadastro atualizado em" in normalized.casefold() and update is None:
        raise fail("Data de atualização inválida na publicação")
    edition = list(
        dict.fromkeys(
            compact(line)
            for line in text.splitlines()
            if "Atualização periódica" in line or "Cadastro atualizado" in line
        )
    )
    return {
        "title": _TITLE,
        "periodic_update": periodic_date.date().isoformat(),
        "registry_updated_at": update.date().isoformat() if update else None,
        "edition_text": " ".join(edition),
        "notes": _notes(text),
    }


def read_companion(data: bytes) -> tuple[list[list[str]], dict[str, Any]]:
    try:
        text = data.decode(encoding.detect_encoding_chain(data))
        rows = list(csv.reader(io.StringIO(text, newline=""), delimiter="\t", strict=True))
    except (UnicodeError, csv.Error) as exc:
        raise fail("TXT companheiro inválido ou encoding incompatível") from exc
    context = publication(text)
    found = False
    records = []
    for position, row in enumerate(rows, 1):
        if not row or not any(value.strip() for value in row):
            continue
        if row[0].strip() == "ID":
            header(row)
            found = True
        elif found and len(row) == len(models.SOURCE_COLUMNS) and row[0].strip().isdigit():
            records.append(row)
        elif sum(bool(value.strip()) for value in row) != 1 or not known_decoration(" ".join(row)):
            raise fail(f"TXT linha {position}: conteúdo não reconhecido")
    if not found or not records:
        raise fail("TXT companheiro sem tabela válida")
    return records, context
