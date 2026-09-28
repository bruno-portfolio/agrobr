from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
from collections.abc import Iterator
from typing import Any, TypeVar, cast

import pandas as pd
import pydantic

from agrobr.exceptions import ParseError
from agrobr.normalize import encoding

from . import models

PARSER_VERSION = 3

_REQUIRED_FORMULADOS = {"NR_REGISTRO", "MARCA_COMERCIAL", "INGREDIENTE_ATIVO", "CULTURA"}
_COMPOSITE_IA_COL = "INGREDIENTE_ATIVO(GRUPO_QUIMICI)(CONCENTRACAO)"
_NUMBER = r"[+-]?(?:\d+(?:[.,]\d*)?|[.,]\d+)(?:[eE][+-]?\d+)?"
_SCIENTIFIC = re.compile(
    rf"^\s*({_NUMBER})\s*(?:[x×*]\s*)?10\s*(?:\^|\*\*)\s*([+-]?\d+)\s+(.+?)\s*$"
)
_SCALAR = re.compile(rf"^\s*({_NUMBER})(?:\s+(.+?))?\s*$")
_Model = TypeVar("_Model", bound=pydantic.BaseModel)


def _legacy_value(value: str | None) -> str | None:
    return value.replace("\x96", "–").strip() or None if value is not None else None


def _validate(model: type[_Model], record: dict[str, Any], row: int) -> dict[str, Any]:
    try:
        return model.model_validate(record).model_dump()
    except pydantic.ValidationError as exc:
        fields = sorted({str(error["loc"][0]) for error in exc.errors() if error["loc"]})
        raise ParseError(
            source="defensivos",
            parser_version=PARSER_VERSION,
            reason=f"Linha {row}: campos inválidos {fields}",
        ) from exc


def _csv_records(data: bytes) -> tuple[list[str], Iterator[dict[str, str]]]:
    if not data or data.isspace():
        raise ParseError(source="defensivos", parser_version=PARSER_VERSION, reason="CSV vazio")
    stream = io.TextIOWrapper(
        io.BytesIO(data), encoding=encoding.detect_encoding_chain(data), newline=""
    )
    reader = csv.reader(stream, delimiter=";")
    try:
        header = next(reader)
    except (StopIteration, csv.Error, UnicodeDecodeError) as exc:
        raise ParseError(
            source="defensivos", parser_version=PARSER_VERSION, reason="Cabeçalho CSV inválido"
        ) from exc
    if len(header) != len(set(header)):
        raise ParseError(
            source="defensivos",
            parser_version=PARSER_VERSION,
            reason="Cabeçalho CSV com colunas duplicadas",
        )

    def records() -> Iterator[dict[str, str]]:
        try:
            for row in reader:
                if not row:
                    continue
                if len(row) != len(header):
                    raise ParseError(
                        source="defensivos",
                        parser_version=PARSER_VERSION,
                        reason=f"Linha {reader.line_num}: quantidade de campos incompatível com cabeçalho",
                    )
                yield dict(zip(header, row, strict=True))
        except (csv.Error, UnicodeDecodeError) as exc:
            raise ParseError(
                source="defensivos",
                parser_version=PARSER_VERSION,
                reason=f"CSV inválido na linha {reader.line_num}",
            ) from exc
        finally:
            stream.close()

    return header, records()


def _mapped(row: dict[str, str], rename: dict[str, str]) -> dict[str, str | None]:
    mapped: dict[str, str | None] = {}
    for source, target in rename.items():
        if source in row:
            value = row[source]
            if target in mapped and mapped[target] != value:
                raise ParseError(
                    source="defensivos",
                    parser_version=PARSER_VERSION,
                    reason=f"Aliases divergentes para {target}",
                )
            mapped[target] = value
    return mapped


def _split_components(value: str) -> tuple[list[str], bool]:
    depth = 0
    start = 0
    parts = []
    for index, char in enumerate(value):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                return [value], False
        elif char == "+" and depth == 0:
            parts.append(value[start:index])
            start = index + 1
    if depth:
        return [value], False
    parts.append(value[start:])
    return (parts, True) if all(part.strip() for part in parts) else ([value], False)


def _tail_groups(value: str) -> tuple[str, str, str] | None:
    remaining = value.rstrip()
    groups = []
    for _ in range(2):
        if not remaining.endswith(")"):
            return None
        depth = 0
        for index in range(len(remaining) - 1, -1, -1):
            if remaining[index] == ")":
                depth += 1
            elif remaining[index] == "(":
                depth -= 1
                if depth == 0:
                    groups.append(remaining[index + 1 : -1])
                    remaining = remaining[:index].rstrip()
                    break
        else:
            return None
    return remaining.strip(), groups[1], groups[0]


def _concentration(value: str) -> tuple[float | None, str | None, str | None]:
    scientific = _SCIENTIFIC.fullmatch(value)
    scalar = _SCALAR.fullmatch(value)
    try:
        if scientific:
            number = float(scientific[1].replace(",", ".")) * 10.0 ** int(scientific[2])
            unit = scientific[3]
        elif scalar:
            number = float(scalar[1].replace(",", "."))
            unit = scalar[2]
            if unit and re.match(r"[\d.x×*+-]", unit):
                return None, None, "expressão numérica ambígua"
        else:
            return None, None, "concentração sem interpretação numérica"
    except (ValueError, OverflowError):
        return None, None, "concentração numérica fora do intervalo finito"
    if not math.isfinite(number) or number < 0:
        return None, None, "concentração negativa ou não finita"
    token = scientific[1] if scientific else scalar[1] if scalar else ""
    if number == 0 and any(char in "123456789" for char in token.lower().split("e")[0]):
        return None, None, "concentração abaixo do intervalo representável sem zero"
    return number, unit, None


def _components(
    value: str | None,
    tipo: str,
    register: str,
    source_row: int,
    legacy_group: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if value is None or not value.strip():
        return [], [
            {"nr_registro": register, "reason": "composição ausente", "source_row": source_row}
        ]
    parts, balanced = _split_components(value)
    records = []
    issues = []
    for order, part in enumerate(parts, start=1):
        tail = _tail_groups(part) if balanced else None
        ingredient, group, concentration = tail if tail else (part.strip(), legacy_group, None)
        number, unit, reason = (
            _concentration(concentration)
            if concentration is not None
            else (None, None, "concentração ou estrutura ausente")
        )
        if reason:
            issues.append(
                {
                    "nr_registro": register,
                    "ordem_componente": order,
                    "reason": reason,
                    "source_row": source_row,
                }
            )
        records.append(
            _validate(
                models.AgrofitComponente,
                {
                    "tipo": tipo,
                    "nr_registro": register,
                    "ordem_componente": order,
                    "ingrediente_ativo": ingredient or None,
                    "grupo_quimico": group or None,
                    "componente_texto": part,
                    "concentracao_texto": concentration,
                    "concentracao_valor": number,
                    "concentracao_unidade": unit,
                },
                source_row,
            )
        )
    return records, issues


def _split_composite_ia(value: str) -> tuple[str, str]:
    parts, balanced = _split_components(value)
    tails = [_tail_groups(part) if balanced else None for part in parts]
    names = [tail[0] if tail else part.strip() for tail, part in zip(tails, parts, strict=True)]
    groups = [tail[1] for tail in tails if tail]
    return " + ".join(names), " + ".join(groups)


def _frame(records: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame(records, columns=columns, dtype=object)
    if "ordem_componente" in columns:
        frame["ordem_componente"] = frame["ordem_componente"].astype("Int64")
        frame["concentracao_valor"] = frame["concentracao_valor"].astype("Float64")
    return frame


def _details(
    header: list[str],
    source_rows: int,
    products: int,
    authorizations: int,
    components: int,
    issues: list[dict[str, Any]],
    used_columns: set[str],
) -> dict[str, Any]:
    normalized = [" ".join(name.strip().lower().split()) for name in header]
    digest = hashlib.sha256(
        json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "source_rows": source_rows,
        "product_rows": products,
        "authorization_rows": authorizations,
        "component_rows": components,
        "unparsed_count": len(issues),
        "unparsed_components": issues,
        "layout_fingerprint": {
            "algorithm": "sha256",
            "version": 1,
            "parser_version": PARSER_VERSION,
            "sha256": digest,
        },
        "source_columns": header,
        "ignored_columns": [name for name in header if name not in used_columns],
        "warnings": [
            f"{len(issues)} componentes/composições sem interpretação numérica completa; texto original preservado."
        ]
        if issues
        else [],
    }


def _remember_product(
    products: dict[str, dict[str, Any]],
    signatures: dict[str, dict[str, Any]],
    register: str,
    raw: dict[str, Any],
    normalized: dict[str, Any],
) -> bool:
    if register in signatures:
        changed = [name for name, value in raw.items() if signatures[register].get(name) != value]
        if changed:
            raise ParseError(
                source="defensivos",
                parser_version=PARSER_VERSION,
                reason=f"Registro {register}: campos de produto divergentes {changed}",
            )
        return False
    signatures[register] = raw
    products[register] = normalized
    return True


def parse_formulados_bundle(data: bytes) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    header, rows = _csv_records(data)
    missing = _REQUIRED_FORMULADOS - set(header)
    if missing:
        raise ParseError(
            source="defensivos",
            parser_version=PARSER_VERSION,
            reason=f"Colunas faltando no CSV formulados: {sorted(missing)}",
        )
    products: dict[str, dict[str, Any]] = {}
    signatures: dict[str, dict[str, Any]] = {}
    authorizations = []
    components = []
    issues = []
    count = 0
    for count, row in enumerate(rows, start=1):
        mapped = _mapped(row, models.FORMULADOS_RENAME)
        raw_product = {name: mapped.get(name) for name in models.FORMULADOS_PRODUCT_COLS}
        raw_product["composicao_texto"] = row["INGREDIENTE_ATIVO"]
        product = {
            name: value if name in {"situacao", "composicao_texto"} else _legacy_value(value)
            for name, value in raw_product.items()
        }
        product = _validate(models.AgrofitFormulado, product, count)
        register = cast(str, product["nr_registro"])
        if _remember_product(products, signatures, register, raw_product, product):
            records, diagnostics = _components(
                product["composicao_texto"], "formulados", register, count
            )
            components.extend(records)
            issues.extend(diagnostics)
        authorization = {
            name: mapped.get(name) if name == "situacao" else _legacy_value(mapped.get(name))
            for name in models.AUTORIZACOES_COLS
        }
        authorizations.append(_validate(models.AgrofitAutorizacao, authorization, count))
    tables = {
        "formulados": _frame(list(products.values()), models.FORMULADOS_PRODUCT_COLS),
        "autorizacoes": _frame(authorizations, models.AUTORIZACOES_COLS),
        "composicao": _frame(components, models.COMPOSICAO_COLS),
    }
    return tables, _details(
        header,
        count,
        len(products),
        len(authorizations),
        len(components),
        issues,
        set(models.FORMULADOS_RENAME),
    )


def parse_tecnicos_bundle(data: bytes) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    header, rows = _csv_records(data)
    if "CLASSE" not in header or not {"NR_REGISTRO", "NUMERO_REGISTRO"}.intersection(header):
        raise ParseError(
            source="defensivos",
            parser_version=PARSER_VERSION,
            reason="Colunas faltando no CSV técnicos: CLASSE e número de registro são obrigatórios",
        )
    products: dict[str, dict[str, Any]] = {}
    signatures: dict[str, dict[str, Any]] = {}
    components = []
    issues = []
    count = 0
    for count, row in enumerate(rows, start=1):
        mapped = _mapped(row, models.TECNICOS_RENAME)
        composition = row.get(_COMPOSITE_IA_COL, row.get("INGREDIENTE_ATIVO"))
        raw_product = {name: mapped.get(name) for name in models.TECNICOS_COLS}
        raw_product["composicao_texto"] = composition
        product = {
            name: value if name == "composicao_texto" else _legacy_value(value)
            for name, value in raw_product.items()
        }
        if _COMPOSITE_IA_COL in row:
            ingredient, group = _split_composite_ia(composition or "")
            product["ingrediente_ativo"] = _legacy_value(ingredient)
            product["grupo_quimico"] = _legacy_value(group)
        product = _validate(models.AgrofitTecnico, product, count)
        register = cast(str, product["nr_registro"])
        if _remember_product(products, signatures, register, raw_product, product):
            records, diagnostics = _components(
                composition, "tecnicos", register, count, mapped.get("grupo_quimico")
            )
            components.extend(records)
            issues.extend(diagnostics)
    tables = {
        "tecnicos": _frame(list(products.values()), models.TECNICOS_COLS),
        "composicao": _frame(components, models.COMPOSICAO_COLS),
    }
    return tables, _details(
        header,
        count,
        len(products),
        0,
        len(components),
        issues,
        set(models.TECNICOS_RENAME) | {_COMPOSITE_IA_COL},
    )
