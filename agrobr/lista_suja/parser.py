from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import pandas as pd
import pydantic

from agrobr import _log

from . import _parsing, _pdf, models

logger = _log.get_logger(__name__)
PARSER_VERSION = models.PARSER_VERSION


def _integer(value: str, field: str, row_id: str) -> int | None:
    if not value:
        return None
    if len(value) > 19 or not re.fullmatch(r"[0-9]+", value):
        raise _parsing.fail(f"ID {row_id}: inteiro inválido em {field}")
    if field == "ano_acao_fiscal" and len(value) != 4:
        raise _parsing.fail(f"ID {row_id}: ano inválido")
    return int(value)


def _index(records: list[list[str]]) -> dict[str, list[str]]:
    indexed = {}
    for row in records:
        row_id = row[0].strip()
        if row_id in indexed:
            raise _parsing.fail(f"ID {row_id}: registro duplicado")
        indexed[row_id] = row
    return indexed


def _compare_companion(records: list[list[str]], companion: list[list[str]]) -> None:
    original, other = _index(records), _index(companion)
    if original.keys() != other.keys():
        raise _parsing.fail("CSV/TXT com conjuntos de IDs distintos")
    for row_id, row in original.items():
        if [value.strip() for value in row] != [value.strip() for value in other[row_id]]:
            raise _parsing.fail(f"ID {row_id}: campos divergentes entre CSV e TXT")


def _record(row: list[str], update: datetime | None) -> tuple[dict[str, Any], bool]:
    values = [_parsing.compact(value) for value in row]
    row_id, year, uf, employer, document, establishment, workers, cnae, decision, _ = values
    inclusion, compound = _parsing.inclusion(row[9], row_id)
    payload = {
        "empregador": employer,
        "cpf_cnpj": document,
        "estabelecimento": establishment or None,
        "uf": uf or None,
        "cnae": cnae or None,
        "data_inclusao": inclusion,
        "trabalhadores_resgatados": _integer(workers, "trabalhadores_resgatados", row_id),
        "ano_acao_fiscal": _integer(year, "ano_acao_fiscal", row_id),
        "id_registro": row_id,
        "data_decisao": _parsing.date_value(decision, "data_decisao", row_id),
        "data_atualizacao": update,
        "data_inclusao_texto": row[9],
    }
    try:
        return models.EmployerRecord.model_validate(payload).model_dump(), compound
    except pydantic.ValidationError as exc:
        fields = sorted({str(error["loc"][0]) for error in exc.errors() if error["loc"]})
        raise _parsing.fail(f"ID {row_id}: campos inválidos {fields}") from exc


def _frame(records: list[list[str]], context: dict[str, Any]) -> tuple[pd.DataFrame, list[str]]:
    _index(records)
    date = context.get("registry_updated_at")
    update = datetime.fromisoformat(date) if date else None
    output = []
    compound_ids = []
    for row in records:
        record, compound = _record(row, update)
        output.append(record)
        if compound:
            compound_ids.append(record["id_registro"])
    frame = pd.DataFrame(output, columns=models.COLUNAS_SAIDA, dtype=object)
    for column in ("trabalhadores_resgatados", "ano_acao_fiscal"):
        frame[column] = frame[column].astype("Int64")
    for column in ("data_inclusao", "data_decisao", "data_atualizacao"):
        try:
            frame[column] = pd.to_datetime(frame[column], errors="raise").astype("datetime64[ns]")
        except (ValueError, OverflowError) as exc:
            raise _parsing.fail(f"Data fora do intervalo suportado em {column}") from exc
    texto = pd.Series([""]).dtype
    return frame.astype(
        {name: texto for name in frame.columns if frame[name].dtype == object}
    ), compound_ids


def parse_empregadores_bundle(
    data: bytes, *, formato: str, companion: bytes | None = None
) -> tuple[pd.DataFrame, dict[str, Any]]:
    warnings = []
    page_count = None
    if formato == "csv":
        records = _parsing.read_csv(data)
        context: dict[str, Any] = {
            "title": None,
            "periodic_update": None,
            "registry_updated_at": None,
            "edition_text": None,
            "notes": [],
        }
        if companion is not None:
            other, context = _parsing.read_companion(companion)
            _compare_companion(records, other)
        else:
            warnings.append(
                "TXT companheiro indisponível: edição e data de atualização não comprovadas."
            )
    elif formato == "pdf":
        records, context, page_count = _pdf.read_pdf(data)
    else:
        raise _parsing.fail("Formato deve ser csv ou pdf")
    frame, compound_ids = _frame(records, context)
    if compound_ids:
        warnings.append(
            f"{len(compound_ids)} registros com inclusão composta: escalar nulo e texto preservado."
        )
    details = {
        "publication": context,
        "source_rows": len(records),
        "output_rows": len(frame),
        "null_counts": {column: int(frame[column].isna().sum()) for column in frame.columns},
        "compound_inclusion_ids": compound_ids,
        "layout_fingerprint": _parsing.fingerprint(models.SOURCE_COLUMNS),
        "warnings": warnings,
        "formato": formato,
        "pages": page_count,
        "companion_validated": formato == "csv" and companion is not None,
    }
    logger.info("lista_suja_parse_ok", records=len(frame), formato=formato)
    return frame, details


def parse_empregadores(data: bytes) -> pd.DataFrame:
    return parse_empregadores_bundle(data, formato="pdf")[0]
