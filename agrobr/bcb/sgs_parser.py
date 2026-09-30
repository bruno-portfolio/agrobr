from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd
import pydantic

from agrobr import _log, constants
from agrobr.exceptions import ParseError

from . import sgs_models

logger = _log.get_logger(__name__)
PARSER_VERSION = sgs_models.PARSER_VERSION


class _PublishedObservation(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="allow")

    data: str
    valor: str | None
    data_fim: str | None = pydantic.Field(default=None, alias="dataFim")

    @pydantic.field_validator("data", "data_fim")
    @classmethod
    def validate_date(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if re.fullmatch(constants.BCB_SGS_DATE_PATTERN, value) is None:
            raise ValueError("data deve usar DD/MM/YYYY")
        datetime.strptime(value, "%d/%m/%Y")
        return value

    @pydantic.field_validator("valor")
    @classmethod
    def validate_value(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if re.fullmatch(constants.BCB_SGS_VALUE_PATTERN, value) is None:
            raise ValueError("valor deve ser texto numérico decimal")
        if float(value) == 0:
            try:
                underflow = Decimal(value) != 0
            except InvalidOperation as exc:
                raise ValueError("expoente de valor fora do domínio numérico") from exc
            if underflow:
                raise ValueError("valor não pode perder magnitude na conversão para float64")
        return value


def _error(reason: str) -> ParseError:
    return ParseError(source="bcb_sgs", parser_version=PARSER_VERSION, reason=reason)


def _json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _error("Objeto JSON SGS contém campo repetido")
        result[key] = value
    return result


def _decode(content: bytes) -> list[dict[str, Any]]:
    try:
        payload = json.loads(content, object_pairs_hook=_json_object)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _error("Resposta SGS não é JSON válido") from exc
    if not isinstance(payload, list):
        raise _error("Resposta SGS deve ser uma lista de observações")
    if any(not isinstance(row, dict) for row in payload):
        raise _error("Cada observação SGS deve ser um objeto")
    return payload


def _validate(payload: list[dict[str, Any]]) -> list[sgs_models.SGSObservation]:
    result = []
    dates = set()
    for position, row in enumerate(payload, start=1):
        try:
            published = _PublishedObservation.model_validate(row)
            observation = sgs_models.SGSObservation(
                data=datetime.strptime(published.data, "%d/%m/%Y").date(),
                valor=float(published.valor) if published.valor is not None else None,
                data_fim=datetime.strptime(published.data_fim, "%d/%m/%Y").date()
                if published.data_fim is not None
                else None,
            )
        except pydantic.ValidationError as exc:
            fields = sorted({str(error["loc"][0]) for error in exc.errors() if error["loc"]})
            raise _error(f"Observação SGS inválida na linha {position}; campos: {fields}") from exc
        if observation.data in dates:
            raise _error(f"Data duplicada no mesmo corpo SGS, linha {position}")
        dates.add(observation.data)
        result.append(observation)
    return result


def parse_observations(content: bytes) -> sgs_models.SGSParsedBlock:
    payload = _decode(content)
    records = _validate(payload)
    layouts = sorted({tuple(sorted(row)) for row in payload})
    fingerprint = hashlib.sha256(
        json.dumps(layouts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    extra_fields = sorted(
        {key for row in payload for key in row if key not in {"data", "valor", "dataFim"}}
    )
    warnings = [f"SGS retornou campos adicionais: {extra_fields}"] if extra_fields else []
    logger.info("bcb_sgs_parse_ok", records=len(records), layouts=len(layouts))
    return sgs_models.SGSParsedBlock(
        records=records,
        source_rows=len(payload),
        layout_fingerprint=fingerprint,
        warnings=warnings,
    )


def build_frame(
    records: list[sgs_models.SGSObservation], codigo: int, nome_serie: str | None
) -> pd.DataFrame:
    ordered = sorted(records, key=lambda record: record.data)
    try:
        dates = pd.Series([record.data for record in ordered], dtype="datetime64[ns]")
        ends = pd.Series([record.data_fim for record in ordered], dtype="datetime64[ns]")
    except (ValueError, OverflowError) as exc:
        raise _error("Data SGS fora do domínio datetime64[ns]") from exc
    frame = pd.DataFrame(
        {
            "data": dates,
            "valor": pd.Series([record.valor for record in ordered], dtype="float64"),
            "codigo": pd.Series([codigo] * len(ordered), dtype="int64"),
            "nome_serie": pd.Series(
                [nome_serie if nome_serie is not None else pd.NA] * len(ordered), dtype=object
            ),
        },
        columns=sgs_models.COLUNAS_SAIDA,
    )
    if ends.notna().any():
        frame["data_fim"] = ends
    return frame
