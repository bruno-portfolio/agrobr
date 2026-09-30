from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime
from typing import Any, Literal

import pandas as pd
import pydantic

from agrobr import _log, constants
from agrobr.exceptions import ParseError

from . import focus_models

logger = _log.get_logger(__name__)
PARSER_VERSION = focus_models.PARSER_VERSION


class _PublishedRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="allow", allow_inf_nan=False)

    indicador: str = pydantic.Field(alias="Indicador")
    data: str = pydantic.Field(alias="Data")
    data_referencia: str = pydantic.Field(alias="DataReferencia")
    media: float | None = pydantic.Field(alias="Media")
    mediana: float | None = pydantic.Field(alias="Mediana")
    desvio_padrao: float | None = pydantic.Field(alias="DesvioPadrao")
    minimo: float | None = pydantic.Field(alias="Minimo")
    maximo: float | None = pydantic.Field(alias="Maximo")
    numero_respondentes: int | None = pydantic.Field(alias="numeroRespondentes", ge=0, le=2**31 - 1)
    base_calculo: int | None = pydantic.Field(alias="baseCalculo", ge=0, le=2**31 - 1)

    @pydantic.field_validator("data")
    @classmethod
    def valid_date(cls, value: str) -> str:
        if re.fullmatch(constants.BCB_FOCUS_DATE_PATTERN, value) is None:
            raise ValueError("Data deve usar YYYY-MM-DD")
        datetime.strptime(value, "%Y-%m-%d")
        return value


class _AnnualRecord(_PublishedRecord):
    indicador_detalhe: str | None = pydantic.Field(alias="IndicadorDetalhe")


class _MonthlyRecord(_PublishedRecord):
    indicador_detalhe: None = pydantic.Field(default=None, alias="IndicadorDetalhe")


class _Envelope(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="allow")

    value: list[dict[str, Any]]
    reported_count: int | None = pydantic.Field(default=None, alias="@odata.count", ge=0)
    next_link: str | None = pydantic.Field(default=None, alias="@odata.nextLink")

    @pydantic.field_validator("reported_count", mode="before")
    @classmethod
    def count_not_null(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("@odata.count presente deve ser inteiro")
        return value

    @pydantic.field_validator("next_link", mode="before")
    @classmethod
    def link_is_text(cls, value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("@odata.nextLink presente deve conter texto")
        return value


def _error(reason: str) -> ParseError:
    return ParseError(source="bcb_focus", parser_version=PARSER_VERSION, reason=reason)


def _json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _error("Objeto JSON Focus contém campo repetido")
        result[key] = value
    return result


def _json_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise _error("Número JSON Focus fora do domínio finito float64")
    mantissa = text.lower().split("e", 1)[0]
    if value == 0 and any(character in "123456789" for character in mantissa):
        raise _error("Número JSON Focus perde magnitude na conversão para float64")
    return value


def _invalid_constant(value: str) -> Any:
    raise _error(f"Constante JSON Focus não finita: {value}")


def _decode(content: bytes) -> tuple[dict[str, Any], _Envelope]:
    try:
        payload = json.loads(
            content,
            object_pairs_hook=_json_object,
            parse_float=_json_float,
            parse_constant=_invalid_constant,
        )
        envelope = _Envelope.model_validate(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise _error(
            "Envelope Focus inválido: exige objeto com value lista e anotações tipadas"
        ) from exc
    return payload, envelope


def _parse_records(
    rows: list[dict[str, Any]], periodicidade: Literal["anual", "mensal"]
) -> list[focus_models.FocusObservation]:
    model = _AnnualRecord if periodicidade == "anual" else _MonthlyRecord
    records = []
    identities = set()
    for position, row in enumerate(rows, start=1):
        try:
            published = model.model_validate(row)
            values = published.model_dump(include=set(model.model_fields))
            values["data"] = datetime.strptime(published.data, "%Y-%m-%d").date()
            values["periodicidade"] = periodicidade
            record = focus_models.FocusObservation.model_validate(values)
        except (pydantic.ValidationError, OverflowError) as exc:
            raise _error(f"Observação Focus inválida na linha {position}") from exc
        key = focus_models.identity(record)
        if key in identities:
            raise _error(f"Chave Focus duplicada na página, linha {position}")
        identities.add(key)
        records.append(record)
    return records


def _statistical_warnings(records: list[focus_models.FocusObservation]) -> list[str]:
    warnings = []
    for position, row in enumerate(records, start=1):
        issues = []
        if row.desvio_padrao is not None and row.desvio_padrao < 0:
            issues.append("desvio_padrao negativo")
        if row.minimo is not None and row.maximo is not None and row.minimo > row.maximo:
            issues.append("minimo maior que maximo")
        for name in ("media", "mediana"):
            value = getattr(row, name)
            if value is not None and (
                (row.minimo is not None and value < row.minimo)
                or (row.maximo is not None and value > row.maximo)
            ):
                issues.append(f"{name} fora dos extremos disponíveis")
        if row.base_calculo not in {None, 0, 1}:
            issues.append("base_calculo não documentada neste conjunto de capturas")
        if issues:
            warnings.append(f"Focus linha {position}: {'; '.join(issues)}; valores preservados.")
    return warnings


def _layout(
    payload: dict[str, Any], rows: list[dict[str, Any]], periodicidade: str
) -> tuple[str, list[str]]:
    record_fields = sorted({tuple(sorted(row)) for row in rows})
    layout = {"envelope": sorted(payload), "records": record_fields}
    fingerprint = hashlib.sha256(
        json.dumps(layout, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    warnings = []
    envelope_extras = set(payload) - {"value", "@odata.context", "@odata.count", "@odata.nextLink"}
    if envelope_extras:
        warnings.append(f"Campos adicionais no envelope Focus: {sorted(envelope_extras)}")
    expected = {field.alias for field in _AnnualRecord.model_fields.values()}
    extras = {key for row in rows for key in row} - expected
    if extras:
        warnings.append(f"Campos adicionais nas observações Focus: {sorted(extras)}")
    if periodicidade == "mensal" and any("IndicadorDetalhe" in row for row in rows):
        warnings.append(
            "Focus mensal incluiu IndicadorDetalhe null; variante de layout preservada."
        )
    return fingerprint, warnings


def parse_page(
    content: bytes, periodicidade: Literal["anual", "mensal"]
) -> focus_models.FocusParsedPage:
    if periodicidade not in {"anual", "mensal"}:
        raise _error("Periodicidade Focus não suportada")
    payload, envelope = _decode(content)
    records = _parse_records(envelope.value, periodicidade)
    fingerprint, warnings = _layout(payload, envelope.value, periodicidade)
    warnings.extend(_statistical_warnings(records))
    logger.info("bcb_focus_parse_ok", records=len(records), periodicidade=periodicidade)
    return focus_models.FocusParsedPage(
        records=records,
        source_rows=len(records),
        reported_count=envelope.reported_count,
        next_link=envelope.next_link,
        layout_fingerprint=fingerprint,
        warnings=warnings,
    )


def build_frame(records: list[focus_models.FocusObservation]) -> pd.DataFrame:
    values = [record.model_dump() for record in records]
    frame = pd.DataFrame(values, columns=focus_models.COLUNAS_SAIDA)
    try:
        frame["data"] = pd.Series([record.data for record in records], dtype="datetime64[ns]")
    except (ValueError, OverflowError) as exc:
        raise _error("Data Focus fora do domínio datetime64[ns]") from exc
    for column in ("media", "mediana", "desvio_padrao", "minimo", "maximo"):
        frame[column] = frame[column].astype("float64")
    for column in ("numero_respondentes", "base_calculo"):
        frame[column] = frame[column].astype("Int64")
    for column in ("indicador", "data_referencia", "periodicidade", "indicador_detalhe"):
        frame[column] = frame[column].astype(pd.Series([""]).dtype)
    return frame
