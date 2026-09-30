from __future__ import annotations

import hashlib
import json
import math
from typing import Any

import pandas as pd
import pydantic

from agrobr import _log, constants
from agrobr.exceptions import ParseError

from . import ptax_models

logger = _log.get_logger(__name__)
PARSER_VERSION = ptax_models.PARSER_VERSION


class _PublishedQuote(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="allow", allow_inf_nan=False)

    cotacao_compra: float | None = pydantic.Field(alias="cotacaoCompra")
    cotacao_venda: float | None = pydantic.Field(alias="cotacaoVenda")
    data_hora: str = pydantic.Field(alias="dataHoraCotacao")
    paridade_compra: float | None = pydantic.Field(alias="paridadeCompra")
    paridade_venda: float | None = pydantic.Field(alias="paridadeVenda")
    tipo_boletim: str | None = pydantic.Field(alias="tipoBoletim")


class _PublishedCurrency(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="allow")

    moeda: str = pydantic.Field(alias="simbolo")
    nome: str = pydantic.Field(alias="nomeFormatado")
    tipo_moeda: str = pydantic.Field(alias="tipoMoeda")


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
    return ParseError(source="bcb_ptax", parser_version=PARSER_VERSION, reason=reason)


def _json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _error("Objeto JSON PTAX contém campo repetido")
        result[key] = value
    return result


def _json_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise _error("Número JSON PTAX fora do domínio finito float64")
    mantissa = text.lower().split("e", 1)[0]
    if value == 0 and any(character in "123456789" for character in mantissa):
        raise _error("Número JSON PTAX perde magnitude na conversão para float64")
    return value


def _invalid_constant(value: str) -> Any:
    raise _error(f"Constante JSON PTAX não finita: {value}")


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
            "Envelope PTAX inválido: exige objeto com value lista e anotações tipadas"
        ) from exc
    return payload, envelope


def _layout(
    payload: dict[str, Any], rows: list[dict[str, Any]], model: type[pydantic.BaseModel]
) -> tuple[str, list[str]]:
    layout = {"envelope": sorted(payload), "records": sorted({tuple(sorted(row)) for row in rows})}
    fingerprint = hashlib.sha256(
        json.dumps(layout, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    warnings = []
    extras = set(payload) - {"value", "@odata.context", "@odata.count", "@odata.nextLink"}
    if extras:
        warnings.append(f"Campos adicionais no envelope PTAX: {sorted(extras)}")
    expected = {field.alias for field in model.model_fields.values()}
    extras = {key for row in rows for key in row} - expected
    if extras:
        warnings.append(f"Campos adicionais nos registros PTAX: {sorted(extras)}")
    return fingerprint, warnings


def _quotes(rows: list[dict[str, Any]], moeda: str) -> list[ptax_models.PtaxObservation]:
    records = []
    identities = set()
    for position, row in enumerate(rows, start=1):
        try:
            published = _PublishedQuote.model_validate(row)
            values = published.model_dump(include=set(_PublishedQuote.model_fields))
            values.update(moeda=moeda, data=ptax_models.timestamp(published.data_hora).date())
            record = ptax_models.PtaxObservation.model_validate(values)
        except (ValueError, OverflowError) as exc:
            raise _error(f"Cotação PTAX inválida na linha {position}") from exc
        key = ptax_models.identity(record)
        if key in identities:
            raise _error(f"Chave PTAX duplicada na página, linha {position}")
        identities.add(key)
        records.append(record)
    return records


def _quote_warnings(records: list[ptax_models.PtaxObservation]) -> list[str]:
    warnings = []
    for position, record in enumerate(records, start=1):
        issues = []
        if record.tipo_boletim not in constants.BCB_PTAX_BULLETIN_LABELS:
            issues.append("tipo_boletim ausente ou não reconhecido")
        for prefix in ("cotacao", "paridade"):
            compra = getattr(record, f"{prefix}_compra")
            venda = getattr(record, f"{prefix}_venda")
            if any(value is not None and value <= 0 for value in (compra, venda)):
                issues.append(f"{prefix} não positiva")
            if compra is not None and venda is not None and compra > venda:
                issues.append(f"{prefix}_compra maior que venda")
        if issues:
            warnings.append(f"PTAX linha {position}: {'; '.join(issues)}; valores preservados.")
    return warnings


def parse_quotes_page(content: bytes, moeda: str) -> ptax_models.PtaxQuotesPage:
    payload, envelope = _decode(content)
    records = _quotes(envelope.value, moeda)
    fingerprint, warnings = _layout(payload, envelope.value, _PublishedQuote)
    warnings.extend(_quote_warnings(records))
    logger.info("bcb_ptax_quotes_parsed", records=len(records))
    return ptax_models.PtaxQuotesPage(
        records=records,
        source_rows=len(records),
        reported_count=envelope.reported_count,
        next_link=envelope.next_link,
        layout_fingerprint=fingerprint,
        warnings=warnings,
    )


def parse_currencies_page(content: bytes) -> ptax_models.PtaxCurrenciesPage:
    payload, envelope = _decode(content)
    records = []
    currencies = set()
    fingerprint, warnings = _layout(payload, envelope.value, _PublishedCurrency)
    for position, row in enumerate(envelope.value, start=1):
        try:
            published = _PublishedCurrency.model_validate(row)
            values = published.model_dump(include=set(_PublishedCurrency.model_fields))
            record = ptax_models.PtaxCurrency.model_validate(values)
        except pydantic.ValidationError as exc:
            raise _error(f"Moeda PTAX inválida na linha {position}") from exc
        if record.moeda in currencies:
            raise _error(f"Moeda PTAX duplicada na página, linha {position}")
        currencies.add(record.moeda)
        if record.tipo_moeda not in {"A", "B"}:
            warnings.append(f"PTAX moeda linha {position}: tipo_moeda novo; texto preservado.")
        records.append(record)
    logger.info("bcb_ptax_currencies_parsed", records=len(records))
    return ptax_models.PtaxCurrenciesPage(
        records=records,
        source_rows=len(records),
        reported_count=envelope.reported_count,
        next_link=envelope.next_link,
        layout_fingerprint=fingerprint,
        warnings=warnings,
    )


def build_quotes_frame(records: list[ptax_models.PtaxObservation]) -> pd.DataFrame:
    frame = pd.DataFrame(
        [record.model_dump() for record in records], columns=ptax_models.COLUNAS_SAIDA
    )
    frame["data_hora"] = pd.Series([record.timestamp for record in records], dtype="datetime64[ns]")
    frame["data"] = frame["data_hora"].dt.normalize()
    for column in ("cotacao_compra", "cotacao_venda", "paridade_compra", "paridade_venda"):
        frame[column] = frame[column].astype("float64")
    for column in ("moeda", "tipo_boletim"):
        frame[column] = frame[column].astype(object)
        frame.loc[frame[column].isna(), column] = pd.NA
    return frame


def build_currencies_frame(records: list[ptax_models.PtaxCurrency]) -> pd.DataFrame:
    frame = pd.DataFrame(
        [record.model_dump() for record in records], columns=ptax_models.COLUNAS_MOEDAS
    )
    return frame.astype(object)
