from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any

import pandas as pd
import pydantic
import structlog

from agrobr.exceptions import ParseError

from . import models

logger = structlog.get_logger()
PARSER_VERSION = 2

COLUNAS_MAP: dict[str, str] = {
    "period": "periodo",
    "reporterCode": "reporter_code",
    "reporterISO": "reporter_iso",
    "reporterDesc": "reporter",
    "partnerCode": "partner_code",
    "partnerISO": "partner_iso",
    "partnerDesc": "partner",
    "flowCode": "fluxo_code",
    "flowDesc": "fluxo",
    "cmdCode": "hs_code",
    "cmdDesc": "produto_desc",
    "netWgt": "peso_liquido_kg",
    "grossWgt": "peso_bruto_kg",
    "fobvalue": "valor_fob_usd",
    "cifvalue": "valor_cif_usd",
    "primaryValue": "valor_primario_usd",
    "qty": "quantidade",
    "qtyUnitAbbr": "unidade_qtd",
    "aggrLevel": "nivel_hs",
    "classificationCode": "classificacao",
    "isOriginalClassification": "classificacao_original",
    "isNetWgtEstimated": "peso_liquido_estimado",
    "isGrossWgtEstimated": "peso_bruto_estimado",
    "isQtyEstimated": "quantidade_estimada",
}

_INTEGER_COLS = {"ano", "mes", "reporter_code", "partner_code", "nivel_hs"}
_BOOLEAN_COLS = {
    "classificacao_original",
    "peso_liquido_estimado",
    "peso_bruto_estimado",
    "quantidade_estimada",
    "classificacao_original_reporter",
    "classificacao_original_partner",
}
_NUMERIC_COLS = {
    "peso_liquido_kg",
    "peso_bruto_kg",
    "volume_ton",
    "valor_fob_usd",
    "valor_cif_usd",
    "valor_primario_usd",
    "quantidade",
    "peso_liquido_kg_reporter",
    "valor_fob_usd_reporter",
    "volume_ton_reporter",
    "peso_liquido_kg_partner",
    "valor_fob_usd_partner",
    "valor_cif_usd_partner",
    "volume_ton_partner",
    "diff_peso_kg",
    "diff_valor_fob_usd",
    "ratio_valor",
    "ratio_peso",
}
_TRADE_KEY = ["periodo", "reporter_code", "partner_code", "hs_code", "fluxo_code", "classificacao"]
_MIRROR_KEY = ["periodo", "hs_code"]


def _fail(reason: str) -> ParseError:
    return ParseError(source="comtrade", parser_version=PARSER_VERSION, reason=reason)


def _typed_frame(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = frame.reindex(columns=columns).copy()
    for column in columns:
        if column in _INTEGER_COLS:
            result[column] = result[column].astype("Int64")
        elif column in _BOOLEAN_COLS:
            result[column] = result[column].astype("boolean")
        elif column in _NUMERIC_COLS:
            result[column] = result[column].astype("float64")
        else:
            result[column] = result[column].astype(object)
            result.loc[result[column].isna(), column] = pd.NA
    return result


def _validated(records: Sequence[models.TradeRecord | dict[str, Any]]) -> list[models.TradeRecord]:
    validated = []
    for position, record in enumerate(records, 1):
        try:
            validated.append(models.TradeRecord.model_validate(record))
        except pydantic.ValidationError as exc:
            fields = sorted({str(error["loc"][0]) for error in exc.errors() if error["loc"]})
            raise _fail(f"Registro {position}: campos inválidos {fields}") from exc
    if len({record.freq_code for record in validated}) > 1:
        raise _fail("Resposta mistura frequências anual e mensal")
    return validated


def parse_trade_data(records: Sequence[models.TradeRecord | dict[str, Any]]) -> pd.DataFrame:
    validated = _validated(records)
    output = []
    for record in validated:
        raw = record.model_dump(by_alias=True)
        row = {target: raw[source] for source, target in COLUNAS_MAP.items()}
        row["ano"] = record.ref_year
        row["mes"] = record.ref_month if record.freq_code == "M" else None
        row["volume_ton"] = record.net_wgt / 1000.0 if record.net_wgt is not None else None
        output.append(row)
    frame = _typed_frame(pd.DataFrame(output, dtype=object), models.COLUNAS_SAIDA)
    if frame.duplicated(_TRADE_KEY).any():
        raise _fail("Resposta contém chave bilateral duplicada")
    frame = frame.sort_values(_TRADE_KEY).reset_index(drop=True)
    logger.info("comtrade_parse_ok", records=len(frame))
    return frame


def parse_details(
    records: Sequence[models.TradeRecord | dict[str, Any]], frame: pd.DataFrame
) -> dict[str, Any]:
    layouts = []
    for record in records:
        if isinstance(record, models.TradeRecord):
            fields = {
                models.TradeRecord.model_fields[name].alias or name
                for name in record.model_fields_set
                if name in models.TradeRecord.model_fields
            }
            fields.update((record.model_extra or {}).keys())
        else:
            fields = set(record)
        layouts.append(tuple(sorted(fields)))
    layout = sorted(set(layouts))
    raw = json.dumps(layout, ensure_ascii=False, separators=(",", ":")).encode()
    return {
        "source_rows": len(records),
        "output_rows": len(frame),
        "null_counts": {column: int(frame[column].isna().sum()) for column in frame.columns},
        "layout_fingerprint": {
            "algorithm": "sha256",
            "version": 1,
            "parser_version": PARSER_VERSION,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "layouts": [list(item) for item in layout],
        },
    }


def _mirror_leg(frame: pd.DataFrame, side: str) -> pd.DataFrame:
    if not set(models.COLUNAS_SAIDA).issubset(frame.columns):
        raise _fail(f"Perna {side}: colunas bilaterais ausentes")
    if frame[_MIRROR_KEY].isna().any().any() or frame.duplicated(_MIRROR_KEY).any():
        raise _fail(f"Perna {side}: chave período/HS ausente ou duplicada")
    flow = "X" if side == "reporter" else "M"
    if not frame["fluxo_code"].eq(flow).all():
        raise _fail(f"Perna {side}: fluxo incompatível")
    for column in ("reporter_code", "partner_code"):
        if frame[column].isna().any() or frame[column].nunique() > 1:
            raise _fail(f"Perna {side}: país ausente ou múltiplo")
    columns = [
        *_MIRROR_KEY,
        "peso_liquido_kg",
        "valor_fob_usd",
        "volume_ton",
        "produto_desc",
        "classificacao",
        "classificacao_original",
        "reporter_iso",
        "partner_iso",
        "reporter_code",
        "partner_code",
    ]
    if side == "partner":
        columns.append("valor_cif_usd")
    return frame[columns].rename(
        columns={column: f"{column}_{side}" for column in columns if column not in _MIRROR_KEY}
    )


def _ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    result = numerator / denominator.mask(denominator.eq(0))
    return result.replace([float("inf"), -float("inf")], float("nan"))


def _mirror_identity(
    frame: pd.DataFrame,
    reporter_iso: str | None,
    partner_iso: str | None,
    reporter_code: int | None,
    partner_code: int | None,
) -> None:
    pairs = (
        ("reporter_iso", "reporter_iso_reporter", "partner_iso_partner", reporter_iso),
        ("partner_iso", "partner_iso_reporter", "reporter_iso_partner", partner_iso),
        ("reporter_code", "reporter_code_reporter", "partner_code_partner", reporter_code),
        ("partner_code", "partner_code_reporter", "reporter_code_partner", partner_code),
    )
    for column, left_name, right_name, expected in pairs:
        left, right = frame[left_name], frame[right_name]
        both = left.notna() & right.notna()
        if left.loc[both].ne(right.loc[both]).any():
            raise _fail(f"Espelho: identidade incompatível entre pernas em {column}")
        frame[column] = left.combine_first(right)
        if expected is not None and frame[column].dropna().ne(expected).any():
            raise _fail(f"Espelho: identidade incompatível com seleção em {column}")


def parse_mirror(
    df_reporter: pd.DataFrame,
    df_partner: pd.DataFrame,
    reporter_iso: str | None,
    partner_iso: str | None,
    *,
    reporter_code: int | None = None,
    partner_code: int | None = None,
) -> pd.DataFrame:
    left, right = _mirror_leg(df_reporter, "reporter"), _mirror_leg(df_partner, "partner")
    frame = pd.merge(left, right, on=_MIRROR_KEY, how="outer", validate="one_to_one")
    both = frame["classificacao_reporter"].notna() & frame["classificacao_partner"].notna()
    if frame.loc[both, "classificacao_reporter"].ne(frame.loc[both, "classificacao_partner"]).any():
        raise _fail("Espelho contém edições HS incompatíveis na mesma célula período/HS")
    frame["produto_desc"] = frame["produto_desc_reporter"].combine_first(
        frame["produto_desc_partner"]
    )
    _mirror_identity(frame, reporter_iso, partner_iso, reporter_code, partner_code)
    frame["ano"] = frame["periodo"].map(lambda value: int(value[:4]))
    frame["mes"] = frame["periodo"].map(lambda value: int(value[4:]) if len(value) == 6 else None)
    frame["diff_peso_kg"] = frame["peso_liquido_kg_reporter"] - frame["peso_liquido_kg_partner"]
    frame["diff_valor_fob_usd"] = frame["valor_fob_usd_reporter"] - frame["valor_fob_usd_partner"]
    frame["ratio_valor"] = _ratio(frame["valor_fob_usd_reporter"], frame["valor_cif_usd_partner"])
    frame["ratio_peso"] = _ratio(
        frame["peso_liquido_kg_reporter"], frame["peso_liquido_kg_partner"]
    )
    result = (
        _typed_frame(frame, models.COLUNAS_MIRROR).sort_values(_MIRROR_KEY).reset_index(drop=True)
    )
    logger.info("comtrade_mirror_parse_ok", records=len(result))
    return result
