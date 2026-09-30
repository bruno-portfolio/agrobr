from __future__ import annotations

from numbers import Integral
from typing import Any

import pandas as pd

from agrobr import _log
from agrobr.exceptions import ParseError

from . import models

logger = _log.get_logger(__name__)

PARSER_VERSION = 2

CAMPOS = (
    "commodityCode",
    "countryCode",
    "marketYear",
    "calendarYear",
    "month",
    "attributeId",
    "unitId",
    "value",
)

COLUNAS = [
    "commodity_code",
    "commodity",
    "country_code",
    "country",
    "market_year",
    "attribute",
    "attribute_br",
    "value",
    "unit",
    "attribute_id",
    "unit_id",
    "last_update_year",
    "last_update_month",
]

COLUNAS_INTEIRAS = (
    "market_year",
    "attribute_id",
    "unit_id",
    "last_update_year",
    "last_update_month",
)
COLUNAS_TEXTO = tuple(c for c in COLUNAS if c not in (*COLUNAS_INTEIRAS, "value"))

CHAVE = ["commodity_code", "country_code", "market_year", "attribute_id"]


def _falha(motivo: str, trecho: object = "") -> ParseError:
    return ParseError(
        source="usda", parser_version=PARSER_VERSION, reason=motivo, html_snippet=str(trecho)
    )


def _conferir_layout(records: Any) -> None:
    if not isinstance(records, list) or not all(isinstance(r, dict) for r in records):
        raise _falha("o corpo do PSD não é uma lista de registros", records)
    faltando = sorted({campo for r in records for campo in CAMPOS if campo not in r})
    if faltando:
        raise _falha(f"registro do PSD sem {faltando}: o layout do gateway mudou", records[0])


def _rotulos(codigos: pd.Series, catalogo: dict[str | int, str], campo: str) -> pd.Series:
    rotulos = codigos.map(catalogo)
    desconhecidos = sorted(
        {int(c) if isinstance(c, Integral) else c for c in codigos[rotulos.isna()]}
    )
    if desconhecidos:
        raise _falha(f"{campo} fora do catálogo oficial local do agrobr: {desconhecidos}")
    return rotulos


def _tipados(records: list[dict[str, Any]]) -> pd.DataFrame:
    bruto = pd.DataFrame(records)
    try:
        return pd.DataFrame(
            {
                "commodity_code": bruto["commodityCode"].astype(str),
                "country_code": bruto["countryCode"].astype(str),
                "market_year": bruto["marketYear"].astype("Int64"),
                "attribute_id": bruto["attributeId"].astype("Int64"),
                "unit_id": bruto["unitId"].astype("Int64"),
                "value": bruto["value"].astype(float),
                "last_update_year": bruto["calendarYear"].astype("Int64"),
                "last_update_month": bruto["month"].astype("Int64"),
            }
        )
    except (TypeError, ValueError) as exc:
        raise _falha(f"campo do PSD com tipo inesperado: {exc}", records[0]) from exc


def parse_psd_response(records: Any) -> pd.DataFrame:
    _conferir_layout(records)
    if not records:
        return pd.DataFrame(
            {
                coluna: pd.Series(
                    dtype="Int64"
                    if coluna in COLUNAS_INTEIRAS
                    else "float64"
                    if coluna == "value"
                    else pd.Series([""]).dtype
                )
                for coluna in COLUNAS
            }
        )

    df = _tipados(records)
    repetidos = df[df.duplicated(CHAVE, keep=False)]
    if not repetidos.empty:
        raise _falha(
            f"registro repetido no corpo do PSD: {repetidos[CHAVE].head(3).values.tolist()}"
        )

    _rotulos(df["commodity_code"], models.nomes_de_produto(), "commodityCode")
    df["commodity"] = df["commodity_code"].map(models.commodity_name)
    paises = {**models.nomes_de_pais(), models.MUNDO: models.NOME_MUNDO}
    df["country"] = _rotulos(df["country_code"], paises, "countryCode")
    df["attribute"] = _rotulos(df["attribute_id"], models.nomes_de_atributo(), "attributeId")
    df["unit"] = _rotulos(df["unit_id"], models.unidades(), "unitId")
    df["attribute_br"] = [
        models.attribute_br(c, a)
        for c, a in zip(df["commodity_code"], df["attribute_id"], strict=True)
    ]
    for coluna in COLUNAS_TEXTO:
        df[coluna] = df[coluna].astype(pd.Series([""]).dtype)
    df["last_update_month"] = df["last_update_month"].mask(df["last_update_month"] == 0)

    df = df[COLUNAS].sort_values(["market_year", "country_code", "attribute"], kind="stable")
    logger.info("usda_parse_ok", records=len(df))
    return df.reset_index(drop=True)


def filter_attributes(
    df: pd.DataFrame,
    attributes: list[str] | None = None,
) -> pd.DataFrame:
    if not attributes or df.empty:
        return df

    pedidos = {a.strip().lower() for a in attributes}
    mask = df["attribute"].str.lower().isin(pedidos)
    mask = mask | df["attribute_br"].fillna("").str.lower().isin(pedidos)
    return df[mask].reset_index(drop=True)


def pivot_attributes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    index_cols = ["commodity_code", "commodity", "country_code", "country", "market_year"]
    rotulado = df.assign(_rotulo=df["attribute_br"].fillna(df["attribute"]))
    ambiguos = rotulado[rotulado.duplicated([*index_cols, "_rotulo"], keep=False)]
    if not ambiguos.empty:
        raise _falha(
            "pivot ambíguo: dois atributos com o mesmo rótulo na mesma série: "
            f"{sorted({(r, int(a)) for r, a in zip(ambiguos['_rotulo'], ambiguos['attribute_id'], strict=True)})}"
        )

    result = rotulado.pivot(index=index_cols, columns="_rotulo", values="value").reset_index()
    result.columns.name = None
    return result
