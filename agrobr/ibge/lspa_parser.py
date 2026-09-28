from __future__ import annotations

import pandas as pd
from pydantic import BaseModel, ValidationError

from agrobr.exceptions import ParseError

PARSER_VERSION = 2

_RENAME = {
    "D1C": "localidade_cod",
    "D1N": "localidade",
    "D2C": "periodo",
    "D3N": "produto_raw",
    "D4C": "variavel_cod",
    "D4N": "variavel",
    "MN": "unidade",
    "V": "valor",
}
OUTPUT_COLUMNS = [
    "ano",
    "mes",
    "localidade",
    "localidade_cod",
    "produto",
    "variavel",
    "variavel_cod",
    "valor",
    "unidade",
    "fonte",
]


class LspaObservation(BaseModel):
    localidade_cod: int
    localidade: str
    periodo: str
    variavel_cod: int
    variavel: str
    unidade: str


def _validate_dimensions(frame: pd.DataFrame) -> None:
    columns = list(LspaObservation.model_fields)
    try:
        for values in frame[columns].to_dict("records"):
            LspaObservation.model_validate(values)
    except ValidationError as exc:
        raise ParseError(
            source="ibge_lspa",
            parser_version=PARSER_VERSION,
            reason=f"Dimensões da tabela 6588 inválidas: {exc}",
        ) from exc


def parse_lspa(df: pd.DataFrame, produto: str) -> pd.DataFrame:
    if df.empty:
        result = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        required = set(_RENAME) - {"D3N"}
        missing = required - set(df.columns)
        if missing:
            raise ParseError(
                source="ibge_lspa",
                parser_version=PARSER_VERSION,
                reason=f"Dimensões da tabela 6588 ausentes: {sorted(missing)}",
            )
        result = df.rename(columns=_RENAME).copy()
        _validate_dimensions(result)
        period = result["periodo"].astype(str)
        valid = period.str.fullmatch(r"\d{4}(0[1-9]|1[0-2])")
        if not valid.all():
            raise ParseError(
                source="ibge_lspa",
                parser_version=PARSER_VERSION,
                reason="Período LSPA inválido: esperado YYYYMM em D2C",
            )
        result["ano"] = period.str[:4]
        result["mes"] = period.str[4:]
        result["produto"] = produto
        result["fonte"] = "ibge_lspa"
        result["valor"] = pd.to_numeric(result["valor"].replace("-", "0"), errors="coerce")
        result = result[OUTPUT_COLUMNS]
    for column in ["ano", "mes", "localidade_cod", "variavel_cod"]:
        result[column] = pd.to_numeric(result[column], errors="coerce").astype("Int64")
    result["valor"] = pd.to_numeric(result["valor"], errors="coerce").astype("float64")
    return result.reset_index(drop=True)
