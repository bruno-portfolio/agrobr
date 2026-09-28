from __future__ import annotations

import pandas as pd

from agrobr import constants
from agrobr.exceptions import ParseError


def add_unit_columns(df: pd.DataFrame, produto: str) -> pd.DataFrame:
    result = df.copy()
    years = pd.to_numeric(result["ano"], errors="coerce")
    result["unidade_producao"] = "ton"
    result["unidade_rendimento"] = "kg/ha"
    if produto == "laranja":
        result.loc[years < 2001, "unidade_producao"] = "mil_frutos"
        result.loc[years < 2001, "unidade_rendimento"] = "frutos/ha"
    result["unidade_valor_producao"] = "mil_reais"
    result.loc[years < 1994, "unidade_valor_producao"] = "mil_cruzeiros"
    result.loc[years.between(1986, 1988), "unidade_valor_producao"] = "mil_cruzados"
    result.loc[years == 1989, "unidade_valor_producao"] = "mil_cruzados_novos"
    result.loc[years == 1993, "unidade_valor_producao"] = "mil_cruzeiros_reais"
    result["condicao_produto"] = pd.Series(pd.NA, index=result.index, dtype=object)
    if produto == "cafe":
        result["condicao_produto"] = "beneficiado"
        result.loc[years < 2002, "condicao_produto"] = "em_coco"
    return result


def pivot_observations(frame: pd.DataFrame) -> pd.DataFrame:
    dimensions = [c for c in ("localidade", "localidade_cod") if c in frame.columns] + ["ano"]
    fields = frame["variavel"].map(constants.IBGE_PAM_VARIABLE_LABELS)
    if fields.isna().any():
        unknown = frame.loc[fields.isna(), "variavel"].unique().tolist()
        raise ParseError(
            source="ibge_pam",
            parser_version=constants.IBGE_PAM_PARSER_VERSION,
            reason=f"Variáveis PAM sem mapeamento: {unknown}",
        )
    observations = frame.assign(campo=fields)
    if observations.duplicated([*dimensions, "campo"]).any():
        raise ParseError(
            source="ibge_pam",
            parser_version=constants.IBGE_PAM_PARSER_VERSION,
            reason="Observações PAM duplicadas ou ambíguas para a mesma localidade, ano e medida",
        )
    result = observations.pivot(index=dimensions, columns="campo", values="valor")
    result = result.astype("float64").reset_index()
    result.columns.name = "variavel"
    return result
