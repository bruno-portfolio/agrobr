from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

logger = structlog.get_logger()

PARSER_VERSION = 2

COLUNAS_MAP: dict[str, str] = {
    "Safra": "safra",
    "AnoEmissao": "ano_emissao",
    "MesEmissao": "mes_emissao",
    "cdUF": "cd_uf",
    "UF": "uf",
    "cdMunicipio": "cd_municipio",
    "Municipio": "municipio",
    "Produto": "produto",
    "Finalidade": "finalidade",
    "Fonte": "fonte_recurso",
    "Programa": "programa",
    "Valor": "valor",
    "AreaFinanciada": "area_financiada",
    "QtdContratos": "qtd_contratos",
    "VlrMedio": "valor_medio",
    "nomeUF": "uf",
    "nomeRegiao": "regiao",
    "nomeProduto": "produto",
    "cdEstado": "cd_uf",
    "VlCusteio": "valor",
    "AreaCusteio": "area_financiada",
    "QtdCusteio": "qtd_contratos",
    "VlInvestimento": "valor",
    "AreaInvestimento": "area_financiada",
    "QtdInvestimento": "qtd_contratos",
    "codIbge": "cd_municipio",
    "cdPrograma": "cd_programa",
    "cdSubPrograma": "cd_sub_programa",
    "cdFonteRecurso": "cd_fonte_recurso",
    "cdTipoSeguro": "cd_tipo_seguro",
    "cdModalidade": "cd_modalidade",
    "Atividade": "cd_atividade",
}

ENRIQUECIMENTO_MAP: dict[str, tuple[str, str]] = {
    "cd_programa": ("programa", "programa"),
    "cd_fonte_recurso": ("fonte_recurso", "fonte_recurso"),
    "cd_tipo_seguro": ("tipo_seguro", "tipo_seguro"),
    "cd_modalidade": ("modalidade", "modalidade"),
    "cd_atividade": ("atividade", "atividade"),
}


def _enriquecer_dimensoes(df: pd.DataFrame) -> pd.DataFrame:
    from agrobr.bcb import models

    resolve_fns = {
        "programa": models.resolve_programa,
        "fonte_recurso": models.resolve_fonte_recurso,
        "tipo_seguro": models.resolve_tipo_seguro,
        "modalidade": models.resolve_modalidade,
        "atividade": models.resolve_atividade,
    }

    for cd_col, (nome_col, dominio) in ENRIQUECIMENTO_MAP.items():
        if cd_col in df.columns:
            fn = resolve_fns[dominio]
            df[nome_col] = df[cd_col].astype(str).map(fn)

    return df


def parse_credito_rural(
    dados: list[dict[str, Any]],
    finalidade: str = "custeio",
) -> pd.DataFrame:
    if not dados:
        raise ParseError(
            source="bcb",
            parser_version=PARSER_VERSION,
            reason="Resposta SICOR vazia",
        )

    df = pd.DataFrame(dados)

    rename = {k: v for k, v in COLUNAS_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    for col in ("valor", "area_financiada", "valor_medio"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ("ano_emissao", "mes_emissao", "qtd_contratos"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "produto" in df.columns:
        df["produto"] = df["produto"].str.strip().str.strip('"').str.lower().str.strip()

    if "uf" in df.columns:
        df["uf"] = df["uf"].str.upper().str.strip()

    if "municipio" in df.columns:
        df["municipio"] = df["municipio"].str.strip()

    if "finalidade" not in df.columns:
        df["finalidade"] = finalidade

    df = _enriquecer_dimensoes(df)

    sort_cols = [c for c in ("safra", "uf", "municipio", "produto") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    logger.info(
        "bcb_parsed",
        records=len(df),
        columns=df.columns.tolist(),
    )

    return df


def agregar_por_uf(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = [c for c in ("safra", "uf", "produto") if c in df.columns]
    if not group_cols:
        return df

    agg_dict: dict[str, str | tuple[str, str]] = {}
    if "valor" in df.columns:
        agg_dict["valor"] = "sum"
    if "area_financiada" in df.columns:
        agg_dict["area_financiada"] = "sum"
    if "qtd_contratos" in df.columns:
        agg_dict["qtd_contratos"] = "sum"

    if not agg_dict:
        return df

    result = df.groupby(group_cols, as_index=False).agg(agg_dict)

    return result.sort_values(group_cols).reset_index(drop=True)


def agregar_por_programa(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = [
        c
        for c in ("safra", "ano_emissao", "uf", "produto", "programa", "cd_programa")
        if c in df.columns
    ]
    if not group_cols:
        return df

    agg_dict: dict[str, str] = {}
    if "valor" in df.columns:
        agg_dict["valor"] = "sum"
    if "area_financiada" in df.columns:
        agg_dict["area_financiada"] = "sum"
    if "qtd_contratos" in df.columns:
        agg_dict["qtd_contratos"] = "sum"

    if not agg_dict:
        return df

    result = df.groupby(group_cols, as_index=False).agg(agg_dict)

    return result.sort_values(group_cols).reset_index(drop=True)
