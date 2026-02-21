from __future__ import annotations

from io import StringIO

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

logger = structlog.get_logger()

PARSER_VERSION = 1

COLUNAS_MAP: dict[str, str] = {
    "CO_ANO": "ano",
    "CO_MES": "mes",
    "CO_NCM": "ncm",
    "CO_UNID": "cod_unidade",
    "CO_PAIS": "cod_pais",
    "SG_UF_NCM": "uf",
    "CO_VIA": "cod_via",
    "CO_URF": "cod_porto",
    "QT_ESTAT": "qtd_estatistica",
    "KG_LIQUIDO": "kg_liquido",
    "VL_FOB": "valor_fob_usd",
}


def _detect_separator(csv_text: str) -> str:
    first_line = csv_text.split("\n")[0]
    if ";" in first_line:
        return ";"
    return ","


def parse_exportacao(
    csv_text: str,
    ncm: str | None = None,
    uf: str | None = None,
) -> pd.DataFrame:
    if not csv_text or len(csv_text.strip()) < 10:
        raise ParseError(
            source="comexstat",
            parser_version=PARSER_VERSION,
            reason="CSV de exportação vazio",
        )

    sep = _detect_separator(csv_text)

    try:
        df = pd.read_csv(
            StringIO(csv_text),
            sep=sep,
            dtype={"CO_NCM": str, "SG_UF_NCM": str},
            low_memory=False,
        )
    except Exception as e:
        raise ParseError(
            source="comexstat",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler CSV: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="comexstat",
            parser_version=PARSER_VERSION,
            reason="CSV parseado mas sem registros",
        )

    rename = {k: v for k, v in COLUNAS_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    if "ncm" in df.columns:
        df["ncm"] = df["ncm"].astype(str).str.zfill(8)

    if ncm and "ncm" in df.columns:
        df = df[df["ncm"].str.startswith(ncm)]

    if uf and "uf" in df.columns:
        df = df[df["uf"] == uf.upper()]

    for col in ("kg_liquido", "valor_fob_usd", "qtd_estatistica"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ("ano", "mes"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "uf" in df.columns:
        df["uf"] = df["uf"].str.upper().str.strip()

    sort_cols = [c for c in ("ano", "mes", "ncm", "uf") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    logger.info(
        "comexstat_parsed",
        records=len(df),
        ncm_filter=ncm,
        uf_filter=uf,
    )

    return df


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = [c for c in ("ano", "mes", "ncm", "uf") if c in df.columns]
    if not group_cols:
        return df

    agg_dict: dict[str, str] = {}
    if "kg_liquido" in df.columns:
        agg_dict["kg_liquido"] = "sum"
    if "valor_fob_usd" in df.columns:
        agg_dict["valor_fob_usd"] = "sum"

    if not agg_dict:
        return df

    result = df.groupby(group_cols, as_index=False).agg(agg_dict)

    if "kg_liquido" in result.columns:
        result["volume_ton"] = result["kg_liquido"] / 1000.0

    return result.sort_values(group_cols).reset_index(drop=True)
