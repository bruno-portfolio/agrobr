from __future__ import annotations

import io

import pandas as pd

from agrobr import _log
from agrobr.antaq.models import (
    COLUNAS_ATRACACAO,
    COLUNAS_CARGA,
    COLUNAS_MERCADORIA,
    PARSER_VERSION,
    RENAME_FINAL,
)
from agrobr.exceptions import ParseError
from agrobr.normalize import dates
from agrobr.normalize.dates import month_to_number

logger = _log.get_logger(__name__)

COLUNAS_TIPADAS = {
    "ano": "Int64",
    "mes": "Int64",
    "data_atracacao": "datetime64[ns]",
    "peso_bruto_ton": "float64",
    "qt_carga": "float64",
    "teu": "Int64",
}

_COLUNAS_EXIGIDAS = {
    "atracação": (
        "IDAtracacao",
        "Porto Atracação",
        "Complexo Portuário",
        "Terminal",
        "Município",
        "SGUF",
        "Região Geográfica",
        "Ano",
        "Mes",
        "Data Atracação",
    ),
    "carga": (
        "IDAtracacao",
        "CDMercadoria",
        "Tipo Navegação",
        "Natureza da Carga",
        "Sentido",
        "VLPesoCargaBruta",
    ),
    "mercadoria": ("CDMercadoria", "Nomenclatura Simplificada Mercadoria"),
}


def _read_txt(content: str, usecols: list[str] | None = None) -> pd.DataFrame:
    df = pd.read_csv(
        io.StringIO(content),
        sep=";",
        encoding="utf-8",
        dtype=str,
        usecols=usecols,
        low_memory=False,
    )
    df.columns = df.columns.str.strip()
    return df


def parse_atracacao(content: str) -> pd.DataFrame:
    available_cols = (
        pd.read_csv(io.StringIO(content), sep=";", nrows=0).columns.str.strip().tolist()
    )

    usecols = [c for c in COLUNAS_ATRACACAO if c in available_cols]

    df = _read_txt(content, usecols=usecols)
    logger.info("antaq_parse_atracacao", rows=len(df))
    return df


def parse_carga(content: str) -> pd.DataFrame:
    available_cols = (
        pd.read_csv(io.StringIO(content), sep=";", nrows=0).columns.str.strip().tolist()
    )

    usecols = [c for c in COLUNAS_CARGA if c in available_cols]

    df = _read_txt(content, usecols=usecols)

    if "VLPesoCargaBruta" in df.columns:
        df["VLPesoCargaBruta"] = (
            df["VLPesoCargaBruta"]
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )

    if "QTCarga" in df.columns:
        df["QTCarga"] = pd.to_numeric(
            df["QTCarga"].str.replace(",", ".", regex=False),
            errors="coerce",
        )

    if "TEU" in df.columns:
        df["TEU"] = pd.to_numeric(df["TEU"], errors="coerce").astype("Int64")

    logger.info("antaq_parse_carga", rows=len(df))
    return df


def parse_mercadoria(content: str) -> pd.DataFrame:
    available_cols = (
        pd.read_csv(io.StringIO(content), sep=";", nrows=0).columns.str.strip().tolist()
    )

    usecols = [c for c in COLUNAS_MERCADORIA if c in available_cols]

    df = _read_txt(content, usecols=usecols)
    logger.info("antaq_parse_mercadoria", rows=len(df))
    return df


def join_movimentacao(
    df_atracacao: pd.DataFrame,
    df_carga: pd.DataFrame,
    df_mercadoria: pd.DataFrame,
) -> pd.DataFrame:
    arquivos = (("atracação", df_atracacao), ("carga", df_carga), ("mercadoria", df_mercadoria))
    for arquivo, frame in arquivos:
        faltando = [c for c in _COLUNAS_EXIGIDAS[arquivo] if c not in frame.columns]
        if faltando:
            raise ParseError(
                source="antaq",
                parser_version=PARSER_VERSION,
                reason=f"TXT de {arquivo} sem as colunas {faltando}",
            )
    df = df_carga.merge(
        df_atracacao[list(_COLUNAS_EXIGIDAS["atracação"])],
        on="IDAtracacao",
        how="left",
    )

    if "CDMercadoria" in df.columns and "CDMercadoria" in df_mercadoria.columns:
        merc_cols = [
            c
            for c in ["CDMercadoria", "Grupo de Mercadoria", "Nomenclatura Simplificada Mercadoria"]
            if c in df_mercadoria.columns
        ]
        catalogo = df_mercadoria[
            [coluna for coluna in COLUNAS_MERCADORIA if coluna in df_mercadoria.columns]
        ].drop_duplicates()
        conflitos = catalogo.duplicated(subset=["CDMercadoria"], keep=False)
        if conflitos.any():
            codigos = catalogo.loc[conflitos, "CDMercadoria"].drop_duplicates().tolist()
            raise ParseError(
                source="antaq",
                parser_version=PARSER_VERSION,
                reason=f"Catálogo de mercadorias com nomes conflitantes para CDMercadoria {codigos}",
            )
        df = df.merge(
            catalogo[merc_cols],
            on="CDMercadoria",
            how="left",
        )

    rename = {k: v for k, v in RENAME_FINAL.items() if k in df.columns}
    df = df.rename(columns=rename)

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    if "mes" in df.columns:
        mes_numerico = pd.to_numeric(df["mes"], errors="coerce")
        mes_por_nome = df["mes"].map(month_to_number, na_action="ignore")
        df["mes"] = mes_numerico.fillna(mes_por_nome).astype("Int64")

    if "data_atracacao" in df.columns:
        bruto = df["data_atracacao"].astype(object)
        nao_texto = bruto.notna() & ~bruto.map(lambda valor: isinstance(valor, str))
        if nao_texto.any():
            raise ParseError(
                source="antaq",
                parser_version=PARSER_VERSION,
                reason="data_atracacao com valor que não é texto; a fonte publica DD/MM/AAAA HH:MM:SS",
                errors=[("data_atracacao", "DD/MM/AAAA HH:MM:SS", str(int(nao_texto.sum())))],
            )
        texto = pd.Series(bruto.array, index=df.index, dtype=pd.Series([""]).dtype).str.strip()
        presentes = texto.notna() & texto.ne("")
        formato = r"[0-9]{2}/[0-9]{2}/[0-9]{4}(?: [0-9]{2}:[0-9]{2}:[0-9]{2})?"
        invalidas = presentes & ~texto.str.fullmatch(formato, na=False)
        if invalidas.any():
            raise ParseError(
                source="antaq",
                parser_version=PARSER_VERSION,
                reason="data_atracacao contém texto fora do formato publicado DD/MM/AAAA HH:MM:SS",
                errors=[("data_atracacao", "DD/MM/AAAA HH:MM:SS", str(int(invalidas.sum())))],
            )
        df["data_atracacao"] = texto.mask(texto.str.len().eq(10), texto + " 00:00:00")
        dates.converter_coluna(df, "data_atracacao", fonte="antaq", formato="%d/%m/%Y %H:%M:%S")
    df = df.astype({coluna: dtype for coluna, dtype in COLUNAS_TIPADAS.items() if coluna in df})

    final_cols = [
        c
        for c in [
            "ano",
            "mes",
            "data_atracacao",
            "tipo_navegacao",
            "tipo_operacao",
            "natureza_carga",
            "sentido",
            "porto",
            "complexo_portuario",
            "terminal",
            "municipio",
            "uf",
            "regiao",
            "cd_mercadoria",
            "mercadoria",
            "grupo_mercadoria",
            "origem",
            "destino",
            "peso_bruto_ton",
            "qt_carga",
            "teu",
        ]
        if c in df.columns
    ]

    df = df[final_cols]

    df = df.sort_values([c for c in ["ano", "mes", "uf", "porto"] if c in df.columns]).reset_index(
        drop=True
    )

    logger.info(
        "antaq_join_ok",
        rows=len(df),
        columns=df.columns.tolist(),
        parser_version=PARSER_VERSION,
    )
    return df
