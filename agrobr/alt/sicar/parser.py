"""Parser de CSV do WFS SICAR."""

from __future__ import annotations

import io

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import COLUNAS_IMOVEIS, RENAME_MAP

logger = structlog.get_logger()

PARSER_VERSION = 1


def parse_imoveis_csv(pages: list[bytes]) -> pd.DataFrame:
    """Parseia paginas CSV do WFS e retorna DataFrame normalizado.

    Args:
        pages: Lista de bytes (CSV) retornados pelo WFS paginado.

    Returns:
        DataFrame com colunas padrao COLUNAS_IMOVEIS.

    Raises:
        ParseError: Se CSV invalido ou colunas obrigatorias ausentes.
    """
    if not pages:
        return pd.DataFrame(columns=COLUNAS_IMOVEIS)

    dfs: list[pd.DataFrame] = []
    for i, data in enumerate(pages):
        try:
            df = pd.read_csv(io.BytesIO(data), encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(data), encoding="latin-1")
        except Exception as e:
            raise ParseError(
                source="sicar",
                parser_version=PARSER_VERSION,
                reason=f"Erro ao ler CSV pagina {i}: {e}",
            ) from e

        if not df.empty:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame(columns=COLUNAS_IMOVEIS)

    df = pd.concat(dfs, ignore_index=True)

    # Validar colunas obrigatorias
    required = {"cod_imovel", "status_imovel", "dat_criacao", "area", "uf"}
    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            source="sicar",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    # Rename
    df = df.rename(columns=RENAME_MAP)

    # Parse datas
    df["data_criacao"] = pd.to_datetime(df["data_criacao"], errors="coerce")
    df["data_atualizacao"] = pd.to_datetime(
        df.get("data_atualizacao", pd.Series(dtype=str)), errors="coerce"
    )

    # Cast numericos
    df["area_ha"] = pd.to_numeric(df["area_ha"], errors="coerce")
    df["cod_municipio_ibge"] = pd.to_numeric(
        df.get("cod_municipio_ibge", pd.Series(dtype=str)), errors="coerce"
    ).astype("Int64")
    df["modulos_fiscais"] = pd.to_numeric(
        df.get("modulos_fiscais", pd.Series(dtype=str)), errors="coerce"
    )

    # Normalizar strings
    df["uf"] = df["uf"].fillna("").str.strip().str.upper()
    df["status"] = df["status"].fillna("").str.strip().str.upper()
    df["tipo"] = df.get("tipo", pd.Series(dtype=str)).fillna("").str.strip().str.upper()
    df["municipio"] = df.get("municipio", pd.Series(dtype=str)).fillna("").str.strip()
    df["condicao"] = df.get("condicao", pd.Series(dtype=str)).fillna("").str.strip()
    df["cod_imovel"] = df["cod_imovel"].fillna("").astype(str).str.strip()

    # Selecionar e ordenar colunas de saida
    output_cols = [c for c in COLUNAS_IMOVEIS if c in df.columns]
    df = df[output_cols].copy()
    df = df.reset_index(drop=True)

    logger.info("sicar_parse_ok", records=len(df))
    return df


def agregar_resumo(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega DataFrame de imoveis em resumo estatistico.

    Args:
        df: DataFrame com colunas padrao de imoveis.

    Returns:
        DataFrame com uma linha contendo estatisticas agregadas.
    """
    total = len(df)

    if total == 0:
        return pd.DataFrame(
            [
                {
                    "total": 0,
                    "ativos": 0,
                    "pendentes": 0,
                    "suspensos": 0,
                    "cancelados": 0,
                    "area_total_ha": 0.0,
                    "area_media_ha": 0.0,
                    "modulos_fiscais_medio": 0.0,
                    "por_tipo_IRU": 0,
                    "por_tipo_AST": 0,
                    "por_tipo_PCT": 0,
                }
            ]
        )

    status_counts = df["status"].value_counts()
    tipo_counts = df["tipo"].value_counts() if "tipo" in df.columns else pd.Series(dtype=int)

    resumo = {
        "total": total,
        "ativos": int(status_counts.get("AT", 0)),
        "pendentes": int(status_counts.get("PE", 0)),
        "suspensos": int(status_counts.get("SU", 0)),
        "cancelados": int(status_counts.get("CA", 0)),
        "area_total_ha": float(df["area_ha"].sum()) if "area_ha" in df.columns else 0.0,
        "area_media_ha": float(df["area_ha"].mean()) if "area_ha" in df.columns else 0.0,
        "modulos_fiscais_medio": (
            float(df["modulos_fiscais"].mean()) if "modulos_fiscais" in df.columns else 0.0
        ),
        "por_tipo_IRU": int(tipo_counts.get("IRU", 0)),
        "por_tipo_AST": int(tipo_counts.get("AST", 0)),
        "por_tipo_PCT": int(tipo_counts.get("PCT", 0)),
    }

    return pd.DataFrame([resumo])
