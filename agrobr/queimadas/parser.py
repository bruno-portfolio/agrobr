from __future__ import annotations

import io

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import COLUNAS_SAIDA, estado_para_uf, normalizar_bioma

logger = structlog.get_logger()

PARSER_VERSION = 1


def parse_focos_csv(data: bytes) -> pd.DataFrame:
    """Parseia CSV de focos de calor do INPE.

    Args:
        data: Bytes do CSV.

    Returns:
        DataFrame com colunas padronizadas.

    Raises:
        ParseError: Se parsing falhar.
    """
    dtype_map: dict[str, str | type[str] | type[float]] = {
        "id": str,
        "lat": float,
        "lon": float,
        "satelite": str,
        "municipio": str,
        "estado": str,
        "pais": str,
        "municipio_id": "Int64",
        "estado_id": "Int64",
        "pais_id": "Int64",
        "bioma": str,
    }
    try:
        df = pd.read_csv(io.BytesIO(data), encoding="utf-8", dtype=dtype_map)  # type: ignore[arg-type]
    except UnicodeDecodeError:
        df = pd.read_csv(io.BytesIO(data), encoding="latin-1", dtype=dtype_map)  # type: ignore[arg-type]
    except Exception as e:
        raise ParseError(
            source="queimadas",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler CSV: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="queimadas",
            parser_version=PARSER_VERSION,
            reason="CSV vazio",
        )

    required = {"lat", "lon", "data_hora_gmt", "satelite"}
    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            source="queimadas",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    df["data_hora_gmt"] = pd.to_datetime(df["data_hora_gmt"], errors="coerce")
    df["data"] = df["data_hora_gmt"].dt.date
    df["hora_gmt"] = df["data_hora_gmt"].dt.strftime("%H:%M")

    if "estado" in df.columns:
        df["uf"] = df["estado"].fillna("").apply(estado_para_uf)
    else:
        df["uf"] = ""

    if "bioma" in df.columns:
        df["bioma"] = df["bioma"].fillna("").apply(normalizar_bioma)

    for col in ["numero_dias_sem_chuva", "precipitacao", "risco_fogo", "frp"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    output_cols = [c for c in COLUNAS_SAIDA if c in df.columns]
    if "uf" in df.columns:
        output_cols = [*output_cols, "uf"]

    df = df[output_cols].copy()
    df = df.reset_index(drop=True)

    logger.info("queimadas_parse_ok", records=len(df), columns=list(df.columns))
    return df
