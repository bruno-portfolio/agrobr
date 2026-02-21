from __future__ import annotations

import io
from datetime import date

import chardet
import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import (
    ANO_INICIO_V2,
    CATEGORIA_MAP,
    COLUNAS_FLUXO,
    COLUNAS_V2,
    EIXOS_TIPO_MAP,
)

logger = structlog.get_logger()

PARSER_VERSION = 1


def _detect_encoding(content: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "windows-1252", "iso-8859-1"):
        try:
            content[:4096].decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue

    detected = chardet.detect(content[:8192])
    return detected.get("encoding") or "utf-8"


def _parse_numeric(v: object) -> float | None:
    if v is None or (isinstance(v, str) and v.strip() in ("", "-")):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        raw = str(v).strip().replace(" ", "")
        if "," in raw and "." in raw:
            raw = raw.replace(".", "").replace(",", ".")
        elif "," in raw:
            raw = raw.replace(",", ".")
        return float(raw)
    except (ValueError, TypeError):
        return None


def _parse_date_v1(val: str) -> date | None:
    val = val.strip()
    if not val:
        return None
    try:
        parts = val.split("/")
        if len(parts) == 3:
            return date(int(parts[2]), int(parts[1]), 1)
    except (ValueError, IndexError):
        pass
    return None


def _parse_date_v2(val: str) -> date | None:
    val = val.strip().strip('"')
    if not val:
        return None
    try:
        parts = val.split("/")
        if len(parts) == 3:
            return date(int(parts[2]), int(parts[1]), 1)
        if len(parts) == 2:
            return date(int(parts[1]), int(parts[0]), 1)
    except (ValueError, IndexError):
        pass
    return None


def _has_header(text: str) -> bool:
    first_line = text.split("\n", 1)[0].lower()
    return "concessionaria" in first_line or "praca" in first_line


def parse_trafego_v1(content: bytes) -> pd.DataFrame:
    encoding = _detect_encoding(content)
    try:
        text = content.decode(encoding)
    except (UnicodeDecodeError, LookupError) as e:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason=f"Erro de encoding ({encoding}): {e}",
        ) from e

    try:
        df = pd.read_csv(
            io.StringIO(text),
            sep=";",
            dtype=str,
            on_bad_lines="skip",
            low_memory=False,
        )
    except Exception as e:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler CSV V1: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason="CSV V1 vazio",
        )

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if "mes_ano" in df.columns:
        df["data"] = df["mes_ano"].apply(_parse_date_v1)
    elif "data" not in df.columns:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason=f"Coluna de data nao encontrada. Colunas: {list(df.columns)}",
        )

    if "categoria" in df.columns:
        df["_cat_clean"] = df["categoria"].str.strip()
        df["n_eixos"] = df["_cat_clean"].map({k: v[0] for k, v in CATEGORIA_MAP.items()})
        df["tipo_veiculo"] = df["_cat_clean"].map({k: v[1] for k, v in CATEGORIA_MAP.items()})
        df = df.drop(columns=["_cat_clean"])
    else:
        df["n_eixos"] = None
        df["tipo_veiculo"] = None

    vol_col = None
    for candidate in ("quantidade", "volume", "qtd"):
        if candidate in df.columns:
            vol_col = candidate
            break
    if vol_col:
        df["volume"] = df[vol_col].apply(_parse_numeric).fillna(0).astype(int)
    else:
        df["volume"] = 0

    for col in ("concessionaria", "praca", "sentido"):
        if col in df.columns:
            df[col] = df[col].str.strip()

    group_cols = ["data", "concessionaria", "praca", "sentido", "n_eixos", "tipo_veiculo"]
    present_group = [c for c in group_cols if c in df.columns]
    if present_group and "volume" in df.columns:
        df = df.groupby(present_group, dropna=False)["volume"].sum().reset_index()

    for col in ("concessionaria", "praca", "sentido"):
        if col not in df.columns:
            df[col] = None

    df = df.dropna(subset=["data"]).copy()

    logger.debug(
        "antt_pedagio_parse_v1_ok",
        records=len(df),
    )

    return df


def parse_trafego_v2(content: bytes) -> pd.DataFrame:
    encoding = _detect_encoding(content)
    try:
        text = content.decode(encoding)
    except (UnicodeDecodeError, LookupError) as e:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason=f"Erro de encoding ({encoding}): {e}",
        ) from e

    has_hdr = _has_header(text)

    try:
        if has_hdr:
            df = pd.read_csv(
                io.StringIO(text),
                sep=";",
                dtype=str,
                on_bad_lines="skip",
                low_memory=False,
            )
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        else:
            df = pd.read_csv(
                io.StringIO(text),
                sep=";",
                dtype=str,
                header=None,
                names=COLUNAS_V2,
                on_bad_lines="skip",
                low_memory=False,
            )
    except Exception as e:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler CSV V2: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason="CSV V2 vazio",
        )

    date_col = None
    for candidate in ("mes_ano", "data"):
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col:
        df["data"] = df[date_col].apply(_parse_date_v2)
    else:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason=f"Coluna de data nao encontrada. Colunas: {list(df.columns)}",
        )

    eixo_col = None
    for candidate in ("categoria_eixo", "eixo", "n_eixos"):
        if candidate in df.columns:
            eixo_col = candidate
            break
    if eixo_col:
        df["n_eixos"] = pd.to_numeric(df[eixo_col], errors="coerce").astype("Int64")

    tipo_col = None
    for candidate in ("tipo_de_veiculo", "tipo_veiculo"):
        if candidate in df.columns:
            tipo_col = candidate
            break
    if tipo_col:
        df["tipo_veiculo"] = df[tipo_col].str.strip()
    elif eixo_col:
        df["tipo_veiculo"] = df["n_eixos"].map(EIXOS_TIPO_MAP)
    else:
        df["n_eixos"] = None
        df["tipo_veiculo"] = None

    vol_col = None
    for candidate in ("volume_total", "quantidade", "volume", "qtd"):
        if candidate in df.columns:
            vol_col = candidate
            break
    if vol_col:
        df["volume"] = df[vol_col].apply(_parse_numeric).fillna(0).astype(int)
    else:
        df["volume"] = 0

    for col in ("concessionaria", "praca", "sentido"):
        if col in df.columns:
            df[col] = df[col].str.strip()

    group_cols = ["data", "concessionaria", "praca", "sentido", "n_eixos", "tipo_veiculo"]
    present_group = [c for c in group_cols if c in df.columns]
    if present_group and "volume" in df.columns:
        df = df.groupby(present_group, dropna=False)["volume"].sum().reset_index()

    for col in ("concessionaria", "praca", "sentido"):
        if col not in df.columns:
            df[col] = None

    df = df.dropna(subset=["data"]).copy()

    logger.debug(
        "antt_pedagio_parse_v2_ok",
        records=len(df),
    )

    return df


def parse_trafego(content: bytes, ano: int) -> pd.DataFrame:
    if ano >= ANO_INICIO_V2:
        return parse_trafego_v2(content)
    return parse_trafego_v1(content)


def parse_pracas(content: bytes) -> pd.DataFrame:
    encoding = _detect_encoding(content)
    try:
        text = content.decode(encoding)
    except (UnicodeDecodeError, LookupError) as e:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason=f"Erro de encoding pracas ({encoding}): {e}",
        ) from e

    for sep in (";", ","):
        try:
            df = pd.read_csv(
                io.StringIO(text),
                sep=sep,
                dtype=str,
                on_bad_lines="skip",
                low_memory=False,
            )
            if len(df.columns) > 2:
                break
        except Exception:
            continue
    else:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason="Erro ao ler CSV de pracas",
        )

    if df.empty:
        raise ParseError(
            source="antt_pedagio",
            parser_version=PARSER_VERSION,
            reason="CSV de pracas vazio",
        )

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    for col in ("concessionaria", "praca_de_pedagio", "rodovia", "uf", "municipio", "situacao"):
        if col in df.columns:
            df[col] = df[col].str.strip()

    if "uf" in df.columns:
        df["uf"] = df["uf"].str.upper()

    lat_lon_remap = {"latitude": "lat", "longitude": "lon"}
    df = df.rename(columns={k: v for k, v in lat_lon_remap.items() if k in df.columns})

    for col in ("lat", "lon"):
        if col in df.columns:
            df[col] = df[col].apply(_parse_numeric)

    logger.debug(
        "antt_pedagio_parse_pracas_ok",
        records=len(df),
        ufs=df["uf"].nunique() if "uf" in df.columns else 0,
    )

    return df


def join_fluxo_pracas(
    df_fluxo: pd.DataFrame,
    df_pracas: pd.DataFrame,
) -> pd.DataFrame:
    if df_pracas.empty or df_fluxo.empty:
        for col in ("rodovia", "uf", "municipio"):
            if col not in df_fluxo.columns:
                df_fluxo[col] = None
        return df_fluxo

    pracas = df_pracas.copy()

    if "concessionaria" in pracas.columns:
        pracas["_join_conc"] = pracas["concessionaria"].str.strip().str.upper()
    else:
        pracas["_join_conc"] = ""

    praca_col = "praca_de_pedagio" if "praca_de_pedagio" in pracas.columns else "praca"
    if praca_col in pracas.columns:
        pracas["_join_praca"] = pracas[praca_col].str.strip().str.upper()
    else:
        pracas["_join_praca"] = ""

    join_cols = ["_join_conc", "_join_praca"]
    enrich_cols = ["rodovia", "uf", "municipio"]
    available = [c for c in enrich_cols if c in pracas.columns]
    pracas_slim = pracas[join_cols + available].drop_duplicates(subset=join_cols)

    fluxo = df_fluxo.copy()
    if "concessionaria" in fluxo.columns:
        fluxo["_join_conc"] = fluxo["concessionaria"].str.strip().str.upper()
    else:
        fluxo["_join_conc"] = ""
    if "praca" in fluxo.columns:
        fluxo["_join_praca"] = fluxo["praca"].str.strip().str.upper()
    else:
        fluxo["_join_praca"] = ""

    merged = fluxo.merge(pracas_slim, on=join_cols, how="left", suffixes=("", "_pracas"))

    for col in enrich_cols:
        pracas_col = f"{col}_pracas"
        if pracas_col in merged.columns:
            if col in merged.columns:
                merged[col] = merged[col].fillna(merged[pracas_col])
            else:
                merged[col] = merged[pracas_col]
            merged = merged.drop(columns=[pracas_col])
        elif col not in merged.columns:
            merged[col] = None

    merged = merged.drop(columns=["_join_conc", "_join_praca"], errors="ignore")

    final_cols = [c for c in COLUNAS_FLUXO if c in merged.columns]
    merged = merged[final_cols].copy()

    logger.debug(
        "antt_pedagio_join_ok",
        records=len(merged),
        matched_pct=round((1 - merged["rodovia"].isna().mean()) * 100, 1)
        if "rodovia" in merged.columns
        else 0,
    )

    return merged
