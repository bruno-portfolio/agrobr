from __future__ import annotations

import io

import chardet
import pandas as pd
import structlog

from agrobr.exceptions import ParseError

logger = structlog.get_logger()

PARSER_VERSION = 1


def _detect_encoding(content: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "iso-8859-1", "windows-1252"):
        try:
            content[:4096].decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue

    detected = chardet.detect(content[:8192])
    return detected.get("encoding") or "utf-8"


def _detect_separator(text: str) -> str:
    first_line = text.split("\n", 1)[0]
    if first_line.count(";") > first_line.count(","):
        return ";"
    return ","


def _normalize_column_name(col: str) -> str:
    return col.strip()


def parse_apolices(
    content: bytes,
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    municipio: str | None = None,
) -> pd.DataFrame:
    from agrobr.alt.mapa_psr.models import (
        COLUNAS_CSV,
        COLUNAS_DROP,
        COLUNAS_FLOAT,
    )

    encoding = _detect_encoding(content)
    try:
        text = content.decode(encoding)
    except (UnicodeDecodeError, LookupError) as e:
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason=f"Erro de encoding ({encoding}): {e}",
        ) from e

    sep = _detect_separator(text)

    try:
        df = pd.read_csv(
            io.StringIO(text),
            sep=sep,
            dtype=str,
            on_bad_lines="skip",
            low_memory=False,
        )
    except Exception as e:
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler CSV: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason="CSV vazio",
        )

    df.columns = [_normalize_column_name(c) for c in df.columns]

    upper_cols = {c.upper(): c for c in df.columns}
    drop_cols = [upper_cols[d.upper()] for d in COLUNAS_DROP if d.upper() in upper_cols]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    rename_map: dict[str, str] = {}
    for csv_col, df_col in COLUNAS_CSV.items():
        if csv_col in df.columns:
            rename_map[csv_col] = df_col
        else:
            upper = csv_col.upper()
            if upper in upper_cols and upper_cols[upper] in df.columns:
                rename_map[upper_cols[upper]] = df_col

    df = df.rename(columns=rename_map)

    present_cols = [c for c in COLUNAS_CSV.values() if c in df.columns]
    if not present_cols:
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason=f"Nenhuma coluna esperada encontrada. Colunas: {list(df.columns)}",
        )

    missing_critical = {"ano_apolice", "uf", "cultura"} - set(df.columns)
    if missing_critical:
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason=f"Colunas criticas faltando: {missing_critical}",
        )

    if "ano_apolice" in df.columns:
        df["ano_apolice"] = pd.to_numeric(df["ano_apolice"], errors="coerce")
        df = df.dropna(subset=["ano_apolice"]).copy()
        df["ano_apolice"] = df["ano_apolice"].astype(int)

    for col in COLUNAS_FLOAT:
        if col in df.columns:
            df[col] = df[col].apply(_parse_numeric)

    if "uf" in df.columns:
        df["uf"] = df["uf"].str.strip().str.upper()

    if "municipio" in df.columns:
        df["municipio"] = df["municipio"].str.strip().str.upper()

    if "cultura" in df.columns:
        df["cultura"] = df["cultura"].str.strip().str.upper()

    if "classificacao" in df.columns:
        df["classificacao"] = df["classificacao"].str.strip().str.upper()

    if "evento" in df.columns:
        df["evento"] = df["evento"].fillna("").str.strip().str.lower()

    if "seguradora" in df.columns:
        df["seguradora"] = df["seguradora"].str.strip()

    if "cd_ibge" in df.columns:
        df["cd_ibge"] = df["cd_ibge"].fillna("").str.strip()

    if "nr_apolice" in df.columns:
        df["nr_apolice"] = df["nr_apolice"].fillna("").str.strip()

    if uf:
        df = df[df["uf"] == uf.upper()].copy()

    if cultura:
        mask = df["cultura"].str.contains(cultura.upper(), na=False)
        df = df[mask].copy()

    if ano and "ano_apolice" in df.columns:
        df = df[df["ano_apolice"] == ano].copy()

    if municipio and "municipio" in df.columns:
        mask = df["municipio"].str.contains(municipio.upper(), na=False)
        df = df[mask].copy()

    from agrobr.alt.mapa_psr.models import COLUNAS_APOLICES

    final_cols = [c for c in COLUNAS_APOLICES if c in df.columns]
    df = df[final_cols].copy()

    df = df.sort_values("ano_apolice").reset_index(drop=True)

    logger.debug(
        "mapa_psr_parse_apolices_ok",
        records=len(df),
        culturas=df["cultura"].nunique() if "cultura" in df.columns else 0,
        ufs=df["uf"].nunique() if "uf" in df.columns else 0,
    )

    return df


def parse_sinistros(
    content: bytes,
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    municipio: str | None = None,
    evento: str | None = None,
) -> pd.DataFrame:
    df = parse_apolices(content, cultura=cultura, uf=uf, ano=ano, municipio=municipio)

    if "valor_indenizacao" in df.columns:
        mask_indenizacao = df["valor_indenizacao"].fillna(0) > 0
        df = df[mask_indenizacao].copy()

    if "evento" in df.columns:
        mask_evento = df["evento"].fillna("").str.strip().ne("")
        df = df[mask_evento].copy()

    if evento and "evento" in df.columns:
        mask = df["evento"].str.contains(evento.lower(), na=False)
        df = df[mask].copy()

    from agrobr.alt.mapa_psr.models import COLUNAS_SINISTROS

    final_cols = [c for c in COLUNAS_SINISTROS if c in df.columns]
    df = df[final_cols].copy()

    df = df.sort_values("ano_apolice").reset_index(drop=True)

    logger.debug(
        "mapa_psr_parse_sinistros_ok",
        records=len(df),
        eventos=df["evento"].nunique() if "evento" in df.columns else 0,
    )

    return df


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
