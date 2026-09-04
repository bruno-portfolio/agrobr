from __future__ import annotations

import io

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.normalize.encoding import detect_encoding_chain
from agrobr.normalize.numeric import parse_numeric_br
from agrobr.normalize.regions import remover_acentos

logger = structlog.get_logger()

PARSER_VERSION = 1


def _detect_separator(text: str) -> str:
    first_line = text.split("\n", 1)[0]
    if first_line.count(";") > first_line.count(","):
        return ";"
    return ","


def _normalize_column_name(col: str) -> str:
    return col.strip()


def _read_apolices_csv(content: bytes) -> pd.DataFrame:
    encoding = detect_encoding_chain(content)
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

    return df


def _drop_ignored_columns(df: pd.DataFrame) -> pd.DataFrame:
    from agrobr.alt.mapa_psr.models import COLUNAS_DROP

    df.columns = [_normalize_column_name(c) for c in df.columns]
    upper_cols = {c.upper(): c for c in df.columns}
    drop_cols = [upper_cols[d.upper()] for d in COLUNAS_DROP if d.upper() in upper_cols]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df


def _build_rename_map(df: pd.DataFrame) -> dict[str, str]:
    from agrobr.alt.mapa_psr.models import COLUNAS_CSV

    upper_cols = {c.upper(): c for c in df.columns}
    rename_map: dict[str, str] = {}
    for csv_col, df_col in COLUNAS_CSV.items():
        if csv_col in df.columns:
            rename_map[csv_col] = df_col
        else:
            upper = csv_col.upper()
            if upper in upper_cols and upper_cols[upper] in df.columns:
                rename_map[upper_cols[upper]] = df_col
    return rename_map


def _normalize_apolices_columns(df: pd.DataFrame) -> pd.DataFrame:
    from agrobr.alt.mapa_psr.models import COLUNAS_CSV

    df = _drop_ignored_columns(df)
    df = df.rename(columns=_build_rename_map(df))

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

    return df


def _normalize_apolices_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("uf", "municipio", "cultura", "classificacao"):
        if col in df.columns:
            df[col] = df[col].str.strip().str.upper()

    for col in ("cd_ibge", "nr_apolice"):
        if col in df.columns:
            df[col] = df[col].fillna("").str.strip()

    if "evento" in df.columns:
        df["evento"] = df["evento"].fillna("").str.strip().str.lower()

    if "seguradora" in df.columns:
        df["seguradora"] = df["seguradora"].str.strip()

    return df


def _convert_apolices_types(df: pd.DataFrame) -> pd.DataFrame:
    from agrobr.alt.mapa_psr.models import COLUNAS_FLOAT

    if "ano_apolice" in df.columns:
        df["ano_apolice"] = pd.to_numeric(df["ano_apolice"], errors="coerce")
        df = df.dropna(subset=["ano_apolice"]).copy()
        df["ano_apolice"] = df["ano_apolice"].astype(int)

    for col in COLUNAS_FLOAT:
        if col in df.columns:
            df[col] = df[col].apply(parse_numeric_br)

    return _normalize_apolices_strings(df)


def _filter_apolices(
    df: pd.DataFrame,
    cultura: str | None,
    uf: str | None,
    ano: int | None,
    municipio: str | None,
) -> pd.DataFrame:
    if uf:
        df = df[df["uf"] == uf.upper()]

    if cultura:
        cultura_norm = remover_acentos(cultura.upper())
        mask = df["cultura"].apply(
            lambda x, cn=cultura_norm: (
                cn in remover_acentos(str(x).upper()) if pd.notna(x) else False
            )
        )
        df = df[mask]

    if ano and "ano_apolice" in df.columns:
        df = df[df["ano_apolice"] == ano]

    if municipio and "municipio" in df.columns:
        mask = df["municipio"].str.contains(municipio.upper(), na=False, regex=False)
        df = df[mask]

    return df


def _parse_apolices(
    content: bytes,
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    municipio: str | None = None,
    *,
    ensure_valor_indenizacao: bool,
) -> pd.DataFrame:
    from agrobr.alt.mapa_psr.models import COLUNAS_APOLICES

    df = _read_apolices_csv(content)
    df = _normalize_apolices_columns(df)
    df = _convert_apolices_types(df)
    if ensure_valor_indenizacao and "valor_indenizacao" not in df.columns:
        df["valor_indenizacao"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
    df = _filter_apolices(df, cultura=cultura, uf=uf, ano=ano, municipio=municipio)

    final_cols = [c for c in COLUNAS_APOLICES if c in df.columns]
    df = df[final_cols]

    df = df.sort_values("ano_apolice").reset_index(drop=True)

    logger.debug(
        "mapa_psr_parse_apolices_ok",
        records=len(df),
        culturas=df["cultura"].nunique() if "cultura" in df.columns else 0,
        ufs=df["uf"].nunique() if "uf" in df.columns else 0,
    )

    return df


def parse_apolices(
    content: bytes,
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    municipio: str | None = None,
) -> pd.DataFrame:
    return _parse_apolices(
        content,
        cultura=cultura,
        uf=uf,
        ano=ano,
        municipio=municipio,
        ensure_valor_indenizacao=True,
    )


def parse_sinistros(
    content: bytes,
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    municipio: str | None = None,
    evento: str | None = None,
) -> pd.DataFrame:
    df = _parse_apolices(
        content,
        cultura=cultura,
        uf=uf,
        ano=ano,
        municipio=municipio,
        ensure_valor_indenizacao=False,
    )

    if "valor_indenizacao" in df.columns:
        mask_indenizacao = df["valor_indenizacao"].fillna(0) > 0
        df = df[mask_indenizacao]

    if "evento" in df.columns:
        mask_evento = df["evento"].fillna("").str.strip().ne("")
        df = df[mask_evento]

    if evento and "evento" in df.columns:
        mask = df["evento"].str.contains(evento.lower(), na=False, regex=False)
        df = df[mask]

    from agrobr.alt.mapa_psr.models import COLUNAS_SINISTROS

    final_cols = [c for c in COLUNAS_SINISTROS if c in df.columns]
    df = df[final_cols]

    df = df.sort_values("ano_apolice").reset_index(drop=True)

    logger.debug(
        "mapa_psr_parse_sinistros_ok",
        records=len(df),
        eventos=df["evento"].nunique() if "evento" in df.columns else 0,
    )

    return df
