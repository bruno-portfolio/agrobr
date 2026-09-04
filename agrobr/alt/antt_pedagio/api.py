from __future__ import annotations

import time

import httpx
import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_year_uf

from . import client, parser
from .models import (
    ANO_INICIO,
    COLUNAS_FLUXO,
    DATASET_PRACAS_SLUG,
    DATASET_TRAFEGO_SLUG,
    _resolve_anos,
)

logger = structlog.get_logger()


def _montar_fluxo(trafego_data: list[tuple[int, bytes]], pracas_raw: bytes) -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []
    for ano_val, content in trafego_data:
        dfs.append(parser.parse_trafego(content, ano=ano_val))

    df_out = pd.DataFrame(columns=COLUNAS_FLUXO) if not dfs else pd.concat(dfs, ignore_index=True)

    if pracas_raw:
        try:
            df_pracas = parser.parse_pracas(pracas_raw)
            return parser.join_fluxo_pracas(df_out, df_pracas)
        except (ParseError, KeyError, ValueError):
            logger.warning("antt_pedagio_join_fallback", reason="parse/join failed")

    for col in ("rodovia", "uf", "municipio"):
        if col not in df_out.columns:
            df_out[col] = None
    return df_out


def _filtrar_fluxo(
    df_out: pd.DataFrame,
    *,
    concessionaria: str | None,
    praca: str | None,
    rodovia: str | None,
    uf: str | None,
    tipo_veiculo: str | None,
    apenas_pesados: bool,
) -> pd.DataFrame:
    filtros = (
        ("concessionaria", concessionaria, "contains"),
        ("praca", praca, "contains"),
        ("rodovia", rodovia, "upper_eq"),
        ("uf", uf.upper() if uf else None, "eq"),
        ("tipo_veiculo", tipo_veiculo, "eq"),
    )
    for col, valor, modo in filtros:
        if not valor or col not in df_out.columns:
            continue
        if modo == "contains":
            df_out = df_out[df_out[col].str.contains(valor, case=False, na=False, regex=False)]
        elif modo == "upper_eq":
            df_out = df_out[df_out[col].str.upper() == valor.upper()]
        else:
            df_out = df_out[df_out[col] == valor]

    if apenas_pesados:
        mask = (df_out["n_eixos"] >= 3) & (df_out["tipo_veiculo"] == "Comercial")
        df_out = df_out[mask]

    return df_out


async def fluxo_pedagio(
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    concessionaria: str | None = None,
    rodovia: str | None = None,
    uf: str | None = None,
    praca: str | None = None,
    tipo_veiculo: str | None = None,
    apenas_pesados: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    validate_year_uf(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, ano_min=ANO_INICIO)

    anos = _resolve_anos(ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)

    t0 = time.monotonic()
    trafego_data = await client.fetch_trafego_anos(anos)

    try:
        pracas_raw = await client.fetch_pracas()
    except httpx.HTTPError:
        logger.warning("antt_pedagio_pracas_fallback", reason="fetch failed")
        pracas_raw = b""

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df_out = _montar_fluxo(trafego_data, pracas_raw)
    df_out = _filtrar_fluxo(
        df_out,
        concessionaria=concessionaria,
        praca=praca,
        rodovia=rodovia,
        uf=uf,
        tipo_veiculo=tipo_veiculo,
        apenas_pesados=apenas_pesados,
    )

    final_cols = [c for c in COLUNAS_FLUXO if c in df_out.columns]
    df_out = df_out[final_cols]

    df_out = df_out.sort_values(
        ["data", "concessionaria", "praca"], na_position="last"
    ).reset_index(drop=True)

    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "antt_pedagio",
        f"https://dados.antt.gov.br/dataset/{DATASET_TRAFEGO_SLUG}",
        "httpx",
        fetch_ms,
        parse_ms,
        df_out,
        parser.PARSER_VERSION,
    )
    return finalize_result(df_out, meta, as_polars=as_polars, return_meta=return_meta)


async def pracas_pedagio(
    uf: str | None = None,
    rodovia: str | None = None,
    situacao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    validate_year_uf(uf=uf)

    t0 = time.monotonic()
    raw = await client.fetch_pracas()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_pracas(raw)

    if uf and "uf" in df.columns:
        df = df[df["uf"] == uf.upper()]

    if rodovia and "rodovia" in df.columns:
        mask = df["rodovia"].str.upper() == rodovia.upper()
        df = df[mask]

    if situacao and "situacao" in df.columns:
        mask = df["situacao"].str.contains(situacao, case=False, na=False, regex=False)
        df = df[mask]

    df = df.reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "antt_pedagio",
        f"https://dados.antt.gov.br/dataset/{DATASET_PRACAS_SLUG}",
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
