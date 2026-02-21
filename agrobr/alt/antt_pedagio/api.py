from __future__ import annotations

import time
from datetime import UTC, datetime

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import (
    ANO_INICIO,
    COLUNAS_FLUXO,
    DATASET_PRACAS_SLUG,
    DATASET_TRAFEGO_SLUG,
    UFS_VALIDAS,
    _resolve_anos,
)

logger = structlog.get_logger()


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
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    _validate_params(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)

    anos = _resolve_anos(ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim)

    t0 = time.monotonic()
    trafego_data = await client.fetch_trafego_anos(anos)

    try:
        pracas_raw = await client.fetch_pracas()
    except Exception:
        logger.warning("antt_pedagio_pracas_fallback", reason="fetch failed")
        pracas_raw = b""

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    dfs: list[pd.DataFrame] = []
    for ano_val, content in trafego_data:
        df = parser.parse_trafego(content, ano=ano_val)
        dfs.append(df)

    df_out = pd.DataFrame(columns=COLUNAS_FLUXO) if not dfs else pd.concat(dfs, ignore_index=True)

    if pracas_raw:
        try:
            df_pracas = parser.parse_pracas(pracas_raw)
            df_out = parser.join_fluxo_pracas(df_out, df_pracas)
        except Exception:
            logger.warning("antt_pedagio_join_fallback", reason="parse/join failed")
            for col in ("rodovia", "uf", "municipio"):
                if col not in df_out.columns:
                    df_out[col] = None
    else:
        for col in ("rodovia", "uf", "municipio"):
            if col not in df_out.columns:
                df_out[col] = None

    if concessionaria and "concessionaria" in df_out.columns:
        mask = df_out["concessionaria"].str.contains(concessionaria, case=False, na=False)
        df_out = df_out[mask].copy()

    if praca and "praca" in df_out.columns:
        mask = df_out["praca"].str.contains(praca, case=False, na=False)
        df_out = df_out[mask].copy()

    if rodovia and "rodovia" in df_out.columns:
        mask = df_out["rodovia"].str.upper() == rodovia.upper()
        df_out = df_out[mask].copy()

    if uf and "uf" in df_out.columns:
        df_out = df_out[df_out["uf"] == uf.upper()].copy()

    if tipo_veiculo and "tipo_veiculo" in df_out.columns:
        df_out = df_out[df_out["tipo_veiculo"] == tipo_veiculo].copy()

    if apenas_pesados:
        mask = (df_out["n_eixos"] >= 3) & (df_out["tipo_veiculo"] == "Comercial")
        df_out = df_out[mask].copy()

    final_cols = [c for c in COLUNAS_FLUXO if c in df_out.columns]
    df_out = df_out[final_cols].copy()

    df_out = df_out.sort_values(
        ["data", "concessionaria", "praca"], na_position="last"
    ).reset_index(drop=True)

    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = MetaInfo(
            source="antt_pedagio",
            source_url=f"https://dados.antt.gov.br/dataset/{DATASET_TRAFEGO_SLUG}",
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df_out),
            columns=df_out.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["antt_pedagio"],
            selected_source="antt_pedagio",
            fetch_timestamp=datetime.now(UTC),
        )
        return df_out, meta

    return df_out


async def pracas_pedagio(
    uf: str | None = None,
    rodovia: str | None = None,
    situacao: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if uf and uf.upper() not in UFS_VALIDAS:
        raise ValueError(f"UF '{uf}' invalida. Opcoes: {sorted(UFS_VALIDAS)}")

    t0 = time.monotonic()
    raw = await client.fetch_pracas()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_pracas(raw)

    if uf and "uf" in df.columns:
        df = df[df["uf"] == uf.upper()].copy()

    if rodovia and "rodovia" in df.columns:
        mask = df["rodovia"].str.upper() == rodovia.upper()
        df = df[mask].copy()

    if situacao and "situacao" in df.columns:
        mask = df["situacao"].str.contains(situacao, case=False, na=False)
        df = df[mask].copy()

    df = df.reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = MetaInfo(
            source="antt_pedagio",
            source_url=f"https://dados.antt.gov.br/dataset/{DATASET_PRACAS_SLUG}",
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["antt_pedagio"],
            selected_source="antt_pedagio",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


def _validate_params(
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
) -> None:
    if uf and uf.upper() not in UFS_VALIDAS:
        raise ValueError(f"UF '{uf}' invalida. Opcoes: {sorted(UFS_VALIDAS)}")

    current_year = datetime.now().year
    if ano is not None and (ano < ANO_INICIO or ano > current_year):
        raise ValueError(f"Ano {ano} fora do range valido ({ANO_INICIO}-{current_year})")
    if ano_inicio is not None and ano_inicio < ANO_INICIO:
        raise ValueError(f"ano_inicio {ano_inicio} anterior a {ANO_INICIO}")
    if ano_fim is not None and ano_fim > current_year:
        raise ValueError(f"ano_fim {ano_fim} posterior ao ano atual ({current_year})")
    if ano_inicio is not None and ano_fim is not None and ano_inicio > ano_fim:
        raise ValueError(f"ano_inicio ({ano_inicio}) > ano_fim ({ano_fim})")
