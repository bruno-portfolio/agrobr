from __future__ import annotations

import time
import warnings
from typing import Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.ibge import ftp_client, legacy_parser
from agrobr.ibge._helpers import normalizar_opcao, tipar_resultado
from agrobr.models import MetaInfo
from agrobr.utils.result import (
    ATRIBUTO_AVISOS,
    DataFrame,
    DataFrameResult,
    build_source_meta,
    finalize_result,
)
from agrobr.utils.validation import validate_uf

logger = _log.get_logger(__name__)

TEMAS_LEGADO: list[str] = legacy_parser.TEMAS_LEGADO


async def _fetch_tables(tema: str, uf: str | None) -> tuple[list[pd.DataFrame], list[str]]:
    uf_dir = ftp_client.UF_DIRS[uf] if uf else "Brasil"
    tables = (ftp_client.LEGACY_TEMAS[tema],) if uf else ftp_client.LEGACY_TEMAS_BRASIL[tema]
    frames = []
    urls = []
    for table in tables:
        content = await ftp_client.download_legacy_zip(table, uf_dir=uf_dir)
        files = ftp_client.extract_tables_from_zip(content)
        if not files:
            raise ParseError(
                source="ibge_censo_agro_legado",
                parser_version=legacy_parser.PARSER_VERSION,
                reason=f"ZIP sem tabelas XLS/HTML: {uf_dir}/{table}",
            )
        for filename, data in files:
            if filename.lower().endswith(".xls"):
                frame = legacy_parser.parse_legacy_xls(data, tema, filename)
            elif uf:
                frame = legacy_parser.parse_legacy_html(data, tema, uf, filename)
            else:
                raise ParseError(
                    source="ibge_censo_agro_legado",
                    parser_version=legacy_parser.PARSER_VERSION,
                    reason=f"Layout HTML nacional não reconhecido: {filename}",
                )
            if uf and not frame["uf"].eq(uf).all():
                raise ParseError(
                    source="ibge_censo_agro_legado",
                    parser_version=legacy_parser.PARSER_VERSION,
                    reason=f"Geografia do cabeçalho diverge do diretório {uf_dir}: {filename}",
                )
            frames.append(frame)
        urls.append(ftp_client.legacy_zip_url(table, uf_dir))
    return frames, urls


@overload
async def censo_agro_legado(
    tema: str,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def censo_agro_legado(
    tema: str,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def censo_agro_legado(
    tema: str,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def censo_agro_legado(
    tema: str,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


async def censo_agro_legado(
    tema: str,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    tema = normalizar_opcao(tema, "Tema", TEMAS_LEGADO)
    nivel_normalizado = normalizar_opcao(nivel, "Nível", ["brasil", "uf", "municipio"])
    uf = validate_uf(uf)
    if uf and nivel_normalizado == "brasil":
        raise InvalidParameterError("O filtro uf exige nivel='uf' ou nivel='municipio'.")

    t0 = time.monotonic()

    frames: list[pd.DataFrame] = []
    source_urls: list[str] = []
    avisos: list[str] = []
    locations: list[str | None] = (
        [None] if nivel_normalizado == "brasil" else [uf] if uf else list(ftp_client.UF_DIRS)
    )
    for location in locations:
        try:
            parsed, urls = await _fetch_tables(tema, location)
        except ParseError as exc:
            lacuna = ftp_client.LEGACY_LACUNAS.get((location, tema)) if location else None
            if lacuna is None:
                raise
            if uf:
                raise SourceUnavailableError(
                    source="ibge_censo_agro_legado", last_error=f"{lacuna}."
                ) from exc
            avisos.append(f"censo_agro_legado: {location} fica fora do tema {tema}; {lacuna}.")
            continue
        frames.extend(parsed)
        source_urls.extend(urls)

    if not frames:
        df = pd.DataFrame(columns=legacy_parser._OUTPUT_COLS)
    else:
        df = pd.concat(frames, ignore_index=True)

    if "nivel_geo" in df.columns:
        df = df[df["nivel_geo"] == nivel_normalizado].reset_index(drop=True)

    if "nivel_geo" in df.columns:
        df = df.drop(columns=["nivel_geo"])
    df = tipar_resultado(df, "censo_agropecuario_legado", list(df.columns))

    if not df.empty:
        sort_cols = [c for c in ["localidade", "categoria"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols).reset_index(drop=True)

    for aviso in avisos:
        df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=2)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    logger.info(
        "censo_agro_legado_ok",
        tema=tema,
        uf=uf,
        nivel=nivel,
        records=len(df),
        elapsed_s=round(fetch_ms / 1000, 2),
        source_urls=source_urls,
    )

    meta = build_source_meta(
        "ibge_censo_agro_legado",
        source_urls[0],
        "ftp_download",
        fetch_ms,
        0,
        df,
        legacy_parser.PARSER_VERSION,
        attempted_sources=["ibge_censo_agro_legado"],
        selected_source="ibge_censo_agro_legado",
    )
    meta.dataset = "censo_agropecuario_legado"
    meta.contract_version = contracts.get_contract("censo_agropecuario_legado").version
    meta.schema_version = meta.contract_version
    meta.data_sources = sorted(df["fonte"].dropna().unique().tolist())
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def temas_censo_agro_legado() -> list[str]:
    return list(TEMAS_LEGADO)
