from __future__ import annotations

import hashlib
import importlib
import time
from datetime import date
from typing import Any, Literal, overload

import pandas as pd

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta
from agrobr.utils.validation import validate_year_uf

from . import _fluxo, client, parser, query
from .models import DATASET_PRACAS_SLUG


def _output_guards(*, as_polars: bool, return_meta: bool) -> None:
    from agrobr.datasets.deterministic import get_snapshot

    query.validate_flags(as_polars=as_polars, return_meta=return_meta)
    if get_snapshot() is not None:
        raise InvalidParameterError(
            "antt_pedagio não suporta deterministic: recursos CKAN mutáveis"
        )
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError(
                "polars é necessário para as_polars=True. Instale com: pip install agrobr[polars]"
            ) from None


@overload
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
    *,
    return_meta: Literal[False] = False,
    frequencia: Literal["mensal", "diaria"] = "mensal",
    tipo_cobranca: str | None = None,
    data_inicio: date | str | None = None,
    data_fim: date | str | None = None,
    enriquecer: bool = True,
    max_linhas: int = constants.ANTT_PARSER_MAX_ROWS,
    max_memoria_bytes: int = constants.ANTT_PARSER_MAX_MEMORY_BYTES,
) -> pd.DataFrame: ...


@overload
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
    *,
    return_meta: Literal[True],
    frequencia: Literal["mensal", "diaria"] = "mensal",
    tipo_cobranca: str | None = None,
    data_inicio: date | str | None = None,
    data_fim: date | str | None = None,
    enriquecer: bool = True,
    max_linhas: int = constants.ANTT_PARSER_MAX_ROWS,
    max_memoria_bytes: int = constants.ANTT_PARSER_MAX_MEMORY_BYTES,
) -> tuple[pd.DataFrame, MetaInfo]: ...


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
    *,
    frequencia: Literal["mensal", "diaria"] = "mensal",
    tipo_cobranca: str | None = None,
    data_inicio: date | str | None = None,
    data_fim: date | str | None = None,
    enriquecer: bool = True,
    max_linhas: int = constants.ANTT_PARSER_MAX_ROWS,
    max_memoria_bytes: int = constants.ANTT_PARSER_MAX_MEMORY_BYTES,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    query.validate_flags(as_polars=as_polars, return_meta=return_meta)
    validated = query.build_query(
        ano=ano,
        ano_inicio=ano_inicio,
        ano_fim=ano_fim,
        frequencia=frequencia,
        concessionaria=concessionaria,
        rodovia=rodovia,
        uf=uf,
        praca=praca,
        tipo_veiculo=tipo_veiculo,
        tipo_cobranca=tipo_cobranca,
        data_inicio=data_inicio,
        data_fim=data_fim,
        apenas_pesados=apenas_pesados,
        enriquecer=enriquecer,
        max_linhas=max_linhas,
        max_memoria_bytes=max_memoria_bytes,
    )
    _output_guards(as_polars=as_polars, return_meta=return_meta)
    return await _fluxo.fetch(validated, as_polars=as_polars, return_meta=return_meta)


def _pracas_polars(frame: pd.DataFrame) -> Any:
    module = importlib.import_module("polars")
    return module.DataFrame(
        {
            name: module.Series(
                name,
                [None if pd.isna(value) else value for value in frame[name]],
                dtype=module.Float64 if name in ("lat", "lon") else module.Utf8,
                strict=True,
            )
            for name in frame.columns
        }
    )


@overload
async def pracas_pedagio(
    uf: str | None = None,
    rodovia: str | None = None,
    situacao: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def pracas_pedagio(
    uf: str | None = None,
    rodovia: str | None = None,
    situacao: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def pracas_pedagio(
    uf: str | None = None,
    rodovia: str | None = None,
    situacao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    query.validate_flags(as_polars=as_polars, return_meta=return_meta)
    query._text("uf", uf)
    query._text("rodovia", rodovia)
    query._text("situacao", situacao)
    validate_year_uf(uf=uf)
    _output_guards(as_polars=as_polars, return_meta=return_meta)
    if uf is not None:
        uf = uf.strip().upper()

    source_urls: list[str] = []
    t0 = time.monotonic()
    async with client.session(reuse=False):
        raw = await client.fetch_pracas(source_urls=source_urls)
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
        source_urls[0]
        if source_urls
        else f"https://dados.antt.gov.br/dataset/{DATASET_PRACAS_SLUG}",
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        schema_version="1.0.1",
        raw_content_hash=hashlib.sha256(raw).hexdigest(),
    )
    meta.dataset = "antt_pedagio_pracas"
    meta.contract_version = "1.0.1"
    meta.data_sources = ["antt_pedagio"]
    meta.raw_content_size = len(raw)
    result = _pracas_polars(df) if as_polars else df
    return (result, meta) if return_meta else result
