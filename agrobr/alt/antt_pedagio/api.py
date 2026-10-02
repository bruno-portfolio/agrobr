from __future__ import annotations

import hashlib
import importlib
import time
import warnings
from datetime import date, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.models import MetaInfo
from agrobr.normalize import dates
from agrobr.utils.result import ATRIBUTO_AVISOS, DataFrameResult, build_source_meta
from agrobr.utils.validation import validate_year_uf

from . import _fluxo, client, parser, query
from .models import DATASET_PRACAS_SLUG

PRACAS_SCHEMA_VERSION = "2.0"
_TEXTO = pd.Series([""]).dtype
_NUMEROS_PRACAS = {"km_m": "float64", "ano_do_pnv_snv": "Int64"}
_DATA_BR = r"[0-9]{2}/[0-9]{2}/[0-9]{4}"


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
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    frequencia: Literal["mensal", "diaria"] = "mensal",
    tipo_cobranca: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
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
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    frequencia: Literal["mensal", "diaria"] = "mensal",
    tipo_cobranca: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    enriquecer: bool = True,
    max_linhas: int = constants.ANTT_PARSER_MAX_ROWS,
    max_memoria_bytes: int = constants.ANTT_PARSER_MAX_MEMORY_BYTES,
) -> tuple[pd.DataFrame, MetaInfo]: ...


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
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    frequencia: Literal["mensal", "diaria"] = "mensal",
    tipo_cobranca: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    enriquecer: bool = True,
    max_linhas: int = constants.ANTT_PARSER_MAX_ROWS,
    max_memoria_bytes: int = constants.ANTT_PARSER_MAX_MEMORY_BYTES,
) -> DataFrameResult: ...


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
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    frequencia: Literal["mensal", "diaria"] = "mensal",
    tipo_cobranca: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    enriquecer: bool = True,
    max_linhas: int = constants.ANTT_PARSER_MAX_ROWS,
    max_memoria_bytes: int = constants.ANTT_PARSER_MAX_MEMORY_BYTES,
) -> DataFrameResult:
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
        inicio=inicio,
        fim=fim,
        apenas_pesados=apenas_pesados,
        enriquecer=enriquecer,
        max_linhas=max_linhas,
        max_memoria_bytes=max_memoria_bytes,
    )
    _output_guards(as_polars=as_polars, return_meta=return_meta)
    return await _fluxo.fetch(validated, as_polars=as_polars, return_meta=return_meta)


def _pracas_polars(frame: pd.DataFrame) -> Any:
    module = importlib.import_module("polars")
    return module.from_pandas(
        frame,
        schema_overrides={
            name: module.Utf8 for name in frame.columns if frame[name].dtype == _TEXTO
        },
    )


def _avisar(frame: pd.DataFrame, aviso: str) -> None:
    frame.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
    warnings.warn(aviso, UserWarning, stacklevel=3)


def _pracas_saida(frame: pd.DataFrame) -> pd.DataFrame:
    saida = frame.astype(
        {
            name: _TEXTO
            for name in frame.columns
            if name not in ("lat", "lon", *_NUMEROS_PRACAS, "data_da_inativacao")
        }
    )
    for name, dtype in _NUMEROS_PRACAS.items():
        if name not in saida:
            continue
        texto = saida[name].astype("string").str.strip()
        valido = texto.str.fullmatch(r"[0-9]+(?:[.,][0-9]+)?" if dtype == "float64" else "[0-9]+")
        numeros = pd.to_numeric(texto.where(valido.fillna(False)).str.replace(",", "."))
        saida[name] = numeros.astype("float64") if dtype == "float64" else numeros.astype("Int64")
        descartados = int((texto.fillna("").ne("") & ~valido.fillna(False)).sum())
        if descartados:
            _avisar(
                saida, f"antt_pedagio: {descartados} valor(es) de {name} ilegíveis viraram nulo"
            )
    if "data_da_inativacao" in saida:
        texto = saida["data_da_inativacao"].astype("string").str.strip()
        fora = texto.fillna("").ne("") & ~texto.str.fullmatch(_DATA_BR).fillna(False)
        if fora.any():
            raise ParseError(
                source="antt_pedagio",
                parser_version=parser.PARSER_VERSION,
                reason=f"data_da_inativacao fora de DD/MM/AAAA: {texto[fora].iloc[0]!r}",
                errors=[("data_da_inativacao", "DD/MM/AAAA", str(int(fora.sum())))],
            )
        dates.converter_coluna(
            saida, "data_da_inativacao", fonte="antt_pedagio", formato="%d/%m/%Y"
        )
    return saida


def _filtrar_pracas(
    frame: pd.DataFrame, uf: str | None, rodovia: str | None, situacao: str | None
) -> pd.DataFrame:
    filtros = {
        name: valor
        for name, valor in (("uf", uf), ("rodovia", rodovia), ("situacao", situacao))
        if valor is not None
    }
    ausentes = sorted(set(filtros) - set(frame.columns))
    if ausentes:
        raise ParseError(
            source="antt_pedagio",
            parser_version=parser.PARSER_VERSION,
            reason=f"Cadastro sem as colunas {ausentes} usadas nos filtros",
        )
    mascara = pd.Series(True, index=frame.index)
    if uf is not None:
        mascara &= frame["uf"].eq(uf)
    if rodovia is not None:
        mascara &= (
            frame["rodovia"].map(query._rodovia, na_action="ignore").eq(query._rodovia(rodovia))
        )
    if situacao is not None:
        mascara &= frame["situacao"].str.contains(situacao, case=False, na=False, regex=False)
    selecionado = frame[mascara].reset_index(drop=True)
    if filtros and selecionado.empty:
        publicados = {name: sorted(frame[name].dropna().unique()) for name in filtros}
        _avisar(
            selecionado,
            f"antt_pedagio: nenhuma praça com {filtros}; valores publicados: {publicados}",
        )
    return selecionado


@overload
async def pracas_pedagio(
    uf: str | None = None,
    rodovia: str | None = None,
    situacao: str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def pracas_pedagio(
    uf: str | None = None,
    rodovia: str | None = None,
    situacao: str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def pracas_pedagio(
    uf: str | None = None,
    rodovia: str | None = None,
    situacao: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def pracas_pedagio(
    uf: str | None = None,
    rodovia: str | None = None,
    situacao: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
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
    df = _filtrar_pracas(_pracas_saida(parser.parse_pracas(raw)), uf, rodovia, situacao)
    parse_ms = int((time.monotonic() - t1) * 1000)
    if "municipal" in df.columns:
        aviso = (
            "antt_pedagio: a coluna 'municipal' do cadastro repete 'municipio' e sai na próxima "
            "versão major; use 'municipio'"
        )
        df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
        warnings.warn(aviso, FutureWarning, stacklevel=2)

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
        schema_version=PRACAS_SCHEMA_VERSION,
        raw_content_hash=hashlib.sha256(raw).hexdigest(),
    )
    meta.dataset = "antt_pedagio_pracas"
    meta.contract_version = PRACAS_SCHEMA_VERSION
    meta.data_sources = ["antt_pedagio"]
    meta.raw_content_size = len(raw)
    result = _pracas_polars(df) if as_polars else df
    return (result, meta) if return_meta else result
