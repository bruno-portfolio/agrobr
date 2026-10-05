from __future__ import annotations

import asyncio
import hashlib
import time
import warnings
from contextlib import closing
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.exceptions import (
    ContractViolationError,
    ParseError,
    SourceUnavailableError,
)
from agrobr.models import MetaInfo
from agrobr.normalize import municipalities
from agrobr.utils import time as time_utils
from agrobr.utils.result import DataFrameResult, build_source_meta, check_polars, finalize_result
from agrobr.utils.validation import validate_year_uf
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import (
    ANO_INICIO_PSR,
    CATALOGO_URL,
    COLUNAS_APOLICES,
    COLUNAS_SINISTROS,
    CSV_RESOURCES,
    ULTIMO_ANO_FIXO,
    _resolve_periodos,
    get_csv_url,
)

logger = _log.get_logger(__name__)


@overload
async def sinistros(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    evento: str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def sinistros(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    evento: str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def sinistros(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    evento: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def sinistros(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    evento: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    return await _fetch(
        produto=produto,
        uf=uf,
        ano=ano,
        ano_inicio=ano_inicio,
        ano_fim=ano_fim,
        municipio=municipio,
        evento=evento,
        sinistros=True,
        as_polars=as_polars,
        return_meta=return_meta,
    )


@overload
async def apolices(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def apolices(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def apolices(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def apolices(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    return await _fetch(
        produto=produto,
        uf=uf,
        ano=ano,
        ano_inicio=ano_inicio,
        ano_fim=ano_fim,
        municipio=municipio,
        evento=None,
        sinistros=False,
        as_polars=as_polars,
        return_meta=return_meta,
    )


async def _fetch(
    *,
    produto: str | None,
    uf: str | None,
    ano: int | None,
    ano_inicio: int | None,
    ano_fim: int | None,
    municipio: int | str | None,
    evento: str | None,
    sinistros: bool,
    as_polars: bool,
    return_meta: bool,
) -> DataFrameResult:
    validate_year_uf(uf=uf, ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim, ano_min=ANO_INICIO_PSR)
    if uf is not None:
        uf = uf.strip().upper()
    alvo = None if municipio is None else municipalities.resolver_municipio(municipio, uf)
    check_polars(as_polars)

    effective_inicio, effective_fim = _resolve_range(ano, ano_inicio, ano_fim)
    urls, avisos = await _urls_dos_periodos(effective_inicio, effective_fim)

    fetch_ms = 0
    parse_ms = 0
    dfs: list[pd.DataFrame] = []
    corpos: list[dict[str, Any]] = []
    contrato = "mapa_psr_sinistros" if sinistros else "mapa_psr_apolices"
    colunas = COLUNAS_SINISTROS if sinistros else COLUNAS_APOLICES
    empty = contracts.get_contract(contrato).empty_frame()[colunas]
    for periodo, url in urls.items():
        period_frames: list[pd.DataFrame] = []
        t0 = time.monotonic()
        abrir = client.open_periodo(periodo) if periodo in CSV_RESOURCES else client.open_url(url)
        async with abrir as stream:
            fetch_ms += int((time.monotonic() - t0) * 1000)
            digest = hashlib.sha256()
            for bloco in iter(lambda: stream.read(1 << 20), b""):
                digest.update(bloco)
            corpos.append({"url": url, "sha256": digest.hexdigest(), "bytes": stream.tell()})
            stream.seek(0)
            t1 = time.monotonic()
            with closing(
                parser.iter_apolices(
                    stream,
                    cultura=produto,
                    uf=uf,
                    ano=ano,
                    municipio=alvo,
                    evento=evento,
                    sinistros=sinistros,
                    ano_inicio=effective_inicio,
                    ano_fim=effective_fim,
                )
            ) as frames:
                for df in frames:
                    period_frames.append(df)
                    await asyncio.sleep(0)
            period_frame = pd.concat(period_frames, ignore_index=True)
            dfs.append(period_frame.sort_values("ano_apolice").reset_index(drop=True))
            parse_ms += int((time.monotonic() - t1) * 1000)

    t1 = time.monotonic()
    df_out = pd.concat(dfs, ignore_index=True) if dfs else empty
    df_out = df_out.sort_values("ano_apolice").reset_index(drop=True)
    df_out, colapsadas = _colapsar_reenvios(df_out, contrato)
    parse_ms += int((time.monotonic() - t1) * 1000)

    source_url = next(iter(urls.values()), CATALOGO_URL)
    meta = build_source_meta(
        "mapa_psr",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df_out,
        parser.PARSER_VERSION,
        schema_version=contracts.get_contract(contrato).version,
        raw_content_hash=corpos[0]["sha256"] if len(corpos) == 1 else None,
        raw_content_size=corpos[0]["bytes"] if len(corpos) == 1 else 0,
        source_details={"duplicatas_colapsadas": colapsadas, "corpos": corpos},
    )
    for aviso in avisos:
        meta.validation_warnings.append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=3)
    return finalize_result(
        df_out,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(
            coluna.name
            for coluna in contracts.get_contract(contrato).columns
            if coluna.type == contracts.ColumnType.STRING
        ),
    )


def _colapsar_reenvios(df: pd.DataFrame, contrato: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Colapsa registros publicados duas vezes (proposta reenviada ao MAPA).

    Só sai a cópia idêntica em todas as colunas publicadas; repetição da chave com qualquer valor
    diferente levanta ``ContractViolationError``.
    """
    repetidas = df.duplicated(keep="first")
    resumo: dict[str, Any] = {
        "linhas": int(repetidas.sum()),
        "apolices": sorted(
            {
                f"{linha.nr_apolice}/{linha.ano_apolice}/{linha.seguradora}"
                for linha in df[repetidas].itertuples()
            }
        )
        if "seguradora" in df.columns
        else [],
    }
    if resumo["linhas"]:
        warn_once(
            f"mapa_psr_reenvios_{contrato}",
            f"PSR: {resumo['linhas']} registro(s) publicados em dobro pelo MAPA, iguais em todas as "
            f"colunas, saem uma vez só: {', '.join(resumo['apolices'])}",
        )
        df = df[~repetidas].reset_index(drop=True)
    chave = [c for c in contracts.get_contract(contrato).primary_key if c in df.columns]
    conflitos = df.duplicated(chave, keep=False)
    if conflitos.any():
        raise ContractViolationError(
            dataset=contrato,
            violation=f"{int(conflitos.sum())} linhas repetem a chave {chave} com valores diferentes",
            expected=chave,
            got=df.loc[conflitos, chave].drop_duplicates().to_dict("records"),
        )
    return df, resumo


def _resolve_range(
    ano: int | None,
    ano_inicio: int | None,
    ano_fim: int | None,
) -> tuple[int | None, int | None]:
    if ano is not None:
        return ano, ano
    return ano_inicio, ano_fim


async def _urls_dos_periodos(
    inicio: int | None, fim: int | None
) -> tuple[dict[str, str], list[str]]:
    """As URLs dos CSV do período e os avisos dos anos que o catálogo do MAPA não tem.

    O dicionário fixo é atalho e reserva: o catálogo só é consultado quando o período passa do
    último ano fixo (sem ``fim``, o período vai até o ano corrente). Com o catálogo fora, os anos
    fixos seguem com aviso; sem nenhum ano fixo no período, a falha sobe.
    """
    urls = {periodo: get_csv_url(periodo) for periodo in _resolve_periodos(inicio, fim)}
    primeiro = max(inicio or 0, ULTIMO_ANO_FIXO + 1)
    ultimo = fim if fim is not None else time_utils.hoje().year
    if ultimo < primeiro:
        return urls, []
    try:
        catalogo = parser.parse_catalogo(await client.fetch_catalogo())
    except (SourceUnavailableError, ParseError) as erro:
        if not urls:
            raise
        return urls, [
            f"Catálogo do PSR indisponível ({erro}); os anos depois de {ULTIMO_ANO_FIXO} ficam de fora"
        ]
    novos = {p: catalogo[p] for p in _resolve_periodos(primeiro, ultimo, catalogo) if p not in urls}
    avisos = [
        f"O catálogo do PSR não tem arquivo para {ano}"
        for ano in range(primeiro, ultimo + 1)
        if not _resolve_periodos(ano, ano, novos)
    ]
    return {**urls, **novos}, avisos
