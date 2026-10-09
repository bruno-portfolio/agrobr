from __future__ import annotations

import asyncio
import hashlib
import re
import time
import warnings
from datetime import date, timedelta
from typing import Any, Literal, overload

import httpx
import pandas as pd

from agrobr import _log, constants, contracts
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.utils import time as time_utils
from agrobr.utils.result import DataFrameResult, build_source_meta, finalize_result
from agrobr.utils.validation import parse_data
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import B3_CONTRATOS_AGRO, TICKERS_AGRO, parse_vencimento

logger = _log.get_logger(__name__)

_RE_VENCIMENTO_MES = re.compile(r"[FGHJKMNQUVXZ]\d{2}")
_RE_VENCIMENTO_OPCAO = re.compile(r"[FGHJKMNQUVXZ][A-Z]{2}[A-Z0-9]")


def _codigo_de_vencimento(vencimento: str) -> str:
    codigo = vencimento.strip().upper() if isinstance(vencimento, str) else ""
    if _RE_VENCIMENTO_MES.fullmatch(codigo) or _RE_VENCIMENTO_OPCAO.fullmatch(codigo):
        return codigo
    raise InvalidParameterError(
        f"vencimento {vencimento!r} inválido: use o código do mês do contrato (ex.: 'V26') "
        "ou o código publicado da opção (ex.: 'VVJK')"
    )


def _codigo_do_mes_do_futuro(vencimento: str) -> str:
    codigo = vencimento.strip().upper() if isinstance(vencimento, str) else ""
    if _RE_VENCIMENTO_MES.fullmatch(codigo):
        return codigo
    raise InvalidParameterError(
        f"vencimento {vencimento!r} inválido: historico traz só futuros; use o código do mês "
        "do contrato (ex.: 'V26')"
    )


def _ticker(contrato: str) -> str:
    chave = contrato.strip() if isinstance(contrato, str) else ""
    ticker = B3_CONTRATOS_AGRO.get(chave.lower(), chave.upper())
    if ticker not in TICKERS_AGRO:
        raise InvalidParameterError(
            f"contrato {contrato!r} inválido. Valores válidos: {', '.join(sorted(B3_CONTRATOS_AGRO))}"
            f", ou os tickers {', '.join(sorted(TICKERS_AGRO))}"
        )
    return ticker


def _periodo(inicio: str | date, fim: str | date) -> tuple[date, date]:
    inicio_dt, fim_dt = parse_data(inicio, "inicio"), parse_data(fim, "fim")
    if inicio_dt > fim_dt:
        raise InvalidParameterError(f"inicio ({inicio_dt}) posterior a fim ({fim_dt})")
    return inicio_dt, fim_dt


def _validar_tipo(tipo: object) -> None:
    if tipo not in (None, "futuro", "opcao"):
        raise InvalidParameterError(f"tipo {tipo!r} inválido. Valores válidos: futuro, opcao")


@overload
async def ajustes(
    *,
    data: str | date,
    contrato: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ajustes(
    *,
    data: str | date,
    contrato: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def ajustes(
    *,
    data: str | date,
    contrato: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def ajustes(
    *,
    data: str | date,
    contrato: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    warn_once(
        "b3_ajustes",
        (
            "B3: classificação zona_cinza; a dispensa D-1 da FAQ e os termos do website têm "
            "alcances distintos. Confira o canal, o uso e a vigência da política. Veja "
            "https://www.agrobr.dev/docs/licenses/."
        ),
    )

    logger.info("b3_ajustes", data=str(data), contrato=contrato)

    dia = parse_data(data, "data")
    ticker = _ticker(contrato) if contrato is not None else None
    data_str = dia.strftime("%d/%m/%Y")

    t0 = time.monotonic()
    try:
        zip_bytes, source_url = await client.fetch_ajustes_zip(data_str)
        sem_pregao = False
    except client.PregaoNaoPublicadoError as exc:
        zip_bytes, source_url, sem_pregao = exc.conteudo, exc.url, True
    adquirido = time_utils.utcnow()
    fetch_ms = int((time.monotonic() - t0) * 1000)
    t1 = time.monotonic()
    identidade: dict[str, Any] = {}
    if sem_pregao:
        df = contracts.get_contract("ajuste_diario").empty_frame()
    else:
        df = parser.parse_ajustes_zip(zip_bytes)
        identidade = df.attrs.pop("identidade")
        df = df[df["data"] == pd.Timestamp(dia)].reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if ticker is not None:
        df = df[df["ticker"] == ticker].reset_index(drop=True)
    if df.empty:
        df = contracts.get_contract("ajuste_diario").empty_frame()

    meta = build_source_meta(
        "b3",
        source_url,
        "httpx+zip+xml",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION_ZIP,
        raw_content_hash=hashlib.sha256(zip_bytes).hexdigest(),
        raw_content_size=len(zip_bytes),
        source_details=identidade,
    )
    meta.fetched_at = meta.fetch_timestamp = adquirido
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Ajustes diários de um contrato em cada dia útil do período.

    Os pregões são baixados com no máximo ``AGROBR_HTTP_MAX_CONCURRENT_B3`` dias abertos ao mesmo tempo;
    cada dia é um ZIP de ~11 MB, e o período inteiro soma um download por dia útil.
    """
    logger.info("b3_historico", contrato=contrato, inicio=str(inicio), fim=str(fim))

    ticker = _ticker(contrato)
    inicio_dt, fim_dt = _periodo(inicio, fim)
    codigo_vencimento = _codigo_do_mes_do_futuro(vencimento) if vencimento is not None else None

    t0 = time.monotonic()

    weekdays = [
        inicio_dt + timedelta(days=i)
        for i in range((fim_dt - inicio_dt).days + 1)
        if (inicio_dt + timedelta(days=i)).weekday() < 5
    ]
    vagas = asyncio.Semaphore(constants.HTTPSettings().max_concurrent_b3)

    async def _fetch_day(d: date) -> tuple[pd.DataFrame, MetaInfo] | Exception:
        try:
            async with vagas:
                return await ajustes(data=d, contrato=ticker, return_meta=True)
        except (httpx.HTTPError, SourceUnavailableError, ParseError) as exc:
            logger.warning(
                "b3_historico_skip",
                data=str(d),
                contrato=contrato,
                error=str(exc)[:200],
            )
            return exc

    results = await asyncio.gather(*[_fetch_day(d) for d in weekdays])
    frames = [result[0] for result in results if isinstance(result, tuple) and not result[0].empty]
    recebidos = [
        (day, value[1])
        for day, value in zip(weekdays, results, strict=True)
        if isinstance(value, tuple)
    ]
    failures = [result for result in results if isinstance(result, Exception)]
    if results and len(failures) == len(results):
        raise SourceUnavailableError(source="b3", last_error=str(failures[-1])) from failures[-1]

    failed_days = [
        {"data": day.isoformat(), "error_type": type(value).__name__, "error": str(value)}
        for day, value in zip(weekdays, results, strict=True)
        if isinstance(value, Exception)
    ]
    messages = []
    if failed_days:
        message = "Histórico B3 incompleto; falha nas datas: " + ", ".join(
            item["data"] for item in failed_days
        )
        messages.append(message)
        warnings.warn(message, UserWarning, stacklevel=2)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else contracts.get_contract("ajuste_diario").empty_frame()
    )

    if codigo_vencimento is not None:
        df = df[df["vencimento_codigo"] == codigo_vencimento].reset_index(drop=True)

    meta = build_source_meta(
        "b3",
        client.BASE_URL_ZIP,
        "httpx+zip+xml",
        fetch_ms,
        0,
        df,
        parser.PARSER_VERSION_ZIP,
    )
    _registrar_corpos(meta, recebidos)
    meta.validation_warnings.extend(messages)
    meta.source_details["coverage"] = {
        "status": "partial" if failed_days else "all_requests_succeeded",
        "requested_dates": [day.isoformat() for day in weekdays],
        "failed_dates": failed_days,
        "empty_dates": [
            day.isoformat()
            for day, value in zip(weekdays, results, strict=True)
            if isinstance(value, tuple) and value[0].empty
        ],
    }
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def _registrar_corpos(meta: MetaInfo, recebidos: list[tuple[date, MetaInfo]]) -> None:
    if len(recebidos) == 1:
        unico = recebidos[0][1]
        meta.source_url = unico.source_url
        meta.raw_content_hash = unico.raw_content_hash
        meta.raw_content_size = unico.raw_content_size
    if recebidos:
        meta.fetched_at = meta.fetch_timestamp = max(dia.fetched_at for _, dia in recebidos)
    meta.source_details["corpos"] = [
        {
            "data": day.isoformat(),
            "url": dia.source_url,
            "sha256": dia.raw_content_hash,
            "bytes": dia.raw_content_size,
            "fetch_timestamp": dia.fetch_timestamp.isoformat() if dia.fetch_timestamp else None,
            **dia.source_details,
        }
        for day, dia in recebidos
    ]


def contratos() -> list[str]:
    return sorted(B3_CONTRATOS_AGRO.keys())


@overload
async def posicoes_abertas(
    *,
    data: str | date,
    contrato: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def posicoes_abertas(
    *,
    data: str | date,
    contrato: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def posicoes_abertas(
    *,
    data: str | date,
    contrato: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def posicoes_abertas(
    *,
    data: str | date,
    contrato: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Posições em aberto de futuros/opções agro de uma data.

    A B3 mantém alguns dias recentes, sem garantir um histórico completo.
    HTTP 400/404 na solicitação do token indica arquivo não publicado.
    No download, apenas HTTP 404 retorna vazio; HTTP 400 é erro de fonte.
    """
    warn_once(
        "b3_posicoes",
        (
            "B3: classificação zona_cinza; a dispensa D-1 da FAQ e os termos do website têm "
            "alcances distintos. Confira o canal, o uso e a vigência da política. Veja "
            "https://www.agrobr.dev/docs/licenses/."
        ),
    )

    logger.info("b3_posicoes_abertas", data=str(data), contrato=contrato, tipo=tipo)

    data_str = parse_data(data, "data").isoformat()
    ticker = _ticker(contrato) if contrato is not None else None
    _validar_tipo(tipo)

    t0 = time.monotonic()
    csv_bytes, source_url = await client.fetch_posicoes_abertas(data_str)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = (
        parser.parse_posicoes_abertas(csv_bytes)
        if csv_bytes
        else contracts.get_contract("posicoes_abertas").empty_frame()
    )
    parse_ms = int((time.monotonic() - t1) * 1000)

    if ticker is not None:
        df = df[df["ticker"] == ticker].reset_index(drop=True)

    if tipo is not None:
        df = df[df["tipo"] == tipo].reset_index(drop=True)
    if df.empty:
        df = contracts.get_contract("posicoes_abertas").empty_frame()

    meta = build_source_meta(
        "b3",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION_OI,
        schema_version=contracts.get_contract("posicoes_abertas").version,
        raw_content_hash=hashlib.sha256(csv_bytes).hexdigest() if csv_bytes else None,
        raw_content_size=len(csv_bytes),
        source_details={"ticket_url": client.ticket_url(data_str)},
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def posicoes_abertas_historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def posicoes_abertas_historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def posicoes_abertas_historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def posicoes_abertas_historico(
    *,
    contrato: str,
    inicio: str | date,
    fim: str | date,
    vencimento: str | None = None,
    tipo: Literal["futuro", "opcao"] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Itera `posicoes_abertas` pelos dias úteis do período.

    A fonte mantém alguns dias recentes, sem garantir todo o período.
    Dias sem arquivo (HTTP 400/404 no token ou 404 no download) são ignorados;
    falhas de requisição ou parsing
    são propagadas. Para posicionamento semanal histórico em Chicago/NY,
    use `cftc.cot()` (2006+).

    `vencimento` aceita o código do mês do contrato (ex.: "V26"), que casa o
    futuro e as opções daquele mês, ou o código publicado de uma opção
    (ex.: "VVJK").
    """
    logger.info(
        "b3_posicoes_abertas_historico", contrato=contrato, inicio=str(inicio), fim=str(fim)
    )

    ticker = _ticker(contrato)
    inicio_dt, fim_dt = _periodo(inicio, fim)
    _validar_tipo(tipo)
    codigo_vencimento = _codigo_de_vencimento(vencimento) if vencimento is not None else None

    t0 = time.monotonic()

    weekdays = [
        inicio_dt + timedelta(days=i)
        for i in range((fim_dt - inicio_dt).days + 1)
        if (inicio_dt + timedelta(days=i)).weekday() < 5
    ]

    results = [
        await posicoes_abertas(data=d, contrato=ticker, tipo=tipo, return_meta=True)
        for d in weekdays
    ]
    frames = [df_dia for df_dia, _ in results if not df_dia.empty]
    recebidos = [
        (day, meta_dia)
        for day, (_, meta_dia) in zip(weekdays, results, strict=True)
        if meta_dia.raw_content_hash is not None
    ]

    fetch_ms = int((time.monotonic() - t0) * 1000)

    df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else contracts.get_contract("posicoes_abertas").empty_frame()
    )

    if codigo_vencimento is not None and len(codigo_vencimento) == 3:
        ano, mes = parse_vencimento(codigo_vencimento)
        df = df[(df["vencimento_ano"] == ano) & (df["vencimento_mes"] == mes)].reset_index(
            drop=True
        )
    elif codigo_vencimento is not None:
        df = df[df["vencimento_codigo"] == codigo_vencimento].reset_index(drop=True)

    meta = build_source_meta(
        "b3",
        client.BASE_URL_ARQUIVOS,
        "httpx+csv",
        fetch_ms,
        0,
        df,
        parser.PARSER_VERSION_OI,
        schema_version=contracts.get_contract("posicoes_abertas").version,
    )
    _registrar_corpos(meta, recebidos)
    returned_dates = set(pd.to_datetime(df["data"]).dt.date)
    missing_dates = [day.isoformat() for day in weekdays if day not in returned_dates]
    if missing_dates:
        meta.validation_warnings.append(
            "Sem posições retornadas para o filtro nos dias úteis: " + ", ".join(missing_dates)
        )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
