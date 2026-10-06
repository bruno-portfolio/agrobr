from __future__ import annotations

import asyncio
import hashlib
import io
import os
import re
import time
import weakref
import zipfile
import zlib
from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from agrobr import _log, constants
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.time import hoje

from . import models, parser, transport

logger = _log.get_logger(__name__)

BASE_URL = URLS[Fonte.INMET]["base"]

TIMEOUT = get_timeout()

MAX_DAYS_PER_REQUEST = 365


def _get_token() -> str | None:
    return os.getenv("AGROBR_INMET_TOKEN")


async def _get_json(
    path: str,
    *,
    http: httpx.AsyncClient | None = None,
    requires_token: bool = False,
) -> list[dict[str, Any]]:
    """Dados observacionais exigem token no path (`/token{path}/{token}`): sem ele
    a API responde 204 vazio (vira erro com hint); com token, 204 é período sem
    dados. Token inválido volta 200 com body texto "CHAVE INVÁLIDA!". O token
    nunca aparece em logs ou mensagens de erro."""
    token = _get_token() if requires_token else None
    public_url = f"{BASE_URL}{path}"
    url = f"{BASE_URL}/token{path}/{token}" if token else public_url

    if requires_token and not token:
        logger.warning(
            "inmet_no_token",
            hint="Dados observacionais exigem token; defina AGROBR_INMET_TOKEN",
        )

    async def _do_request(c: httpx.AsyncClient) -> list[dict[str, Any]]:
        response = await retry_on_status(
            lambda: transport.get(c, url, public_url=public_url, token=token),
            source="inmet",
        )

        if response.status_code == 204:
            if requires_token and not token:
                raise SourceUnavailableError(
                    source="inmet",
                    url=public_url,
                    last_error=(
                        "HTTP 204 — dados observacionais do INMET exigem token "
                        "(defina AGROBR_INMET_TOKEN)"
                    ),
                )
            logger.info("inmet_no_content", path=path)
            return []

        try:
            responses.raise_for_status(response, source="inmet")
        except SourceUnavailableError as e:
            if response.status_code == 403:
                raise SourceUnavailableError(
                    source="inmet",
                    url=public_url,
                    last_error="HTTP 403 Forbidden — defina AGROBR_INMET_TOKEN",
                ) from e.__cause__
            raise

        try:
            data = response.json()
        except ValueError as e:
            body = response.text
            if token:
                body = transport.redact_token(body, token)
                transport.sanitize_parse_error(e, token)
            body = body[:200]
            last_error = (
                "Token INMET inválido (AGROBR_INMET_TOKEN)"
                if "CHAVE" in body.upper()
                else f"Resposta não-JSON do INMET: {body!r}"
            )
            raise SourceUnavailableError(
                source="inmet",
                url=public_url,
                last_error=last_error,
            ) from e

        if not isinstance(data, list):
            raise ParseError(
                source="inmet",
                parser_version=parser.PARSER_VERSION,
                reason=f"Resposta INMET fora do formato: esperada lista JSON, veio {type(data).__name__}",
            )
        return data

    if http is not None:
        return await _do_request(http)

    headers = UserAgentRotator.get_headers(source="inmet")
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as c:
        return await _do_request(c)


async def fetch_estacoes(tipo: str = "T") -> list[dict[str, Any]]:
    if tipo not in ("T", "M"):
        raise InvalidParameterError(
            f"tipo inválido: {tipo!r}. Valores válidos: 'T' (automática) e 'M' (convencional)"
        )

    logger.info("inmet_fetch_estacoes", tipo=tipo)
    return await _get_json(f"/estacoes/{tipo}")


async def fetch_dados_estacao(
    codigo: str,
    inicio: date,
    fim: date,
    *,
    http: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]]:
    codigo = models.validate_codigo(codigo)
    inicio, fim = models.validate_periodo(inicio, fim)

    logger.info(
        "inmet_fetch_dados",
        estacao=codigo,
        inicio=str(inicio),
        fim=str(fim),
    )

    async def _run(c: httpx.AsyncClient | None) -> list[dict[str, Any]]:
        all_data: list[dict[str, Any]] = []
        chunk_start = inicio

        while chunk_start <= fim:
            chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1), fim)

            path = f"/estacao/{chunk_start.isoformat()}/{chunk_end.isoformat()}/{codigo}"

            try:
                chunk_data = await _get_json(path, http=c, requires_token=True)
                parser.validate_observation_scope(chunk_data, chunk_start, chunk_end, codigo=codigo)
                all_data.extend(chunk_data)
                logger.debug(
                    "inmet_chunk_ok",
                    estacao=codigo,
                    chunk_start=str(chunk_start),
                    chunk_end=str(chunk_end),
                    records=len(chunk_data),
                )
            except SourceUnavailableError as e:
                logger.warning(
                    "inmet_chunk_unavailable",
                    estacao=codigo,
                    error=str(e),
                    chunk_start=str(chunk_start),
                )
                raise SourceUnavailableError(
                    source=e.source,
                    url=e.url,
                    last_error=f"bloco {chunk_start} a {chunk_end}: {e.last_error}",
                    attempted_sources=e.attempted_sources,
                ) from e

            chunk_start = chunk_end + timedelta(days=1)

        return all_data

    if http is not None:
        return await _run(http)

    headers = UserAgentRotator.get_headers(source="inmet")
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as c:
        return await _run(c)


HISTORICO_TIMEOUT = get_timeout(read=600.0)

MIN_HISTORICO_ZIP = 100_000


@dataclass(frozen=True)
class HistoricoArquivo:
    ano: int
    content: bytes
    url: str
    sha256: str
    fetched_at: datetime
    expires_at: float
    from_cache: bool = False


_historico_zip_cache: OrderedDict[int, HistoricoArquivo] | None = None
_historico_locks: weakref.WeakKeyDictionary[Any, dict[int, asyncio.Lock]] = (
    weakref.WeakKeyDictionary()
)


def _cache_historico(archive: HistoricoArquivo) -> None:
    global _historico_zip_cache
    if _historico_zip_cache is None:
        _historico_zip_cache = OrderedDict()
    if len(archive.content) > constants.INMET_HISTORICO_CACHE_MAX_BYTES:
        return
    _historico_zip_cache[archive.ano] = archive
    _historico_zip_cache.move_to_end(archive.ano)
    while (
        sum(len(item.content) for item in _historico_zip_cache.values())
        > constants.INMET_HISTORICO_CACHE_MAX_BYTES
    ):
        _historico_zip_cache.popitem(last=False)


def invalidate_historico(ano: int) -> None:
    if _historico_zip_cache is not None:
        _historico_zip_cache.pop(ano, None)


async def fetch_historico_arquivo(ano: int) -> HistoricoArquivo:
    models.validate_ano(ano)
    loop_locks = _historico_locks.setdefault(asyncio.get_running_loop(), {})
    async with loop_locks.setdefault(ano, asyncio.Lock()):
        if _historico_zip_cache is not None and ano in _historico_zip_cache:
            cached = _historico_zip_cache[ano]
            if time.monotonic() < cached.expires_at:
                _historico_zip_cache.move_to_end(ano)
                return replace(cached, from_cache=True)
            invalidate_historico(ano)
        url = f"{URLS[Fonte.INMET]['dadoshistoricos']}/{ano}.zip"
        headers = UserAgentRotator.get_headers(source="inmet")
        async with httpx.AsyncClient(
            timeout=HISTORICO_TIMEOUT, headers=headers, follow_redirects=True
        ) as http:
            response = await retry_on_status(lambda: http.get(url), source="inmet")
            if response.status_code == 404:
                raise SourceUnavailableError(
                    source="inmet",
                    url=url,
                    last_error=f"HTTP 404: ano {ano} indisponível no dadoshistoricos",
                )
            responses.raise_for_status(response, source="inmet")
            content = response.content
        if not zipfile.is_zipfile(io.BytesIO(content)):
            raise SourceUnavailableError(
                source="inmet", url=url, last_error="Resposta não é um ZIP válido"
            )
        if len(content) < MIN_HISTORICO_ZIP:
            raise SourceUnavailableError(
                source="inmet",
                url=url,
                last_error=f"ZIP anual com {len(content)} bytes — possível truncamento",
            )
        ttl = (
            constants.INMET_HISTORICO_CACHE_CURRENT_TTL
            if ano == hoje().year
            else constants.INMET_HISTORICO_CACHE_CLOSED_TTL
        )
        archive = HistoricoArquivo(
            ano,
            content,
            url,
            hashlib.sha256(content).hexdigest(),
            datetime.now(UTC),
            time.monotonic() + ttl,
        )
        _cache_historico(archive)
        return archive


def historico_membros(
    archive: HistoricoArquivo, *, codigo: str | None = None, uf: str | None = None
) -> list[tuple[str, str, str, bytes]]:
    selected: list[tuple[str, str, str, bytes]] = []
    try:
        with zipfile.ZipFile(io.BytesIO(archive.content)) as zipped:
            entries = [
                info
                for info in zipped.infolist()
                if not info.is_dir() and info.filename.lower().endswith(".csv")
            ]
            if not entries or len({info.filename for info in entries}) != len(entries):
                raise ParseError(
                    source="inmet", parser_version=2, reason="ZIP sem CSVs ou com nomes duplicados"
                )
            members: list[tuple[zipfile.ZipInfo, str, str]] = []
            for info in sorted(entries, key=lambda entry: entry.filename):
                name = info.filename.replace("\\", "/").rsplit("/", 1)[-1]
                match = re.match(r"INMET_[A-Z]+_([A-Z]{2})_([A-Z0-9]{4,8})_", name.upper())
                if match is None:
                    raise ParseError(
                        source="inmet",
                        parser_version=2,
                        reason=f"Membro histórico não reconhecido: {info.filename}",
                    )
                member_uf, member_code = match.groups()
                if (codigo is None or codigo == member_code) and (uf is None or uf == member_uf):
                    members.append((info, member_code, member_uf))
            if (
                any(
                    info.file_size > constants.INMET_HISTORICO_MAX_MEMBER_BYTES
                    for info, _, _ in members
                )
                or sum(info.file_size for info, _, _ in members)
                > constants.INMET_HISTORICO_MAX_EXPANDED_BYTES
            ):
                raise ResourceLimitError(
                    "inmet",
                    "Expansão dos CSVs históricos selecionados excede o orçamento",
                    url=archive.url,
                )
            for info, member_code, member_uf in members:
                with zipped.open(info) as member:
                    content = member.read(constants.INMET_HISTORICO_MAX_MEMBER_BYTES + 1)
                if len(content) > constants.INMET_HISTORICO_MAX_MEMBER_BYTES:
                    raise ResourceLimitError(
                        "inmet", "CSV histórico excede o orçamento", url=archive.url
                    )
                selected.append((info.filename, member_code, member_uf, content))
    except (
        zipfile.BadZipFile,
        RuntimeError,
        NotImplementedError,
        OSError,
        EOFError,
        zlib.error,
    ) as exc:
        invalidate_historico(archive.ano)
        raise ParseError(
            source="inmet", parser_version=2, reason=f"ZIP/CRC inválido: {exc}"
        ) from exc
    except (ParseError, ResourceLimitError):
        invalidate_historico(archive.ano)
        raise
    return selected


async def fetch_dados_estacoes_uf(
    uf: str,
    inicio: date,
    fim: date,
    tipo: str = "T",
) -> list[dict[str, Any]]:
    if _get_token() is None:
        raise SourceUnavailableError(
            source="inmet",
            url=BASE_URL,
            last_error="Dados observacionais exigem token; defina AGROBR_INMET_TOKEN",
        )

    estacoes = await fetch_estacoes(tipo)

    uf_upper = uf.upper()
    estacoes_uf = [
        e for e in estacoes if e.get("SG_ESTADO") == uf_upper and e.get("CD_SITUACAO") == "Operante"
    ]

    if not estacoes_uf:
        raise SourceUnavailableError(
            source="inmet",
            url=f"{BASE_URL}/estacoes/{tipo}",
            last_error=f"Nenhuma estação operante encontrada para UF={uf_upper} tipo={tipo}",
        )

    logger.info(
        "inmet_fetch_uf",
        uf=uf_upper,
        estacoes=len(estacoes_uf),
        inicio=str(inicio),
        fim=str(fim),
    )

    all_data: list[dict[str, Any]] = []

    semaphore = asyncio.Semaphore(5)

    headers = UserAgentRotator.get_headers(source="inmet")
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as shared:

        async def _fetch_one(codigo: str) -> list[dict[str, Any]]:
            async with semaphore:
                try:
                    return await fetch_dados_estacao(codigo, inicio, fim, http=shared)
                except SourceUnavailableError:
                    raise
                except (httpx.HTTPError, httpx.TimeoutException) as e:
                    raise SourceUnavailableError(
                        source="inmet",
                        url=f"{BASE_URL}/estacao/{inicio}/{fim}/{codigo}",
                        last_error=f"Falha HTTP na estação {codigo}: {e}",
                    ) from e

        tasks: list[asyncio.Task[list[dict[str, Any]]]] = []
        source_error: SourceUnavailableError | ParseError | None = None
        try:
            async with asyncio.TaskGroup() as task_group:
                tasks = [
                    task_group.create_task(_fetch_one(estacao["CD_ESTACAO"]))
                    for estacao in estacoes_uf
                ]
        except* SourceUnavailableError as error_group:
            source_error = next(
                error
                for error in error_group.exceptions
                if isinstance(error, SourceUnavailableError)
            )
        except* ParseError as parse_group:
            source_error = next(
                error for error in parse_group.exceptions if isinstance(error, ParseError)
            )

        if source_error is not None:
            raise source_error

        results = [task.result() for task in tasks]

    for result in results:
        all_data.extend(result)

    return all_data
