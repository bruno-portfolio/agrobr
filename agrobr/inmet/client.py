from __future__ import annotations

import io
import os
import zipfile
from datetime import date, timedelta
from typing import Any

import httpx
import structlog

from agrobr.constants import URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

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
            lambda: c.get(url),
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
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise SourceUnavailableError(
                    source="inmet",
                    url=public_url,
                    last_error="HTTP 403 Forbidden — defina AGROBR_INMET_TOKEN",
                ) from e
            raise

        try:
            data = response.json()
        except ValueError as e:
            body = response.text[:200]
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
            return []
        return data

    if http is not None:
        return await _do_request(http)

    headers = UserAgentRotator.get_headers(source="inmet")
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as c:
        return await _do_request(c)


async def fetch_estacoes(tipo: str = "T") -> list[dict[str, Any]]:
    if tipo not in ("T", "M"):
        raise ValueError(f"Tipo deve ser 'T' (automática) ou 'M' (convencional), got '{tipo}'")

    logger.info("inmet_fetch_estacoes", tipo=tipo)
    return await _get_json(f"/estacoes/{tipo}")


async def fetch_dados_estacao(
    codigo: str,
    inicio: date,
    fim: date,
    *,
    http: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]]:
    if inicio > fim:
        raise ValueError(f"inicio ({inicio}) deve ser <= fim ({fim})")

    logger.info(
        "inmet_fetch_dados",
        estacao=codigo,
        inicio=str(inicio),
        fim=str(fim),
    )

    async def _run(c: httpx.AsyncClient | None) -> list[dict[str, Any]]:
        all_data: list[dict[str, Any]] = []
        chunk_start = inicio
        chunks = 0
        failed = 0
        last_error: SourceUnavailableError | None = None

        while chunk_start <= fim:
            chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1), fim)

            path = f"/estacao/{chunk_start.isoformat()}/{chunk_end.isoformat()}/{codigo}"
            chunks += 1

            try:
                chunk_data = await _get_json(path, http=c, requires_token=True)
                all_data.extend(chunk_data)
                logger.debug(
                    "inmet_chunk_ok",
                    estacao=codigo,
                    chunk_start=str(chunk_start),
                    chunk_end=str(chunk_end),
                    records=len(chunk_data),
                )
            except SourceUnavailableError as e:
                failed += 1
                last_error = e
                logger.warning(
                    "inmet_chunk_unavailable",
                    estacao=codigo,
                    error=str(e),
                    chunk_start=str(chunk_start),
                )

            chunk_start = chunk_end + timedelta(days=1)

        if failed == chunks and last_error is not None:
            raise last_error
        return all_data

    if http is not None:
        return await _run(http)

    headers = UserAgentRotator.get_headers(source="inmet")
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as c:
        return await _run(c)


HISTORICO_MIN_ANO = 2000

HISTORICO_TIMEOUT = get_timeout(read=600.0)

MIN_HISTORICO_ZIP = 1_000_000

_historico_zip_cache: tuple[int, bytes] | None = None


async def fetch_historico_estacao(codigo: str, ano: int) -> tuple[bytes, str]:
    """Baixa o ZIP anual do dadoshistoricos (~100 MB, todas as estações; cache
    de 1 ano por processo) e extrai apenas o CSV da estação pedida. Fonte
    pública sem token — alternativa ao apitempo para dados históricos."""
    global _historico_zip_cache
    if ano < HISTORICO_MIN_ANO:
        raise ValueError(f"Ano {ano} fora do dadoshistoricos (disponível de {HISTORICO_MIN_ANO}+)")

    url = f"{URLS[Fonte.INMET]['dadoshistoricos']}/{ano}.zip"

    if _historico_zip_cache is not None and _historico_zip_cache[0] == ano:
        logger.debug("inmet_historico_cache_hit", ano=ano)
        zip_bytes = _historico_zip_cache[1]
    else:
        logger.info("inmet_historico_request", url=url, ano=ano)
        headers = UserAgentRotator.get_headers(source="inmet")
        async with httpx.AsyncClient(
            timeout=HISTORICO_TIMEOUT, headers=headers, follow_redirects=True
        ) as c:
            response = await retry_on_status(lambda: c.get(url), source="inmet")
            if response.status_code == 404:
                raise SourceUnavailableError(
                    source="inmet",
                    url=url,
                    last_error=f"Ano {ano} indisponível no dadoshistoricos",
                )
            response.raise_for_status()
            zip_bytes = response.content

        if len(zip_bytes) < MIN_HISTORICO_ZIP:
            raise SourceUnavailableError(
                source="inmet",
                url=url,
                last_error=f"ZIP anual com {len(zip_bytes)} bytes — possível truncamento",
            )
        _historico_zip_cache = (ano, zip_bytes)
        logger.info("inmet_historico_zip_ok", ano=ano, bytes=len(zip_bytes))

    alvo = codigo.strip().upper()
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            membros = [n for n in zf.namelist() if f"_{alvo}_" in n.upper()]
            if not membros:
                raise SourceUnavailableError(
                    source="inmet",
                    url=url,
                    last_error=f"Estação {alvo} sem dados no ano {ano}",
                )
            return zf.read(membros[0]), url
    except zipfile.BadZipFile as e:
        raise SourceUnavailableError(
            source="inmet",
            url=url,
            last_error=f"Resposta não é um ZIP válido: {e}",
        ) from e


async def fetch_dados_estacoes_uf(
    uf: str,
    inicio: date,
    fim: date,
    tipo: str = "T",
) -> list[dict[str, Any]]:
    import asyncio

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
                    logger.warning(
                        "inmet_station_error",
                        estacao=codigo,
                        error=str(e),
                    )
                    return []

        codigos = [e["CD_ESTACAO"] for e in estacoes_uf]
        results = await asyncio.gather(*[_fetch_one(c) for c in codigos])

    for result in results:
        all_data.extend(result)

    return all_data
