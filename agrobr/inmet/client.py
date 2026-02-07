"""Cliente HTTP para API INMET (apitempo.inmet.gov.br)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import httpx
import structlog

from agrobr.constants import RETRIABLE_STATUS_CODES

logger = structlog.get_logger()

BASE_URL = "https://apitempo.inmet.gov.br"

TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)

MAX_DAYS_PER_REQUEST = 365

RATE_LIMIT_DELAY = 0.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


async def _get_json(path: str) -> list[dict[str, Any]]:
    """Faz GET na API INMET e retorna JSON parseado."""
    url = f"{BASE_URL}{path}"
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            return []
        return data


async def fetch_estacoes(tipo: str = "T") -> list[dict[str, Any]]:
    """Lista estações meteorológicas.

    Args:
        tipo: "T" para automáticas, "M" para convencionais.

    Returns:
        Lista de dicts com metadados das estações.
    """
    if tipo not in ("T", "M"):
        raise ValueError(f"Tipo deve ser 'T' (automática) ou 'M' (convencional), got '{tipo}'")

    logger.info("inmet_fetch_estacoes", tipo=tipo)
    return await _get_json(f"/estacoes/{tipo}")


async def fetch_dados_estacao(
    codigo: str,
    inicio: date,
    fim: date,
) -> list[dict[str, Any]]:
    """Busca dados observacionais de uma estação por período.

    Divide automaticamente em chunks de até 365 dias para respeitar
    limites práticos da API INMET.

    Args:
        codigo: Código da estação (ex: "A001").
        inicio: Data inicial.
        fim: Data final.

    Returns:
        Lista de dicts com observações horárias.
    """
    if inicio > fim:
        raise ValueError(f"inicio ({inicio}) deve ser <= fim ({fim})")

    logger.info(
        "inmet_fetch_dados",
        estacao=codigo,
        inicio=str(inicio),
        fim=str(fim),
    )

    all_data: list[dict[str, Any]] = []
    chunk_start = inicio

    while chunk_start <= fim:
        chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1), fim)

        path = f"/estacao/dados/{codigo}/{chunk_start.isoformat()}/{chunk_end.isoformat()}"

        try:
            chunk_data = await _get_json(path)
            all_data.extend(chunk_data)
            logger.debug(
                "inmet_chunk_ok",
                estacao=codigo,
                chunk_start=str(chunk_start),
                chunk_end=str(chunk_end),
                records=len(chunk_data),
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code in RETRIABLE_STATUS_CODES:
                logger.warning(
                    "inmet_chunk_retriable_error",
                    estacao=codigo,
                    status=e.response.status_code,
                    chunk_start=str(chunk_start),
                )
            else:
                raise

        chunk_start = chunk_end + timedelta(days=1)

    return all_data


async def fetch_dados_estacoes_uf(
    uf: str,
    inicio: date,
    fim: date,
    tipo: str = "T",
) -> list[dict[str, Any]]:
    """Busca dados de todas as estações operantes de uma UF.

    Args:
        uf: Sigla da UF (ex: "MT", "SP").
        inicio: Data inicial.
        fim: Data final.
        tipo: "T" para automáticas, "M" para convencionais.

    Returns:
        Lista consolidada de observações de todas as estações da UF.
    """
    import asyncio

    estacoes = await fetch_estacoes(tipo)

    uf_upper = uf.upper()
    estacoes_uf = [
        e for e in estacoes if e.get("SG_ESTADO") == uf_upper and e.get("CD_SITUACAO") == "Operante"
    ]

    if not estacoes_uf:
        raise ValueError(f"Nenhuma estação operante encontrada para UF={uf_upper} tipo={tipo}")

    logger.info(
        "inmet_fetch_uf",
        uf=uf_upper,
        estacoes=len(estacoes_uf),
        inicio=str(inicio),
        fim=str(fim),
    )

    all_data: list[dict[str, Any]] = []

    semaphore = asyncio.Semaphore(5)

    async def _fetch_one(codigo: str) -> list[dict[str, Any]]:
        async with semaphore:
            try:
                return await fetch_dados_estacao(codigo, inicio, fim)
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
