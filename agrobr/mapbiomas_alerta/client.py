from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any

import httpx

from agrobr import _log
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from .models import (
    ALERT_DATE_RANGE_QUERY,
    ALERTS_QUERY,
    GRAPHQL_URL,
    LAST_PUBLICATION_QUERY,
    PAGE_SIZE,
)
from .parser import PARSER_VERSION

logger = _log.get_logger(__name__)

TIMEOUT = get_timeout(read=60.0)

_THROTTLE_AFTER_PAGE = 5
_THROTTLE_DELAY = 3.0


@dataclass
class Coleta:
    registros: list[dict[str, Any]] = field(default_factory=list)
    total_anunciado: int = 0
    corpos: list[bytes] = field(default_factory=list)
    avisos: list[str] = field(default_factory=list)


def _get_token(token: str | None = None) -> str:
    if token:
        return token
    env_token = os.environ.get("AGROBR_MAPBIOMAS_ALERTA_TOKEN")
    if not env_token:
        raise SourceUnavailableError(
            source="mapbiomas_alerta",
            url=GRAPHQL_URL,
            last_error="Token nao encontrado. Defina AGROBR_MAPBIOMAS_ALERTA_TOKEN ou passe token=",
        )
    return env_token


async def _graphql_request(
    query: str,
    variables: dict[str, Any],
    *,
    token: str | None = None,
    client: httpx.AsyncClient | None = None,
    corpos: list[bytes] | None = None,
) -> dict[str, Any]:
    headers = {**UserAgentRotator.get_bot_headers()}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    payload = {"query": query, "variables": variables}

    async def _do(http: httpx.AsyncClient) -> dict[str, Any]:
        response = await retry_on_status(
            lambda: http.post(GRAPHQL_URL, json=payload, headers=headers),
            source="mapbiomas_alerta",
        )
        responses.raise_for_status(response, source="mapbiomas_alerta")
        if corpos is not None:
            corpos.append(response.content)
        data = responses.parse_json_response(
            response,
            source="mapbiomas_alerta",
            url=GRAPHQL_URL,
            secrets=(token,),
        )
        if "errors" in data:
            errors = data["errors"]
            msg = errors[0].get("message", str(errors)) if errors else "Unknown GraphQL error"
            raise SourceUnavailableError(
                source="mapbiomas_alerta",
                url=GRAPHQL_URL,
                last_error=f"GraphQL error: {responses.redact_secrets(str(msg), token)}",
            )
        result: dict[str, Any] = data.get("data", {})
        return result

    if client is not None:
        return await _do(client)
    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as http:
        return await _do(http)


def _falha(motivo: str) -> ParseError:
    return ParseError(source="mapbiomas_alerta", parser_version=PARSER_VERSION, reason=motivo)


def _pagina(data: dict[str, Any]) -> tuple[list[dict[str, Any]], int, int]:
    alerts = data.get("alerts")
    metadata = alerts.get("metadata") if isinstance(alerts, dict) else None
    collection = alerts.get("collection") if isinstance(alerts, dict) else None
    total = metadata.get("totalCount") if isinstance(metadata, dict) else None
    paginas = metadata.get("totalPages") if isinstance(metadata, dict) else None
    if not isinstance(collection, list) or type(total) is not int or type(paginas) is not int:
        raise _falha(
            "Resposta sem alerts.collection ou sem metadata.totalCount/totalPages inteiros; "
            "o layout da API mudou"
        )
    return collection, total, paginas


async def fetch_alertas(
    *,
    token: str,
    start_date: str | None = None,
    end_date: str | None = None,
    sources: list[str] | None = None,
    bounding_box: list[float] | None = None,
    max_registros: int | None = None,
    date_type: str = "DetectedAt",
) -> tuple[Coleta, str]:
    """Pagina a coleção em ordem de código e a reconcilia com o ``totalCount`` anunciado.

    Returns:
        A coleta (registros, total anunciado, corpo de cada página e avisos) e a URL.

    Raises:
        ParseError: código repetido entre páginas ou menos alertas que o anunciado.
    """
    tamanho = PAGE_SIZE if max_registros is None else min(PAGE_SIZE, max_registros)
    coleta = Coleta()
    codigos: set[Any] = set()
    anunciado: int | None = None

    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as http:
        page_num = 0
        while True:
            page_num += 1
            variables: dict[str, Any] = {
                "limit": tamanho,
                "page": page_num,
                "sortField": "ALERT_CODE",
                "sortDirection": "ASC",
                "dateType": date_type,
            }
            if start_date:
                variables["startDate"] = start_date
            if end_date:
                variables["endDate"] = end_date
            if sources:
                variables["sources"] = sources
            if bounding_box:
                variables["boundingBox"] = bounding_box

            if page_num > _THROTTLE_AFTER_PAGE:
                await asyncio.sleep(_THROTTLE_DELAY)

            data = await _graphql_request(
                ALERTS_QUERY,
                variables,
                token=token,
                client=http,
                corpos=coleta.corpos,
            )
            collection, total, total_pages = _pagina(data)
            if anunciado is not None and total != anunciado:
                coleta.avisos.append(
                    f"totalCount mudou de {anunciado} para {total} durante a paginação; a "
                    "plataforma pode ter publicado alertas durante a consulta"
                )
                logger.warning(
                    "mapbiomas_alerta_count_changed", previous_total=anunciado, observed_total=total
                )
            anunciado = total
            for registro in collection:
                codigo = registro.get("alertCode")
                if codigo in codigos:
                    raise _falha(
                        f"Alerta {codigo} repetido na paginação; a plataforma pode ter sido "
                        "atualizada durante a consulta, repita a consulta"
                    )
                codigos.add(codigo)
            coleta.registros.extend(collection)
            logger.debug(
                "mapbiomas_alerta_page",
                page=page_num,
                total_pages=total_pages,
                records=len(collection),
            )
            if (
                not collection
                or page_num >= total_pages
                or (max_registros is not None and len(coleta.registros) >= max_registros)
            ):
                break

    coleta.total_anunciado = anunciado or 0
    esperado = (
        coleta.total_anunciado
        if max_registros is None
        else min(coleta.total_anunciado, max_registros)
    )
    if len(coleta.registros) < esperado:
        raise _falha(
            f"Coleção incompleta: {len(coleta.registros)} de {esperado} alertas anunciados "
            f"(totalCount {coleta.total_anunciado}); repita a consulta"
        )
    if max_registros is not None:
        del coleta.registros[max_registros:]
        if coleta.total_anunciado > max_registros:
            coleta.avisos.append(
                f"max_registros={max_registros}: {max_registros} de {coleta.total_anunciado} "
                "alertas, os de menor código; restrinja o período ou use max_registros=None"
            )
    return coleta, GRAPHQL_URL


def _objeto(data: dict[str, Any], chave: str, campos: tuple[str, ...]) -> dict[str, Any]:
    valor = data.get(chave)
    if not isinstance(valor, dict) or not set(campos) <= set(valor):
        raise _falha(f"Resposta sem {chave} com {', '.join(campos)}; o layout da API mudou")
    return valor


async def fetch_alert_date_range() -> tuple[dict[str, Any], str]:
    data = await _graphql_request(ALERT_DATE_RANGE_QUERY, {})
    campos = ("minDetectedAt", "maxDetectedAt", "minPublishedAt", "maxPublishedAt")
    return _objeto(data, "alertDateRange", campos), GRAPHQL_URL


async def fetch_last_publication() -> tuple[dict[str, Any], str]:
    data = await _graphql_request(LAST_PUBLICATION_QUERY, {})
    return _objeto(data, "lastAlertPublication", ("publishedAt", "total")), GRAPHQL_URL
