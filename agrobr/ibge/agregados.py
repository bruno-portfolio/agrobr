from __future__ import annotations

import asyncio
import contextlib
import re
from datetime import datetime
from typing import Any

import httpx
import pandas as pd
import pydantic

from agrobr import _log, constants
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.rate_limiter import RateLimiter
from agrobr.http.retry import (
    RETRIABLE_EXCEPTIONS,
    RetriableStatusError,
    retry_async,
    should_retry_status,
)
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = _log.get_logger(__name__)

BASE_URL = constants.URLS[constants.Fonte.IBGE]["agregados"]
FETCH_TIMEOUT = 120.0
_LAST_N = re.compile(r"^last(?:\s+(\d+))?$")
_IN_LEVEL = re.compile(r"^in\s+N(\d+)\s+(\S+)$", re.IGNORECASE)
_periodos_cache: dict[str, dict[str, str]] = {}
PARSER_VERSION = 1


class Nivel(pydantic.BaseModel):
    id: str
    nome: str


class Localidade(pydantic.BaseModel):
    id: str
    nome: str
    nivel: Nivel


class Classificacao(pydantic.BaseModel):
    id: str
    nome: str = ""
    categoria: dict[str, str]


class Serie(pydantic.BaseModel):
    localidade: Localidade
    serie: dict[str, str]


class Resultado(pydantic.BaseModel):
    classificacoes: list[Classificacao]
    series: list[Serie]


class Variavel(pydantic.BaseModel):
    id: str
    variavel: str
    unidade: str
    resultados: list[Resultado]


_PAYLOAD = pydantic.TypeAdapter(list[Variavel])


def _join(value: str | list[str] | None) -> str | None:
    if value is None:
        return None
    return ",".join(value) if isinstance(value, list) else str(value)


def periodos_segment(period: str | list[str] | None) -> str:
    text = _join(period)
    if text is None:
        return "-1"
    match = _LAST_N.match(text.strip())
    if match:
        return f"-{match.group(1) or 1}"
    return text.strip()


def variaveis_segment(variable: str | list[str] | None) -> str:
    text = _join(variable)
    if text is None or text in ("all", "allxp"):
        return "all"
    return text


def localidades_param(territorial_level: str, ibge_territorial_code: str) -> str:
    code = ibge_territorial_code.strip()
    nested = _IN_LEVEL.match(code)
    if nested:
        return f"N{territorial_level}[N{nested.group(1)}[{nested.group(2)}]]"
    return f"N{territorial_level}[{code}]"


def classificacao_param(classifications: dict[str, str | list[str]] | None) -> str | None:
    if not classifications:
        return None
    return "|".join(f"{key}[{_join(value)}]" for key, value in classifications.items())


def agregados_url(
    table_code: str,
    territorial_level: str,
    ibge_territorial_code: str,
    variable: str | list[str] | None,
    period: str | list[str] | None,
    classifications: dict[str, str | list[str]] | None,
) -> str:
    query = f"localidades={localidades_param(territorial_level, ibge_territorial_code)}"
    classificacao = classificacao_param(classifications)
    if classificacao:
        query += f"&classificacao={classificacao}"
    return (
        f"{BASE_URL}/{table_code}/periodos/{periodos_segment(period)}"
        f"/variaveis/{variaveis_segment(variable)}?{query}"
    )


def dimension_order(
    variable: str | list[str] | None, classifications: dict[str, Any] | None
) -> list[str]:
    order = ["n", "p"]
    if variable:
        order.append("v")
    order.extend(f"c{key}" for key in (classifications or {}))
    if not variable:
        order.append("v")
    return order


def to_sidra_frame(
    payload: Any,
    *,
    variable: str | list[str] | None,
    classifications: dict[str, str | list[str]] | None,
    period_names: dict[str, str] | None = None,
) -> pd.DataFrame:
    try:
        variables = _PAYLOAD.validate_python(payload)
    except pydantic.ValidationError as exc:
        raise ParseError(
            source="ibge",
            parser_version=PARSER_VERSION,
            reason=f"Resposta da API de agregados fora da estrutura esperada: {exc.errors()[0]}",
        ) from exc
    order = dimension_order(variable, classifications)
    names = period_names or {}
    rows: list[dict[str, str]] = []
    for item in variables:
        for result in item.resultados:
            categories = {
                f"c{classification.id}": next(iter(classification.categoria.items()), ("", ""))
                for classification in result.classificacoes
            }
            for serie in result.series:
                base = {
                    "NC": serie.localidade.nivel.id.lstrip("N"),
                    "NN": serie.localidade.nivel.nome,
                    "MC": "",
                    "MN": item.unidade,
                }
                dims: dict[str, tuple[str, str]] = {
                    "n": (serie.localidade.id, serie.localidade.nome),
                    "v": (item.id, item.variavel),
                    **categories,
                }
                for period_code, value in serie.serie.items():
                    dims["p"] = (period_code, names.get(period_code, period_code))
                    row = dict(base, V=value)
                    for index, key in enumerate(order, start=1):
                        code, name = dims.get(key, ("", ""))
                        row[f"D{index}C"] = code
                        row[f"D{index}N"] = name
                    rows.append(row)
    columns = ["NC", "NN", "MC", "MN", "V"]
    for index in range(1, len(order) + 1):
        columns += [f"D{index}C", f"D{index}N"]
    return pd.DataFrame(rows, columns=columns)


async def _get_json(http: httpx.AsyncClient, url: str, timeout: float = FETCH_TIMEOUT) -> Any:
    async def _do_fetch() -> Any:
        async with RateLimiter.acquire(constants.Fonte.IBGE):
            async with asyncio.timeout(timeout):
                response = await http.get(url)
        if should_retry_status(response.status_code):
            raise RetriableStatusError(
                f"HTTP {response.status_code}", request=response.request, response=response
            )
        responses.raise_for_status(response, source="ibge")
        return responses.parse_json_response(response, source="ibge", url=url)

    return await retry_async(_do_fetch, retriable_exceptions=(*RETRIABLE_EXCEPTIONS, TimeoutError))


async def fetch_period_names(http: httpx.AsyncClient, table_code: str) -> dict[str, str]:
    if table_code not in _periodos_cache:
        try:
            payload = await _get_json(http, f"{BASE_URL}/{table_code}/periodos")
        except (httpx.HTTPError, TimeoutError, SourceUnavailableError):
            return {}
        if isinstance(payload, list):
            _periodos_cache[table_code] = {
                str(item.get("id")): str((item.get("literals") or [item.get("id")])[0])
                for item in payload
                if isinstance(item, dict)
            }
    return _periodos_cache.get(table_code, {})


def _data_modificacao(texto: object) -> str | None:
    if not isinstance(texto, str) or texto == "01/01/0001":
        return None
    with contextlib.suppress(ValueError):
        return datetime.strptime(texto, "%d/%m/%Y").date().isoformat()
    return None


async def fetch_periodos_modificacao(table_code: str) -> dict[str, str | None]:
    """Data de modificação (ISO) de cada período da tabela, de `/agregados/{tabela}/periodos`.

    O `01/01/0001` que o IBGE publica para período sem data sai nulo.
    """
    async with httpx.AsyncClient(
        timeout=get_timeout(read=FETCH_TIMEOUT),
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        payload = await _get_json(http, f"{BASE_URL}/{table_code}/periodos")
    if not isinstance(payload, list):
        raise ParseError(
            source="ibge",
            parser_version=PARSER_VERSION,
            reason=f"/agregados/{table_code}/periodos sem a lista de períodos",
        )
    return {
        str(item["id"]): _data_modificacao(item.get("modificacao"))
        for item in payload
        if isinstance(item, dict) and "id" in item
    }


async def fetch_localidades(table_code: str, territorial_level: str) -> list[str]:
    """Códigos das localidades que a tabela publica no nível, de `/agregados/{tabela}/localidades`."""
    async with httpx.AsyncClient(
        timeout=get_timeout(read=FETCH_TIMEOUT),
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        payload = await _get_json(http, f"{BASE_URL}/{table_code}/localidades/N{territorial_level}")
    if not isinstance(payload, list):
        raise ParseError(
            source="ibge",
            parser_version=PARSER_VERSION,
            reason=f"/agregados/{table_code}/localidades/N{territorial_level} sem a lista de localidades",
        )
    return [str(item["id"]) for item in payload if isinstance(item, dict) and "id" in item]


async def fetch_agregados(
    table_code: str,
    territorial_level: str,
    ibge_territorial_code: str,
    variable: str | list[str] | None,
    period: str | list[str] | None,
    classifications: dict[str, str | list[str]] | None,
    timeout: float = FETCH_TIMEOUT,
) -> tuple[pd.DataFrame, str]:
    url = agregados_url(
        table_code, territorial_level, ibge_territorial_code, variable, period, classifications
    )
    logger.info("ibge_agregados_fetch", table=table_code, url=url)
    async with httpx.AsyncClient(
        timeout=get_timeout(read=timeout),
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        try:
            payload = await _get_json(http, url, timeout)
        except (httpx.HTTPError, TimeoutError) as exc:
            raise SourceUnavailableError(
                source="ibge",
                url=url,
                last_error=f"API de agregados: {type(exc).__name__}: {exc}",
            ) from exc
        period_names = await fetch_period_names(http, table_code)
    frame = to_sidra_frame(
        payload, variable=variable, classifications=classifications, period_names=period_names
    )
    logger.info("ibge_agregados_success", table=table_code, rows=len(frame))
    return frame, url
