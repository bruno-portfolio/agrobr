"""SICAR WFS client with verified TLS and the GeoServer-compatible cipher policy."""

from __future__ import annotations

import asyncio
import math
import os
import ssl
from collections.abc import AsyncGenerator
from typing import Any
from urllib.parse import quote

import certifi
import httpx
import pydantic

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.geo import fetch_wfs, parse_wfs_hits

from . import models
from .models import (
    MAX_FEATURES_GEO,
    PAGE_SIZE,
    WFS_BASE,
    WFS_VERSION,
    layer_name,
)

logger = _log.get_logger(__name__)

TIMEOUT = get_timeout(read=180.0)

THROTTLE_AFTER_PAGE = 5
THROTTLE_DELAY = 2.0


def _create_ssl_context() -> ssl.SSLContext:
    if os.environ.get("SSL_CERT_FILE"):
        return ssl.create_default_context(cafile=os.environ["SSL_CERT_FILE"])
    if os.environ.get("SSL_CERT_DIR"):
        return ssl.create_default_context(capath=os.environ["SSL_CERT_DIR"])
    return ssl.create_default_context(cafile=certifi.where())


_ssl_ctx: ssl.SSLContext | None = None


def _ssl_context() -> ssl.SSLContext:
    global _ssl_ctx
    if _ssl_ctx is None:
        try:
            context = _create_ssl_context()
        except OSError as exc:
            origem = next(
                (nome for nome in ("SSL_CERT_FILE", "SSL_CERT_DIR") if os.environ.get(nome)),
                "certifi",
            )
            raise OSError(
                f"SICAR: os certificados de {origem} não carregaram ({exc}); corrija a variável "
                "ou remova-a para usar o certifi"
            ) from exc
        context.set_ciphers("DEFAULT:@SECLEVEL=1")
        _ssl_ctx = context
    return _ssl_ctx


def make_session() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
        verify=_ssl_context(),
    )


def _build_wfs_url(
    uf: str,
    *,
    cql_filter: str | None = None,
    count: int | None = PAGE_SIZE,
    start_index: int | None = 0,
    result_type: str | None = None,
    output_format: str = "application/json",
    property_names: list[str] | None = None,
    srs_name: str | None = None,
) -> str:
    props = ",".join(property_names or models.property_names(uf))
    layer = layer_name(uf)

    url = (
        f"{WFS_BASE}"
        f"?service=WFS&version={WFS_VERSION}&request=GetFeature"
        f"&typeNames=sicar:{layer}"
        f"&outputFormat={output_format}"
        f"&propertyName={props}&sortBy=cod_imovel"
    )
    if count is not None:
        url += f"&count={count}"
    if start_index is not None:
        url += f"&startIndex={start_index}"
    if srs_name:
        url += f"&srsName={srs_name}"
    if result_type:
        url += f"&resultType={result_type}"
    if cql_filter:
        url += f"&CQL_FILTER={quote(cql_filter)}"
    return url


async def fetch_hits(
    uf: str,
    cql_filter: str | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> int:
    url = _build_wfs_url(uf, cql_filter=cql_filter, result_type="hits")
    content = await fetch_wfs(url, source="sicar", timeout=TIMEOUT, client=client)
    return parse_wfs_hits(content, source="sicar")


async def fetch_imoveis(
    uf: str,
    cql_filter: str | None = None,
    *,
    validation_warnings: list[str] | None = None,
    source_details: dict[str, Any] | None = None,
) -> tuple[list[bytes], str]:
    async with make_session() as http:
        total = await fetch_hits(uf, cql_filter, client=http)
        logger.info("sicar_hits", uf=uf, total=total, cql_filter=cql_filter)

        if total == 0:
            if source_details is not None:
                source_details.update(anunciados=0, features_unicas=0)
            url = _build_wfs_url(uf, cql_filter=cql_filter, count=None, start_index=None)
            return [], url

        latest_total = total
        pages: list[bytes] = []
        seen: set[str] = set()
        base_url = _build_wfs_url(uf, cql_filter=cql_filter, count=None, start_index=None)

        i = 0
        while i * PAGE_SIZE < total:
            start_index = i * PAGE_SIZE
            url = _build_wfs_url(
                uf,
                cql_filter=cql_filter,
                count=PAGE_SIZE,
                start_index=start_index,
            )
            content = await fetch_wfs(
                url,
                source="sicar",
                timeout=TIMEOUT,
                client=http,
            )
            latest_total = _validate_tabular_page(
                content, latest_total, seen, validation_warnings=validation_warnings
            )
            total = max(total, latest_total)
            pages.append(content)
            logger.debug(
                "sicar_page",
                uf=uf,
                page=i + 1,
                total_pages=math.ceil(total / PAGE_SIZE),
                size=len(content),
            )
            if i >= THROTTLE_AFTER_PAGE:
                await asyncio.sleep(THROTTLE_DELAY)
            i += 1

        if len(seen) != latest_total:
            raise ParseError(
                source="sicar",
                parser_version=models.PARSER_VERSION,
                reason=(
                    f"Varredura inconsistente: {len(seen)} unicas x {latest_total} anunciadas; "
                    "a fonte pode ter sido atualizada durante a consulta, repita a consulta"
                ),
            )

    if source_details is not None:
        source_details.update(anunciados=latest_total, features_unicas=len(seen))
    return pages, base_url


def _validate_tabular_page(
    content: bytes,
    total: int,
    seen: set[str],
    *,
    validation_warnings: list[str] | None = None,
) -> int:
    try:
        collection = models.SicarFeatureCollection.model_validate_json(content)
    except pydantic.ValidationError as exc:
        raise ParseError(
            source="sicar",
            parser_version=models.PARSER_VERSION,
            reason=f"Pagina JSON invalida: {exc}",
        ) from exc
    if isinstance(collection.numberMatched, int) and collection.numberMatched != total:
        message = (
            f"Contagem mudou de {total} para {collection.numberMatched} durante a paginacao; "
            "a fonte pode ter sido atualizada durante a consulta"
        )
        logger.warning(
            "sicar_count_changed", previous_total=total, observed_total=collection.numberMatched
        )
        if validation_warnings is not None:
            validation_warnings.append(message)
        total = collection.numberMatched
    models.validate_feature_ids(collection.features, seen)
    return total


async def stream_imoveis_geo(
    uf: str,
    cql_filter: str | None = None,
    max_features: int | None = MAX_FEATURES_GEO,
) -> AsyncGenerator[tuple[list[bytes], str], None]:
    """Yields (pages, source_url) conforme as paginas sao baixadas.

    Quando max_features e None, cada yield corresponde a uma pagina, baixada
    sequencialmente com o throttle pos-pagina. Isso evita acumular todo o
    estado bruto em memoria.
    """
    models.validate_max_features(max_features)
    if max_features is not None and max_features <= PAGE_SIZE:
        url = _build_wfs_url(
            uf,
            cql_filter=cql_filter,
            count=max_features,
            output_format="application/json",
            property_names=models.property_names(uf, geo=True),
            srs_name=models.SICAR_CRS,
        )
        async with make_session() as http:
            content = await fetch_wfs(url, source="sicar", timeout=TIMEOUT, client=http)
        logger.info("sicar_imoveis_geojson", source="sicar", size=len(content), uf=uf)
        yield [content], url
        return

    async with make_session() as http:
        total = await fetch_hits(uf, cql_filter, client=http)
        logger.info("sicar_geo_hits", uf=uf, total=total, cql_filter=cql_filter)

        limit = min(total, max_features) if max_features is not None else total

        base_url = _build_wfs_url(
            uf,
            cql_filter=cql_filter,
            count=None,
            start_index=None,
            output_format="application/json",
            property_names=models.property_names(uf, geo=True),
            srs_name=models.SICAR_CRS,
        )

        if limit == 0:
            yield [], base_url
            return

        n_pages = math.ceil(limit / PAGE_SIZE)

        async def fetch_page(i: int) -> bytes:
            count = min(PAGE_SIZE, limit - i * PAGE_SIZE)
            url = _build_wfs_url(
                uf,
                cql_filter=cql_filter,
                count=count,
                start_index=i * PAGE_SIZE,
                output_format="application/json",
                property_names=models.property_names(uf, geo=True),
                srs_name=models.SICAR_CRS,
            )
            content = await fetch_wfs(
                url,
                source="sicar",
                timeout=TIMEOUT,
                client=http,
            )
            logger.debug(
                "sicar_geo_page",
                uf=uf,
                page=i + 1,
                total_pages=n_pages,
                size=len(content),
            )
            if i >= THROTTLE_AFTER_PAGE:
                await asyncio.sleep(THROTTLE_DELAY)
            return content

        if max_features is None:
            for i in range(n_pages):
                yield [await fetch_page(i)], base_url
        else:
            pages = [await fetch_page(i) for i in range(n_pages)]
            yield pages, base_url

    logger.info("sicar_imoveis_geojson", source="sicar", pages=n_pages, uf=uf)


async def fetch_imoveis_geo(
    uf: str,
    cql_filter: str | None = None,
    max_features: int | None = MAX_FEATURES_GEO,
) -> tuple[list[bytes], str]:
    """Acumula todas as paginas em memoria. Use stream_imoveis_geo para baixo consumo."""
    all_pages: list[bytes] = []
    source_url = ""
    async for batch, url in stream_imoveis_geo(uf, cql_filter, max_features):
        all_pages.extend(batch)
        source_url = url
    return all_pages, source_url
