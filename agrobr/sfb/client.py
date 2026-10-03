from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Any

import httpx

from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import geo
from agrobr.utils.geo import fetch_arcgis_layer

from . import models, parser
from .models import LAYERS, SFB_BASE

TIMEOUT = get_timeout(read=120.0)


async def fetch_layer(
    layer_key: str,
    *,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    f: str = "geojson",
) -> tuple[list[bytes], str]:
    return await fetch_arcgis_layer(
        SFB_BASE,
        LAYERS[layer_key],
        source="sfb",
        timeout=TIMEOUT,
        where=where,
        bbox=bbox,
        max_registros=max_registros,
        f=f,
        return_geometry=False if f == "json" else None,
    )


@dataclass(frozen=True)
class IfnDownload:
    pontos: list[bytes]
    lotes: list[bytes]
    source_url: str
    resources: list[dict[str, Any]]


async def _fetch_ifn_pages(
    config: geo.LayerConfig,
    *,
    role: str,
    where: str,
    bbox: tuple[float, float, float, float] | None = None,
    f: str = "json",
) -> tuple[list[bytes], str, list[dict[str, Any]]]:
    service = f"{SFB_BASE}/{config['service_path']}"
    total = await geo.fetch_arcgis_count(
        service,
        where=where,
        bbox=bbox,
        source="sfb",
        timeout=TIMEOUT,
    )
    pages: list[bytes] = []
    resources: list[dict[str, Any]] = []
    first_url = f"{service}/query"
    collected = 0
    last_oid: int | None = None
    oid_field = config["oid_field"]
    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        while collected < total:
            page_size = min(config["max_record_count"], total - collected)
            url = geo.build_arcgis_query_url(
                service,
                where=where if last_oid is None else f"({where}) AND {oid_field} > {last_oid}",
                bbox=bbox,
                out_fields=config["fields"],
                f=f,
                return_geometry=False if f == "json" else None,
                order_by_fields=oid_field,
                result_record_count=page_size,
            )
            if not pages:
                first_url = url
            raw = await geo.fetch_wfs(url, source="sfb", timeout=TIMEOUT, client=http)
            rows = parser.ifn_atributos(raw, lotes=role == "lotes")
            oids = [int(row[oid_field]) for row in rows]
            if not oids:
                break
            if oids != sorted(set(oids)) or (last_oid is not None and oids[0] <= last_oid):
                raise ParseError(
                    source="sfb",
                    parser_version=parser.PARSER_VERSION,
                    reason=f"IFN: {oid_field} duplicado ou fora de ordem na paginação",
                )
            if len(oids) > page_size:
                raise SourceUnavailableError(
                    source="sfb",
                    url=url,
                    last_error="IFN: página excede a contagem solicitada",
                )
            pages.append(raw)
            resources.append(
                {
                    "role": role,
                    "url": url,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "bytes": len(raw),
                }
            )
            collected += len(oids)
            last_oid = oids[-1]
            if len(pages) > 5:
                await asyncio.sleep(2)
    if collected != total:
        raise SourceUnavailableError(
            source="sfb",
            url=first_url,
            last_error=f"Paginação IFN incompleta: {collected} de {total} feições",
        )
    return pages, first_url, resources


async def fetch_ifn(
    *, where: str = "1=1", bbox: tuple[float, float, float, float] | None = None, f: str = "json"
) -> IfnDownload:
    pontos, url, resources = await _fetch_ifn_pages(
        LAYERS["ifn_conglomerados"],
        role="pontos",
        where=where,
        bbox=bbox,
        f=f,
    )
    codes = sorted(
        {
            int(row["co_lote"])
            for page in pontos
            for row in parser.ifn_atributos(page)
            if row["co_lote"] is not None
        }
    )
    lotes: list[bytes] = []
    for start in range(0, len(codes), models.IFN_LOTES_POR_CONSULTA):
        if start:
            await asyncio.sleep(1)
        requested = codes[start : start + models.IFN_LOTES_POR_CONSULTA]
        pages, _, details = await _fetch_ifn_pages(
            models.IFN_LOTES,
            role="lotes",
            where=f"co_lote IN ({','.join(map(str, requested))})",
        )
        returned = {
            row["co_lote"] for page in pages for row in parser.ifn_atributos(page, lotes=True)
        }
        if extra := returned - set(requested):
            raise ParseError(
                source="sfb",
                parser_version=parser.PARSER_VERSION,
                reason=f"Lotes IFN não solicitados na consulta: {sorted(extra)}",
            )
        lotes.extend(pages)
        resources.extend(details)
    return IfnDownload(pontos, lotes, url, resources)
