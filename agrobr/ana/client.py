from __future__ import annotations

import asyncio
import json

import httpx

from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize import regions
from agrobr.utils.geo import (
    build_arcgis_query_url,
    fetch_arcgis_count,
    fetch_arcgis_layer_with_total,
    fetch_wfs,
)

from .models import ANA_BASE, ANA_SPR, LAYERS, MASSAS_DAGUA

TIMEOUT = get_timeout(read=180.0)

_FID = MASSAS_DAGUA["oid_field"]
_PAGINA = MASSAS_DAGUA["max_record_count"]
_PAUSA_APOS_PAGINA = 5
_PAUSA_SEGUNDOS = 2.0


def massas_where(uf: str | None) -> str:
    if uf is None:
        return "1=1"
    nome = regions.uf_para_nome(uf).upper()
    return (
        f"(nmufe = '{nome}' OR nmufe LIKE '{nome}, %' OR nmufe LIKE '%, {nome}' "
        f"OR nmufe LIKE '%, {nome}, %')"
    )


async def fetch_layer(
    layer_key: str,
    *,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    f: str = "geojson",
) -> tuple[list[bytes], str, int]:
    """Devolve as páginas, a URL da primeira e o total da camada contado antes da coleta."""
    return await fetch_arcgis_layer_with_total(
        ANA_BASE,
        LAYERS[layer_key],
        source="ana",
        timeout=TIMEOUT,
        where=where,
        bbox=bbox,
        max_registros=max_registros,
        f=f,
        return_geometry=False if f == "json" else None,
    )


def _fids(content: bytes) -> list[int]:
    try:
        features = json.loads(content).get("features") or []
        return [
            int((feature.get("attributes") or feature.get("properties") or {})[_FID])
            for feature in features
        ]
    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
        AttributeError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        raise ParseError(
            source="ana", parser_version=1, reason=f"Pagina ArcGIS ilegivel ou sem {_FID}: {exc!r}"
        ) from exc


async def _fetch_ids(
    http: httpx.AsyncClient,
    service_url: str,
    *,
    where: str,
    bbox: tuple[float, float, float, float] | None,
) -> list[int]:
    url = (
        build_arcgis_query_url(service_url, where=where, bbox=bbox, out_fields=_FID, f="json")
        + "&returnIdsOnly=true"
    )
    response = await retry_on_status(lambda: http.get(url), source="ana")
    responses.raise_for_status(response, source="ana")
    responses.raise_for_service_error(response, source="ana", url=url)
    data = responses.parse_json_response(response, source="ana", url=url)
    ids = data.get("objectIds") if isinstance(data, dict) else None
    if not isinstance(ids, list) or not all(
        isinstance(fid, int) and not isinstance(fid, bool) for fid in ids
    ):
        raise ParseError(
            source="ana",
            parser_version=1,
            reason=f"returnIdsOnly sem lista de {_FID}: {str(data)[:200]}",
        )
    return sorted(ids)


async def fetch_massas_dagua(
    *,
    where: str,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    f: str = "geojson",
) -> tuple[list[bytes], str, int]:
    """Pagina por faixa de ``FID``: o servidor SPR recusa ``resultRecordCount`` e ``orderByFields``.

    Lê a lista oficial de ``FID`` (``returnIdsOnly``) e pede cada faixa de até 1.000 no ``where``;
    a página tem de trazer exatamente os ``FID`` da faixa. Devolve também o total da camada.
    """
    if max_registros is not None and (
        isinstance(max_registros, bool) or not isinstance(max_registros, int) or max_registros < 1
    ):
        raise InvalidParameterError(
            f"max_registros deve ser inteiro positivo ou None, recebeu {max_registros!r}"
        )
    service_url = f"{ANA_SPR}/{MASSAS_DAGUA['service_path']}"
    total = await fetch_arcgis_count(
        service_url, where=where, bbox=bbox, source="ana", timeout=TIMEOUT
    )
    if total == 0:
        return [], f"{service_url}/query", 0

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as http:
        ids = await _fetch_ids(http, service_url, where=where, bbox=bbox)
        if len(ids) != total:
            raise SourceUnavailableError(
                source="ana",
                url=service_url,
                last_error=f"returnIdsOnly trouxe {len(ids)} {_FID} para a contagem {total}",
            )
        ids = ids[:max_registros]
        pages: list[bytes] = []
        urls: list[str] = []
        for inicio in range(0, len(ids), _PAGINA):
            faixa = ids[inicio : inicio + _PAGINA]
            url = build_arcgis_query_url(
                service_url,
                where=f"({where}) AND {_FID} >= {faixa[0]} AND {_FID} <= {faixa[-1]}",
                bbox=bbox,
                out_fields=MASSAS_DAGUA["fields"],
                f=f,
                return_geometry=False if f == "json" else None,
            )
            content = await fetch_wfs(url, source="ana", timeout=TIMEOUT, client=http)
            recebidos = sorted(_fids(content))
            if recebidos != faixa:
                raise SourceUnavailableError(
                    source="ana",
                    url=url,
                    last_error=(
                        f"Pagina ArcGIS com {len(recebidos)} de {len(faixa)} feicoes da faixa "
                        f"{_FID} {faixa[0]}-{faixa[-1]}"
                    ),
                )
            pages.append(content)
            urls.append(url)
            if len(pages) > _PAUSA_APOS_PAGINA:
                await asyncio.sleep(_PAUSA_SEGUNDOS)
    return pages, urls[0], total
