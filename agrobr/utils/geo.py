from __future__ import annotations

import json
import math
import re
from typing import Any, Literal, TypedDict
from urllib.parse import quote, urlencode

import httpx
import pandas as pd
import structlog

from agrobr.constants import MIN_WFS_SIZE
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()


class LayerConfig(TypedDict):
    service_path: str
    max_record_count: int
    fields: str
    rename_map: dict[str, str]
    colunas_saida: list[str]
    required_cols: set[str]


def check_geopandas() -> Any:
    try:
        import geopandas

        return geopandas
    except ImportError:
        raise ImportError(
            "geopandas is required for geo functions. Install with: pip install agrobr[geo]"
        ) from None


def validate_bbox(
    bbox: tuple[float, float, float, float] | None,
) -> tuple[float, float, float, float] | None:
    if bbox is None:
        return None
    if len(bbox) != 4:
        raise ValueError(
            f"BBOX deve ter 4 valores (minlon, minlat, maxlon, maxlat), recebeu {len(bbox)}"
        )
    minlon, minlat, maxlon, maxlat = bbox
    if minlon >= maxlon:
        raise ValueError(f"BBOX minlon ({minlon}) deve ser menor que maxlon ({maxlon})")
    if minlat >= maxlat:
        raise ValueError(f"BBOX minlat ({minlat}) deve ser menor que maxlat ({maxlat})")
    return bbox


def build_wfs_url(
    base: str,
    namespace: str,
    layer: str,
    version: str,
    property_names: list[str],
    *,
    max_features: int,
    output_format: str = "csv",
    cql_filter: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    bbox_crs: str = "EPSG:4674",
    start_index: int | None = None,
    result_type: str | None = None,
) -> str:
    props = ",".join(property_names)
    is_v2 = version.startswith("2.")
    type_key = "typeNames" if is_v2 else "typeName"
    count_key = "count" if is_v2 else "maxFeatures"

    url = (
        f"{base}"
        f"?service=WFS&version={version}&request=GetFeature"
        f"&{type_key}={namespace}:{layer}"
        f"&outputFormat={quote(output_format)}"
        f"&propertyName={props}"
        f"&{count_key}={max_features}"
    )
    if start_index is not None:
        url += f"&startIndex={start_index}"
    if result_type is not None:
        url += f"&resultType={result_type}"
    if cql_filter:
        url += f"&CQL_FILTER={quote(cql_filter)}"
    if bbox is not None:
        minlon, minlat, maxlon, maxlat = bbox
        url += f"&BBOX={minlon},{minlat},{maxlon},{maxlat},{bbox_crs}"
    return url


async def fetch_wfs(
    url: str,
    *,
    source: str,
    timeout: httpx.Timeout,
    base_delay: float | None = None,
    client: httpx.AsyncClient | None = None,
) -> bytes:
    async def _do_fetch(http: httpx.AsyncClient) -> bytes:
        logger.debug(f"{source}_request", url=url)
        response = await retry_on_status(
            lambda: http.get(url),
            source=source,
            base_delay=base_delay,
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source=source, url=url, last_error="HTTP 404")

        response.raise_for_status()

        content = response.content
        if content[:50].lstrip().lower().startswith((b"<!doctype", b"<html")):
            raise SourceUnavailableError(
                source=source,
                url=url,
                last_error="WFS returned HTML instead of feature data (possible maintenance or URL redirect)",
            )
        head = content[:500]
        if b"<ServiceException" in head or b"<ows:Exception" in head:
            text = content.decode("utf-8", errors="replace")
            m = re.search(
                r"<(?:ows:)?(?:ServiceException|ExceptionText)[^>]*>(.*?)</", text, re.DOTALL
            )
            msg = m.group(1).strip() if m else text[:300].strip()
            raise SourceUnavailableError(
                source=source,
                url=url,
                last_error=f"WFS server exception: {msg}",
            )
        if len(content) < MIN_WFS_SIZE:
            raise SourceUnavailableError(
                source=source,
                url=url,
                last_error=(
                    f"WFS response too small ({len(content)} bytes), expected WFS feature data"
                ),
            )
        return content

    if client is not None:
        return await _do_fetch(client)

    async with httpx.AsyncClient(
        timeout=timeout, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as auto_client:
        return await _do_fetch(auto_client)


def parse_geojson_base(
    data: bytes,
    gpd: Any,
    *,
    source: str,
    parser_version: int,
    required_cols: set[str],
    max_features: int | None,
    output_cols_empty: list[str],
    truncation_event: str,
    on_empty: Literal["empty", "raise"] = "empty",
    warn_null_geom: bool = False,
    crs: str = "EPSG:4326",
) -> Any:
    try:
        geojson = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ParseError(
            source=source,
            parser_version=parser_version,
            reason=f"Erro ao ler GeoJSON {source}: {e}",
        ) from e

    features = geojson.get("features", [])
    if not features:
        if on_empty == "raise":
            raise ParseError(
                source=source,
                parser_version=parser_version,
                reason="GeoJSON sem features",
            )
        empty = gpd.GeoDataFrame(columns=output_cols_empty)
        empty = empty.set_geometry("geometry")
        return empty

    if max_features is not None and len(features) >= max_features:
        logger.warning(
            truncation_event,
            features=len(features),
            max_features=max_features,
        )

    if warn_null_geom:
        null_geom_count = sum(1 for f in features if f.get("geometry") is None)
        if null_geom_count > 0:
            logger.warning(
                f"{source}_null_geometry",
                null_count=null_geom_count,
                total=len(features),
            )

    gdf = gpd.GeoDataFrame.from_features(features, crs=crs)

    missing = required_cols - set(gdf.columns)
    if missing:
        raise ParseError(
            source=source,
            parser_version=parser_version,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    return gdf


def build_arcgis_query_url(
    base_url: str,
    *,
    where: str = "1=1",
    out_fields: str = "*",
    bbox: tuple[float, float, float, float] | None = None,
    in_sr: int = 4326,
    out_sr: int = 4326,
    f: str = "geojson",
    result_record_count: int | None = None,
    result_offset: int | None = None,
    return_count_only: bool = False,
) -> str:
    params: dict[str, str | int] = {
        "where": where,
        "outFields": out_fields,
        "outSR": out_sr,
        "f": f,
    }
    if bbox is not None:
        minlon, minlat, maxlon, maxlat = bbox
        params["geometry"] = f"{minlon},{minlat},{maxlon},{maxlat}"
        params["geometryType"] = "esriGeometryEnvelope"
        params["inSR"] = in_sr
        params["spatialRel"] = "esriSpatialRelIntersects"
    if return_count_only:
        params["returnCountOnly"] = "true"
    if result_record_count is not None:
        params["resultRecordCount"] = result_record_count
    if result_offset is not None:
        params["resultOffset"] = result_offset

    return f"{base_url}/query?{urlencode(params)}"


async def fetch_arcgis_count(
    base_url: str,
    *,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    source: str,
    timeout: httpx.Timeout,
) -> int:
    url = build_arcgis_query_url(
        base_url,
        where=where,
        bbox=bbox,
        return_count_only=True,
        f="json",
    )
    async with httpx.AsyncClient(
        timeout=timeout, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as http:
        response = await retry_on_status(lambda: http.get(url), source=source)
        response.raise_for_status()
        data = response.json()
    count: int = data.get("count", 0)
    logger.info(f"{source}_arcgis_count", count=count, url=url[:120])
    return count


def parse_wfs_hits(content: bytes, *, source: str) -> int:
    text = content.decode("utf-8", errors="replace")
    match = re.search(r'numberMatched="(\d+)"', text)
    if match:
        return int(match.group(1))
    match = re.search(r"numberMatched=(\d+)", text)
    if match:
        return int(match.group(1))
    raise ParseError(
        source=source,
        parser_version=1,
        reason=f"Nao encontrou numberMatched na resposta hits: {text[:200]}",
    )


async def fetch_wfs_paginated(
    base: str,
    namespace: str,
    layer: str,
    version: str,
    property_names: list[str],
    page_size: int,
    *,
    source: str,
    timeout: httpx.Timeout,
    cql: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    throttle_after_page: int = 5,
    throttle_delay: float = 2.0,
) -> tuple[list[bytes], str]:
    hits_url = build_wfs_url(
        base,
        namespace,
        layer,
        version,
        [],
        max_features=1,
        cql_filter=cql,
        result_type="hits",
    )
    hits_content = await fetch_wfs(hits_url, source=source, timeout=timeout)
    total = parse_wfs_hits(hits_content, source=source)
    logger.info("wfs_paginated_hits", source=source, layer=layer, total=total)

    if total == 0:
        url = build_wfs_url(
            base,
            namespace,
            layer,
            version,
            property_names,
            max_features=page_size,
            cql_filter=cql,
            bbox=bbox,
        )
        return [], url

    n_pages = math.ceil(total / page_size)
    pages: list[bytes] = []
    first_url = ""

    async with httpx.AsyncClient(
        timeout=timeout,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        for i in range(n_pages):
            url = build_wfs_url(
                base,
                namespace,
                layer,
                version,
                property_names,
                max_features=page_size,
                cql_filter=cql,
                start_index=i * page_size,
                bbox=bbox,
            )
            if i == 0:
                first_url = url
            delay = throttle_delay if i >= throttle_after_page else None
            content = await fetch_wfs(
                url,
                source=source,
                timeout=timeout,
                base_delay=delay,
                client=http,
            )
            pages.append(content)
            logger.debug(
                "wfs_paginated_page",
                source=source,
                layer=layer,
                page=i + 1,
                total_pages=n_pages,
                size=len(content),
            )

    return pages, first_url


async def fetch_arcgis_layer(
    base_url: str,
    layer_config: LayerConfig,
    *,
    source: str,
    timeout: httpx.Timeout,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    max_features: int | None = None,
    f: str = "geojson",
    throttle_after_page: int = 5,
    throttle_delay: float = 2.0,
) -> tuple[list[bytes], str]:
    import asyncio
    import math

    service_url = f"{base_url}/{layer_config['service_path']}"
    max_record_count = layer_config["max_record_count"]
    fields = layer_config["fields"]

    total = await fetch_arcgis_count(
        service_url,
        where=where,
        bbox=bbox,
        source=source,
        timeout=timeout,
    )
    logger.info(f"{source}_layer_count", total=total)

    if total == 0:
        return [], f"{service_url}/query"

    if max_features and total > max_features:
        total = max_features

    n_pages = math.ceil(total / max_record_count)
    pages: list[bytes] = []
    first_url = ""

    async with httpx.AsyncClient(
        timeout=timeout,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        for i in range(n_pages):
            offset = i * max_record_count
            url = build_arcgis_query_url(
                service_url,
                where=where,
                bbox=bbox,
                out_fields=fields,
                out_sr=4326,
                f=f,
                result_record_count=max_record_count,
                result_offset=offset,
            )
            if i == 0:
                first_url = url
            content = await fetch_wfs(url, source=source, timeout=timeout, client=http)
            pages.append(content)
            logger.debug(f"{source}_page", page=i + 1, total_pages=n_pages, size=len(content))
            if i >= throttle_after_page:
                await asyncio.sleep(throttle_delay)

    return pages, first_url


def parse_arcgis_tabular(
    pages: list[bytes],
    *,
    source: str,
    layer_config: LayerConfig,
    parser_version: int,
    numeric_cols: frozenset[str] | None = None,
) -> pd.DataFrame:
    colunas = layer_config["colunas_saida"]
    rename_map = layer_config["rename_map"]

    if not pages:
        return pd.DataFrame(columns=colunas)

    all_rows: list[dict[str, Any]] = []
    for i, page_data in enumerate(pages):
        try:
            data = json.loads(page_data)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ParseError(
                source=source,
                parser_version=parser_version,
                reason=f"Erro ao ler JSON pagina {i}: {e}",
            ) from e
        for feat in data.get("features", []):
            row = feat.get("properties") or feat.get("attributes", {})
            if row:
                all_rows.append(row)

    if not all_rows:
        return pd.DataFrame(columns=colunas)

    df = pd.DataFrame(all_rows)
    df = df.rename(columns=rename_map)

    if numeric_cols:
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
    if "UF" in df.columns:
        df["UF"] = df["UF"].fillna("").str.strip().str.upper()

    output_cols = [c for c in colunas if c in df.columns]
    df = df[output_cols].reset_index(drop=True)
    logger.info(f"{source}_parse_tabular_ok", records=len(df))
    return df


def parse_arcgis_geojson(
    pages: list[bytes],
    *,
    source: str,
    layer_config: LayerConfig,
    parser_version: int,
    numeric_cols: frozenset[str] | None = None,
) -> Any:
    gpd = check_geopandas()
    colunas_geo = layer_config["colunas_saida"] + ["geometry"]
    rename_map = layer_config["rename_map"]
    required = layer_config["required_cols"]
    max_record_count = layer_config["max_record_count"]

    if not pages:
        empty = gpd.GeoDataFrame(columns=colunas_geo)
        empty = empty.set_geometry("geometry")
        return empty

    gdfs = []
    for page_data in pages:
        gdf = parse_geojson_base(
            page_data,
            gpd,
            source=source,
            parser_version=parser_version,
            required_cols=required,
            max_features=max_record_count,
            output_cols_empty=colunas_geo,
            truncation_event=f"{source}_truncated",
            warn_null_geom=True,
        )
        if not gdf.empty:
            gdfs.append(gdf)

    if not gdfs:
        empty = gpd.GeoDataFrame(columns=colunas_geo)
        empty = empty.set_geometry("geometry")
        return empty

    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs="EPSG:4326")
    gdf = gdf.rename(columns=rename_map)

    if numeric_cols:
        for col in numeric_cols:
            if col in gdf.columns:
                gdf[col] = pd.to_numeric(gdf[col], errors="coerce")
    if "UF" in gdf.columns:
        gdf["UF"] = gdf["UF"].fillna("").str.strip().str.upper()

    output_cols = [c for c in colunas_geo if c in gdf.columns]
    gdf = gdf[output_cols].reset_index(drop=True)
    logger.info(f"{source}_parse_geojson_ok", records=len(gdf))
    return gdf
