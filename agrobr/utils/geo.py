from __future__ import annotations

import asyncio
import json
import math
import re
from numbers import Real
from typing import Any, Literal, NotRequired, TypedDict
from urllib.parse import quote, urlencode

import httpx
import pandas as pd

from agrobr import _log
from agrobr.constants import MIN_WFS_SIZE
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = _log.get_logger(__name__)


class LayerConfig(TypedDict):
    service_path: str
    max_record_count: int
    fields: str
    rename_map: dict[str, str]
    colunas_saida: list[str]
    required_cols: set[str]
    oid_field: NotRequired[str]


def _arcgis_error_message(data: object) -> str | None:
    return responses.arcgis_error_message(data)


def check_geopandas() -> Any:
    try:
        import geopandas

        return geopandas
    except ImportError:
        raise ImportError(
            "geopandas is required for geo functions. Install with: pip install agrobr[geo]"
        ) from None


def check_pyogrio() -> Any:
    try:
        import pyogrio

        return pyogrio
    except ImportError:
        raise ImportError(
            "pyogrio is required for Acervo Fundiario. Install with: pip install agrobr[geo]"
        ) from None


def validate_bbox(
    bbox: tuple[float, float, float, float] | None,
) -> tuple[float, float, float, float] | None:
    if bbox is None:
        return None
    try:
        valores = () if isinstance(bbox, str | bytes) else tuple(bbox)
    except TypeError:
        valores = ()
    if len(valores) != 4:
        raise InvalidParameterError(
            f"BBOX deve ter 4 valores (minlon, minlat, maxlon, maxlat), recebeu {bbox!r}"
        )
    if not all(
        isinstance(v, Real) and not isinstance(v, bool) and math.isfinite(v) for v in valores
    ):
        raise InvalidParameterError(f"BBOX deve ter 4 números finitos, recebeu {bbox!r}")
    minlon, minlat, maxlon, maxlat = valores
    if not all(-180 <= lon <= 180 for lon in (minlon, maxlon)) or not all(
        -90 <= lat <= 90 for lat in (minlat, maxlat)
    ):
        raise InvalidParameterError(
            "BBOX fora dos limites geográficos de longitude/latitude "
            f"(longitude em [-180, 180], latitude em [-90, 90]): {bbox!r}"
        )
    if minlon >= maxlon:
        raise InvalidParameterError(f"BBOX minlon ({minlon}) deve ser menor que maxlon ({maxlon})")
    if minlat >= maxlat:
        raise InvalidParameterError(f"BBOX minlat ({minlat}) deve ser menor que maxlat ({maxlat})")
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
    srs_name: str | None = None,
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
    if srs_name is not None:
        url += f"&srsName={srs_name}"
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

        responses.raise_for_status(response, source=source)

        content = response.content
        responses.raise_for_service_error(response, source=source, url=url)
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


_EPSG_NAME = re.compile(r"EPSG:{1,2}(\d+)$")


def _declared_crs(geojson: dict[str, Any]) -> str | None:
    declared = geojson.get("crs")
    try:
        return None if declared is None else str(declared["properties"]["name"])
    except (KeyError, TypeError):
        return repr(declared)


def _epsg_code(name: str) -> str | None:
    found = _EPSG_NAME.search(name)
    return found[1] if found else None


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

    error_message = _arcgis_error_message(geojson)
    if error_message:
        raise SourceUnavailableError(source=source, last_error=error_message)
    if not isinstance(geojson, dict) or not isinstance(geojson.get("features"), list):
        raise ParseError(
            source=source,
            parser_version=parser_version,
            reason="GeoJSON inválido: esperado um objeto com lista de features",
        )
    features = geojson["features"]
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

    declared = _declared_crs(geojson)
    if declared is not None and _epsg_code(declared) != _epsg_code(crs):
        raise ParseError(
            source=source,
            parser_version=parser_version,
            reason=f"CRS declarado {declared} diverge do {crs} solicitado",
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
    return_geometry: bool | None = None,
    order_by_fields: str | None = None,
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
    if return_geometry is not None:
        params["returnGeometry"] = str(return_geometry).lower()
    if order_by_fields is not None:
        params["orderByFields"] = order_by_fields
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
        responses.raise_for_status(response, source=source)
        data = responses.parse_json_response(response, source=source, url=url)
    error_message = _arcgis_error_message(data)
    if error_message is not None:
        raise SourceUnavailableError(source=source, url=url, last_error=error_message)
    if not isinstance(data, dict):
        raise SourceUnavailableError(
            source=source,
            url=url,
            last_error="ArcGIS returned JSON that is not an object",
        )
    count = data.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ParseError(
            source=source,
            parser_version=1,
            reason=f"Contagem ArcGIS sem 'count' inteiro não negativo: {str(data)[:200]}",
        )
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


def _page_features(content: bytes, *, source: str) -> list[dict[str, Any]]:
    try:
        features: list[dict[str, Any]] = json.loads(content).get("features") or []
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError) as e:
        raise ParseError(
            source=source, parser_version=1, reason=f"Pagina ArcGIS ilegivel: {e}"
        ) from e
    return features


def _page_oids(content: bytes, oid_field: str, *, source: str) -> list[int]:
    features = _page_features(content, source=source)
    try:
        return [
            int((feature.get("attributes") or feature.get("properties") or {})[oid_field])
            for feature in features
        ]
    except (KeyError, TypeError, ValueError, AttributeError) as e:
        raise ParseError(
            source=source, parser_version=1, reason=f"Pagina ArcGIS sem {oid_field} valido: {e!r}"
        ) from e


def _require_complete(collected: int, total: int, *, source: str, url: str) -> None:
    if collected != total:
        raise SourceUnavailableError(
            source=source,
            url=url,
            last_error=f"Paginacao ArcGIS incompleta: {collected} de {total} feicoes",
        )


async def _fetch_keyset_pages(
    http: httpx.AsyncClient,
    service_url: str,
    *,
    oid_field: str,
    where: str,
    bbox: tuple[float, float, float, float] | None,
    fields: str,
    f: str,
    total: int,
    page_size: int,
    source: str,
    timeout: httpx.Timeout,
    return_geometry: bool | None,
    throttle_after_page: int,
    throttle_delay: float,
) -> tuple[list[bytes], str]:
    pages: list[bytes] = []
    first_url = ""
    collected = 0
    last_oid: int | None = None
    while collected < total:
        url = build_arcgis_query_url(
            service_url,
            where=where if last_oid is None else f"({where}) AND {oid_field} > {last_oid}",
            bbox=bbox,
            out_fields=fields,
            out_sr=4326,
            f=f,
            result_record_count=min(page_size, total - collected),
            return_geometry=return_geometry,
            order_by_fields=oid_field,
        )
        first_url = first_url or url
        content = await fetch_wfs(url, source=source, timeout=timeout, client=http)
        oids = _page_oids(content, oid_field, source=source)
        if not oids:
            break
        if last_oid is not None and max(oids) <= last_oid:
            raise SourceUnavailableError(
                source=source,
                url=url,
                last_error=f"Paginacao ArcGIS nao avancou alem de {oid_field}={last_oid}",
            )
        pages.append(content)
        collected += len(oids)
        last_oid = max(oids)
        logger.debug(f"{source}_page", page=len(pages), collected=collected, total=total)
        if len(pages) > throttle_after_page:
            await asyncio.sleep(throttle_delay)
    _require_complete(collected, total, source=source, url=first_url)
    return pages, first_url


async def fetch_arcgis_layer(
    base_url: str,
    layer_config: LayerConfig,
    *,
    source: str,
    timeout: httpx.Timeout,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    f: str = "geojson",
    throttle_after_page: int = 5,
    throttle_delay: float = 2.0,
    return_geometry: bool | None = None,
) -> tuple[list[bytes], str]:
    if max_registros is not None and (
        isinstance(max_registros, bool) or not isinstance(max_registros, int) or max_registros < 1
    ):
        raise InvalidParameterError(
            f"max_registros deve ser inteiro positivo ou None, recebeu {max_registros!r}"
        )
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

    if max_registros is not None and total > max_registros:
        total = max_registros

    n_pages = math.ceil(total / max_record_count)
    pages: list[bytes] = []
    first_url = ""
    collected = 0

    async with httpx.AsyncClient(
        timeout=timeout,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        oid_field = layer_config.get("oid_field")
        if oid_field is not None:
            return await _fetch_keyset_pages(
                http,
                service_url,
                oid_field=oid_field,
                where=where,
                bbox=bbox,
                fields=fields,
                f=f,
                total=total,
                page_size=max_record_count,
                source=source,
                timeout=timeout,
                return_geometry=return_geometry,
                throttle_after_page=throttle_after_page,
                throttle_delay=throttle_delay,
            )
        for i in range(n_pages):
            offset = i * max_record_count
            url = build_arcgis_query_url(
                service_url,
                where=where,
                bbox=bbox,
                out_fields=fields,
                out_sr=4326,
                f=f,
                result_record_count=min(max_record_count, total - offset),
                result_offset=offset,
                return_geometry=return_geometry,
            )
            if i == 0:
                first_url = url
            content = await fetch_wfs(url, source=source, timeout=timeout, client=http)
            collected += len(_page_features(content, source=source))
            pages.append(content)
            logger.debug(f"{source}_page", page=i + 1, total_pages=n_pages, size=len(content))
            if i >= throttle_after_page:
                await asyncio.sleep(throttle_delay)

    _require_complete(collected, total, source=source, url=first_url)
    return pages, first_url


def parse_arcgis_tabular(
    pages: list[bytes],
    *,
    source: str,
    layer_config: LayerConfig,
    parser_version: int,
    numeric_cols: frozenset[str] | None = None,
    validate_required: bool = False,
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
            if validate_required:
                missing = layer_config["required_cols"] - row.keys()
                if missing:
                    raise ParseError(
                        source=source,
                        parser_version=parser_version,
                        reason=f"Colunas obrigatorias ausentes na pagina {i}: {sorted(missing)}",
                    )
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

    if not pages:
        empty = gpd.GeoDataFrame(columns=colunas_geo)
        empty = empty.set_geometry("geometry", crs="EPSG:4326")
        return empty

    gdfs = []
    for page_data in pages:
        gdf = parse_geojson_base(
            page_data,
            gpd,
            source=source,
            parser_version=parser_version,
            required_cols=required,
            max_features=None,
            output_cols_empty=colunas_geo,
            truncation_event=f"{source}_truncated",
            warn_null_geom=True,
        )
        if not gdf.empty:
            gdfs.append(gdf)

    if not gdfs:
        empty = gpd.GeoDataFrame(columns=colunas_geo)
        empty = empty.set_geometry("geometry", crs="EPSG:4326")
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
