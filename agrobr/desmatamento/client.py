from __future__ import annotations

from urllib.parse import quote

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status

from .models import (
    DETER_COLUNAS_WFS_AMZ,
    DETER_COLUNAS_WFS_CERRADO,
    DETER_LAYERS,
    DETER_WORKSPACES,
    PRODES_COLUNAS_WFS,
    PRODES_LAYER,
    PRODES_WORKSPACES,
)

logger = structlog.get_logger()

GEOSERVER_BASE = "https://terrabrasilis.dpi.inpe.br/geoserver"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=120.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {"User-Agent": "agrobr (https://github.com/bruno-portfolio/agrobr)"}

MAX_FEATURES_PER_REQUEST = 50000


def _build_wfs_url(
    workspace: str,
    layer: str,
    property_names: list[str],
    cql_filter: str | None = None,
    max_features: int = MAX_FEATURES_PER_REQUEST,
) -> str:
    props = ",".join(property_names)
    url = (
        f"{GEOSERVER_BASE}/{workspace}/ows"
        f"?service=WFS&version=1.0.0&request=GetFeature"
        f"&typeName={workspace}:{layer}"
        f"&outputFormat=csv"
        f"&propertyName={props}"
        f"&maxFeatures={max_features}"
    )
    if cql_filter:
        url += f"&CQL_FILTER={quote(cql_filter)}"
    return url


async def _fetch_url(url: str) -> bytes:
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        logger.debug("desmatamento_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="desmatamento",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source="desmatamento", url=url, last_error="HTTP 404")

        response.raise_for_status()
        return response.content


async def fetch_prodes(
    bioma: str,
    ano: int | None = None,
    uf: str | None = None,
) -> tuple[bytes, str]:
    workspace = PRODES_WORKSPACES.get(bioma)
    if not workspace:
        raise SourceUnavailableError(
            source="desmatamento",
            url="",
            last_error=f"Bioma PRODES nao suportado: {bioma}",
        )

    filters: list[str] = []
    if ano is not None:
        filters.append(f"year={ano}")
    if uf is not None:
        estado = _uf_to_estado(uf)
        if estado:
            filters.append(f"state='{estado}'")

    cql = " AND ".join(filters) if filters else None
    url = _build_wfs_url(workspace, PRODES_LAYER, PRODES_COLUNAS_WFS, cql)
    content = await _fetch_url(url)
    logger.info("desmatamento_prodes_csv", url=url, size=len(content), bioma=bioma)
    return content, url


async def fetch_deter(
    bioma: str,
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
) -> tuple[bytes, str]:
    workspace = DETER_WORKSPACES.get(bioma)
    layer = DETER_LAYERS.get(bioma)
    if not workspace or not layer:
        raise SourceUnavailableError(
            source="desmatamento",
            url="",
            last_error=f"Bioma DETER nao suportado: {bioma}",
        )

    cols = DETER_COLUNAS_WFS_AMZ if bioma == "Amazônia" else DETER_COLUNAS_WFS_CERRADO

    filters: list[str] = []
    if uf is not None:
        filters.append(f"uf='{uf.upper()}'")
    if data_inicio is not None:
        filters.append(f"view_date>='{data_inicio}'")
    if data_fim is not None:
        filters.append(f"view_date<='{data_fim}'")

    cql = " AND ".join(filters) if filters else None
    url = _build_wfs_url(workspace, layer, cols, cql)
    content = await _fetch_url(url)
    logger.info("desmatamento_deter_csv", url=url, size=len(content), bioma=bioma)
    return content, url


_UF_TO_ESTADO: dict[str, str] = {
    v: k
    for k, v in {
        "ACRE": "AC",
        "ALAGOAS": "AL",
        "AMAPÁ": "AP",
        "AMAZONAS": "AM",
        "BAHIA": "BA",
        "CEARÁ": "CE",
        "DISTRITO FEDERAL": "DF",
        "ESPÍRITO SANTO": "ES",
        "GOIÁS": "GO",
        "MARANHÃO": "MA",
        "MATO GROSSO": "MT",
        "MATO GROSSO DO SUL": "MS",
        "MINAS GERAIS": "MG",
        "PARÁ": "PA",
        "PARAÍBA": "PB",
        "PARANÁ": "PR",
        "PERNAMBUCO": "PE",
        "PIAUÍ": "PI",
        "RIO DE JANEIRO": "RJ",
        "RIO GRANDE DO NORTE": "RN",
        "RIO GRANDE DO SUL": "RS",
        "RONDÔNIA": "RO",
        "RORAIMA": "RR",
        "SANTA CATARINA": "SC",
        "SÃO PAULO": "SP",
        "SERGIPE": "SE",
        "TOCANTINS": "TO",
    }.items()
}


def _uf_to_estado(uf: str) -> str | None:
    return _UF_TO_ESTADO.get(uf.upper())
