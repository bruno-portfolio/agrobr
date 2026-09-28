from __future__ import annotations

import hashlib
import zipfile
import zlib
from io import BytesIO
from urllib.parse import parse_qs, urlencode

import httpx
import structlog
from bs4 import BeautifulSoup

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

from . import models, resources

logger = structlog.get_logger()

DATAVERSE_BASE = constants.URLS[constants.Fonte.MAPBIOMAS]["dataverse"]
BIOME_STATE_FILE_ID = constants.URLS[constants.Fonte.MAPBIOMAS]["biome_state_file_id"]
BIOME_STATE_MUNICIPALITY_FILE_ID = constants.URLS[constants.Fonte.MAPBIOMAS][
    "biome_state_municipality_file_id"
]

TIMEOUT = get_timeout(read=120.0)


def _build_xlsx_url(nivel: str, colecao: int = models.COLECAO_ATUAL) -> str:
    if (
        isinstance(colecao, bool)
        or not isinstance(colecao, int)
        or colecao not in models.ANOS_FINAIS
    ):
        raise InvalidParameterError(f"Coleção {colecao} não suportada")
    if not isinstance(nivel, str) or nivel.upper() not in {
        "BIOME_STATE",
        "BIOME_STATE_MUNICIPALITY",
    }:
        raise InvalidParameterError("Nível deve ser BIOME_STATE ou BIOME_STATE_MUNICIPALITY")
    municipal = nivel.upper() == "BIOME_STATE_MUNICIPALITY"
    if colecao == 11:
        key = "biome_state_municipality_collection_11" if municipal else "biome_state_collection_11"
        return constants.URLS[constants.Fonte.MAPBIOMAS][key]
    if municipal:
        return f"{DATAVERSE_BASE}/{BIOME_STATE_MUNICIPALITY_FILE_ID}"
    return f"{DATAVERSE_BASE}/{BIOME_STATE_FILE_ID}?format=original"


def _drive_confirmation_url(response: httpx.Response, source_url: str) -> str:
    form = BeautifulSoup(response.text, "html.parser").find("form", id="download-form")
    action = "https://drive.usercontent.google.com/download"
    if form is not None and form.get("action") == action:
        fields = [
            field
            for field in form.find_all("input")
            if field.get("name") in {"id", "export", "confirm", "uuid"}
        ]
        params = {str(field.get("name")): str(field.get("value", "")) for field in fields}
        original_id = parse_qs(httpx.URL(source_url).query.decode()).get("id", [None])[0]
        if (
            len(params) == len(fields)
            and params.get("id") == original_id
            and params.get("export") == "download"
            and params.get("confirm") == "t"
        ):
            return f"{action}?{urlencode(params)}"
    raise SourceUnavailableError(
        source="mapbiomas", url=source_url, last_error="Confirmação de download do Drive inválida"
    )


def _municipal_member(content: bytes, url: str) -> tuple[bytes, resources.WorkbookMember]:
    filename = constants.MAPBIOMAS_MUNICIPAL_MEMBER_11
    try:
        with zipfile.ZipFile(BytesIO(content)) as archive:
            entries = archive.infolist()
            if len(entries) != 1 or entries[0].filename != filename:
                raise KeyError(filename)
            entry = entries[0]
            if entry.flag_bits & 1:
                raise zipfile.BadZipFile("Membro criptografado")
            xlsx = io_utils.read_zip_member(archive, entry, source="mapbiomas", url=url)
    except (zipfile.BadZipFile, zlib.error, EOFError, KeyError, NotImplementedError) as exc:
        raise SourceUnavailableError(
            source="mapbiomas",
            url=url,
            last_error=f"Arquivo municipal esperado ausente ou inválido: {filename}",
        ) from exc
    io_utils.validate_download(
        xlsx, kinds=("xlsx",), source="mapbiomas", url=url, min_size=constants.MIN_XLSX_SIZE
    )
    member = resources.WorkbookMember(
        name=filename,
        sha256=hashlib.sha256(xlsx).hexdigest(),
        crc32=f"{entry.CRC:08x}",
        compressed_size_bytes=entry.compress_size,
        size_bytes=entry.file_size,
    )
    return xlsx, member


async def _get_resource(client: httpx.AsyncClient, url: str) -> httpx.Response:
    logger.debug("mapbiomas_request", url=url)
    response = await retry_on_status(lambda: client.get(url), source="mapbiomas")
    if response.status_code == 404:
        raise SourceUnavailableError(source="mapbiomas", url=url, last_error="HTTP 404")
    responses.raise_for_status(response, source="mapbiomas")
    return response


async def _fetch_bundle(url: str) -> resources.WorkbookAcquisition:
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as http_client:
        response = await _get_resource(http_client, url)
        confirmation = None
        requested_url = url
        if (
            httpx.URL(url).host == "drive.google.com"
            and "text/html" in response.headers.get("content-type", "").lower()
        ):
            requested_url = _drive_confirmation_url(response, url)
            confirmation = resources.HTTPResource.from_response(response, url)
            response = await _get_resource(http_client, requested_url)
        content = response.content
        io_utils.validate_download(
            content,
            kinds=("xlsx",),
            source="mapbiomas",
            url=url,
            min_size=constants.MIN_XLSX_SIZE,
        )
        return resources.WorkbookAcquisition(
            content=content,
            source_url=url,
            resource=resources.HTTPResource.from_response(response, requested_url),
            confirmation=confirmation,
        )


async def fetch_biome_state_bundle(
    colecao: int = models.COLECAO_ATUAL,
) -> resources.WorkbookAcquisition:
    acquired = await _fetch_bundle(_build_xlsx_url("BIOME_STATE", colecao))
    logger.info("mapbiomas_xlsx_found", source="mapbiomas", size=len(acquired.content))
    return acquired


async def fetch_biome_state_municipality_bundle(
    colecao: int = models.COLECAO_ATUAL,
) -> resources.WorkbookAcquisition:
    acquired = await _fetch_bundle(_build_xlsx_url("BIOME_STATE_MUNICIPALITY", colecao))
    if colecao == 11:
        acquired.content, acquired.member = _municipal_member(acquired.content, acquired.source_url)
    logger.info("mapbiomas_xlsx_found", source="mapbiomas", size=len(acquired.content))
    return acquired
