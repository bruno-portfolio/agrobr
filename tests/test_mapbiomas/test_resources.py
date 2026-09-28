from __future__ import annotations

import hashlib
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.mapbiomas import client

CONFIRMATION = Path(__file__).parent / "fixtures/drive_collection11_confirmation.html"


def _archive(xlsx: bytes, *, member: str = constants.MAPBIOMAS_MUNICIPAL_MEMBER_11) -> bytes:
    output = BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(member, xlsx)
    return output.getvalue()


def test_membro_municipal_rejeita_nomes_duplicados():
    output = BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(constants.MAPBIOMAS_MUNICIPAL_MEMBER_11, b"PK\x03\x04" + b"x" * 10000)
        with pytest.warns(UserWarning, match="Duplicate name"):
            archive.writestr(constants.MAPBIOMAS_MUNICIPAL_MEMBER_11, b"PK\x03\x04" + b"y" * 10000)
    with pytest.raises(SourceUnavailableError):
        client._municipal_member(output.getvalue(), "https://example.test/data.zip")


@pytest.mark.parametrize("collection", [True, 11.0, "11", [], 9])
def test_url_rejeita_colecao_invalida(collection: object):
    with pytest.raises(InvalidParameterError):
        client._build_xlsx_url("BIOME_STATE", collection)


@pytest.mark.parametrize("level", [None, [], "municipal", "invalid_MUNICIPALITY_suffix"])
def test_url_rejeita_nivel_invalido(level: object):
    with pytest.raises(InvalidParameterError):
        client._build_xlsx_url(level, 11)


@pytest.mark.asyncio
async def test_recurso_preserva_zip_xlsx_confirmacao_redirecionamento_e_headers():
    xlsx = b"PK\x03\x04" + b"x" * 10000
    zipped = _archive(xlsx)
    html = CONFIRMATION.read_bytes()
    requested: list[httpx.Request] = []
    initial = client._build_xlsx_url("BIOME_STATE_MUNICIPALITY", 11)
    redirected = "https://drive.google.com/download?public=1"

    def respond(request: httpx.Request) -> httpx.Response:
        requested.append(request)
        if str(request.url) == initial:
            return httpx.Response(303, headers={"location": redirected}, request=request)
        if str(request.url) == redirected:
            return httpx.Response(
                200,
                content=html,
                headers={"content-type": "TEXT/HTML; charset=utf-8"},
                request=request,
            )
        assert request.url.host == "drive.usercontent.google.com"
        return httpx.Response(
            200,
            content=zipped,
            headers={
                "content-type": "application/zip",
                "etag": '"published-etag"',
                "last-modified": "Mon, 07 Sep 2026 00:00:00 GMT",
            },
            request=request,
        )

    async_client = httpx.AsyncClient
    with patch.object(
        client.httpx,
        "AsyncClient",
        side_effect=lambda **kwargs: async_client(transport=httpx.MockTransport(respond), **kwargs),
    ):
        acquired = await client.fetch_biome_state_municipality_bundle(11)
    assert len(requested) == 3
    assert acquired.content == xlsx
    assert acquired.source_url == initial
    assert acquired.resource.sha256 == hashlib.sha256(zipped).hexdigest()
    assert acquired.resource.size_bytes == len(zipped)
    assert acquired.resource.content_type == "application/zip"
    assert acquired.resource.etag == '"published-etag"'
    assert acquired.resource.last_modified == "Mon, 07 Sep 2026 00:00:00 GMT"
    assert acquired.resource.fetched_at.utcoffset().total_seconds() == 0
    assert acquired.confirmation is not None
    assert acquired.confirmation.sha256 == hashlib.sha256(html).hexdigest()
    assert acquired.confirmation.final_url == redirected
    assert acquired.confirmation.redirects[0].status_code == 303
    assert acquired.confirmation.redirects[0].url == initial
    assert acquired.member is not None
    assert acquired.member.name == constants.MAPBIOMAS_MUNICIPAL_MEMBER_11
    assert acquired.member.sha256 == hashlib.sha256(xlsx).hexdigest()
    assert acquired.member.size_bytes == len(xlsx)
    assert acquired.member.sha256 != acquired.resource.sha256
    provenance = acquired.provenance()
    assert "content" not in provenance
    assert provenance["resource"]["fetched_at"].endswith("Z")
    assert provenance["member"]["sha256"] == acquired.member.sha256
    provenance["resource"]["etag"] = "changed"
    assert acquired.resource.etag == '"published-etag"'
