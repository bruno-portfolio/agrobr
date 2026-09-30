from unittest.mock import AsyncMock, patch

import pytest

from agrobr.conab._serie_historica import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import make_mock_async_client, make_mock_response

_URL = "https://www.gov.br/conab/test"


_HEADERS = {"content-type": "application/vnd.ms-excel"}


def _resp(status_code: int = 200, *, content: bytes = b"xls-data", text: str = "<html></html>"):
    return make_mock_response(
        status_code,
        content=content,
        text=text,
        url=_URL,
        headers=_HEADERS,
    )


"""Testes de resiliência HTTP para agrobr.conab._serie_historica.client."""


class TestConabSerieHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_404_raises(self):
        resp_404 = _resp(404)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_404)

        with (
            patch(
                "agrobr.conab._serie_historica.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError, match="conab_serie_historica unavailable: .*404"),
        ):
            await client.download_xls("soja")
