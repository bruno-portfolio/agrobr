from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.conab.progresso.client import (
    _extract_plantio_link,
    _extract_week_links,
    fetch_xlsx_semanal,
    list_semanas,
)
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import (
    RETRY_SLEEP,
    make_mock_async_client,
    make_mock_response,
)

PATCH_CLIENT = "agrobr.conab.progresso.client.httpx.AsyncClient"


def _week_page_html(links: list[tuple[str, str]]) -> str:
    anchors = "\n".join(f'<a href="{href}">{text}</a>' for text, href in links)
    return f"<html><body>{anchors}</body></html>"


def _plantio_page_html(xlsx_href: str) -> str:
    return f'<html><body><a href="{xlsx_href}">Plantio e Colheita</a></body></html>'


class TestExtractWeekLinks:
    def test_ignores_non_matching_links(self):
        html = '<html><body><a href="/other">Acompanhamento</a></body></html>'
        result = _extract_week_links(html)
        assert result == []


class TestExtractPlantioLink:
    def test_returns_none_when_no_link(self):
        html = '<html><body><a href="/other">Other</a></body></html>'
        result = _extract_plantio_link(html)
        assert result is None


@pytest.mark.asyncio()
class TestListSemanas:
    async def test_stops_on_non_200(self):
        page1_html = _week_page_html(
            [
                ("Acompanhamento P1", "/pt-br/acompanhamento-das-lavouras/p1"),
            ]
        )
        error_html = _week_page_html(
            [
                ("Acompanhamento P2", "/pt-br/acompanhamento-das-lavouras/p2"),
            ]
        )
        resp1 = make_mock_response(200, text=page1_html)
        resp2 = make_mock_response(404, text=error_html)
        resp3 = make_mock_response(200, text="<html><body>No links</body></html>")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp1, resp2, resp3])

        with (
            patch(PATCH_CLIENT, return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await list_semanas(max_pages=3)

        assert [title for title, _ in result] == ["Acompanhamento P1"]

    async def test_stops_on_empty_page(self):
        page1_html = _week_page_html(
            [
                ("Acompanhamento P1", "/pt-br/acompanhamento-das-lavouras/p1"),
            ]
        )
        resp1 = make_mock_response(200, text=page1_html)
        resp2 = make_mock_response(200, text="<html><body>No links</body></html>")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp1, resp2])

        with (
            patch(PATCH_CLIENT, return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await list_semanas(max_pages=3)

        assert len(result) == 1


@pytest.mark.asyncio()
class TestFetchXlsxSemanal:
    async def test_invalid_signature_raises(self):
        week_html = _plantio_page_html("https://cdn.com/plantio_colheita.xlsx")
        resp_week = make_mock_response(200, text=week_html)
        resp_xlsx = make_mock_response(
            200,
            content=b"tiny",
            headers={"content-type": "text/html"},
        )
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp_week, resp_xlsx])

        with (
            patch(PATCH_CLIENT, return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError, match="assinatura"),
        ):
            await fetch_xlsx_semanal("https://example.com/week1")
