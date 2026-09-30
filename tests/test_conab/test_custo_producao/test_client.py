from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.conab._custo_producao import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import make_mock_async_client, make_mock_response

_BASE = client.BASE_URL


_URL = "https://www.gov.br/conab/test"
_HEADERS = {"content-type": "text/html"}


def _resp(status_code: int = 200, *, text: str = "<html></html>", content: bytes = b"xlsx-data"):
    return make_mock_response(
        status_code,
        text=text,
        content=content,
        url=_URL,
        headers=_HEADERS,
    )


class TestConabCustoEmptyResponse:
    @pytest.mark.asyncio
    async def test_all_tabs_empty_fallback_to_main_page(self):
        resp_ok = _resp(200, text="<html>main page</html>")
        call_count = 0

        async def side_effect(_url: str, **_kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count <= len(client._TAB_SLUGS):
                raise httpx.HTTPError("tab failed")
            return resp_ok

        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=side_effect)

        with patch(
            "agrobr.conab._custo_producao.client.httpx.AsyncClient", return_value=mock_client
        ):
            result = await client.fetch_custos_page()

        assert "main page" in result


class TestCrawlFolder:
    @pytest.mark.asyncio
    async def test_strips_view_suffix(self):
        folder_html = (
            f'<a href="{_BASE}/milho_2024_GO.xls/view">Milho 2024/25 GO</a>'
            f'<a href="{_BASE}/milho_2024_MT.xls/view">Milho 2024/25 MT</a>'
        )
        resp = _resp(200, text=folder_html)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch(
            "agrobr.conab._custo_producao.client.httpx.AsyncClient",
            return_value=mock_client,
        ):
            links = await client._crawl_folder(f"{_BASE}/milho")

        assert len(links) == 2
        for link in links:
            assert not link["url"].endswith("/view")
            assert link["url"].endswith(".xls")

    @pytest.mark.asyncio
    async def test_graceful_error(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        with patch(
            "agrobr.conab._custo_producao.client.httpx.AsyncClient",
            return_value=mock_client,
        ):
            links = await client._crawl_folder(f"{_BASE}/milho")

        assert links == []


class TestExtractFolderUrls:
    def test_detects_folder_links(self):
        html = (
            '<a href="/pt-br/arquivos-custo-de-producao/milho/">Milho</a>'
            '<a href="/pt-br/arquivos-custo-de-producao/arroz">Arroz</a>'
        )
        folders = client._extract_folder_urls(html)
        assert len(folders) == 2
        assert any("milho" in f for f in folders)
        assert any("arroz" in f for f in folders)

    def test_ignores_xls_files(self):
        html = '<a href="/pt-br/arquivos-custo-de-producao/soja_2024.xls">Soja</a>'
        folders = client._extract_folder_urls(html)
        assert folders == []

    def test_ignores_extensionless_serie_historica(self):
        html = '<a href="/pt-br/arquivos-custo-de-producao/serie-historica-custos-soja">Série</a>'
        folders = client._extract_folder_urls(html)
        assert folders == []


class TestConabCustoDownloadXlsx:
    @pytest.mark.asyncio
    async def test_relative_url_prepends_base(self):
        resp = _resp(200, content=b"xlsx")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch(
            "agrobr.conab._custo_producao.client.httpx.AsyncClient", return_value=mock_client
        ):
            await client.download_xlsx("/conab/test.xlsx")

        call_url = mock_client.get.call_args[0][0]
        assert call_url.startswith("https://www.gov.br")


class TestParseLinksRegex:
    def test_parse_links_matches_xls(self):
        html = f'<a href="{_BASE}/soja_2024.xls">Soja 2024/25 MT</a>'
        links = client.parse_links_from_html(html)
        assert len(links) == 1
        assert links[0]["url"].endswith(".xls")


class TestConabCustoHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_403_on_download_raises(self):
        resp_403 = _resp(403)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_403)

        with (
            patch(
                "agrobr.conab._custo_producao.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError, match="conab_custo"),
        ):
            await client.download_xlsx("https://www.gov.br/test.xlsx")

    @pytest.mark.asyncio
    async def test_http_500_on_page_raises(self):
        resp_500 = _resp(500)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        with (
            patch(
                "agrobr.conab._custo_producao.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError, match="^conab_custo unavailable: .*500"),
        ):
            await client.fetch_custos_page()


class TestEnrichLinkHints:
    def test_no_hints_when_absent(self):
        link = {"url": "http://x/test.xls", "text": "planilha custos"}
        client._enrich_link_hints(link)
        assert "safra_hint" not in link
        assert "uf_hint" not in link

    def test_safra_and_uf_hints(self):
        link = {"url": "http://x/test.xls", "text": "Soja 2024/25 MT"}
        client._enrich_link_hints(link)
        assert link["safra_hint"] == "2024/25"
        assert link["uf_hint"] == "MT"
