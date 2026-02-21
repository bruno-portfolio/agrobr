"""Testes para agrobr.alt.sicar.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.alt.sicar import client
from agrobr.alt.sicar.client import _build_wfs_url, fetch_hits, fetch_imoveis
from agrobr.alt.sicar.models import PAGE_SIZE, WFS_BASE, WFS_VERSION
from agrobr.exceptions import ParseError


class TestBuildWfsUrl:
    def test_basic_url(self):
        url = _build_wfs_url("DF")
        assert "service=WFS" in url
        assert f"version={WFS_VERSION}" in url
        assert "request=GetFeature" in url
        assert "typeNames=sicar:sicar_imoveis_df" in url
        assert "outputFormat=csv" in url
        assert f"count={PAGE_SIZE}" in url
        assert "startIndex=0" in url

    def test_with_cql_filter(self):
        url = _build_wfs_url("MT", cql_filter="status_imovel='AT'")
        assert "CQL_FILTER=" in url
        assert "status_imovel" in url

    def test_with_pagination(self):
        url = _build_wfs_url("SP", count=5000, start_index=10000)
        assert "count=5000" in url
        assert "startIndex=10000" in url

    def test_result_type_hits(self):
        url = _build_wfs_url("BA", result_type="hits")
        assert "resultType=hits" in url

    def test_property_names_included(self):
        url = _build_wfs_url("GO")
        assert "propertyName=" in url
        assert "cod_imovel" in url
        assert "status_imovel" in url
        assert "area" in url

    def test_no_geometry_in_url(self):
        url = _build_wfs_url("PR")
        assert "geo_area" not in url
        assert "the_geom" not in url

    def test_base_url(self):
        url = _build_wfs_url("RS")
        assert url.startswith(WFS_BASE)

    def test_cql_filter_encoded(self):
        url = _build_wfs_url("SC", cql_filter="municipio ILIKE '%JOINVILLE%'")
        assert "%27" in url or "ILIKE" in url


class TestFetchHits:
    @pytest.mark.asyncio
    async def test_parses_number_matched(self):
        xml_response = (
            b'<?xml version="1.0"?><wfs:FeatureCollection numberMatched="42" numberReturned="0"/>'
        )
        with patch.object(client, "_fetch_url", new_callable=AsyncMock, return_value=xml_response):
            result = await fetch_hits("DF")
        assert result == 42

    @pytest.mark.asyncio
    async def test_parses_number_matched_no_quotes(self):
        xml_response = b"<wfs:FeatureCollection numberMatched=100 numberReturned=0/>"
        with patch.object(client, "_fetch_url", new_callable=AsyncMock, return_value=xml_response):
            result = await fetch_hits("MT")
        assert result == 100

    @pytest.mark.asyncio
    async def test_raises_on_missing_number_matched(self):
        xml_response = b"<wfs:FeatureCollection/>"
        with (
            patch.object(client, "_fetch_url", new_callable=AsyncMock, return_value=xml_response),
            pytest.raises(ParseError, match="numberMatched"),
        ):
            await fetch_hits("SP")

    @pytest.mark.asyncio
    async def test_with_cql_filter(self):
        xml_response = b'<wfs:FeatureCollection numberMatched="15"/>'
        mock_fetch = AsyncMock(return_value=xml_response)
        with patch.object(client, "_fetch_url", mock_fetch):
            result = await fetch_hits("BA", "status_imovel='AT'")
        assert result == 15
        call_url = mock_fetch.call_args[0][0]
        assert "resultType=hits" in call_url


class TestFetchImoveis:
    @pytest.mark.asyncio
    async def test_empty_results(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="0"/>'
        with patch.object(client, "_fetch_url", new_callable=AsyncMock, return_value=xml_hits):
            pages, url = await fetch_imoveis("DF")
        assert pages == []
        assert "sicar" in url

    @pytest.mark.asyncio
    async def test_single_page(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="5"/>'
        csv_data = b"cod_imovel,status_imovel\nFOO,AT\n"

        call_count = 0

        async def mock_fetch(url, *, base_delay=None):  # noqa: ARG001
            nonlocal call_count
            call_count += 1
            if "resultType=hits" in url:
                return xml_hits
            return csv_data

        with patch.object(client, "_fetch_url", side_effect=mock_fetch):
            pages, url = await fetch_imoveis("DF")

        assert len(pages) == 1
        assert pages[0] == csv_data

    @pytest.mark.asyncio
    async def test_multi_page(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="15000"/>'
        csv_page1 = b"cod_imovel,status_imovel\nFOO1,AT\n"
        csv_page2 = b"cod_imovel,status_imovel\nFOO2,PE\n"

        page_idx = 0

        async def mock_fetch(url, *, base_delay=None):  # noqa: ARG001
            nonlocal page_idx
            if "resultType=hits" in url:
                return xml_hits
            page_idx += 1
            return csv_page1 if page_idx == 1 else csv_page2

        with patch.object(client, "_fetch_url", side_effect=mock_fetch):
            pages, url = await fetch_imoveis("MT")

        assert len(pages) == 2

    @pytest.mark.asyncio
    async def test_progressive_delay_after_page_5(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="70000"/>'
        csv_data = b"cod_imovel,status_imovel\nFOO,AT\n"
        delays: list[float | None] = []

        async def mock_fetch(url, *, base_delay=None):
            if "resultType=hits" in url:
                return xml_hits
            delays.append(base_delay)
            return csv_data

        with patch.object(client, "_fetch_url", side_effect=mock_fetch):
            pages, url = await fetch_imoveis("BA")

        assert len(pages) == 7
        # Pages 0-4 have None delay, pages 5-6 have 2.0 delay
        assert delays[0] is None
        assert delays[4] is None
        assert delays[5] == 2.0
        assert delays[6] == 2.0

    @pytest.mark.asyncio
    async def test_with_cql_filter_passed_through(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="3"/>'
        csv_data = b"cod_imovel,status_imovel\nFOO,AT\n"
        fetched_urls: list[str] = []

        async def mock_fetch(url, *, base_delay=None):  # noqa: ARG001
            fetched_urls.append(url)
            if "resultType=hits" in url:
                return xml_hits
            return csv_data

        with patch.object(client, "_fetch_url", side_effect=mock_fetch):
            await fetch_imoveis("GO", "status_imovel='AT'")

        # Both hits and data request should have the filter
        for u in fetched_urls:
            assert "status_imovel" in u


class TestTimeout:
    def test_read_timeout_is_180s(self):
        from agrobr.alt.sicar.client import TIMEOUT

        assert TIMEOUT.read == 180.0
