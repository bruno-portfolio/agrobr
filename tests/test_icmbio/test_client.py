from __future__ import annotations

from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.icmbio.client import fetch_ucs, fetch_ucs_geo


class TestFetchUcs:
    @pytest.mark.asyncio
    async def test_successful_fetch(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            content, url = await fetch_ucs()
        assert len(content) >= 5000
        assert "ICMBio" in url

    @pytest.mark.asyncio
    async def test_404_raises(self):
        with (
            patch(
                "agrobr.icmbio.client.fetch_wfs",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="icmbio", url="", last_error="HTTP 404"),
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await fetch_ucs()

    @pytest.mark.asyncio
    async def test_too_small_raises(self):
        with (
            patch(
                "agrobr.icmbio.client.fetch_wfs",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(
                    source="icmbio", url="", last_error="WFS response too small"
                ),
            ),
            pytest.raises(SourceUnavailableError, match="too small"),
        ):
            await fetch_ucs()

    @pytest.mark.asyncio
    async def test_uf_filter_cql(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            _, url = await fetch_ucs(uf="MT")
        decoded = unquote(url)
        assert "uf LIKE '%MT%'" in decoded

    @pytest.mark.asyncio
    async def test_url_uses_current_property_names(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            _, url = await fetch_ucs()

        property_names = parse_qs(urlparse(url).query)["propertyName"][0].split(",")
        assert "sigla_cate" in property_names
        assert "uf" in property_names
        assert "siglacateg" not in property_names
        assert "ufabrang" not in property_names

    @pytest.mark.asyncio
    async def test_grupo_filter_cql(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            _, url = await fetch_ucs(grupo="PI")
        decoded = unquote(url)
        assert "grupouc='PI'" in decoded

    @pytest.mark.asyncio
    async def test_combined_filters(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            _, url = await fetch_ucs(uf="MT", grupo="PI")
        decoded = unquote(url)
        assert "uf LIKE '%MT%'" in decoded
        assert "grupouc='PI'" in decoded
        assert " AND " in decoded


class TestFetchUcsGeo:
    @pytest.mark.asyncio
    async def test_successful_fetch(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            content, url = await fetch_ucs_geo()
        assert len(content) >= 5000

    @pytest.mark.asyncio
    async def test_output_format_json(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            _, url = await fetch_ucs_geo()
        assert "outputFormat=application" in url

    @pytest.mark.asyncio
    async def test_geom_column_in_url(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            _, url = await fetch_ucs_geo()
        assert "propertyName=the_geom," in url

    @pytest.mark.asyncio
    async def test_no_cql_in_geo_url(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            _, url = await fetch_ucs_geo()
        assert "CQL_FILTER" not in url

    @pytest.mark.asyncio
    async def test_bbox_only_no_cql(self):
        with patch(
            "agrobr.icmbio.client.fetch_wfs", new_callable=AsyncMock, return_value=b"x" * 5000
        ):
            _, url = await fetch_ucs_geo(bbox=(-60.0, -15.0, -50.0, -10.0))
        assert "BBOX=" in url
        assert "CQL_FILTER" not in url
