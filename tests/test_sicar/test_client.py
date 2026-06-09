"""Testes para agrobr.alt.sicar.client."""

from __future__ import annotations

import re
import ssl
import warnings
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.alt.sicar import client
from agrobr.alt.sicar.client import _build_wfs_url, fetch_hits, fetch_imoveis, fetch_imoveis_geo
from agrobr.alt.sicar.models import MAX_FEATURES_GEO, PAGE_SIZE, WFS_BASE, WFS_VERSION
from agrobr.exceptions import ParseError
from agrobr.utils.warnings import warn_once_reset


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
        with patch.object(client, "fetch_wfs", new_callable=AsyncMock, return_value=xml_response):
            result = await fetch_hits("DF")
        assert result == 42

    @pytest.mark.asyncio
    async def test_parses_number_matched_no_quotes(self):
        xml_response = b"<wfs:FeatureCollection numberMatched=100 numberReturned=0/>"
        with patch.object(client, "fetch_wfs", new_callable=AsyncMock, return_value=xml_response):
            result = await fetch_hits("MT")
        assert result == 100

    @pytest.mark.asyncio
    async def test_raises_on_missing_number_matched(self):
        xml_response = b"<wfs:FeatureCollection/>"
        with (
            patch.object(client, "fetch_wfs", new_callable=AsyncMock, return_value=xml_response),
            pytest.raises(ParseError, match="numberMatched"),
        ):
            await fetch_hits("SP")

    @pytest.mark.asyncio
    async def test_with_cql_filter(self):
        xml_response = b'<wfs:FeatureCollection numberMatched="15"/>'
        mock_fetch = AsyncMock(return_value=xml_response)
        with patch.object(client, "fetch_wfs", mock_fetch):
            result = await fetch_hits("BA", "status_imovel='AT'")
        assert result == 15
        call_url = mock_fetch.call_args[0][0]
        assert "resultType=hits" in call_url


class TestFetchImoveis:
    @pytest.mark.asyncio
    async def test_empty_results(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="0"/>'
        with patch.object(client, "fetch_wfs", new_callable=AsyncMock, return_value=xml_hits):
            pages, url = await fetch_imoveis("DF")
        assert pages == []
        assert "sicar" in url

    @pytest.mark.asyncio
    async def test_single_page(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="5"/>'
        csv_data = b"cod_imovel,status_imovel\nFOO,AT\n"

        call_count = 0

        async def mock_fetch(url, **_kwargs):
            nonlocal call_count
            call_count += 1
            if "resultType=hits" in url:
                return xml_hits
            return csv_data

        with patch.object(client, "fetch_wfs", side_effect=mock_fetch):
            pages, url = await fetch_imoveis("DF")

        assert len(pages) == 1
        assert pages[0] == csv_data

    @pytest.mark.asyncio
    async def test_multi_page(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="15000"/>'
        csv_page1 = b"cod_imovel,status_imovel\nFOO1,AT\n"
        csv_page2 = b"cod_imovel,status_imovel\nFOO2,PE\n"

        page_idx = 0

        async def mock_fetch(url, **_kwargs):
            nonlocal page_idx
            if "resultType=hits" in url:
                return xml_hits
            page_idx += 1
            return csv_page1 if page_idx == 1 else csv_page2

        with patch.object(client, "fetch_wfs", side_effect=mock_fetch):
            pages, url = await fetch_imoveis("MT")

        assert len(pages) == 2

    @pytest.mark.asyncio
    async def test_progressive_delay_after_page_5(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="70000"/>'
        csv_data = b"cod_imovel,status_imovel\nFOO,AT\n"
        delays: list[float | None] = []

        async def mock_fetch(url, **kwargs):
            if "resultType=hits" in url:
                return xml_hits
            delays.append(kwargs.get("base_delay"))
            return csv_data

        with patch.object(client, "fetch_wfs", side_effect=mock_fetch):
            pages, url = await fetch_imoveis("BA")

        assert len(pages) == 7
        assert delays[0] is None
        assert delays[4] is None
        assert delays[5] == 2.0
        assert delays[6] == 2.0

    @pytest.mark.asyncio
    async def test_with_cql_filter_passed_through(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="3"/>'
        csv_data = b"cod_imovel,status_imovel\nFOO,AT\n"
        fetched_urls: list[str] = []

        async def mock_fetch(url, **_kwargs):
            fetched_urls.append(url)
            if "resultType=hits" in url:
                return xml_hits
            return csv_data

        with patch.object(client, "fetch_wfs", side_effect=mock_fetch):
            await fetch_imoveis("GO", "status_imovel='AT'")

        for u in fetched_urls:
            assert "status_imovel" in u


class TestFetchImoveisGeo:
    @pytest.mark.asyncio
    async def test_successful_fetch(self):
        geojson = b'{"type":"FeatureCollection","features":[]}'
        with patch.object(client, "fetch_wfs", new_callable=AsyncMock, return_value=geojson):
            pages, url = await fetch_imoveis_geo("DF")
        assert pages == [geojson]
        assert "sicar" in url

    @pytest.mark.asyncio
    async def test_url_output_format_json(self):
        geojson = b'{"type":"FeatureCollection","features":[]}'
        mock_fetch = AsyncMock(return_value=geojson)
        with patch.object(client, "fetch_wfs", mock_fetch):
            await fetch_imoveis_geo("DF")
        call_url = mock_fetch.call_args[0][0]
        assert "outputFormat=application/json" in call_url

    @pytest.mark.asyncio
    async def test_url_contains_geom_column(self):
        geojson = b'{"type":"FeatureCollection","features":[]}'
        mock_fetch = AsyncMock(return_value=geojson)
        with patch.object(client, "fetch_wfs", mock_fetch):
            await fetch_imoveis_geo("DF")
        call_url = mock_fetch.call_args[0][0]
        assert "geo_area_imovel" in call_url

    @pytest.mark.asyncio
    async def test_url_max_features(self):
        geojson = b'{"type":"FeatureCollection","features":[]}'
        mock_fetch = AsyncMock(return_value=geojson)
        with patch.object(client, "fetch_wfs", mock_fetch):
            await fetch_imoveis_geo("MT")
        call_url = mock_fetch.call_args[0][0]
        assert f"count={MAX_FEATURES_GEO}" in call_url

    @pytest.mark.asyncio
    async def test_url_with_cql_filter(self):
        geojson = b'{"type":"FeatureCollection","features":[]}'
        mock_fetch = AsyncMock(return_value=geojson)
        with patch.object(client, "fetch_wfs", mock_fetch):
            await fetch_imoveis_geo("BA", cql_filter="status_imovel='AT'")
        call_url = mock_fetch.call_args[0][0]
        assert "CQL_FILTER=" in call_url
        assert "status_imovel" in call_url

    @pytest.mark.asyncio
    async def test_no_pagination(self):
        geojson = b'{"type":"FeatureCollection","features":[]}'
        mock_fetch = AsyncMock(return_value=geojson)
        with patch.object(client, "fetch_wfs", mock_fetch):
            await fetch_imoveis_geo("SP")
        assert mock_fetch.call_count == 1


def _geo_paginated_mock(numbers_matched: int, geojson: bytes):
    """Side_effect de fetch_wfs: responde hits e registra o count de cada pagina pedida."""
    xml_hits = f'<wfs:FeatureCollection numberMatched="{numbers_matched}"/>'.encode()
    counts: list[int] = []

    async def mock_fetch(url, **_kwargs):
        if "resultType=hits" in url:
            return xml_hits
        m = re.search(r"count=(\d+)", url)
        assert m is not None
        counts.append(int(m.group(1)))
        return geojson

    return mock_fetch, counts


class TestFetchImoveisGeoPaginated:
    GEOJSON = b'{"type":"FeatureCollection","features":[]}'

    @pytest.mark.asyncio
    async def test_max_features_none_paginates_using_hits_total(self):
        mock_fetch, counts = _geo_paginated_mock(12_345, self.GEOJSON)
        with patch.object(client, "fetch_wfs", side_effect=mock_fetch):
            pages, url = await fetch_imoveis_geo("MT", max_features=None)

        assert len(pages) == 2
        assert "sicar" in url

    @pytest.mark.asyncio
    async def test_max_features_greater_than_page_size_paginates(self):
        mock_fetch, counts = _geo_paginated_mock(50_000, self.GEOJSON)
        with patch.object(client, "fetch_wfs", side_effect=mock_fetch):
            pages, _url = await fetch_imoveis_geo("MT", max_features=15_000)

        assert len(pages) == 2
        assert sum(counts) == 15_000

    @pytest.mark.asyncio
    async def test_max_features_equal_page_size_fetches_single_page(self):
        mock_fetch = AsyncMock(return_value=self.GEOJSON)
        with patch.object(client, "fetch_wfs", mock_fetch):
            pages, _url = await fetch_imoveis_geo("MT", max_features=PAGE_SIZE)

        assert len(pages) == 1
        assert mock_fetch.call_count == 1
        call_url = mock_fetch.call_args[0][0]
        assert f"count={PAGE_SIZE}" in call_url
        assert "resultType=hits" not in call_url

    @pytest.mark.asyncio
    async def test_max_features_page_size_plus_one_paginates(self):
        mock_fetch, counts = _geo_paginated_mock(20_000, self.GEOJSON)
        with patch.object(client, "fetch_wfs", side_effect=mock_fetch):
            pages, _url = await fetch_imoveis_geo("MT", max_features=PAGE_SIZE + 1)

        assert len(pages) == 2
        assert counts == [PAGE_SIZE, 1]

    @pytest.mark.asyncio
    async def test_total_zero_returns_no_pages(self):
        xml_hits = b'<wfs:FeatureCollection numberMatched="0"/>'
        with patch.object(client, "fetch_wfs", new_callable=AsyncMock, return_value=xml_hits):
            pages, url = await fetch_imoveis_geo("DF", max_features=None)

        assert pages == []
        assert "sicar" in url

    @pytest.mark.asyncio
    async def test_last_chunk_uses_min_total_and_max_features(self):
        mock_fetch, counts = _geo_paginated_mock(10_500, self.GEOJSON)
        with patch.object(client, "fetch_wfs", side_effect=mock_fetch):
            pages, _url = await fetch_imoveis_geo("MT", max_features=50_000)

        assert len(pages) == 2
        assert counts == [PAGE_SIZE, 500]


class TestTimeout:
    def test_read_timeout_is_180s(self):
        from agrobr.alt.sicar.client import TIMEOUT

        assert TIMEOUT.read == 180.0


class TestSSLContext:
    """SSL: geoserver.car.gov.br envia o root Sectigo R46 dentro da cadeia.

    Truststores sem esse root (certifi pre-2024, macOS Python 3.13 via uv)
    rejeitam com `CERTIFICATE_VERIFY_FAILED: self-signed certificate in
    certificate chain`. Por isso o client usa verify=False com warning
    estruturado one-shot por sessao.
    """

    def test_ssl_ctx_has_verify_disabled(self):
        assert client._ssl_ctx.verify_mode == ssl.CERT_NONE
        assert client._ssl_ctx.check_hostname is False

    def test_ssl_ctx_keeps_seclevel_1(self):
        assert "AES256-GCM-SHA384" in {c["name"] for c in client._ssl_ctx.get_ciphers()}

    def test_make_session_uses_ssl_ctx(self):
        warn_once_reset("sicar_ssl_verify_off")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            session = client.make_session()
        try:
            assert session._transport._pool._ssl_context is client._ssl_ctx  # type: ignore[attr-defined]
        finally:
            warn_once_reset("sicar_ssl_verify_off")

    def test_warn_once_emitted_on_first_call(self):
        warn_once_reset("sicar_ssl_verify_off")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client.make_session()
        ssl_warns = [w for w in caught if "verify=False" in str(w.message)]
        assert len(ssl_warns) == 1
        assert "Sectigo" in str(ssl_warns[0].message) or "verify=False" in str(ssl_warns[0].message)
        warn_once_reset("sicar_ssl_verify_off")

    def test_warn_once_not_duplicated_in_subsequent_calls(self):
        warn_once_reset("sicar_ssl_verify_off")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client.make_session()
            client.make_session()
            client.make_session()
        ssl_warns = [w for w in caught if "verify=False" in str(w.message)]
        assert len(ssl_warns) == 1
        warn_once_reset("sicar_ssl_verify_off")
