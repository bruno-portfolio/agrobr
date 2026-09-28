from __future__ import annotations

from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from agrobr.icmbio.client import _build_cql_filters, fetch_ucs, fetch_ucs_count


class TestFetchUcs:
    @pytest.mark.asyncio
    async def test_bbox_and_uf_share_one_filter(self):
        with patch("agrobr.icmbio.client.fetch_wfs", AsyncMock(return_value=b"payload")):
            _, url = await fetch_ucs(uf="MT", bbox=(-58.0, -18.0, -52.0, -10.0))
            _, count_url = await fetch_ucs_count(uf="MT", bbox=(-58.0, -18.0, -52.0, -10.0))
        query = parse_qs(urlparse(url).query)
        count_query = parse_qs(urlparse(count_url).query)
        assert "BBOX" not in query
        assert query["CQL_FILTER"] == [
            "uf LIKE '%MT%' AND BBOX(the_geom,-58.0,-18.0,-52.0,-10.0,'EPSG:4674')"
        ]
        assert count_query["CQL_FILTER"] == query["CQL_FILTER"]
        assert count_query["resultType"] == ["hits"]
        assert "maxFeatures" not in count_query
        assert "outputFormat" not in count_query
        assert "propertyName" not in count_query

    def test_cql_escapes_filter_literals(self):
        cql = _build_cql_filters(uf="M'T", grupo="P'I", bioma="Cerrado'")

        assert cql is not None
        assert "uf LIKE '%M''T%'" in cql
        assert "grupouc='P''I'" in cql
        assert "biomas ILIKE '%Cerrado''%'" in cql
