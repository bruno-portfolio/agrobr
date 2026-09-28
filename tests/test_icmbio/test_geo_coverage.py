from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from agrobr.exceptions import ParseError, ResourceLimitError
from agrobr.icmbio import api


async def test_geo_acima_do_limite_rejeita_antes_do_download(monkeypatch):
    body = b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" numberOfFeatures="501"/>'
    count = AsyncMock(return_value=(body, "https://example.test/hits"))
    fetch = AsyncMock()
    monkeypatch.setattr(api, "check_geopandas", Mock())
    monkeypatch.setattr(api.client, "fetch_ucs_count", count)
    monkeypatch.setattr(api.client, "fetch_ucs_geo", fetch)
    with pytest.raises(ResourceLimitError, match="501"):
        await api.ucs_geo()
    fetch.assert_not_awaited()


async def test_geo_contagem_divergente_rejeita_resultado_parcial(monkeypatch):
    body = b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" numberOfFeatures="2"/>'
    monkeypatch.setattr(api, "check_geopandas", Mock())
    monkeypatch.setattr(
        api.client, "fetch_ucs_count", AsyncMock(return_value=(body, "https://example.test/hits"))
    )
    monkeypatch.setattr(
        api.client, "fetch_ucs_geo", AsyncMock(return_value=(b"{}", "https://example.test/geo"))
    )
    monkeypatch.setattr(api.parser, "parse_ucs_geojson", lambda _: pd.DataFrame({"id": [1]}))
    with pytest.raises(ParseError, match="hits=2, recebidas=1"):
        await api.ucs_geo()
