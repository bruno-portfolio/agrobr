from __future__ import annotations

from unittest import mock

import httpx

from agrobr.exceptions import SourceUnavailableError
from agrobr.utils import geo
from tests import helpers


async def test_contagem_arcgis_em_lista_recusa_resposta(monkeypatch):
    resposta = helpers.make_mock_response(json_data=[{"count": 2}])
    obter = mock.AsyncMock(return_value=resposta)
    monkeypatch.setattr(httpx.AsyncClient, "get", obter)

    with helpers.levanta_exatamente(
        SourceUnavailableError, match="ArcGIS returned JSON that is not an object"
    ):
        await geo.fetch_arcgis_count(
            "https://fonte.test/FeatureServer/0", source="fonte", timeout=httpx.Timeout(2)
        )

    obter.assert_awaited_once()
