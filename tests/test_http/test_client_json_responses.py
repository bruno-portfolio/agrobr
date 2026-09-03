from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.alt.antt_pedagio import client as antt_client
from agrobr.b3 import client as b3_client
from agrobr.bcb import client as bcb_client
from agrobr.bcb import focus_client, ptax_client, sgs_client
from agrobr.cftc import client as cftc_client
from agrobr.comtrade import client as comtrade_client
from agrobr.conab.ceasa import client as ceasa_client
from agrobr.exceptions import SourceUnavailableError
from agrobr.imea import client as imea_client
from agrobr.mapbiomas_alerta import client as alerta_client
from agrobr.nasa_power import client as nasa_client
from agrobr.usda import client as usda_client
from agrobr.utils import geo
from agrobr.zarc import client as zarc_client


def _html_response() -> httpx.Response:
    return httpx.Response(
        200,
        text="<html>Service Unavailable</html>",
        headers={"content-type": "text/html"},
        request=httpx.Request("GET", "https://example.test/data"),
    )


class TestClientJsonResponses:
    @pytest.mark.asyncio
    async def test_bcb_odata_html(self):
        with (
            patch.object(
                bcb_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await bcb_client._fetch_odata("CusteioRegiaoUFProduto")

    @pytest.mark.asyncio
    async def test_bcb_sgs_html(self):
        with (
            patch.object(
                sgs_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await sgs_client.fetch_sgs(1)

    @pytest.mark.asyncio
    async def test_bcb_focus_html(self):
        with (
            patch.object(
                focus_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await focus_client.fetch_focus("IPCA")

    @pytest.mark.asyncio
    async def test_bcb_ptax_html(self):
        with (
            patch.object(
                ptax_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await ptax_client.fetch_ptax(data="01/01/2024")

    @pytest.mark.asyncio
    async def test_zarc_html(self):
        with (
            patch.object(
                zarc_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await zarc_client.discover_resources()

    @pytest.mark.asyncio
    async def test_nasa_power_html(self):
        with (
            patch.object(
                nasa_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await nasa_client._get_json({})

    @pytest.mark.asyncio
    async def test_usda_html(self):
        with (
            patch.object(
                usda_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await usda_client._fetch_json("https://example.test/usda", "key")

    @pytest.mark.asyncio
    async def test_cftc_html(self):
        with (
            patch.object(
                cftc_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await cftc_client.fetch_cot(["001602"])

    @pytest.mark.asyncio
    async def test_mapbiomas_alerta_html(self):
        with (
            patch.object(
                alerta_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await alerta_client._graphql_request("query", {}, token="token")

    @pytest.mark.asyncio
    async def test_comtrade_html(self):
        with (
            patch.object(
                comtrade_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await comtrade_client._fetch_chunks(
                AsyncMock(),
                "https://example.test/comtrade",
                {},
                {},
                ["2024"],
            )

    @pytest.mark.asyncio
    async def test_conab_ceasa_html(self):
        with (
            patch.object(
                ceasa_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await ceasa_client.fetch_precos()

    @pytest.mark.asyncio
    async def test_b3_html(self):
        with (
            patch.object(
                b3_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await b3_client.fetch_posicoes_abertas("2025-12-19")

    @pytest.mark.asyncio
    async def test_imea_html(self):
        with (
            patch.object(
                imea_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await imea_client._fetch_json("https://example.test/imea")

    @pytest.mark.asyncio
    async def test_arcgis_html(self):
        with (
            patch.object(
                geo,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await geo.fetch_arcgis_count(
                "https://example.test/arcgis",
                source="arcgis_test",
                timeout=httpx.Timeout(1),
            )

    @pytest.mark.asyncio
    async def test_antt_pedagio_html(self):
        with (
            patch.object(
                antt_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_html_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await antt_client._get_ckan_resources("dataset")

    @pytest.mark.asyncio
    async def test_imea_json_objeto(self):
        response = httpx.Response(
            200,
            json={"error": "unexpected"},
            request=httpx.Request("GET", "https://example.test/imea"),
        )
        with (
            patch.object(
                imea_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=response,
            ),
            pytest.raises(SourceUnavailableError, match="JSON inesperado: dict"),
        ):
            await imea_client._fetch_json("https://example.test/imea")
