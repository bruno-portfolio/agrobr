from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr import bcb, comtrade, nasa_power
from agrobr.alt import antt_pedagio
from agrobr.b3 import client as b3_client
from agrobr.bcb import client as bcb_client
from agrobr.cftc import client as cftc_client
from agrobr.conab.ceasa import client as ceasa_client
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.imea import client as imea_client
from agrobr.mapbiomas_alerta import client as alerta_client
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


@pytest.fixture
def html_transport(monkeypatch):
    original = httpx.AsyncClient

    def install(*, ptax_catalog=False):
        requests = []

        def respond(request):
            requests.append(request)
            if ptax_catalog and request.url.path.endswith("/Moedas"):
                return httpx.Response(
                    200,
                    json={
                        "value": [
                            {
                                "simbolo": "USD",
                                "nomeFormatado": "Dólar dos Estados Unidos",
                                "tipoMoeda": "A",
                            }
                        ],
                        "@odata.count": 1,
                    },
                )
            return httpx.Response(
                200, text="<html>Service Unavailable</html>", headers={"content-type": "text/html"}
            )

        def client(*args, **kwargs):
            return original(*args, transport=httpx.MockTransport(respond), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", client)
        return requests

    return install


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
    async def test_bcb_sgs_html(self, html_transport):
        requests = html_transport()
        with pytest.raises(ParseError):
            await bcb.sgs(1, data_inicial="01/01/2024", data_final="02/01/2024")
        assert len(requests) == 1 and requests[0].url.host == "api.bcb.gov.br"

    @pytest.mark.asyncio
    async def test_bcb_focus_html(self, html_transport):
        requests = html_transport()
        with pytest.raises(ParseError):
            await bcb.focus("IPCA")
        assert len(requests) == 1 and requests[0].url.host == "olinda.bcb.gov.br"

    @pytest.mark.asyncio
    async def test_bcb_ptax_html(self, html_transport):
        requests = html_transport(ptax_catalog=True)
        with pytest.raises(ParseError):
            await bcb.ptax(data="01/01/2024")
        assert len(requests) == 2
        assert requests[0].url.path.endswith("/Moedas")
        assert "CotacaoMoedaDia" in requests[1].url.path

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
    async def test_nasa_power_html(self, html_transport):
        requests = html_transport()
        with pytest.raises(SourceUnavailableError, match="content-type 'text/html'") as caught:
            await nasa_power.clima_ponto(
                lat=-12.6, lon=-56.1, inicio="2024-01-01", fim="2024-01-02"
            )
        assert "Service Unavailable" in str(caught.value)
        assert len(requests) == 1 and requests[0].url.host == "power.larc.nasa.gov"

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
    async def test_comtrade_html(self, html_transport):
        requests = html_transport()
        with pytest.raises(ParseError):
            await comtrade.comercio("soja", periodo=2024)
        assert len(requests) == 1 and requests[0].url.host == "comtradeapi.un.org"

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
    async def test_antt_pedagio_html(self, html_transport):
        requests = html_transport()
        with pytest.raises(SourceUnavailableError, match="WAF") as caught:
            await antt_pedagio.fluxo_pedagio(ano=2026, enriquecer=False)
        assert len(requests) == 1 and requests[0].url.host == "dados.antt.gov.br"
        attempt = caught.value.antt_acquisition["attempts"][0]
        assert attempt["status"] == 200 and attempt["complete_body"] and attempt["closed"]
        assert attempt["error_type"] == "SourceUnavailableError"

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
