from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr import constants
from agrobr.alt.anp_diesel import client as anp_client
from agrobr.alt.mapa_psr import client as mapa_psr_client
from agrobr.anda import client as anda_client
from agrobr.b3 import client as b3_client
from agrobr.deral import client as deral_client
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.ibama import client as ibama_client
from agrobr.ibge import ftp_client
from agrobr.lista_suja import client as lista_suja_client
from agrobr.mapbiomas import client as mapbiomas_client
from agrobr.rio_verde import client as rio_verde_client
from agrobr.zarc import client as zarc_client
from tests.helpers import make_mock_response

_HTML = b"<html><body>manutencao</body></html>" + b"x" * 60_000


def _response():
    return make_mock_response(
        200,
        content=_HTML,
        text=_HTML.decode(),
        headers={"content-type": "text/html"},
    )


class TestDownloadClientsRejectHtml:
    @pytest.mark.asyncio
    async def test_ibge_ftp(self):
        with (
            patch.object(
                ftp_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await ftp_client.download_legacy_zip("Tab_3")

    @pytest.mark.asyncio
    async def test_deral(self):
        with (
            patch.object(
                deral_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await deral_client.fetch_pc_xls()

    @pytest.mark.asyncio
    async def test_lista_suja(self):
        page_url = constants.URLS[constants.Fonte.LISTA_SUJA]["page"]
        csv_url = f"{page_url}/cadastro.csv"
        page = (
            f"<h2>{constants.LISTA_SUJA_PUBLICATION_TITLE}</h2>"
            f'<p><a href="{csv_url}">CSV</a><a href="{page_url}/cadastro.pdf">PDF</a></p>'
        )
        with (
            patch.object(
                lista_suja_client,
                "retry_on_status",
                new_callable=AsyncMock,
                side_effect=[
                    make_mock_response(200, content=page.encode(), url=page_url),
                    make_mock_response(200, content=_HTML, url=csv_url),
                ],
            ) as request,
            pytest.raises(ParseError, match="Corpo CSV incompatível"),
        ):
            await lista_suja_client.fetch_empregadores()
        assert request.await_count == 2

    @pytest.mark.asyncio
    async def test_rio_verde(self):
        safra = next(iter(rio_verde_client.SAFRAS_URLS))
        with (
            patch.object(
                rio_verde_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await rio_verde_client.fetch_ensaio_soja(safra)

    @pytest.mark.asyncio
    async def test_anp_diesel_xlsx(self):
        with (
            patch.object(
                anp_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await anp_client.fetch_precos_resource("https://example.test/file.xlsx")

    @pytest.mark.asyncio
    async def test_anp_diesel_csv(self):
        with (
            patch.object(
                anp_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await anp_client.download_csv("https://example.test/file.csv")

    @pytest.mark.asyncio
    async def test_mapa_psr(self):
        original = httpx.AsyncClient
        transport = httpx.MockTransport(lambda _request: httpx.Response(200, content=_HTML))
        with (
            patch.object(
                mapa_psr_client.httpx,
                "AsyncClient",
                side_effect=lambda **kwargs: original(transport=transport, **kwargs),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            async with mapa_psr_client.open_periodo("2025"):
                raise AssertionError("corpo HTML entregue")

    @pytest.mark.asyncio
    async def test_zarc(self):
        with (
            patch.object(
                zarc_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await zarc_client.download_csv("https://example.test/file.csv")

    @pytest.mark.asyncio
    async def test_mapbiomas(self):
        with (
            patch.object(
                mapbiomas_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await mapbiomas_client._fetch_bundle("https://example.test/file.xlsx")

    @pytest.mark.asyncio
    async def test_anda(self):
        with (
            patch.object(
                anda_client,
                "_get_with_retry",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await anda_client.download_file("https://example.test/file.pdf")

    @pytest.mark.asyncio
    async def test_b3(self):
        with (
            patch.object(
                b3_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await b3_client.fetch_ajustes_zip("03/03/2026")

    @pytest.mark.asyncio
    async def test_ibama(self):
        with (
            patch.object(
                ibama_client,
                "retry_on_status",
                new_callable=AsyncMock,
                return_value=_response(),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida"),
        ):
            await ibama_client.fetch_embargos_csv()
