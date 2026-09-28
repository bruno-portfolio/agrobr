"""Testes para agrobr.queimadas.client — fallback em cascata."""

from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.queimadas import client
from agrobr.utils.io import extract_csv_from_zip
from tests.helpers import levanta_exatamente, make_mock_response


class TestFetchFocosMensal:
    @pytest.mark.asyncio
    async def test_all_404_raises(self):
        resp_404 = make_mock_response(404, content=b"")

        with (
            patch(
                "agrobr.queimadas.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=resp_404,
            ),
            pytest.raises(SourceUnavailableError, match="Tentativas"),
        ):
            await client.fetch_focos_mensal(1990, 1)


class TestFetchFocosDiario:
    @pytest.mark.asyncio
    async def test_404_informa_janela_e_arquivo_mensal(self):
        response = make_mock_response(404, content=b"")

        with (
            patch(
                "agrobr.queimadas.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=response,
            ),
            pytest.raises(
                SourceUnavailableError,
                match="arquivo diário só existe para os últimos dias.*arquivo mensal",
            ),
        ):
            await client.fetch_focos_diario("20250101")


class TestExtractCsvFromZip:
    def test_no_csv_raises(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no csv here")
        with levanta_exatamente(SourceUnavailableError, "não contém arquivo CSV"):
            extract_csv_from_zip(buf.getvalue(), source="queimadas", url="test")
