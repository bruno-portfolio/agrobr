from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.mapbiomas import api
from tests.helpers import mapbiomas_workbook_bundle as workbook_bundle

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "mapbiomas"


def _golden_xlsx() -> bytes:
    return GOLDEN_DIR.joinpath("biome_state_sample", "response.xlsx").read_bytes()


class TestCobertura:
    @pytest.mark.asyncio
    async def test_combined_filters(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_bundle",
            new_callable=AsyncMock,
            return_value=workbook_bundle(
                *(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx")
            ),
        ):
            df = await api.cobertura(colecao=10, bioma="Cerrado", estado="GO", ano=2020)

        assert len(df) >= 1
        assert (df["bioma"] == "Cerrado").all()
        assert (df["estado"].str.upper() == "GO").all()
        assert (df["ano"] == 2020).all()


class TestCoberturaMunicipal:
    @pytest.mark.asyncio
    async def test_invalid_estado_raises_before_municipal_fetch(self):
        with (
            patch.object(
                api.client,
                "fetch_biome_state_municipality_bundle",
                new_callable=AsyncMock,
            ) as mock_fetch,
            pytest.raises(ValueError, match="Estado inválido"),
        ):
            await api.cobertura(colecao=11, nivel="municipio", estado="XX")

        mock_fetch.assert_not_awaited()


class TestTransicao:
    @pytest.mark.asyncio
    async def test_invalid_bioma_raises_before_download(self):
        with (
            patch.object(api.client, "fetch_biome_state_bundle", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="Bioma inválido"),
        ):
            await api.transicao(colecao=10, bioma="Atlantida")

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_filter_estado_nome_completo(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_bundle",
            new_callable=AsyncMock,
            return_value=workbook_bundle(
                *(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx")
            ),
        ):
            df = await api.transicao(colecao=10, estado="Goiás")

        assert not df.empty
        assert set(df["estado"]) == {"GO"}

    @pytest.mark.asyncio
    async def test_filter_periodo(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_bundle",
            new_callable=AsyncMock,
            return_value=workbook_bundle(
                *(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx")
            ),
        ):
            df = await api.transicao(colecao=10, periodo="1985-2024")

        assert len(df) >= 1
        assert (df["periodo"] == "1985-2024").all()


class TestValidacaoColecao:
    @pytest.mark.asyncio
    async def test_transicao_colecao_invalida(self):
        with pytest.raises(ValueError, match="nao suportada"):
            await api.transicao(colecao=9)
