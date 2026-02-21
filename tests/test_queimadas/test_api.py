from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.queimadas import api

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "queimadas" / "focos_sample"


def _golden_csv_bytes() -> bytes:
    return GOLDEN_DIR.joinpath("response.csv").read_bytes()


class TestFocos:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9)

        assert len(df) >= 8
        assert "data" in df.columns
        assert "lat" in df.columns
        assert "satelite" in df.columns
        assert "uf" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df, meta = await api.focos(ano=2024, mes=9, return_meta=True)

        assert meta.source == "queimadas"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert meta.fetch_timestamp is not None
        assert "queimadas" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9, uf="MT")

        assert len(df) >= 1
        assert (df["uf"] == "MT").all()

    @pytest.mark.asyncio
    async def test_filter_bioma(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9, bioma="Cerrado")

        assert len(df) >= 1
        assert (df["bioma"] == "Cerrado").all()

    @pytest.mark.asyncio
    async def test_filter_satelite(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9, satelite="GOES-16")

        assert len(df) >= 1
        assert (df["satelite"] == "GOES-16").all()

    @pytest.mark.asyncio
    async def test_dia_uses_diario_fetch(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_diario",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ) as mock_diario:
            await api.focos(ano=2024, mes=9, dia=15)

        mock_diario.assert_called_once_with("20240915")

    @pytest.mark.asyncio
    async def test_no_dia_uses_mensal_fetch(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ) as mock_mensal:
            await api.focos(ano=2024, mes=9)

        mock_mensal.assert_called_once_with(2024, 9)

    @pytest.mark.asyncio
    async def test_filter_uf_case_insensitive(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9, uf="mt")

        assert len(df) >= 1
        assert (df["uf"] == "MT").all()

    @pytest.mark.asyncio
    async def test_empty_filter_returns_empty(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9, uf="XX")

        assert len(df) == 0
