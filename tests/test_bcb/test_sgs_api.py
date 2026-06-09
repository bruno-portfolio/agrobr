from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.bcb import sgs_api
from agrobr.bcb.sgs_models import SGS_SERIES

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "bcb"
SGS_SAMPLE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados"


def _load_golden_sgs() -> list[dict[str, str]]:
    with open(GOLDEN_DIR / "sgs_sample.json") as f:
        return json.load(f)


class TestSgsBasic:
    @pytest.mark.asyncio
    async def test_returns_dataframe_with_correct_columns(self):
        with patch.object(
            sgs_api.sgs_client,
            "fetch_sgs",
            new_callable=AsyncMock,
            return_value=(_load_golden_sgs(), SGS_SAMPLE_URL),
        ):
            df = await sgs_api.sgs(432)

        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["data", "valor", "codigo", "nome_serie"]
        assert len(df) == 10

    @pytest.mark.asyncio
    async def test_valor_is_numeric(self):
        with patch.object(
            sgs_api.sgs_client,
            "fetch_sgs",
            new_callable=AsyncMock,
            return_value=(_load_golden_sgs(), SGS_SAMPLE_URL),
        ):
            df = await sgs_api.sgs(432)

        assert df["valor"].dtype == float
        assert df["valor"].iloc[0] == pytest.approx(12.15)

    @pytest.mark.asyncio
    async def test_codigo_column(self):
        with patch.object(
            sgs_api.sgs_client,
            "fetch_sgs",
            new_callable=AsyncMock,
            return_value=(_load_golden_sgs(), SGS_SAMPLE_URL),
        ):
            df = await sgs_api.sgs(432)

        assert all(df["codigo"] == 432)


class TestSgsStringCode:
    @pytest.mark.asyncio
    async def test_string_code_resolves(self):
        with patch.object(
            sgs_api.sgs_client,
            "fetch_sgs",
            new_callable=AsyncMock,
            return_value=(_load_golden_sgs(), SGS_SAMPLE_URL),
        ) as mock_fetch:
            df = await sgs_api.sgs("selic")

        mock_fetch.assert_called_once_with(
            SGS_SERIES["selic"],
            data_inicial=None,
            data_final=None,
            ultimos=None,
        )
        assert all(df["nome_serie"] == "selic")

    @pytest.mark.asyncio
    async def test_invalid_string_code_raises(self):
        with pytest.raises(ValueError, match="nao encontrada"):
            await sgs_api.sgs("serie_inexistente")

    @pytest.mark.asyncio
    async def test_numeric_code_reverse_lookup(self):
        with patch.object(
            sgs_api.sgs_client,
            "fetch_sgs",
            new_callable=AsyncMock,
            return_value=(_load_golden_sgs(), SGS_SAMPLE_URL),
        ):
            df = await sgs_api.sgs(432)

        assert all(df["nome_serie"] == "selic")


class TestSgsReturnMeta:
    @pytest.mark.asyncio
    async def test_return_meta_tuple(self):
        with patch.object(
            sgs_api.sgs_client,
            "fetch_sgs",
            new_callable=AsyncMock,
            return_value=(_load_golden_sgs(), SGS_SAMPLE_URL),
        ):
            result = await sgs_api.sgs(432, return_meta=True)

        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert meta.source == "bcb_sgs"
        assert meta.source_method == "httpx"
        assert meta.records_count == len(df)
        assert meta.attempted_sources == ["bcb_sgs"]
        assert meta.selected_source == "bcb_sgs"


class TestSgsAsPolars:
    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        with patch.object(
            sgs_api.sgs_client,
            "fetch_sgs",
            new_callable=AsyncMock,
            return_value=(_load_golden_sgs(), SGS_SAMPLE_URL),
        ):
            result = await sgs_api.sgs(432, as_polars=True)

        assert isinstance(result, pl.DataFrame)


class TestSgsDateParsing:
    @pytest.mark.asyncio
    async def test_dayfirst_parsing(self):
        with patch.object(
            sgs_api.sgs_client,
            "fetch_sgs",
            new_callable=AsyncMock,
            return_value=(_load_golden_sgs(), SGS_SAMPLE_URL),
        ):
            df = await sgs_api.sgs(432)

        first_date = df["data"].iloc[0]
        assert first_date.day == 2
        assert first_date.month == 1
        assert first_date.year == 2026


class TestSgsUltimos:
    @pytest.mark.asyncio
    async def test_ultimos_limits_rows(self):
        with patch.object(
            sgs_api.sgs_client,
            "fetch_sgs",
            new_callable=AsyncMock,
            return_value=(_load_golden_sgs(), SGS_SAMPLE_URL),
        ):
            df = await sgs_api.sgs(432, ultimos=3)

        assert len(df) == 3
