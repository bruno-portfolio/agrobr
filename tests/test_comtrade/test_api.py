from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.comtrade import api
from agrobr.models import MetaInfo

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "comtrade"


def _golden_comercio_records() -> list[dict]:
    raw = json.loads((GOLDEN_DIR / "comercio_sample" / "response.json").read_text())
    return raw["data"]


def _golden_reporter_records() -> list[dict]:
    raw = json.loads((GOLDEN_DIR / "mirror_sample" / "response_reporter.json").read_text())
    return raw["data"]


def _golden_partner_records() -> list[dict]:
    raw = json.loads((GOLDEN_DIR / "mirror_sample" / "response_partner.json").read_text())
    return raw["data"]


class TestComercio:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        records = _golden_comercio_records()
        with patch(
            "agrobr.comtrade.client.fetch_trade_data",
            new_callable=AsyncMock,
            return_value=(records, "https://test"),
        ):
            df = await api.comercio("complexo_soja", reporter="BR", partner="CN", periodo=2024)

        assert len(df) == 3
        assert "periodo" in df.columns
        assert "valor_fob_usd" in df.columns
        assert "volume_ton" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        records = _golden_comercio_records()
        with patch(
            "agrobr.comtrade.client.fetch_trade_data",
            new_callable=AsyncMock,
            return_value=(records, "https://test"),
        ):
            df, meta = await api.comercio(
                "complexo_soja", reporter="BR", partner="CN", periodo=2024, return_meta=True
            )

        assert isinstance(meta, MetaInfo)
        assert meta.source == "comtrade"
        assert meta.records_count == 3

    @pytest.mark.asyncio
    async def test_default_periodo(self):
        with patch(
            "agrobr.comtrade.client.fetch_trade_data",
            new_callable=AsyncMock,
            return_value=([], "https://test"),
        ) as mock_fetch:
            await api.comercio("soja")

        call_kwargs = mock_fetch.call_args.kwargs
        assert call_kwargs["period"].isdigit()

    @pytest.mark.asyncio
    async def test_partner_none_means_world(self):
        with patch(
            "agrobr.comtrade.client.fetch_trade_data",
            new_callable=AsyncMock,
            return_value=([], "https://test"),
        ) as mock_fetch:
            await api.comercio("soja", partner=None)

        call_kwargs = mock_fetch.call_args.kwargs
        assert call_kwargs["partner"] == 0


class TestTradeMirror:
    @pytest.mark.asyncio
    async def test_returns_mirror_dataframe(self):
        reporter_records = _golden_reporter_records()
        partner_records = _golden_partner_records()

        call_count = 0

        async def mock_fetch(**kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs.get("flow", "").upper() == "X":
                return (reporter_records, "https://test")
            return (partner_records, "https://test")

        with patch("agrobr.comtrade.client.fetch_trade_data", side_effect=mock_fetch):
            df = await api.trade_mirror("soja", reporter="BR", partner="CN", periodo=2024)

        assert len(df) == 1
        assert "diff_peso_kg" in df.columns
        assert "ratio_valor" in df.columns
        assert "ratio_peso" in df.columns
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_return_meta(self):
        reporter_records = _golden_reporter_records()
        partner_records = _golden_partner_records()

        async def mock_fetch(**kwargs):
            if kwargs.get("flow", "").upper() == "X":
                return (reporter_records, "https://test")
            return (partner_records, "https://test")

        with patch("agrobr.comtrade.client.fetch_trade_data", side_effect=mock_fetch):
            df, meta = await api.trade_mirror(
                "soja", reporter="BR", partner="CN", periodo=2024, return_meta=True
            )

        assert isinstance(meta, MetaInfo)
        assert meta.source == "comtrade_mirror"
        assert "comtrade_export" in meta.attempted_sources
        assert "comtrade_import" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_discrepancies_calculated(self):
        reporter_records = _golden_reporter_records()
        partner_records = _golden_partner_records()

        async def mock_fetch(**kwargs):
            if kwargs.get("flow", "").upper() == "X":
                return (reporter_records, "https://test")
            return (partner_records, "https://test")

        with patch("agrobr.comtrade.client.fetch_trade_data", side_effect=mock_fetch):
            df = await api.trade_mirror("soja", reporter="BR", partner="CN", periodo=2024)

        row = df.iloc[0]
        assert row["diff_peso_kg"] != 0
        assert row["ratio_valor"] > 0


class TestPaises:
    def test_returns_list(self):
        result = api.paises()
        assert isinstance(result, list)
        assert "BRA" in result
        assert "CHN" in result
        assert "USA" in result


class TestProdutos:
    def test_returns_dict(self):
        result = api.produtos()
        assert isinstance(result, dict)
        assert "soja" in result
        assert result["soja"] == ["1201"]
        assert "complexo_soja" in result
