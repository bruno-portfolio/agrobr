from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.b3 import api, client, parser
from agrobr.b3.models import B3_CONTRATOS_AGRO, COLUNAS_SAIDA
from agrobr.models import MetaInfo

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "b3" / "ajustes_sample"


def _golden_html() -> str:
    return GOLDEN_DIR.joinpath("response.html").read_text(encoding="utf-8")


def _golden_weekend_html() -> str:
    return GOLDEN_DIR.joinpath("response_weekend.html").read_text(encoding="utf-8")


class TestAjustes:
    @pytest.fixture
    def mock_fetch_weekday(self):
        html = _golden_html()
        with patch.object(
            client,
            "fetch_ajustes",
            new_callable=AsyncMock,
            return_value=(html, "https://www2.bmf.com.br/test?txtData=13/02/2025"),
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_fetch_weekend(self):
        html = _golden_weekend_html()
        with patch.object(
            client,
            "fetch_ajustes",
            new_callable=AsyncMock,
            return_value=(html, "https://www2.bmf.com.br/test?txtData=15/02/2025"),
        ) as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_returns_dataframe(self, mock_fetch_weekday):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_columns_match_schema(self, mock_fetch_weekday):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025")
        for col in COLUNAS_SAIDA:
            assert col in df.columns

    @pytest.mark.asyncio
    async def test_accepts_date_object(self, mock_fetch_weekday):
        df = await api.ajustes(data=date(2025, 2, 13))
        assert isinstance(df, pd.DataFrame)
        mock_fetch_weekday.assert_called_once_with("13/02/2025")

    @pytest.mark.asyncio
    async def test_filter_contrato_by_name(self, mock_fetch_weekday):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025", contrato="boi")
        assert len(df) > 0
        assert (df["ticker"] == "BGI").all()

    @pytest.mark.asyncio
    async def test_filter_contrato_by_ticker(self, mock_fetch_weekday):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025", contrato="CCM")
        assert len(df) > 0
        assert (df["ticker"] == "CCM").all()

    @pytest.mark.asyncio
    async def test_filter_contrato_unknown_returns_empty(self, mock_fetch_weekday):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025", contrato="XYZ")
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_return_meta(self, mock_fetch_weekday):  # noqa: ARG002
        result = await api.ajustes(data="13/02/2025", return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(meta, MetaInfo)
        assert meta.source == "b3"
        assert meta.records_count == len(df)
        assert meta.parser_version == parser.PARSER_VERSION
        assert "bmf.com.br" in meta.source_url

    @pytest.mark.asyncio
    async def test_meta_fetch_duration(self, mock_fetch_weekday):  # noqa: ARG002
        _, meta = await api.ajustes(data="13/02/2025", return_meta=True)
        assert meta.fetch_duration_ms >= 0
        assert meta.parse_duration_ms >= 0

    @pytest.mark.asyncio
    async def test_weekend_returns_empty(self, mock_fetch_weekend):  # noqa: ARG002
        df = await api.ajustes(data="15/02/2025")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_weekend_meta_zero_records(self, mock_fetch_weekend):  # noqa: ARG002
        _, meta = await api.ajustes(data="15/02/2025", return_meta=True)
        assert meta.records_count == 0


class TestHistorico:
    @pytest.fixture
    def mock_ajustes(self):
        html = _golden_html()
        with patch.object(
            client,
            "fetch_ajustes",
            new_callable=AsyncMock,
            return_value=(html, "https://www2.bmf.com.br/test"),
        ) as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_returns_dataframe(self, mock_ajustes):  # noqa: ARG002
        with patch("agrobr.b3.api.asyncio.sleep", new_callable=AsyncMock):
            df = await api.historico(
                contrato="boi",
                inicio=date(2025, 2, 13),
                fim=date(2025, 2, 13),
            )
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_accepts_string_dates(self, mock_ajustes):  # noqa: ARG002
        with patch("agrobr.b3.api.asyncio.sleep", new_callable=AsyncMock):
            df = await api.historico(
                contrato="boi",
                inicio="2025-02-13",
                fim="2025-02-13",
            )
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_skips_weekends(self, mock_ajustes):
        with patch("agrobr.b3.api.asyncio.sleep", new_callable=AsyncMock):
            await api.historico(
                contrato="boi",
                inicio=date(2025, 2, 8),
                fim=date(2025, 2, 9),
            )
        assert mock_ajustes.call_count == 0

    @pytest.mark.asyncio
    async def test_multiple_days(self, mock_ajustes):
        with patch("agrobr.b3.api.asyncio.sleep", new_callable=AsyncMock):
            df = await api.historico(
                contrato="boi",
                inicio=date(2025, 2, 10),
                fim=date(2025, 2, 14),
            )
        assert mock_ajustes.call_count == 5
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_filter_vencimento(self, mock_ajustes):  # noqa: ARG002
        with patch("agrobr.b3.api.asyncio.sleep", new_callable=AsyncMock):
            df = await api.historico(
                contrato="boi",
                inicio=date(2025, 2, 13),
                fim=date(2025, 2, 13),
                vencimento="G25",
            )
        assert len(df) > 0
        assert (df["vencimento_codigo"] == "G25").all()

    @pytest.mark.asyncio
    async def test_return_meta(self, mock_ajustes):  # noqa: ARG002
        with patch("agrobr.b3.api.asyncio.sleep", new_callable=AsyncMock):
            result = await api.historico(
                contrato="boi",
                inicio=date(2025, 2, 13),
                fim=date(2025, 2, 13),
                return_meta=True,
            )
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(meta, MetaInfo)
        assert meta.source == "b3"

    @pytest.mark.asyncio
    async def test_empty_range_returns_empty(self, mock_ajustes):  # noqa: ARG002
        with patch("agrobr.b3.api.asyncio.sleep", new_callable=AsyncMock):
            df = await api.historico(
                contrato="boi",
                inicio=date(2025, 2, 15),
                fim=date(2025, 2, 14),
            )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestContratos:
    def test_returns_sorted_list(self):
        result = api.contratos()
        assert isinstance(result, list)
        assert result == sorted(result)

    def test_all_contratos_present(self):
        result = api.contratos()
        for nome in B3_CONTRATOS_AGRO:
            assert nome in result

    def test_count_matches(self):
        result = api.contratos()
        assert len(result) == len(B3_CONTRATOS_AGRO)
