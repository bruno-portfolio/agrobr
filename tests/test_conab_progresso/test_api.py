from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.conab.progresso.api import progresso_safra, semanas_disponiveis
from agrobr.models import MetaInfo

GOLDEN_DIR = Path(__file__).resolve().parent.parent / "golden_data" / "conab_progresso"


@pytest.fixture()
def golden_xlsx() -> bytes:
    return (GOLDEN_DIR / "progresso_sample.xlsx").read_bytes()


@pytest.fixture()
def expected() -> dict:
    return json.loads((GOLDEN_DIR / "expected.json").read_text(encoding="utf-8"))


@pytest.fixture()
def mock_fetch_latest(golden_xlsx: bytes):
    with patch(
        "agrobr.conab.progresso.client.fetch_latest",
        new_callable=AsyncMock,
        return_value=(golden_xlsx, "https://example.com/progresso.xlsx", "Semana 02/02 a 08/02/26"),
    ) as m:
        yield m


@pytest.fixture()
def mock_fetch_semanal(golden_xlsx: bytes):
    with patch(
        "agrobr.conab.progresso.client.fetch_xlsx_semanal",
        new_callable=AsyncMock,
        return_value=(golden_xlsx, "https://example.com/progresso.xlsx"),
    ) as m:
        yield m


@pytest.mark.asyncio()
class TestProgressoSafra:
    async def test_returns_dataframe(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        df = await progresso_safra()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    async def test_all_records(self, mock_fetch_latest: AsyncMock, expected: dict) -> None:  # noqa: ARG002
        df = await progresso_safra()
        assert len(df) == expected["total_records"]

    async def test_filter_cultura(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        df = await progresso_safra(cultura="Soja")
        assert all(df["cultura"] == "Soja")
        assert len(df) == 6

    async def test_filter_cultura_normalized(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        df = await progresso_safra(cultura="soja")
        assert all(df["cultura"] == "Soja")

    async def test_filter_estado(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        df = await progresso_safra(estado="MT")
        assert all(df["estado"] == "MT")

    async def test_filter_operacao(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        df = await progresso_safra(operacao="Semeadura")
        assert all(df["operacao"] == "Semeadura")

    async def test_filter_operacao_case(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        df = await progresso_safra(operacao="semeadura")
        assert all(df["operacao"] == "Semeadura")

    async def test_combined_filters(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        df = await progresso_safra(cultura="Soja", estado="MT", operacao="Colheita")
        assert len(df) == 1
        assert df.iloc[0]["pct_semana_atual"] == pytest.approx(0.468)

    async def test_return_meta(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        result = await progresso_safra(return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(meta, MetaInfo)
        assert meta.source == "conab_progresso"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert meta.source_method == "httpx+xlsx"

    async def test_semana_url(self, mock_fetch_semanal: AsyncMock) -> None:
        df = await progresso_safra(semana_url="https://example.com/week")
        assert isinstance(df, pd.DataFrame)
        mock_fetch_semanal.assert_called_once_with("https://example.com/week")

    async def test_empty_filter(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        df = await progresso_safra(cultura="Trigo")
        assert len(df) == 0

    async def test_columns(self, mock_fetch_latest: AsyncMock, expected: dict) -> None:  # noqa: ARG002
        df = await progresso_safra()
        assert df.columns.tolist() == expected["columns"]


@pytest.mark.asyncio()
class TestSemanasDisponiveis:
    async def test_returns_list(self) -> None:
        mock_weeks = [
            ("Semana 1", "https://example.com/s1"),
            ("Semana 2", "https://example.com/s2"),
        ]
        with patch(
            "agrobr.conab.progresso.client.list_semanas",
            new_callable=AsyncMock,
            return_value=mock_weeks,
        ):
            result = await semanas_disponiveis()
            assert isinstance(result, list)
            assert len(result) == 2
            assert result[0]["descricao"] == "Semana 1"
            assert result[0]["url"] == "https://example.com/s1"

    async def test_max_pages(self) -> None:
        with patch(
            "agrobr.conab.progresso.client.list_semanas",
            new_callable=AsyncMock,
            return_value=[],
        ) as m:
            await semanas_disponiveis(max_pages=2)
            m.assert_called_once_with(max_pages=2)
