from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.conab.progresso.api import progresso_safra, semanas_disponiveis

GOLDEN_DIR = Path(__file__).resolve().parent.parent / "golden_data" / "conab_progresso"


@pytest.fixture()
def golden_xlsx() -> bytes:
    return (GOLDEN_DIR / "progresso_sample" / "response.xlsx").read_bytes()


@pytest.fixture()
def expected() -> dict:
    return json.loads(
        (GOLDEN_DIR / "progresso_sample" / "expected.json").read_text(encoding="utf-8")
    )


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
    async def test_combined_filters(self, mock_fetch_latest: AsyncMock) -> None:  # noqa: ARG002
        df = await progresso_safra(cultura="Soja", estado="MT", operacao="Colheita")
        assert len(df) == 1
        assert df.iloc[0]["pct_semana_atual"] == pytest.approx(0.468)


@pytest.mark.asyncio()
class TestSemanasDisponiveis:
    async def test_max_pages(self) -> None:
        with patch(
            "agrobr.conab.progresso.client.list_semanas",
            new_callable=AsyncMock,
            return_value=[],
        ) as m:
            await semanas_disponiveis(max_pages=2)
            m.assert_called_once_with(max_pages=2)
