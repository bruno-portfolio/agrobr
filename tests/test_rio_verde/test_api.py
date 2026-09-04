from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.exceptions import InvalidParameterError

GOLDEN_DIR = Path(__file__).resolve().parent.parent / "golden_data" / "rio_verde"


def _golden_pdf_bytes():
    return (Path("/tmp") / "rio_verde_soja_2526.pdf").read_bytes()


def _has_golden_pdf():
    return (Path("/tmp") / "rio_verde_soja_2526.pdf").exists()


@pytest.mark.asyncio
@pytest.mark.skipif(not _has_golden_pdf(), reason="Golden PDF not available")
async def test_ensaio_soja_returns_dataframe():
    with patch("agrobr.rio_verde.client.fetch_ensaio_soja", new_callable=AsyncMock) as mock:
        mock.return_value = (_golden_pdf_bytes(), "https://example.com")

        from agrobr.rio_verde import ensaio_soja

        df = await ensaio_soja("2025/2026")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 50
        assert "cultivar" in df.columns


@pytest.mark.asyncio
@pytest.mark.skipif(not _has_golden_pdf(), reason="Golden PDF not available")
async def test_ensaio_soja_filter_empresa():
    with patch("agrobr.rio_verde.client.fetch_ensaio_soja", new_callable=AsyncMock) as mock:
        mock.return_value = (_golden_pdf_bytes(), "https://example.com")

        from agrobr.rio_verde import ensaio_soja

        df = await ensaio_soja("2025/2026", empresa="Brasmax")
        assert len(df) > 0
        assert all("Brasmax" in v for v in df["empresa"].values)


@pytest.mark.asyncio
@pytest.mark.skipif(not _has_golden_pdf(), reason="Golden PDF not available")
async def test_ensaio_soja_return_meta():
    with patch("agrobr.rio_verde.client.fetch_ensaio_soja", new_callable=AsyncMock) as mock:
        mock.return_value = (_golden_pdf_bytes(), "https://example.com")

        from agrobr.rio_verde import ensaio_soja

        result = await ensaio_soja("2025/2026", return_meta=True)
        assert isinstance(result, tuple)
        _, meta = result
        assert meta.source == "rio_verde"


@pytest.mark.asyncio
async def test_safras_disponiveis():
    from agrobr.rio_verde import safras_disponiveis

    safras = await safras_disponiveis()
    assert isinstance(safras, list)
    assert len(safras) >= 2
    assert "2025/2026" in safras


@pytest.mark.asyncio
async def test_invalid_safra_raises_before_download():
    with (
        patch("agrobr.rio_verde.client.fetch_ensaio_soja", new_callable=AsyncMock) as fetch,
        pytest.raises(InvalidParameterError, match="Opções"),
    ):
        from agrobr.rio_verde import ensaio_soja

        await ensaio_soja("2023/2024")

    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensaio_soja_filter_empresa_literal():
    source = pd.DataFrame({"empresa": ["Sementes (Brasil)", "Sementes Brasil"]})
    with (
        patch(
            "agrobr.rio_verde.client.fetch_ensaio_soja",
            new_callable=AsyncMock,
            return_value=(b"pdf", "https://example.com"),
        ),
        patch("agrobr.rio_verde.parser.parse_ensaio_soja", return_value=source),
    ):
        from agrobr.rio_verde import ensaio_soja

        df = await ensaio_soja("2025/2026", empresa="Sementes (Brasil)")

    assert df["empresa"].tolist() == ["Sementes (Brasil)"]
