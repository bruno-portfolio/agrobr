from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

GOLDEN_DIR = Path(__file__).resolve().parent.parent / "golden_data" / "embrapa_solos"


def _perfis_pages():
    return [(GOLDEN_DIR / "perfis_sample.csv").read_bytes()]


def _mapa_pages():
    return [(GOLDEN_DIR / "mapa_sample.csv").read_bytes()]


@pytest.mark.asyncio
async def test_perfis_returns_dataframe():
    with patch("agrobr.embrapa_solos.client.fetch_perfis", new_callable=AsyncMock) as mock:
        mock.return_value = (_perfis_pages(), "https://example.com")

        from agrobr.embrapa_solos import perfis

        df = await perfis()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert "uf" in df.columns


@pytest.mark.asyncio
async def test_perfis_return_meta():
    with patch("agrobr.embrapa_solos.client.fetch_perfis", new_callable=AsyncMock) as mock:
        mock.return_value = (_perfis_pages(), "https://example.com")

        from agrobr.embrapa_solos import perfis

        result = await perfis(return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert meta.source == "embrapa_solos"


@pytest.mark.asyncio
async def test_perfis_as_polars():
    polars = pytest.importorskip("polars")
    with patch("agrobr.embrapa_solos.client.fetch_perfis", new_callable=AsyncMock) as mock:
        mock.return_value = (_perfis_pages(), "https://example.com")

        from agrobr.embrapa_solos import perfis

        df = await perfis(as_polars=True)
        assert isinstance(df, polars.DataFrame)


@pytest.mark.asyncio
async def test_mapa_solos_returns_dataframe():
    with patch("agrobr.embrapa_solos.client.fetch_mapa_solos", new_callable=AsyncMock) as mock:
        mock.return_value = (_mapa_pages(), "https://example.com")

        from agrobr.embrapa_solos import mapa_solos

        df = await mapa_solos()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert "classe_dom" in df.columns


@pytest.mark.asyncio
async def test_mapa_solos_filter_ordem():
    with patch("agrobr.embrapa_solos.client.fetch_mapa_solos", new_callable=AsyncMock) as mock:
        mock.return_value = (_mapa_pages(), "https://example.com")

        from agrobr.embrapa_solos import mapa_solos

        df = await mapa_solos(ordem="DUNAS")
        assert len(df) >= 0


@pytest.mark.asyncio
async def test_mapa_solos_filter_ordem_literal():
    source = pd.DataFrame({"ordem1": ["Classe (Nova)", "Classe Nova"]})
    with (
        patch(
            "agrobr.embrapa_solos.client.fetch_mapa_solos",
            new_callable=AsyncMock,
            return_value=([b"csv"], "https://example.com"),
        ),
        patch("agrobr.embrapa_solos.parser.parse_mapa_csv", return_value=source),
    ):
        from agrobr.embrapa_solos import mapa_solos

        df = await mapa_solos(ordem="Classe (Nova)")

    assert df["ordem1"].tolist() == ["Classe (Nova)"]


@pytest.mark.asyncio
async def test_perfis_uf_filter_post_download():
    with patch("agrobr.embrapa_solos.client.fetch_perfis", new_callable=AsyncMock) as mock:
        mock.return_value = (_perfis_pages(), "https://example.com")

        from agrobr.embrapa_solos import perfis

        df = await perfis(uf="SC")
        assert len(df) > 0
        assert (df["uf"] == "SC").all()


@pytest.mark.asyncio
async def test_perfis_uf_filter_no_match():
    with patch("agrobr.embrapa_solos.client.fetch_perfis", new_callable=AsyncMock) as mock:
        mock.return_value = (_perfis_pages(), "https://example.com")

        from agrobr.embrapa_solos import perfis

        df = await perfis(uf="AM")
        assert len(df) == 0


@pytest.mark.asyncio
async def test_warn_once_fires():
    import warnings

    with patch("agrobr.embrapa_solos.client.fetch_perfis", new_callable=AsyncMock) as mock:
        mock.return_value = (_perfis_pages(), "https://example.com")

        from agrobr.embrapa_solos import perfis
        from agrobr.utils.warnings import warn_once_reset

        warn_once_reset("embrapa_solos")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            await perfis()
            nc_warnings = [x for x in w if "CC BY-NC" in str(x.message)]
            assert len(nc_warnings) >= 1
