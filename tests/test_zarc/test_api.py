from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.zarc import api

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "zarc" / "tabua_risco_sample"


def _golden_csv_bytes() -> bytes:
    return (GOLDEN_DIR / "response.csv").read_bytes()


def _mock_resources() -> list[dict[str, str]]:
    return [
        {"id": "r1", "name": "Safra 2024/2025", "url": "https://x/24.csv", "format": "CSV"},
        {"id": "r2", "name": "Safra 2025/2026", "url": "https://x/25.csv", "format": "CSV"},
        {"id": "r3", "name": "Safra perene", "url": "https://x/perene.csv", "format": "CSV"},
    ]


@pytest.fixture(autouse=True)
def _clear_cache():
    api._cache.clear()
    yield
    api._cache.clear()


class TestZoneamento:
    @pytest.mark.asyncio
    async def test_zoneamento_returns_dataframe(self):
        with patch.object(api.client, "fetch_tabua_risco", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_golden_csv_bytes(), "https://x/25.csv")
            df = await api.zoneamento(safra="2025/2026")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 19

    @pytest.mark.asyncio
    async def test_zoneamento_filter_cultura(self):
        with patch.object(api.client, "fetch_tabua_risco", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_golden_csv_bytes(), "https://x/25.csv")
            df = await api.zoneamento(safra="2025/2026", cultura="soja")

        assert len(df) > 0
        assert (df["cultura"] == "soja").all()

    @pytest.mark.asyncio
    async def test_zoneamento_filter_uf(self):
        with patch.object(api.client, "fetch_tabua_risco", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_golden_csv_bytes(), "https://x/25.csv")
            df = await api.zoneamento(safra="2025/2026", uf="MT")

        assert len(df) > 0
        assert (df["uf"] == "MT").all()

    @pytest.mark.asyncio
    async def test_zoneamento_filter_uf_invalid(self):
        with pytest.raises(ValueError, match="UF invalida"):
            await api.zoneamento(safra="2025/2026", uf="XX")

    @pytest.mark.asyncio
    async def test_zoneamento_filter_municipio_geocodigo(self):
        with patch.object(api.client, "fetch_tabua_risco", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_golden_csv_bytes(), "https://x/25.csv")
            df = await api.zoneamento(safra="2025/2026", municipio=5103403)

        assert len(df) > 0
        assert (df["geocodigo"] == "5103403").all()

    @pytest.mark.asyncio
    async def test_zoneamento_filter_municipio_literal(self):
        cached = pd.DataFrame(
            {
                "municipio": ["Vila (Nova)", "Vila Nova"],
                "geocodigo": ["1", "2"],
            }
        )
        api._cache["2025/2026"] = (cached, "https://x/25.csv")

        df = await api.zoneamento(safra="2025/2026", municipio="Vila (Nova)")

        assert df["municipio"].tolist() == ["Vila (Nova)"]

    @pytest.mark.asyncio
    async def test_zoneamento_default_safra_latest(self):
        with patch.object(
            api.client, "discover_resources", new_callable=AsyncMock
        ) as mock_discover:
            mock_discover.return_value = _mock_resources()
            with patch.object(
                api.client, "fetch_tabua_risco", new_callable=AsyncMock
            ) as mock_fetch:
                mock_fetch.return_value = (_golden_csv_bytes(), "https://x/25.csv")
                await api.zoneamento()

        mock_fetch.assert_awaited_once()
        called_safra = mock_fetch.call_args[0][0]
        assert called_safra == "2025/2026"

    @pytest.mark.asyncio
    async def test_zoneamento_sem_recursos_de_safra(self):
        with (
            patch.object(
                api.client,
                "discover_resources",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch.object(api.client, "fetch_tabua_risco", new_callable=AsyncMock) as fetch,
            pytest.raises(SourceUnavailableError, match="CKAN sem recursos de safra"),
        ):
            await api.zoneamento()

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_zoneamento_return_meta(self):
        with patch.object(api.client, "fetch_tabua_risco", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_golden_csv_bytes(), "https://x/25.csv")
            result = await api.zoneamento(safra="2025/2026", return_meta=True)

        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(meta, MetaInfo)
        assert meta.source == "zarc"


class TestCulturas:
    def test_culturas_returns_sorted_list(self):
        result = api.culturas()
        assert isinstance(result, list)
        assert result == sorted(result)
        assert "soja" in result
        assert "milho_1" in result


class TestSafrasDisponiveis:
    @pytest.mark.asyncio
    async def test_safras_disponiveis(self):
        with patch.object(
            api.client, "discover_resources", new_callable=AsyncMock
        ) as mock_discover:
            mock_discover.return_value = _mock_resources()
            result = await api.safras_disponiveis()

        assert result == ["2024/2025", "2025/2026", "perene"]
