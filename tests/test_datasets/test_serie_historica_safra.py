from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.datasets.serie_historica_safra import (
    SERIE_HISTORICA_SAFRA_INFO,
    SerieHistoricaSafraDataset,
)
from agrobr.exceptions import SourceUnavailableError

from .conftest import make_source, mock_source_meta


def _mock_df():
    return pd.DataFrame(
        {
            "produto": ["soja", "soja"],
            "safra": ["2023/24", "2023/24"],
            "regiao": ["CENTRO-OESTE", "SUL"],
            "uf": ["MT", "PR"],
            "area_plantada_mil_ha": [12000.0, 6000.0],
            "producao_mil_ton": [40000.0, 22000.0],
            "produtividade_kg_ha": [3333.0, 3667.0],
        }
    )


class TestSerieHistoricaSafraFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        dataset = SerieHistoricaSafraDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df = await dataset.fetch("soja")

        assert len(df) == 2
        assert "area_plantada_mil_ha" in df.columns
        assert "producao_mil_ton" in df.columns
        assert "produtividade_kg_ha" in df.columns

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = SerieHistoricaSafraDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.dataset == "serie_historica_safra"
        assert meta.contract_version == "1.0"
        assert meta.attempted_sources == ["conab_serie_historica"]
        assert meta.selected_source == "conab_serie_historica"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_fetch_invalid_produto(self):
        dataset = SerieHistoricaSafraDataset()
        with pytest.raises(ValueError, match="não suportado"):
            await dataset.fetch("banana")

    @pytest.mark.asyncio
    async def test_params_passthrough(self):
        dataset = SerieHistoricaSafraDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch("soja", inicio=2020, fim=2024, uf="MT")

        _, kwargs = mock_fn.call_args
        assert kwargs["inicio"] == 2020
        assert kwargs["fim"] == 2024
        assert kwargs["uf"] == "MT"

    @pytest.mark.asyncio
    async def test_normalize_noop(self):
        df = _mock_df()
        dataset = SerieHistoricaSafraDataset()
        dataset.info.sources[0].fetch_fn = make_source(df.copy())

        result = await dataset.fetch("soja")

        pd.testing.assert_frame_equal(result, df)

    @pytest.mark.asyncio
    async def test_contract_validation_called(self):
        dataset = SerieHistoricaSafraDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        with patch.object(dataset, "_validate_contract") as mock_validate:
            await dataset.fetch("soja")
            mock_validate.assert_called_once()

    @pytest.mark.asyncio
    async def test_source_failure(self):
        dataset = SerieHistoricaSafraDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))

        with pytest.raises(SourceUnavailableError):
            await dataset.fetch("soja")

    @pytest.mark.asyncio
    async def test_deterministic_snapshot(self):
        dataset = SerieHistoricaSafraDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        with patch(
            "agrobr.datasets.serie_historica_safra.get_snapshot",
            return_value="2024-01-01",
        ):
            await dataset.fetch("soja")

        _, kwargs = mock_fn.call_args
        assert kwargs["inicio"] == 2019


class TestSerieHistoricaSafraInfo:
    def test_products_count(self):
        assert len(SERIE_HISTORICA_SAFRA_INFO.products) == 44

    def test_license(self):
        assert SERIE_HISTORICA_SAFRA_INFO.license == "livre"


class TestSerieHistoricaSafraFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_conab_serie_forwards_params(self):
        df = _mock_df()
        meta = mock_source_meta()
        with patch(
            "agrobr.conab.serie_historica",
            new_callable=AsyncMock,
            return_value=(df, meta),
        ) as mock_fn:
            from agrobr.datasets.serie_historica_safra import _fetch_conab_serie

            await _fetch_conab_serie("soja", inicio=2020, fim=2024, uf="MT")
        mock_fn.assert_called_once_with("soja", inicio=2020, fim=2024, uf="MT", return_meta=True)

    @pytest.mark.asyncio
    async def test_fetch_conab_serie_defaults(self):
        df = _mock_df()
        meta = mock_source_meta()
        with patch(
            "agrobr.conab.serie_historica",
            new_callable=AsyncMock,
            return_value=(df, meta),
        ) as mock_fn:
            from agrobr.datasets.serie_historica_safra import _fetch_conab_serie

            await _fetch_conab_serie("milho")
        _, kwargs = mock_fn.call_args
        assert kwargs["inicio"] is None
        assert kwargs["fim"] is None
        assert kwargs["uf"] is None
