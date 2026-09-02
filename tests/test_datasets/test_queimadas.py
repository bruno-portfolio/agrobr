import httpx
import pandas as pd
import pytest

from agrobr.datasets.queimadas import (
    QUEIMADAS_INFO,
    QueimadasDataset,
)
from agrobr.exceptions import SourceUnavailableError

from .conftest import make_source


def _make_df(**overrides):
    row = {
        "data": pd.Timestamp("2024-08-15"),
        "hora_gmt": "1530",
        "lat": -12.5,
        "lon": -49.3,
        "satelite": "NOAA-20",
        "municipio": "Palmas",
        "municipio_id": 1721000,
        "estado": "Tocantins",
        "uf": "TO",
        "bioma": "Cerrado",
        "numero_dias_sem_chuva": 45.0,
        "precipitacao": 0.0,
        "risco_fogo": 0.85,
        "frp": 42.5,
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestQueimadasFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_df(self):
        dataset = QueimadasDataset()
        dataset.info.sources[0].fetch_fn = make_source(_make_df())
        df = await dataset.fetch(ano=2024, mes=8)

        assert len(df) == 1
        assert "data" in df.columns
        assert "lat" in df.columns
        assert "satelite" in df.columns

    @pytest.mark.asyncio
    async def test_fetch_with_dia(self):
        mock_fn = make_source(_make_df())
        dataset = QueimadasDataset()
        dataset.info.sources[0].fetch_fn = mock_fn
        await dataset.fetch(ano=2024, mes=8, dia=15)

        call_kwargs = mock_fn.call_args[1]
        assert call_kwargs["dia"] == 15

    @pytest.mark.asyncio
    async def test_fetch_with_filters(self):
        mock_fn = make_source(_make_df())
        dataset = QueimadasDataset()
        dataset.info.sources[0].fetch_fn = mock_fn
        await dataset.fetch(ano=2024, mes=8, uf="TO", bioma="Cerrado", satelite="NOAA-20")

        call_kwargs = mock_fn.call_args[1]
        assert call_kwargs["uf"] == "TO"
        assert call_kwargs["bioma"] == "Cerrado"
        assert call_kwargs["satelite"] == "NOAA-20"

    @pytest.mark.asyncio
    async def test_fetch_normalizes_bioma(self):
        mock_fn = make_source(_make_df(bioma="Amazônia"))
        dataset = QueimadasDataset()
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch(ano=2025, mes=1, bioma="Amazonia")

        assert mock_fn.call_args.kwargs["bioma"] == "Amazônia"

    @pytest.mark.asyncio
    async def test_fetch_invalid_bioma_raises_before_source(self):
        mock_fn = make_source(_make_df())
        dataset = QueimadasDataset()
        dataset.info.sources[0].fetch_fn = mock_fn

        with pytest.raises(ValueError, match="Bioma inválido.*Atlantida"):
            await dataset.fetch(ano=2025, mes=1, bioma="Atlantida")

        mock_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = QueimadasDataset()
        dataset.info.sources[0].fetch_fn = make_source(_make_df())
        df, meta = await dataset.fetch(ano=2024, mes=8, return_meta=True)

        assert meta.dataset == "queimadas"
        assert meta.contract_version == "1.0"
        assert "inpe" in meta.attempted_sources
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_source_failure(self):
        dataset = QueimadasDataset()
        dataset.info.sources[0].fetch_fn = make_source(
            _make_df(), raises=httpx.ConnectError("connection failed")
        )
        with pytest.raises(SourceUnavailableError):
            await dataset.fetch(ano=2024, mes=8)


class TestQueimadasInfo:
    def test_source_inpe(self):
        assert len(QUEIMADAS_INFO.sources) == 1
        assert QUEIMADAS_INFO.sources[0].name == "inpe"

    def test_products_empty(self):
        assert QUEIMADAS_INFO.products == []

    def test_license(self):
        assert QUEIMADAS_INFO.license == "livre"


class TestQueimadasFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_queimadas_forwards_params(self):
        from unittest.mock import AsyncMock, patch

        from .conftest import mock_source_meta

        df = _make_df()
        meta = mock_source_meta()
        with patch(
            "agrobr.queimadas.focos",
            new_callable=AsyncMock,
            return_value=(df, meta),
        ) as mock_fn:
            from agrobr.datasets.queimadas import _fetch_queimadas

            await _fetch_queimadas(
                "", ano=2024, mes=8, dia=15, uf="TO", bioma="Cerrado", satelite="NOAA-20"
            )
        mock_fn.assert_called_once_with(
            ano=2024,
            mes=8,
            dia=15,
            uf="TO",
            bioma="Cerrado",
            satelite="NOAA-20",
            return_meta=True,
        )

    @pytest.mark.asyncio
    async def test_fetch_queimadas_defaults(self):
        from unittest.mock import AsyncMock, patch

        from .conftest import mock_source_meta

        df = _make_df()
        meta = mock_source_meta()
        with patch(
            "agrobr.queimadas.focos",
            new_callable=AsyncMock,
            return_value=(df, meta),
        ) as mock_fn:
            from agrobr.datasets.queimadas import _fetch_queimadas

            await _fetch_queimadas("", ano=2024, mes=8)
        _, kwargs = mock_fn.call_args
        assert kwargs["dia"] is None
        assert kwargs["uf"] is None
        assert kwargs["bioma"] is None
        assert kwargs["satelite"] is None
