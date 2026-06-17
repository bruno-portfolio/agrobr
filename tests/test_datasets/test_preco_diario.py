from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.preco_diario import (
    PRECO_DIARIO_INFO,
    PrecoDiarioDataset,
    _fetch_cache,
    _fetch_cepea,
    preco_diario,
)
from agrobr.exceptions import ParseError, SourceUnavailableError

from .conftest import make_source, mock_source_meta


def _mock_df():
    return pd.DataFrame(
        [
            {
                "data": pd.Timestamp("2025-01-15"),
                "valor": 145.30,
                "unidade": "R$/saca 60kg",
                "produto": "soja",
                "fonte": "cepea",
                "praca": "Paranaguá",
            },
            {
                "data": pd.Timestamp("2025-01-14"),
                "valor": 144.80,
                "unidade": "R$/saca 60kg",
                "produto": "soja",
                "fonte": "cepea",
                "praca": "Paranaguá",
            },
        ]
    )


class TestPrecoDiarioSpecific:
    def test_info_cepea_priority(self):
        cepea_source = next(s for s in PRECO_DIARIO_INFO.sources if s.name == "cepea")
        cache_source = next(s for s in PRECO_DIARIO_INFO.sources if s.name == "cache")
        assert cepea_source.priority < cache_source.priority

    def test_info_includes_cafe_robusta(self):
        assert "cafe_robusta" in PRECO_DIARIO_INFO.products


class TestPrecoDiarioFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df = await dataset.fetch("soja")

        assert len(df) == 2
        assert "data" in df.columns
        assert "valor" in df.columns

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.dataset == "preco_diario"
        assert meta.contract_version == "1.0"
        assert meta.attempted_sources == ["cepea"]
        assert meta.selected_source == "cepea"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_fetch_invalid_produto(self):
        dataset = PrecoDiarioDataset()
        with pytest.raises(ValueError, match="não suportado"):
            await dataset.fetch("aveia")

    @pytest.mark.asyncio
    async def test_fetch_accepts_cafe_robusta(self):
        df = pd.DataFrame(
            [
                {
                    "data": pd.Timestamp("2026-06-16"),
                    "valor": 988.50,
                    "unidade": "BRL/sc60kg",
                    "produto": "cafe_robusta",
                    "fonte": "cepea",
                    "praca": "Espírito Santo",
                }
            ]
        )
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("cafe_robusta")

        assert len(result) == 1
        assert result.iloc[0]["produto"] == "cafe_robusta"

    @pytest.mark.asyncio
    async def test_fetch_snapshot_filters_dates(self):
        dataset = PrecoDiarioDataset()
        df_with_future = pd.DataFrame(
            [
                {
                    "data": pd.Timestamp("2025-01-20"),
                    "valor": 150.0,
                    "unidade": "R$/saca 60kg",
                },
                {
                    "data": pd.Timestamp("2025-01-15"),
                    "valor": 145.0,
                    "unidade": "R$/saca 60kg",
                },
                {
                    "data": pd.Timestamp("2025-01-10"),
                    "valor": 140.0,
                    "unidade": "R$/saca 60kg",
                },
            ]
        )
        dataset.info.sources[0].fetch_fn = make_source(df_with_future)

        async with deterministic("2025-01-15"):
            df = await dataset.fetch("soja")

        assert len(df) == 2
        assert df["data"].max().date() <= pd.Timestamp("2025-01-15").date()

    @pytest.mark.asyncio
    async def test_fetch_snapshot_sets_fim(self):
        dataset = PrecoDiarioDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2025-01-15"):
            await dataset.fetch("soja")

        _, kwargs = mock_fn.call_args
        assert kwargs["fim"] == "2025-01-15"

    @pytest.mark.asyncio
    async def test_fetch_forwards_kwargs(self):
        dataset = PrecoDiarioDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch("soja", inicio="2025-01-01", fim="2025-01-31")

        _, kwargs = mock_fn.call_args
        assert kwargs["inicio"] == "2025-01-01"
        assert kwargs["fim"] == "2025-01-31"


class TestPrecoDiarioNormalize:
    @pytest.mark.asyncio
    async def test_normalize_sorts_descending(self):
        df = pd.DataFrame(
            [
                {
                    "data": pd.Timestamp("2025-01-10"),
                    "valor": 140.0,
                    "unidade": "R$/saca 60kg",
                },
                {
                    "data": pd.Timestamp("2025-01-15"),
                    "valor": 145.0,
                    "unidade": "R$/saca 60kg",
                },
            ]
        )
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja")

        assert result["data"].iloc[0] > result["data"].iloc[1]

    @pytest.mark.asyncio
    async def test_normalize_adds_produto_fonte(self):
        df = pd.DataFrame(
            [
                {
                    "data": pd.Timestamp("2025-01-15"),
                    "valor": 145.0,
                    "unidade": "R$/saca 60kg",
                },
            ]
        )
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "soja"
        assert result["fonte"].iloc[0] == "cepea"

    @pytest.mark.asyncio
    async def test_normalize_keeps_existing_produto_fonte(self):
        df = pd.DataFrame(
            [
                {
                    "data": pd.Timestamp("2025-01-15"),
                    "valor": 145.0,
                    "unidade": "R$/saca 60kg",
                    "produto": "milho",
                    "fonte": "custom",
                },
            ]
        )
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "milho"
        assert result["fonte"].iloc[0] == "custom"

    @pytest.mark.asyncio
    async def test_normalize_missing_required_raises(self):
        df = pd.DataFrame(
            [
                {
                    "data": pd.Timestamp("2025-01-15"),
                    "unidade": "R$/saca 60kg",
                },
            ]
        )
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        with pytest.raises(ValueError, match="Missing required column"):
            await dataset.fetch("soja")

    @pytest.mark.asyncio
    async def test_normalize_missing_data_column(self):
        df = pd.DataFrame(
            [
                {
                    "valor": 145.0,
                    "unidade": "R$/saca 60kg",
                },
            ]
        )
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        with pytest.raises(ValueError, match="Missing required column: data"):
            await dataset.fetch("soja")

    @pytest.mark.asyncio
    async def test_normalize_missing_unidade_column(self):
        df = pd.DataFrame(
            [
                {
                    "data": pd.Timestamp("2025-01-15"),
                    "valor": 145.0,
                },
            ]
        )
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        with pytest.raises(ValueError, match="Missing required column: unidade"):
            await dataset.fetch("soja")


class TestPrecoDiarioFallback:
    @pytest.mark.asyncio
    async def test_cepea_fails_falls_back_to_cache(self):
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))
        cache_meta = mock_source_meta()
        dataset.info.sources[1].fetch_fn = make_source(_mock_df(), cache_meta)

        df, meta = await dataset.fetch("soja", return_meta=True)

        assert len(df) == 2
        assert meta.attempted_sources == ["cepea", "cache"]
        assert meta.selected_source == "cache"
        assert meta.from_cache is True

    @pytest.mark.asyncio
    async def test_cepea_parse_error_falls_back(self):
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(
            side_effect=ParseError("cepea", 1, "layout changed")
        )
        dataset.info.sources[1].fetch_fn = make_source(_mock_df())

        df, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.selected_source == "cache"
        assert meta.attempted_sources == ["cepea", "cache"]

    @pytest.mark.asyncio
    async def test_all_sources_fail(self):
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))
        dataset.info.sources[1].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))

        with pytest.raises(SourceUnavailableError):
            await dataset.fetch("soja")

    @pytest.mark.asyncio
    async def test_disabled_source_skipped(self):
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].enabled = False
        dataset.info.sources[1].fetch_fn = make_source(_mock_df())

        try:
            df, meta = await dataset.fetch("soja", return_meta=True)

            assert "cepea" not in meta.attempted_sources
            assert meta.selected_source == "cache"
        finally:
            dataset.info.sources[0].enabled = True


class TestPrecoDiarioDedup:
    @pytest.mark.asyncio
    async def test_normalize_dedup_data_produto(self):
        df = pd.DataFrame(
            [
                {
                    "data": pd.Timestamp("2025-01-15"),
                    "valor": 145.30,
                    "unidade": "R$/saca 60kg",
                    "produto": "soja",
                    "fonte": "cepea",
                },
                {
                    "data": pd.Timestamp("2025-01-15"),
                    "valor": 145.00,
                    "unidade": "R$/saca 60kg",
                    "produto": "soja",
                    "fonte": "cache",
                },
                {
                    "data": pd.Timestamp("2025-01-14"),
                    "valor": 144.80,
                    "unidade": "R$/saca 60kg",
                    "produto": "soja",
                    "fonte": "cepea",
                },
            ]
        )
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja")

        assert not result.duplicated(subset=["data", "produto"]).any()
        assert len(result) == 2


class TestPrecoDiarioPublicAPI:
    @pytest.mark.asyncio
    async def test_public_function_delegates(self):
        with patch.object(PrecoDiarioDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _mock_df()
            await preco_diario("soja", inicio="2025-01-01", fim="2025-01-31")

            mock_fetch.assert_called_once_with(
                "soja", inicio="2025-01-01", fim="2025-01-31", return_meta=False
            )

    @pytest.mark.asyncio
    async def test_public_function_return_meta(self):
        with patch.object(PrecoDiarioDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            meta = mock_source_meta()
            mock_fetch.return_value = (_mock_df(), meta)
            result = await preco_diario("soja", return_meta=True)

            assert isinstance(result, tuple)
            assert len(result) == 2
            assert isinstance(result[0], pd.DataFrame)
            mock_fetch.assert_called_once_with("soja", inicio=None, fim=None, return_meta=True)


class TestPrecoDiarioFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_cepea_online(self):
        mock_df = _mock_df()
        meta = mock_source_meta()
        with patch("agrobr.cepea.indicador", new_callable=AsyncMock, return_value=(mock_df, meta)):
            df, m = await _fetch_cepea("soja")
        assert len(df) == 2
        assert m is not None

    @pytest.mark.asyncio
    async def test_fetch_cepea_deterministic_offline(self):
        mock_df = _mock_df()
        meta = mock_source_meta()
        with patch(
            "agrobr.cepea.indicador", new_callable=AsyncMock, return_value=(mock_df, meta)
        ) as mock_ind:
            async with deterministic("2025-01-15"):
                await _fetch_cepea("soja")
            _, kwargs = mock_ind.call_args
            assert kwargs.get("offline") is True

    @pytest.mark.asyncio
    async def test_fetch_cache_str_dates(self):
        mock_store = MagicMock()
        mock_store.indicadores_query.return_value = [
            {"data": "2025-01-15", "valor": 145.0, "unidade": "R$/sc60kg", "produto": "soja"}
        ]
        with patch("agrobr.cache.duckdb_store.get_store", return_value=mock_store):
            df, meta = await _fetch_cache("soja", inicio="2025-01-01", fim="2025-01-31")
        assert len(df) == 1
        assert meta is None
        call_kwargs = mock_store.indicadores_query.call_args[1]
        assert isinstance(call_kwargs["inicio"], datetime)
        assert isinstance(call_kwargs["fim"], datetime)

    @pytest.mark.asyncio
    async def test_fetch_cache_empty_raises(self):
        mock_store = MagicMock()
        mock_store.indicadores_query.return_value = []
        with (
            patch("agrobr.cache.duckdb_store.get_store", return_value=mock_store),
            pytest.raises(ValueError, match="No cached data"),
        ):
            await _fetch_cache("soja")
