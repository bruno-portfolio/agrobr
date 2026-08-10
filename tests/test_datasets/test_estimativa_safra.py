from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.estimativa_safra import (
    ESTIMATIVA_SAFRA_INFO,
    EstimativaSafraDataset,
    estimativa_safra,
)
from agrobr.exceptions import SourceUnavailableError

from .conftest import make_source, mock_source_meta


def _mock_df():
    return pd.DataFrame(
        [
            {
                "fonte": "conab",
                "produto": "soja",
                "safra": "2024/25",
                "uf": "MT",
                "area_plantada": 12500.0,
                "area_colhida": 12400.0,
                "produtividade": 3400.0,
                "producao": 42500.0,
                "levantamento": 3,
                "data_publicacao": pd.Timestamp("2025-01-15"),
            },
        ]
    )


def _sidra_lspa_df(ano: int = 2022) -> pd.DataFrame:
    unidade = {
        "Área plantada": "Hectares",
        "Área colhida": "Hectares",
        "Produção": "Toneladas",
        "Rendimento médio": "Quilogramas por Hectare",
    }
    dados = {
        f"novembro {ano}": {
            "Área plantada": 39_000_000,
            "Área colhida": 38_000_000,
            "Produção": 110_000_000,
            "Rendimento médio": 2894,
        },
        f"dezembro {ano}": {
            "Área plantada": 41_000_000,
            "Área colhida": 40_000_000,
            "Produção": 120_000_000,
            "Rendimento médio": 3000,
        },
    }
    rows = [
        {
            "nivel_territorial": "Brasil",
            "localidade": unidade[var],
            "variavel": mes,
            "classificacao": var,
            "ano": ano,
            "valor": valor,
            "produto": "soja",
            "fonte": "ibge_lspa",
        }
        for mes, variaveis in dados.items()
        for var, valor in variaveis.items()
    ]
    return pd.DataFrame(rows)


class TestEstimativaSafraSpecific:
    def test_info_conab_priority(self):
        conab_source = next(s for s in ESTIMATIVA_SAFRA_INFO.sources if s.name == "conab")
        lspa_source = next(s for s in ESTIMATIVA_SAFRA_INFO.sources if s.name == "ibge_lspa")
        assert conab_source.priority < lspa_source.priority


class TestEstimativaSafraFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        dataset = EstimativaSafraDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df = await dataset.fetch("soja")

        assert len(df) == 1
        assert "produtividade" in df.columns
        assert "levantamento" in df.columns

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = EstimativaSafraDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.dataset == "estimativa_safra"
        assert meta.contract_version == "1.0"
        assert meta.attempted_sources == ["conab"]
        assert meta.selected_source == "conab"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_fetch_invalid_produto(self):
        dataset = EstimativaSafraDataset()
        with pytest.raises(ValueError, match="não suportado"):
            await dataset.fetch("aveia")

    @pytest.mark.asyncio
    async def test_forwards_safra_and_uf(self):
        dataset = EstimativaSafraDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch("soja", safra="2024/25", uf="MT")

        _, kwargs = mock_fn.call_args
        assert kwargs["safra"] == "2024/25"
        assert kwargs["uf"] == "MT"

    @pytest.mark.asyncio
    async def test_snapshot_included_in_meta(self):
        dataset = EstimativaSafraDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        async with deterministic("2025-01-15"):
            df, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.snapshot == "2025-01-15"


class TestEstimativaSafraNormalize:
    @pytest.mark.asyncio
    async def test_normalize_adds_produto_fonte(self):
        df = _mock_df().drop(columns=["produto", "fonte"])
        dataset = EstimativaSafraDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "soja"
        assert result["fonte"].iloc[0] == "conab"

    @pytest.mark.asyncio
    async def test_normalize_keeps_existing_produto_fonte(self):
        dataset = EstimativaSafraDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "soja"
        assert result["fonte"].iloc[0] == "conab"


class TestNormalizeLspa:
    def test_reduces_to_latest_month_and_converts_units(self):
        from agrobr.datasets.estimativa_safra import _normalize_lspa

        df = _normalize_lspa(_sidra_lspa_df(2022), "soja", "2022/23", None)

        assert len(df) == 1
        row = df.iloc[0]
        assert row["fonte"] == "ibge_lspa"
        assert row["produto"] == "soja"
        assert row["safra"] == "2022/23"
        assert row["uf"] is None
        assert row["area_plantada"] == pytest.approx(41000.0)
        assert row["area_colhida"] == pytest.approx(40000.0)
        assert row["producao"] == pytest.approx(120000.0)
        assert row["produtividade"] == pytest.approx(3000.0)
        assert pd.isna(row["levantamento"])
        assert pd.isna(row["data_publicacao"])

    def test_aggregates_subsafras_and_recomputes_yield(self):
        from agrobr.datasets.estimativa_safra import _normalize_lspa

        df_multi = pd.concat([_sidra_lspa_df(2023), _sidra_lspa_df(2023)], ignore_index=True)

        df = _normalize_lspa(df_multi, "milho", "2023/24", None)

        row = df.iloc[0]
        assert row["area_plantada"] == pytest.approx(82000.0)
        assert row["area_colhida"] == pytest.approx(80000.0)
        assert row["producao"] == pytest.approx(240000.0)
        assert row["produtividade"] == pytest.approx(3000.0)

    def test_uf_is_uppercased(self):
        from agrobr.datasets.estimativa_safra import _normalize_lspa

        df = _normalize_lspa(_sidra_lspa_df(2022), "soja", "2022/23", "mt")

        assert df.iloc[0]["uf"] == "MT"

    def test_empty_input_returns_contract_columns(self):
        from agrobr.datasets.estimativa_safra import _SAFRA_OUTPUT_COLS, _normalize_lspa

        df = _normalize_lspa(pd.DataFrame(), "soja", "2022/23", None)

        assert len(df) == 0
        assert list(df.columns) == _SAFRA_OUTPUT_COLS

    def test_non_sidra_input_returns_empty_contract(self):
        from agrobr.datasets.estimativa_safra import _SAFRA_OUTPUT_COLS, _normalize_lspa

        df = _normalize_lspa(_mock_df(), "soja", "2022/23", None)

        assert len(df) == 0
        assert list(df.columns) == _SAFRA_OUTPUT_COLS

    def test_output_passes_contract(self):
        from agrobr.contracts import validate_dataset
        from agrobr.datasets.estimativa_safra import _normalize_lspa

        df = _normalize_lspa(_sidra_lspa_df(2022), "soja", "2022/23", None)

        validate_dataset(df, "estimativa_safra")


class TestEstimativaSafraFallback:
    @pytest.mark.asyncio
    async def test_conab_fails_falls_back_to_lspa(self):
        dataset = EstimativaSafraDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))
        dataset.info.sources[1].fetch_fn = make_source(_mock_df())

        df, meta = await dataset.fetch("soja", return_meta=True)

        assert len(df) == 1
        assert meta.attempted_sources == ["conab", "ibge_lspa"]
        assert meta.selected_source == "ibge_lspa"

    @pytest.mark.asyncio
    async def test_all_sources_fail(self):
        dataset = EstimativaSafraDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))
        dataset.info.sources[1].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))

        with pytest.raises(SourceUnavailableError):
            await dataset.fetch("soja")

    @pytest.mark.asyncio
    async def test_conab_unavailable_lspa_fallback_passes_contract(self):
        from agrobr.datasets.estimativa_safra import _fetch_conab, _fetch_ibge_lspa

        dataset = EstimativaSafraDataset()
        dataset.info.sources[0].fetch_fn = _fetch_conab
        dataset.info.sources[1].fetch_fn = _fetch_ibge_lspa

        with (
            patch(
                "agrobr.conab.safras",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="conab"),
            ),
            patch(
                "agrobr.ibge.lspa",
                new_callable=AsyncMock,
                return_value=(_sidra_lspa_df(2022), mock_source_meta()),
            ),
        ):
            df, meta = await dataset.fetch("soja", safra="2022/23", return_meta=True)

        assert meta.selected_source == "ibge_lspa"
        assert len(df) == 1
        assert df.iloc[0]["fonte"] == "ibge_lspa"
        assert df.iloc[0]["safra"] == "2022/23"


class TestEstimativaSafraPublicAPI:
    @pytest.mark.asyncio
    async def test_public_function_delegates(self):
        with patch.object(EstimativaSafraDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _mock_df()
            await estimativa_safra("soja", safra="2024/25", uf="MT")

            mock_fetch.assert_called_once_with("soja", safra="2024/25", uf="MT", return_meta=False)

    @pytest.mark.asyncio
    async def test_public_function_return_meta(self):
        with patch.object(EstimativaSafraDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_mock_df(), mock_source_meta())
            result = await estimativa_safra("soja", return_meta=True)

            assert isinstance(result, tuple)
            assert len(result) == 2
            assert isinstance(result[0], pd.DataFrame)


class TestEstimativaSafraFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_conab_forwards_params(self):
        meta = mock_source_meta()
        with patch(
            "agrobr.conab.safras",
            new_callable=AsyncMock,
            return_value=(_mock_df(), meta),
        ) as mock_fn:
            from agrobr.datasets.estimativa_safra import _fetch_conab

            await _fetch_conab("soja", safra="2024/25", uf="PR")
        mock_fn.assert_called_once_with("soja", safra="2024/25", uf="PR", return_meta=True)

    @pytest.mark.asyncio
    async def test_fetch_ibge_lspa_maps_ano_and_normalizes(self):
        meta = mock_source_meta()
        with patch(
            "agrobr.ibge.lspa",
            new_callable=AsyncMock,
            return_value=(_sidra_lspa_df(2024), meta),
        ) as mock_fn:
            from agrobr.datasets.estimativa_safra import _fetch_ibge_lspa

            df, _ = await _fetch_ibge_lspa("soja", safra="2024/25", uf="PR")

        mock_fn.assert_called_once_with("soja", ano=2024, uf="PR", return_meta=True)
        assert len(df) == 1
        assert df.iloc[0]["safra"] == "2024/25"
        assert df.iloc[0]["uf"] == "PR"
        assert df.iloc[0]["produtividade"] == pytest.approx(3000.0)
