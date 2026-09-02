from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.contracts import validate_dataset
from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.producao_anual import (
    PRODUCAO_ANUAL_INFO,
    ProducaoAnualDataset,
    _fetch_conab,
    producao_anual,
)
from agrobr.exceptions import SourceUnavailableError

from .conftest import make_source, mock_source_meta


def _mock_df():
    return pd.DataFrame(
        [
            {
                "ano": 2023,
                "localidade": "Mato Grosso",
                "produto": "soja",
                "area_plantada": 12000000.0,
                "producao": 43000000.0,
                "rendimento": 3583.0,
                "fonte": "ibge_pam",
            },
        ]
    )


def _mock_conab_df():
    return pd.DataFrame(
        [
            {
                "fonte": "conab",
                "produto": "soja",
                "safra": "2022/23",
                "uf": "MT",
                "area_plantada": 12000.0,
                "area_colhida": 11981.0,
                "produtividade": 3583.0,
                "producao": 43000.0,
                "levantamento": 12,
                "data_publicacao": pd.Timestamp("2023-09-01"),
            }
        ]
    )


class TestProducaoAnualSpecific:
    def test_info_ibge_pam_priority(self):
        ibge_source = next(s for s in PRODUCAO_ANUAL_INFO.sources if s.name == "ibge_pam")
        conab_source = next(s for s in PRODUCAO_ANUAL_INFO.sources if s.name == "conab")
        assert ibge_source.priority < conab_source.priority

    @pytest.mark.asyncio
    async def test_nivel_invalido_raises(self):
        with pytest.raises(ValueError, match="nível inválido"):
            await producao_anual("soja", nivel="Brasil")
        with pytest.raises(ValueError, match="nível inválido"):
            await producao_anual("soja", nivel="estado")


class TestProducaoAnualFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        dataset = ProducaoAnualDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df = await dataset.fetch("soja")

        assert len(df) == 1
        assert "area_plantada" in df.columns
        assert "rendimento" in df.columns

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = ProducaoAnualDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.dataset == "producao_anual"
        assert meta.contract_version == "1.0"
        assert meta.attempted_sources == ["ibge_pam"]
        assert meta.selected_source == "ibge_pam"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_fetch_invalid_produto(self):
        dataset = ProducaoAnualDataset()
        with pytest.raises(ValueError, match="não suportado"):
            await dataset.fetch("aveia")

    @pytest.mark.asyncio
    async def test_snapshot_sets_ano_minus_1(self):
        dataset = ProducaoAnualDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2025-06-15"):
            await dataset.fetch("soja")

        _, kwargs = mock_fn.call_args
        assert kwargs["ano"] == 2024

    @pytest.mark.asyncio
    async def test_snapshot_does_not_override_explicit_ano(self):
        dataset = ProducaoAnualDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2025-06-15"):
            await dataset.fetch("soja", ano=2022)

        _, kwargs = mock_fn.call_args
        assert kwargs["ano"] == 2022

    @pytest.mark.asyncio
    async def test_forwards_nivel_uf(self):
        dataset = ProducaoAnualDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch("soja", ano=2023, nivel="municipio", uf="MT")

        _, kwargs = mock_fn.call_args
        assert kwargs["nivel"] == "municipio"
        assert kwargs["uf"] == "MT"

    @pytest.mark.asyncio
    async def test_invalid_uf_raises_before_source(self):
        dataset = ProducaoAnualDataset()
        mock_fn = AsyncMock(return_value=(_mock_df(), mock_source_meta()))
        dataset.info.sources[0].fetch_fn = mock_fn

        with pytest.raises(ValueError, match="UF invalida"):
            await dataset.fetch("soja", uf="XX")

        mock_fn.assert_not_awaited()


class TestProducaoAnualNormalize:
    @pytest.mark.asyncio
    async def test_normalize_adds_produto_fonte(self):
        df = _mock_df().drop(columns=["produto", "fonte"])
        dataset = ProducaoAnualDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "soja"
        assert result["fonte"].iloc[0] == "ibge_pam"

    @pytest.mark.asyncio
    async def test_normalize_keeps_existing_produto_fonte(self):
        dataset = ProducaoAnualDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "soja"
        assert result["fonte"].iloc[0] == "ibge_pam"


class TestProducaoAnualFallback:
    @pytest.mark.asyncio
    async def test_ibge_fails_falls_back_to_conab(self):
        dataset = ProducaoAnualDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))
        dataset.info.sources[1].fetch_fn = _fetch_conab

        with patch(
            "agrobr.conab.safras",
            new_callable=AsyncMock,
            return_value=(_mock_conab_df(), mock_source_meta()),
        ) as mock_safras:
            df, meta = await dataset.fetch(
                "soja",
                ano=2023,
                nivel="uf",
                uf="MT",
                return_meta=True,
            )

        assert len(df) == 1
        assert df.iloc[0]["ano"] == 2023
        assert df.iloc[0]["localidade"] == "Mato Grosso"
        assert df.iloc[0]["area_plantada"] == pytest.approx(12_000_000.0)
        assert df.iloc[0]["producao"] == pytest.approx(43_000_000.0)
        assert df.iloc[0]["fonte"] == "conab"
        assert meta.attempted_sources == ["ibge_pam", "conab"]
        assert meta.selected_source == "conab"
        mock_safras.assert_awaited_once_with(
            "soja",
            safra="2022/23",
            uf="MT",
            return_meta=True,
        )

    @pytest.mark.asyncio
    async def test_conab_aggregates_brasil(self):
        conab_df = pd.concat(
            [
                _mock_conab_df(),
                _mock_conab_df().assign(
                    uf="PR",
                    area_plantada=6000.0,
                    area_colhida=5900.0,
                    producao=22000.0,
                    produtividade=3729.0,
                ),
            ],
            ignore_index=True,
        )

        with patch(
            "agrobr.conab.safras",
            new_callable=AsyncMock,
            return_value=(conab_df, mock_source_meta()),
        ) as mock_safras:
            df, _ = await _fetch_conab("soja", ano=2023, nivel="brasil", uf="MT")

        assert len(df) == 1
        assert df.iloc[0]["localidade"] == "Brasil"
        assert df.iloc[0]["area_plantada"] == pytest.approx(18_000_000.0)
        assert pd.isna(df.iloc[0]["area_colhida"])
        assert df.iloc[0]["producao"] == pytest.approx(65_000_000.0)
        assert df.iloc[0]["rendimento"] == pytest.approx(65_000_000 * 1000 / 18_000_000)
        mock_safras.assert_awaited_once_with(
            "soja",
            safra="2022/23",
            uf=None,
            return_meta=True,
        )

    @pytest.mark.asyncio
    async def test_conab_rejects_municipio_before_request(self):
        with (
            patch("agrobr.conab.safras", new_callable=AsyncMock) as mock_safras,
            pytest.raises(SourceUnavailableError, match="granularidade municipal"),
        ):
            await _fetch_conab("soja", ano=2023, nivel="municipio", uf="MT")

        mock_safras.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_all_sources_fail(self):
        dataset = ProducaoAnualDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))
        dataset.info.sources[1].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))

        with pytest.raises(SourceUnavailableError):
            await dataset.fetch("soja")


class TestProducaoAnualPublicAPI:
    @pytest.mark.asyncio
    async def test_public_function_delegates(self):
        with patch.object(ProducaoAnualDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _mock_df()
            await producao_anual("soja", ano=2023, nivel="uf", uf="MT")

            mock_fetch.assert_called_once_with(
                "soja", ano=2023, nivel="uf", uf="MT", return_meta=False
            )

    @pytest.mark.asyncio
    async def test_public_function_return_meta(self):
        with patch.object(ProducaoAnualDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_mock_df(), mock_source_meta())
            result = await producao_anual("soja", return_meta=True)

            assert isinstance(result, tuple)
            assert len(result) == 2
            assert isinstance(result[0], pd.DataFrame)


class TestProducaoAnualFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_ibge_pam_forwards_nivel(self):
        meta = mock_source_meta()
        with patch(
            "agrobr.ibge.pam",
            new_callable=AsyncMock,
            return_value=(_mock_df(), meta),
        ) as mock_fn:
            from agrobr.datasets.producao_anual import _fetch_ibge_pam

            await _fetch_ibge_pam("soja", ano=2024, nivel="municipio", uf="PR")
        mock_fn.assert_called_once_with(
            "soja", ano=2024, nivel="municipio", uf="PR", return_meta=True
        )

    @pytest.mark.asyncio
    async def test_fetch_conab_normalizes_real_golden(self):
        path = Path(__file__).parents[1] / "golden_data" / "conab" / "safra_sample"
        metadata = {
            "url": "golden://conab/safra_sample",
            "safra": "2025/26",
            "levantamento": 3,
        }

        with patch(
            "agrobr.conab.client.fetch_safra_xlsx",
            new_callable=AsyncMock,
            return_value=(BytesIO((path / "response.xlsx").read_bytes()), metadata),
        ) as mock_fetch:
            result_df, _ = await _fetch_conab("soja", ano=2026, nivel="uf", uf="MT")

        assert result_df.columns.tolist() == [
            "ano",
            "localidade",
            "produto",
            "area_plantada",
            "area_colhida",
            "producao",
            "rendimento",
            "valor_producao",
            "fonte",
        ]
        assert len(result_df) == 1
        row = result_df.iloc[0]
        assert row["ano"] == 2026
        assert row["localidade"] == "Mato Grosso"
        assert row["area_plantada"] == pytest.approx(13_006_200.0)
        assert pd.isna(row["area_colhida"])
        assert row["producao"] == pytest.approx(48_643_200.0)
        assert row["rendimento"] == pytest.approx(3740.0)
        assert pd.isna(row["valor_producao"])
        assert row["fonte"] == "conab"
        validate_dataset(result_df, "producao_anual")
        mock_fetch.assert_awaited_once_with(safra="2025/26", levantamento=None)
