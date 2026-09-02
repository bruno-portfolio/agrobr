"""Testes específicos para o dataset credito_rural (fetch com mock)."""

from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.datasets.credito_rural import (
    CREDITO_RURAL_INFO,
    CreditoRuralDataset,
    credito_rural,
)
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import SourceUnavailableError

from .conftest import make_source, mock_source_meta


def _mock_df():
    return pd.DataFrame(
        [
            {
                "safra": "2023/2024",
                "uf": "MT",
                "produto": "soja",
                "finalidade": "custeio",
                "valor": 285431200.0,
                "area_financiada": 98500.0,
                "qtd_contratos": 1240,
            },
        ]
    )


class TestCreditoRuralInfo:
    def test_single_source_bcb(self):
        assert len(CREDITO_RURAL_INFO.sources) == 1
        assert CREDITO_RURAL_INFO.sources[0].name == "bcb"

    def test_contract_version(self):
        assert CREDITO_RURAL_INFO.contract_version == "1.1"

    def test_license_livre(self):
        assert CREDITO_RURAL_INFO.license == "livre"


class TestCreditoRuralFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())
        df = await dataset.fetch("soja", safra="2023/24")

        assert len(df) == 1
        assert "valor" in df.columns
        assert df.iloc[0]["valor"] == 285431200.0

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())
        df, meta = await dataset.fetch("soja", safra="2023/24", return_meta=True)

        assert meta.dataset == "credito_rural"
        assert meta.contract_version == "1.1"
        assert "bcb" in meta.attempted_sources
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_fetch_invalid_produto(self):
        dataset = CreditoRuralDataset()
        with pytest.raises(ValueError, match="não suportado"):
            await dataset.fetch("abacaxi")

    @pytest.mark.asyncio
    async def test_source_failure(self):
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("down"))

        with pytest.raises(SourceUnavailableError):
            await dataset.fetch("soja")

    @pytest.mark.asyncio
    async def test_snapshot_generates_safra(self):
        dataset = CreditoRuralDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2025-03-15"):
            await dataset.fetch("soja")

        _, kwargs = mock_fn.call_args
        assert kwargs["safra"] == "2024/2025"

    @pytest.mark.asyncio
    async def test_snapshot_does_not_override_explicit_safra(self):
        dataset = CreditoRuralDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2025-03-15"):
            await dataset.fetch("soja", safra="2022/23")

        _, kwargs = mock_fn.call_args
        assert kwargs["safra"] == "2022/23"

    @pytest.mark.asyncio
    async def test_forwards_all_kwargs(self):
        dataset = CreditoRuralDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch(
            "soja",
            safra="2023/24",
            finalidade="investimento",
            uf=" mt ",
            agregacao="uf",
            programa="pronaf",
            tipo_seguro="proagro",
        )

        _, kwargs = mock_fn.call_args
        assert kwargs["safra"] == "2023/24"
        assert kwargs["finalidade"] == "investimento"
        assert kwargs["uf"] == "MT"
        assert kwargs["agregacao"] == "uf"
        assert kwargs["programa"] == "pronaf"
        assert kwargs["tipo_seguro"] == "proagro"

    @pytest.mark.asyncio
    async def test_invalid_uf_raises_before_source(self):
        dataset = CreditoRuralDataset()
        mock_fn = AsyncMock(return_value=(_mock_df(), mock_source_meta()))
        dataset.info.sources[0].fetch_fn = mock_fn

        with pytest.raises(ValueError, match="UF invalida"):
            await dataset.fetch("soja", uf="XX")

        mock_fn.assert_not_awaited()


class TestCreditoRuralNormalize:
    @pytest.mark.asyncio
    async def test_normalize_adds_produto(self):
        df = _mock_df().drop(columns=["produto"])
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "soja"

    @pytest.mark.asyncio
    async def test_normalize_adds_finalidade(self):
        df = _mock_df().drop(columns=["finalidade"])
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja", finalidade="investimento")

        assert result["finalidade"].iloc[0] == "investimento"

    @pytest.mark.asyncio
    async def test_normalize_keeps_existing_produto_finalidade(self):
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "soja"
        assert result["finalidade"].iloc[0] == "custeio"


class TestCreditoRuralPublicAPI:
    @pytest.mark.asyncio
    async def test_public_function_delegates(self):
        with patch.object(CreditoRuralDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _mock_df()
            await credito_rural("soja", safra="2023/24", uf="MT")

            mock_fetch.assert_called_once_with(
                "soja",
                safra="2023/24",
                finalidade="custeio",
                uf="MT",
                agregacao="municipio",
                programa=None,
                tipo_seguro=None,
                return_meta=False,
            )

    @pytest.mark.asyncio
    async def test_public_function_return_meta(self):
        with patch.object(CreditoRuralDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_mock_df(), mock_source_meta())
            result = await credito_rural("soja", return_meta=True)

            assert isinstance(result, tuple)
            assert len(result) == 2
            assert isinstance(result[0], pd.DataFrame)


class TestCreditoRuralFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_bcb_odata_forwards_params(self):
        df = _mock_df()
        meta = mock_source_meta()
        with patch(
            "agrobr.bcb.credito_rural", new_callable=AsyncMock, return_value=(df, meta)
        ) as mock_fn:
            from agrobr.datasets.credito_rural import _fetch_bcb_odata

            await _fetch_bcb_odata(
                "soja",
                safra="2024/2025",
                finalidade="investimento",
                uf="PR",
                agregacao="uf",
                programa="PRONAF",
                tipo_seguro="PROAGRO",
            )
        mock_fn.assert_called_once_with(
            "soja",
            safra="2024/2025",
            finalidade="investimento",
            uf="PR",
            agregacao="uf",
            programa="PRONAF",
            tipo_seguro="PROAGRO",
            return_meta=True,
        )
