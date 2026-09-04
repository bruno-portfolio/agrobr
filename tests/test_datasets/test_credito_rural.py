"""Testes específicos para o dataset credito_rural (fetch com mock)."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.bcb.api import _aggregate_credito_rural
from agrobr.bcb.parser import parse_credito_rural
from agrobr.contracts import validate_dataset
from agrobr.datasets.credito_rural import (
    CREDITO_RURAL_INFO,
    CreditoRuralDataset,
    credito_rural,
)
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import SourceUnavailableError

from .conftest import make_source, mock_source_meta


def _golden_df() -> pd.DataFrame:
    path = Path(__file__).parents[1] / "golden_data" / "bcb" / "custeio_sample"
    records = json.loads(path.joinpath("response.json").read_text(encoding="utf-8"))
    parsed = parse_credito_rural(records)
    return _aggregate_credito_rural(parsed, "uf", "odata")


class TestCreditoRuralInfo:
    def test_single_source_bcb(self):
        assert len(CREDITO_RURAL_INFO.sources) == 1
        assert CREDITO_RURAL_INFO.sources[0].name == "bcb"

    def test_contract_version(self):
        assert CREDITO_RURAL_INFO.contract_version == "2.0"

    def test_license_livre(self):
        assert CREDITO_RURAL_INFO.license == "livre"


class TestCreditoRuralFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_golden_df())
        df = await dataset.fetch("soja", safra="2023/24")

        assert not df.empty
        assert "valor" in df.columns
        validate_dataset(df, "credito_rural")

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_golden_df())
        df, meta = await dataset.fetch("soja", safra="2023/24", return_meta=True)

        assert meta.dataset == "credito_rural"
        assert meta.contract_version == "2.0"
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
        mock_fn = make_source(_golden_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2025-03-15"):
            await dataset.fetch("soja")

        _, kwargs = mock_fn.call_args
        assert kwargs["safra"] == "2024/2025"

    @pytest.mark.asyncio
    async def test_snapshot_does_not_override_explicit_safra(self):
        dataset = CreditoRuralDataset()
        mock_fn = make_source(_golden_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2025-03-15"):
            await dataset.fetch("soja", safra="2022/23")

        _, kwargs = mock_fn.call_args
        assert kwargs["safra"] == "2022/23"

    @pytest.mark.asyncio
    async def test_forwards_all_kwargs(self):
        dataset = CreditoRuralDataset()
        mock_fn = make_source(_golden_df())
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
        mock_fn = AsyncMock(return_value=(_golden_df(), mock_source_meta()))
        dataset.info.sources[0].fetch_fn = mock_fn

        with pytest.raises(ValueError, match="UF invalida"):
            await dataset.fetch("soja", uf="XX")

        mock_fn.assert_not_awaited()


class TestCreditoRuralNormalize:
    @pytest.mark.asyncio
    async def test_normalize_adds_produto(self):
        df = _golden_df().drop(columns=["produto"])
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "soja"

    @pytest.mark.asyncio
    async def test_normalize_adds_finalidade(self):
        df = _golden_df().drop(columns=["finalidade"])
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja", finalidade="investimento")

        assert result["finalidade"].iloc[0] == "investimento"

    @pytest.mark.asyncio
    async def test_normalize_keeps_existing_produto_finalidade(self):
        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_golden_df())

        result = await dataset.fetch("soja")

        assert result["produto"].iloc[0] == "soja"
        assert result["finalidade"].iloc[0] == "custeio"


class TestCreditoRuralPublicAPI:
    @pytest.mark.asyncio
    async def test_public_function_delegates(self):
        with patch.object(CreditoRuralDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _golden_df()
            await credito_rural("soja", safra="2023/24", uf="MT")

            mock_fetch.assert_called_once_with(
                "soja",
                safra="2023/24",
                finalidade="custeio",
                uf="MT",
                agregacao="uf",
                programa=None,
                tipo_seguro=None,
                return_meta=False,
            )

    @pytest.mark.asyncio
    async def test_public_function_return_meta(self):
        with patch.object(CreditoRuralDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_golden_df(), mock_source_meta())
            result = await credito_rural("soja", return_meta=True)

            assert isinstance(result, tuple)
            assert len(result) == 2
            assert isinstance(result[0], pd.DataFrame)


class TestCreditoRuralFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_bcb_odata_forwards_params(self):
        df = _golden_df()
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
