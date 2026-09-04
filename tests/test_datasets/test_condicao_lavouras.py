from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.datasets.condicao_lavouras import (
    CONDICAO_LAVOURAS_INFO,
    CondicaoLavourasDataset,
    _fetch_deral,
    condicao_lavouras,
)
from agrobr.deral.parser import parse_pc_xls
from agrobr.exceptions import SourceUnavailableError
from agrobr.models import MetaInfo

from .conftest import make_source

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "deral" / "pc_sample"


def _make_df(**overrides):
    row = {
        "produto": "soja",
        "data": "01/03/2024",
        "condicao": "boa",
        "pct": 70.0,
        "plantio_pct": float("nan"),
        "colheita_pct": float("nan"),
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _make_full_df():
    rows = [
        {
            "produto": "soja",
            "data": "01/03/2024",
            "condicao": "boa",
            "pct": 70.0,
            "plantio_pct": None,
            "colheita_pct": None,
        },
        {
            "produto": "soja",
            "data": "01/03/2024",
            "condicao": "media",
            "pct": 20.0,
            "plantio_pct": None,
            "colheita_pct": None,
        },
        {
            "produto": "soja",
            "data": "01/03/2024",
            "condicao": "ruim",
            "pct": 10.0,
            "plantio_pct": None,
            "colheita_pct": None,
        },
        {
            "produto": "soja",
            "data": "01/03/2024",
            "condicao": "",
            "pct": None,
            "plantio_pct": 95.0,
            "colheita_pct": None,
        },
        {
            "produto": "soja",
            "data": "01/03/2024",
            "condicao": "",
            "pct": None,
            "plantio_pct": None,
            "colheita_pct": 30.0,
        },
    ]
    return pd.DataFrame(rows)


class TestCondicaoLavourasFetch:
    @pytest.mark.asyncio
    async def test_fetch_real_golden_satisfies_contract(self, monkeypatch):
        source_df = parse_pc_xls(GOLDEN_DIR.joinpath("response.xlsx").read_bytes())
        dataset = CondicaoLavourasDataset()
        monkeypatch.setattr(
            dataset.info.sources[0],
            "fetch_fn",
            make_source(source_df),
        )

        df = await dataset.fetch()

        assert not df.duplicated(["produto", "data", "condicao"]).any()
        assert {"feijao_1", "feijao_2", "milho_1", "milho_2"}.issubset(set(df["produto"]))

    @pytest.mark.asyncio
    async def test_fetch_all(self):
        dataset = CondicaoLavourasDataset()
        dataset.info.sources[0].fetch_fn = make_source(_make_df())
        df = await dataset.fetch()

        assert len(df) == 1
        assert "produto" in df.columns
        assert "condicao" in df.columns

    @pytest.mark.asyncio
    async def test_fetch_by_produto(self):
        mock_fn = make_source(_make_df())
        dataset = CondicaoLavourasDataset()
        dataset.info.sources[0].fetch_fn = mock_fn
        await dataset.fetch(produto="soja")

        call_args = mock_fn.call_args
        assert call_args[0][0] == "soja"

    @pytest.mark.asyncio
    async def test_fetch_invalid_produto(self):
        dataset = CondicaoLavourasDataset()
        dataset.info.sources[0].fetch_fn = make_source(_make_df())
        with pytest.raises(ValueError, match="não suportado"):
            await dataset.fetch(produto="banana")

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = CondicaoLavourasDataset()
        dataset.info.sources[0].fetch_fn = make_source(_make_df())
        df, meta = await dataset.fetch(return_meta=True)

        assert meta.dataset == "condicao_lavouras"
        assert meta.contract_version == "1.0"
        assert "deral" in meta.attempted_sources
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_source_failure(self):
        dataset = CondicaoLavourasDataset()
        dataset.info.sources[0].fetch_fn = make_source(
            _make_df(), raises=httpx.ConnectError("connection failed")
        )
        with pytest.raises(SourceUnavailableError):
            await dataset.fetch()


class TestCondicaoLavourasNormalize:
    def test_normalize_plantio(self):
        dataset = CondicaoLavourasDataset()
        df = pd.DataFrame(
            [
                {
                    "produto": "soja",
                    "data": "01/03/2024",
                    "condicao": "",
                    "pct": None,
                    "plantio_pct": 95.0,
                    "colheita_pct": None,
                }
            ]
        )
        result = dataset._normalize(df)
        assert result.iloc[0]["condicao"] == "plantio"

    def test_normalize_colheita(self):
        dataset = CondicaoLavourasDataset()
        df = pd.DataFrame(
            [
                {
                    "produto": "soja",
                    "data": "01/03/2024",
                    "condicao": "",
                    "pct": None,
                    "plantio_pct": None,
                    "colheita_pct": 30.0,
                }
            ]
        )
        result = dataset._normalize(df)
        assert result.iloc[0]["condicao"] == "colheita"

    def test_normalize_conditions_untouched(self):
        dataset = CondicaoLavourasDataset()
        df = _make_full_df()
        result = dataset._normalize(df)

        assert result.iloc[0]["condicao"] == "boa"
        assert result.iloc[1]["condicao"] == "media"
        assert result.iloc[2]["condicao"] == "ruim"
        assert result.iloc[3]["condicao"] == "plantio"
        assert result.iloc[4]["condicao"] == "colheita"

    def test_normalize_empty_df(self):
        dataset = CondicaoLavourasDataset()
        df = pd.DataFrame(
            columns=["produto", "data", "condicao", "pct", "plantio_pct", "colheita_pct"]
        )
        result = dataset._normalize(df)
        assert result.empty


class TestCondicaoLavourasInfo:
    def test_products_count(self):
        assert len(CONDICAO_LAVOURAS_INFO.products) == 14

    def test_license(self):
        assert CONDICAO_LAVOURAS_INFO.license == "livre"


class TestFetchDeral:
    @pytest.mark.asyncio
    async def test_fetch_deral_calls_deral_api(self):
        mock_meta = MagicMock(spec=MetaInfo)
        expected_df = _make_full_df()
        mock_deral = MagicMock()
        mock_deral.condicao_lavouras = AsyncMock(
            return_value=(expected_df, mock_meta),
        )
        import agrobr

        with patch.object(agrobr, "deral", mock_deral, create=True):
            df, meta = await _fetch_deral("soja")

        assert len(df) == len(expected_df)
        mock_deral.condicao_lavouras.assert_called_once_with(
            produto="soja",
            return_meta=True,
        )

    @pytest.mark.asyncio
    async def test_fetch_deral_empty_produto_passes_none(self):
        mock_meta = MagicMock(spec=MetaInfo)
        mock_deral = MagicMock()
        mock_deral.condicao_lavouras = AsyncMock(
            return_value=(_make_df(), mock_meta),
        )
        import agrobr

        with patch.object(agrobr, "deral", mock_deral, create=True):
            await _fetch_deral("")

        mock_deral.condicao_lavouras.assert_called_once_with(
            produto=None,
            return_meta=True,
        )


class TestCondicaoLavourasPublicApi:
    @pytest.mark.asyncio
    async def test_condicao_lavouras_delegates_to_dataset(self):
        expected_df = _make_df()
        with patch.object(
            CondicaoLavourasDataset,
            "fetch",
            new_callable=AsyncMock,
            return_value=expected_df,
        ):
            result = await condicao_lavouras(produto="soja")

        assert isinstance(result, pd.DataFrame)
