from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.contracts import validate_dataset
from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.seguro_rural import (
    SEGURO_RURAL_INFO,
    SeguroRuralDataset,
    _fetch_psr,
    seguro_rural,
)
from agrobr.exceptions import SourceUnavailableError

from .conftest import make_source, mock_source_meta


def _mock_apolices_df():
    return pd.DataFrame(
        {
            "nr_apolice": ["AP001"],
            "ano_apolice": [2023],
            "uf": ["MT"],
            "municipio": ["Sorriso"],
            "cd_ibge": ["5107925"],
            "cultura": ["SOJA"],
            "classificacao": ["Grão"],
            "area_total": [100.0],
            "valor_premio": [5000.0],
            "valor_subvencao": [2000.0],
            "valor_limite_garantia": [50000.0],
            "valor_indenizacao": [0.0],
            "evento": [None],
            "produtividade_estimada": [3500.0],
            "produtividade_segurada": [2800.0],
            "nivel_cobertura": [0.8],
            "taxa": [0.05],
            "seguradora": ["MAPFRE"],
        }
    )


def _mock_sinistros_df():
    return pd.DataFrame(
        {
            "nr_apolice": ["AP002"],
            "ano_apolice": [2023],
            "uf": ["RS"],
            "municipio": ["Cruz Alta"],
            "cd_ibge": ["4306106"],
            "cultura": ["SOJA"],
            "classificacao": ["Grão"],
            "evento": ["SECA"],
            "area_total": [50.0],
            "valor_indenizacao": [25000.0],
            "valor_premio": [3000.0],
            "valor_subvencao": [1200.0],
            "valor_limite_garantia": [30000.0],
            "produtividade_estimada": [3200.0],
            "produtividade_segurada": [2560.0],
            "nivel_cobertura": [0.8],
            "seguradora": ["ALIANÇA"],
        }
    )


class TestSeguroRuralFetch:
    @pytest.mark.asyncio
    async def test_fetch_apolices_default(self):
        dataset = SeguroRuralDataset()
        mock_fn = make_source(_mock_apolices_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        df = await dataset.fetch()

        assert len(df) == 1
        assert "nr_apolice" in df.columns
        _, kwargs = mock_fn.call_args
        assert kwargs["tipo"] == "apolices"

    @pytest.mark.asyncio
    async def test_fetch_sinistros(self):
        dataset = SeguroRuralDataset()
        mock_fn = make_source(_mock_sinistros_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        df = await dataset.fetch(tipo="sinistros")

        assert len(df) == 1
        _, kwargs = mock_fn.call_args
        assert kwargs["tipo"] == "sinistros"

    @pytest.mark.asyncio
    async def test_invalid_tipo(self):
        dataset = SeguroRuralDataset()

        with pytest.raises(ValueError, match="tipo deve ser"):
            await dataset.fetch(tipo="outro")

    @pytest.mark.asyncio
    async def test_produto_maps_to_cultura(self):
        dataset = SeguroRuralDataset()
        mock_fn = make_source(_mock_apolices_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch("soja")

        args, _ = mock_fn.call_args
        assert args[0] == "soja"

    @pytest.mark.asyncio
    async def test_null_produto_sends_empty_string(self):
        dataset = SeguroRuralDataset()
        mock_fn = make_source(_mock_apolices_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch()

        args, _ = mock_fn.call_args
        assert args[0] == ""

    @pytest.mark.asyncio
    async def test_meta_apolices(self):
        dataset = SeguroRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_apolices_df())

        df, meta = await dataset.fetch(return_meta=True)

        assert meta.dataset == "seguro_rural"
        assert meta.contract_version == "1.0"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_meta_sinistros(self):
        dataset = SeguroRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_sinistros_df())

        df, meta = await dataset.fetch(tipo="sinistros", return_meta=True)

        assert meta.dataset == "seguro_rural"
        assert meta.selected_source == "mapa_psr"

    @pytest.mark.asyncio
    async def test_contract_validation_per_tipo(self):
        dataset = SeguroRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_apolices_df())

        with (
            patch("agrobr.contracts.has_contract", return_value=True) as mock_has,
            patch("agrobr.contracts.validate_dataset") as mock_validate,
        ):
            await dataset.fetch(tipo="apolices")
            mock_has.assert_called_with("mapa_psr_apolices")
            mock_validate.assert_called_once()

        dataset.info.sources[0].fetch_fn = make_source(_mock_sinistros_df())

        with (
            patch("agrobr.contracts.has_contract", return_value=True) as mock_has,
            patch("agrobr.contracts.validate_dataset") as mock_validate,
        ):
            await dataset.fetch(tipo="sinistros")
            mock_has.assert_called_with("mapa_psr_sinistros")
            mock_validate.assert_called_once()

    @pytest.mark.asyncio
    async def test_evento_passthrough(self):
        dataset = SeguroRuralDataset()
        mock_fn = make_source(_mock_apolices_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch(tipo="apolices", evento="SECA")

        _, kwargs = mock_fn.call_args
        assert kwargs["evento"] == "SECA"
        assert kwargs["tipo"] == "apolices"

    @pytest.mark.asyncio
    async def test_forwards_all_kwargs(self):
        dataset = SeguroRuralDataset()
        mock_fn = make_source(_mock_apolices_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch(
            "soja",
            tipo="apolices",
            uf="MT",
            ano=2023,
            ano_inicio=2020,
            ano_fim=2023,
            municipio="Sorriso",
            evento="SECA",
        )

        _, kwargs = mock_fn.call_args
        assert kwargs["uf"] == "MT"
        assert kwargs["ano"] == 2023
        assert kwargs["ano_inicio"] == 2020
        assert kwargs["ano_fim"] == 2023
        assert kwargs["municipio"] == "Sorriso"
        assert kwargs["evento"] == "SECA"

    @pytest.mark.asyncio
    async def test_source_failure(self):
        dataset = SeguroRuralDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))

        with pytest.raises(SourceUnavailableError):
            await dataset.fetch()

    @pytest.mark.asyncio
    async def test_snapshot_in_meta(self):
        dataset = SeguroRuralDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_apolices_df())

        async with deterministic("2023-12-15"):
            df, meta = await dataset.fetch(return_meta=True)

        assert meta.snapshot == "2023-12-15"


class TestSeguroRuralInfo:
    def test_products_empty(self):
        assert SEGURO_RURAL_INFO.products == []

    def test_license_livre(self):
        assert SEGURO_RURAL_INFO.license == "livre"

    def test_validate_produto_noop(self):
        dataset = SeguroRuralDataset()
        dataset._validate_produto("anything")


class TestSeguroRuralPublicAPI:
    @pytest.mark.asyncio
    async def test_public_function_delegates(self):
        with patch.object(SeguroRuralDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _mock_apolices_df()
            await seguro_rural("soja", tipo="apolices", uf="MT", ano=2023)

            mock_fetch.assert_called_once_with(
                "soja",
                tipo="apolices",
                uf="MT",
                ano=2023,
                ano_inicio=None,
                ano_fim=None,
                municipio=None,
                evento=None,
                return_meta=False,
            )

    @pytest.mark.asyncio
    async def test_public_function_return_meta(self):
        with patch.object(SeguroRuralDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_mock_apolices_df(), mock_source_meta())
            result = await seguro_rural(return_meta=True)

            assert isinstance(result, tuple)
            assert len(result) == 2
            assert isinstance(result[0], pd.DataFrame)

    @pytest.mark.asyncio
    async def test_public_function_sinistros(self):
        with patch.object(SeguroRuralDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _mock_sinistros_df()
            await seguro_rural(tipo="sinistros", evento="SECA")

            _, kwargs = mock_fetch.call_args
            assert kwargs["tipo"] == "sinistros"
            assert kwargs["evento"] == "SECA"


class TestSeguroRuralFetchFunctions:
    @pytest.mark.asyncio
    async def test_real_apolices_golden_validates_contract(self):
        path = (
            Path(__file__).parents[1]
            / "golden_data"
            / "mapa_psr"
            / "apolices_sample"
            / "response.csv"
        )

        with (
            patch(
                "agrobr.alt.mapa_psr.client.fetch_periodos",
                new_callable=AsyncMock,
                return_value=[path.read_bytes()],
            ),
            patch.object(SeguroRuralDataset.info.sources[0], "fetch_fn", _fetch_psr),
        ):
            df = await SeguroRuralDataset().fetch(tipo="apolices", ano=2007)

        assert "valor_indenizacao" in df.columns
        assert df["valor_indenizacao"].dtype == "Float64"
        assert df["valor_indenizacao"].isna().all()
        validate_dataset(df, "mapa_psr_apolices")

    @pytest.mark.asyncio
    async def test_fetch_psr_apolices_default(self):
        df = pd.DataFrame({"ano": [2024], "cultura": ["soja"], "valor_segurado": [100000.0]})
        meta = mock_source_meta()
        with patch(
            "agrobr.alt.mapa_psr.apolices", new_callable=AsyncMock, return_value=(df, meta)
        ) as mock_fn:
            from agrobr.datasets.seguro_rural import _fetch_psr

            result_df, _ = await _fetch_psr("soja")
        mock_fn.assert_called_once_with(
            cultura="soja",
            uf=None,
            ano=None,
            ano_inicio=None,
            ano_fim=None,
            municipio=None,
            return_meta=True,
        )

    @pytest.mark.asyncio
    async def test_fetch_psr_sinistros_branch(self):
        df = pd.DataFrame({"ano": [2024], "cultura": ["soja"], "valor_indenizacao": [50000.0]})
        meta = mock_source_meta()
        with patch(
            "agrobr.alt.mapa_psr.sinistros", new_callable=AsyncMock, return_value=(df, meta)
        ) as mock_fn:
            from agrobr.datasets.seguro_rural import _fetch_psr

            await _fetch_psr("soja", tipo="sinistros", evento="seca", uf="PR")
        mock_fn.assert_called_once_with(
            cultura="soja",
            evento="seca",
            uf="PR",
            ano=None,
            ano_inicio=None,
            ano_fim=None,
            municipio=None,
            return_meta=True,
        )

    @pytest.mark.asyncio
    async def test_fetch_psr_forwards_all_params(self):
        df = pd.DataFrame({"ano": [2024], "cultura": ["soja"]})
        meta = mock_source_meta()
        with patch(
            "agrobr.alt.mapa_psr.apolices", new_callable=AsyncMock, return_value=(df, meta)
        ) as mock_fn:
            from agrobr.datasets.seguro_rural import _fetch_psr

            await _fetch_psr(
                "milho", uf="SP", ano=2024, ano_inicio=2020, ano_fim=2024, municipio="Campinas"
            )
        mock_fn.assert_called_once_with(
            cultura="milho",
            uf="SP",
            ano=2024,
            ano_inicio=2020,
            ano_fim=2024,
            municipio="Campinas",
            return_meta=True,
        )
