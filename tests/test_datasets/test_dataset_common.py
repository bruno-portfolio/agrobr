"""Testes parametrizados comuns a todos os datasets."""

import warnings
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.datasets import registry
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    SourceFallbackWarning,
    SourceUnavailableError,
)
from tests.test_datasets.conftest import make_source, mock_source_meta

ALL_DATASETS = sorted(registry.list_datasets())

DYNAMIC_PRODUCTS_DATASETS = {
    name for name in ALL_DATASETS if registry.get_dataset(name).info.products == []
}


@pytest.mark.parametrize("dataset_name", ALL_DATASETS)
class TestDatasetInfo:
    def test_info_name(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        assert ds.info.name == dataset_name

    def test_info_has_products(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        if dataset_name in DYNAMIC_PRODUCTS_DATASETS:
            assert ds.info.products == []
        else:
            assert len(ds.info.products) > 0
            assert all(isinstance(p, str) for p in ds.info.products)

    def test_info_has_sources(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        assert len(ds.info.sources) > 0
        for source in ds.info.sources:
            assert isinstance(source.name, str)
            assert source.name
            assert callable(source.fetch_fn)

    def test_info_contract_version(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        assert isinstance(ds.info.contract_version, str)
        assert ds.info.contract_version in {"1.0", "1.1", "2.0"}

    def test_info_update_frequency(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        assert ds.info.update_frequency in {
            "daily",
            "monthly",
            "yearly",
            "quarterly",
            "continuous",
            "weekly",
            "decennial",
            "never",
        }

    def test_info_to_dict(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        info_dict = ds.info.to_dict()
        assert info_dict["name"] == dataset_name
        assert isinstance(info_dict["sources"], list)
        assert len(info_dict["sources"]) > 0
        assert isinstance(info_dict["products"], list)
        if dataset_name not in DYNAMIC_PRODUCTS_DATASETS:
            assert len(info_dict["products"]) > 0
        assert "contract_version" in info_dict


DATASETS_WITH_PRODUCTS = [d for d in ALL_DATASETS if d not in DYNAMIC_PRODUCTS_DATASETS]


@pytest.mark.parametrize("dataset_name", DATASETS_WITH_PRODUCTS)
class TestDatasetValidation:
    def test_validate_produto_valid(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        first_product = ds.info.products[0]
        ds._validate_produto(first_product)
        assert first_product in ds.info.products

    def test_validate_produto_invalid(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        with pytest.raises(ValueError, match="banana_inexistente"):
            ds._validate_produto("banana_inexistente")


@pytest.mark.parametrize("dataset_name", ALL_DATASETS)
class TestDatasetRegistry:
    def test_fetch_aceita_primeiro_argumento_posicional(self, dataset_name):
        import inspect

        from agrobr.datasets.registry import get_dataset

        params = list(inspect.signature(get_dataset(dataset_name).fetch).parameters.values())
        assert params, f"{dataset_name}.fetch() sem parametros"
        assert params[0].kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.POSITIONAL_ONLY,
        ), f"{dataset_name}.fetch() nao aceita o primeiro argumento posicional"

    def test_registered_in_registry(self, dataset_name):
        assert dataset_name in registry.list_datasets()

    def test_accessible_via_get_dataset(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        assert ds.info.name == dataset_name

    def test_list_products(self, dataset_name):
        products = registry.list_products(dataset_name)
        assert isinstance(products, list)
        if dataset_name not in DYNAMIC_PRODUCTS_DATASETS:
            assert len(products) > 0


_VALID_DF = pd.DataFrame(
    [
        {
            "data": pd.Timestamp("2025-01-15"),
            "valor": 145.0,
            "unidade": "R$/sc60kg",
            "praca": "Paranaguá/PR",
        }
    ]
)
_DUMMY_DF = pd.DataFrame()


class TestTrySourcesErrorPaths:
    @pytest.mark.asyncio
    async def test_contract_violation_triggers_fallback(self):
        from agrobr.datasets.preco_diario import PrecoDiarioDataset

        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(
            _DUMMY_DF,
            raises=ContractViolationError("test_dataset", "test_field", "expected X", "got Y"),
        )
        dataset.info.sources[1].fetch_fn = make_source(_VALID_DF)

        with pytest.warns(SourceFallbackWarning, match="usando fallback 'cache'"):
            df, meta = await dataset.fetch("soja", return_meta=True)
        assert meta.attempted_sources == ["cepea", "cache"]
        assert meta.selected_source == "cache"

    @pytest.mark.asyncio
    async def test_unexpected_exception_triggers_fallback(self):
        from agrobr.datasets.preco_diario import PrecoDiarioDataset

        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(
            side_effect=RuntimeError("unexpected boom"),
        )
        dataset.info.sources[1].fetch_fn = make_source(_VALID_DF)

        with pytest.warns(SourceFallbackWarning, match="usando fallback 'cache'"):
            df, meta = await dataset.fetch("soja", return_meta=True)
        assert meta.attempted_sources == ["cepea", "cache"]
        assert meta.selected_source == "cache"

    @pytest.mark.asyncio
    async def test_source_unavailable_classified_and_falls_back(self):
        from agrobr.datasets.preco_diario import PrecoDiarioDataset

        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(
            _DUMMY_DF,
            raises=SourceUnavailableError(source="cepea", last_error="HTTP 500 after 3 retries"),
        )
        dataset.info.sources[1].fetch_fn = make_source(_VALID_DF)

        with pytest.warns(SourceFallbackWarning, match="unavailable"):
            df, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.attempted_sources == ["cepea", "cache"]
        assert meta.selected_source == "cache"

    @pytest.mark.asyncio
    async def test_all_fail_mixed_errors(self):
        from agrobr.datasets.preco_diario import PrecoDiarioDataset

        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(
            _DUMMY_DF,
            raises=ContractViolationError("test", "field", "exp", "got"),
        )
        dataset.info.sources[1].fetch_fn = AsyncMock(
            side_effect=RuntimeError("boom"),
        )

        with pytest.raises(SourceUnavailableError) as exc_info:
            await dataset.fetch("soja")

        errors = exc_info.value.errors
        assert len(errors) == 2
        assert errors[0][1] == "contract"
        assert errors[1][1] == "unexpected"

    @pytest.mark.asyncio
    async def test_invalid_parameter_propagates_without_fallback(self):
        from agrobr.datasets.preco_diario import PrecoDiarioDataset

        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(
            _DUMMY_DF,
            raises=InvalidParameterError("produto inválido"),
        )
        fallback = make_source(_VALID_DF)
        dataset.info.sources[1].fetch_fn = fallback

        with pytest.raises(InvalidParameterError, match="produto inválido"):
            await dataset.fetch("soja")

        fallback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_primary_source_success_does_not_warn_about_fallback(self):
        from agrobr.datasets.preco_diario import PrecoDiarioDataset

        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(_VALID_DF)

        with warnings.catch_warnings():
            warnings.simplefilter("error", SourceFallbackWarning)
            await dataset.fetch("soja")


class TestDatasetMetaProvenance:
    @pytest.mark.asyncio
    async def test_internal_source_cascade_is_preserved(self):
        from agrobr.datasets.preco_diario import PrecoDiarioDataset

        source_meta = mock_source_meta()
        source_meta.attempted_sources = ["cepea", "noticias_agricolas"]
        source_meta.selected_source = "noticias_agricolas"
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(_VALID_DF, source_meta)

        _, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.attempted_sources == ["cepea", "noticias_agricolas"]
        assert meta.selected_source == "noticias_agricolas"
        assert meta.source == "datasets.preco_diario/noticias_agricolas"

    @pytest.mark.asyncio
    async def test_source_cache_provenance_is_preserved(self):
        from agrobr.datasets.preco_diario import PrecoDiarioDataset

        source_meta = mock_source_meta()
        source_meta.from_cache = True
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(_VALID_DF, source_meta)

        _, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.from_cache is True

    @pytest.mark.asyncio
    async def test_single_internal_source_keeps_dataset_source_name(self):
        from agrobr.datasets.preco_diario import PrecoDiarioDataset

        source_meta = mock_source_meta()
        source_meta.attempted_sources = ["cepea_api"]
        source_meta.selected_source = "cepea_api"
        dataset = PrecoDiarioDataset()
        dataset.info.sources[0].fetch_fn = make_source(_VALID_DF, source_meta)

        _, meta = await dataset.fetch("soja", return_meta=True)

        assert meta.attempted_sources == ["cepea"]
        assert meta.selected_source == "cepea"
        assert meta.source == "datasets.preco_diario/cepea"
