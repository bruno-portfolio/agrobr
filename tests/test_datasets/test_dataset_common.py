"""Testes parametrizados comuns a todos os datasets."""

from collections.abc import Callable, Iterable
from contextlib import nullcontext
from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.contracts import validate_dataset
from agrobr.datasets import registry
from agrobr.datasets.abate_trimestral import AbateTrimestralDataset
from agrobr.datasets.balanco import BalancoDataset
from agrobr.datasets.base import DatasetSource
from agrobr.datasets.cadastro_rural import CadastroRuralDataset
from agrobr.datasets.censo_agropecuario import CensoAgropecuarioDataset
from agrobr.datasets.censo_agropecuario_legado import CensoAgropecuarioLegadoDataset
from agrobr.datasets.clima import ClimaDataset
from agrobr.datasets.condicao_lavouras import CondicaoLavourasDataset
from agrobr.datasets.credito_rural import CreditoRuralDataset
from agrobr.datasets.estimativa_safra import (
    EstimativaSafraDataset,
)
from agrobr.datasets.exportacao import ExportacaoDataset
from agrobr.datasets.exportacao_anec import EmbarquesANECDataset
from agrobr.datasets.extrativismo_vegetal import (
    ExtrativsmoVegetalDataset,
)
from agrobr.datasets.fertilizante import FertilizanteDataset
from agrobr.datasets.futuros_agricolas import (
    FuturosAgricolasDataset,
)
from agrobr.datasets.importacao import ImportacaoDataset
from agrobr.datasets.leite_industrial import LeiteIndustrialDataset
from agrobr.datasets.movimentacao_portuaria import (
    MovimentacaoPortuariaDataset,
)
from agrobr.datasets.oferta_demanda_global import (
    OfertaDemandaGlobalDataset,
)
from agrobr.datasets.pecuaria_municipal import PecuariaMunicipalDataset
from agrobr.datasets.pib_agro import PibAgroDataset
from agrobr.datasets.posicionamento_fundos import (
    PosicionamentoFundosDataset,
)
from agrobr.datasets.preco_atacado import PrecoAtacadoDataset
from agrobr.datasets.preco_diario import PrecoDiarioDataset
from agrobr.datasets.producao_anual import ProducaoAnualDataset
from agrobr.datasets.progresso_safra import (
    ProgressoSafraDataset,
)
from agrobr.datasets.queimadas import QueimadasDataset
from agrobr.datasets.serie_historica_safra import (
    SerieHistoricaSafraDataset,
)
from agrobr.datasets.silvicultura import SilviculturaDataset
from agrobr.datasets.uso_do_solo import UsodoSoloDataset
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    SourceFallbackWarning,
    SourceUnavailableError,
)
from tests.helpers import collect_failures, fixture_instance
from tests.test_datasets.conftest import (
    amostra_abate_trimestral,
    amostra_cadastro_rural,
    amostra_censo_agropecuario_legado,
    amostra_leite_industrial,
    amostra_preco_atacado,
    make_source,
    meta_cadastro_rural,
    mock_source_meta,
)
from tests.test_datasets.test_balanco import _mock_df as _balanco_mock_df
from tests.test_datasets.test_censo_agropecuario import _mock_df as _censo_agropecuario_mock_df
from tests.test_datasets.test_clima import (
    _add_inmet_nullable_cols as _clima_add_inmet_nullable_cols,
)
from tests.test_datasets.test_clima import _mock_inmet_df as _clima_mock_inmet_df
from tests.test_datasets.test_clima import make_source as _climamake_source
from tests.test_datasets.test_condicao_lavouras import _make_df as _condicao_lavouras_make_df
from tests.test_datasets.test_credito_rural import _golden_df as _credito_rural_golden_df
from tests.test_datasets.test_estimativa_safra import _mock_df as _estimativa_safra_mock_df
from tests.test_datasets.test_exportacao import _mock_export_df as _exportacao_mock_export_df
from tests.test_datasets.test_exportacao_anec import _mock_df as _exportacao_anec_mock_df
from tests.test_datasets.test_extrativismo_vegetal import _mock_df as _extrativismo_vegetal_mock_df
from tests.test_datasets.test_fertilizante import _mock_df as _fertilizante_mock_df
from tests.test_datasets.test_futuros_agricolas import (
    _mock_ajustes_df as _futuros_agricolas_mock_ajustes_df,
)
from tests.test_datasets.test_importacao import _make_df as _importacao_make_df
from tests.test_datasets.test_movimentacao_portuaria import (
    _make_df as _movimentacao_portuaria_make_df,
)
from tests.test_datasets.test_oferta_demanda_global import (
    _make_df as _oferta_demanda_global_make_df,
)
from tests.test_datasets.test_pecuaria_municipal import _mock_df as _pecuaria_municipal_mock_df
from tests.test_datasets.test_pib_agro import _make_df as _pib_agro_make_df
from tests.test_datasets.test_posicionamento_fundos import (
    _make_df as _posicionamento_fundos_make_df,
)
from tests.test_datasets.test_preco_diario import _mock_df as _preco_diario_mock_df
from tests.test_datasets.test_producao_anual import (
    _isolate_producao_sources as _producao_anual_isolate_producao_sources,
)
from tests.test_datasets.test_producao_anual import _mock_df as _producao_anual_mock_df
from tests.test_datasets.test_progresso_safra import _make_df as _progresso_safra_make_df
from tests.test_datasets.test_queimadas import _make_df as _queimadas_make_df
from tests.test_datasets.test_serie_historica_safra import (
    _mock_df as _serie_historica_safra_mock_df,
)
from tests.test_datasets.test_silvicultura import _mock_df as _silvicultura_mock_df
from tests.test_datasets.test_uso_do_solo import (
    _make_cobertura_df as _uso_do_solo_make_cobertura_df,
)

ALL_DATASETS = sorted(registry.list_datasets())

DYNAMIC_PRODUCTS_DATASETS = {
    name for name in ALL_DATASETS if registry.get_dataset(name).info.products == []
}

VALIDAM_O_PRODUTO_NA_FONTE = {"embarques_anec", "preco_atacado", "zoneamento_agricola"}

DATASETS_WITH_PRODUCTS = [
    d for d in ALL_DATASETS if d not in DYNAMIC_PRODUCTS_DATASETS | VALIDAM_O_PRODUTO_NA_FONTE
]


ALL_DATASETS = sorted(registry.list_datasets())

DYNAMIC_PRODUCTS_DATASETS = {
    name for name in ALL_DATASETS if registry.get_dataset(name).info.products == []
}


@pytest.mark.parametrize("dataset_name", ALL_DATASETS)
class TestDatasetRegistry:
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


def _preco_com_reserva(primaria: Any, reserva: Any) -> PrecoDiarioDataset:
    dataset = PrecoDiarioDataset()
    dataset.info = replace(
        dataset.info,
        sources=[DatasetSource("cepea", 1, primaria), DatasetSource("reserva", 2, reserva)],
    )
    return dataset


class TestTrySourcesErrorPaths:
    @pytest.mark.asyncio
    async def test_contract_violation_triggers_fallback(self):
        dataset = _preco_com_reserva(
            make_source(
                _DUMMY_DF,
                raises=ContractViolationError("test_dataset", "test_field", "expected X", "got Y"),
            ),
            make_source(_VALID_DF),
        )

        with pytest.warns(SourceFallbackWarning, match="usando fallback 'reserva'"):
            df, meta = await dataset.fetch("soja", return_meta=True)
        assert meta.attempted_sources == ["cepea", "reserva"]
        assert meta.selected_source == "reserva"

    @pytest.mark.asyncio
    async def test_source_unavailable_classified_and_falls_back(self):
        dataset = _preco_com_reserva(
            make_source(
                _DUMMY_DF,
                raises=SourceUnavailableError(
                    source="cepea", last_error="HTTP 500 after 3 retries"
                ),
            ),
            make_source(_VALID_DF),
        )

        with pytest.warns(SourceFallbackWarning) as captured:
            df, meta = await dataset.fetch("soja", return_meta=True)

        assert "(cepea: unavailable: cepea unavailable: HTTP 500 after 3 retries)" in str(
            captured[0].message
        )
        assert meta.attempted_sources == ["cepea", "reserva"]
        assert meta.selected_source == "reserva"

    @pytest.mark.asyncio
    async def test_all_fail_mixed_errors(self):
        dataset = _preco_com_reserva(
            make_source(_DUMMY_DF, raises=ContractViolationError("test", "field", "exp", "got")),
            AsyncMock(side_effect=SourceUnavailableError("reserva", last_error="sem reserva")),
        )

        with pytest.raises(SourceUnavailableError) as exc_info:
            await dataset.fetch("soja")

        errors = exc_info.value.errors
        assert len(errors) == 2
        assert errors[0][1] == "contract"
        assert errors[1][1] == "unavailable"

    @pytest.mark.asyncio
    async def test_invalid_parameter_propagates_without_fallback(self):
        fallback = make_source(_VALID_DF)
        dataset = _preco_com_reserva(
            make_source(_DUMMY_DF, raises=InvalidParameterError("produto inválido")), fallback
        )

        with pytest.raises(InvalidParameterError, match="produto inválido"):
            await dataset.fetch("soja")

        fallback.assert_not_awaited()


class TestDatasetMetaProvenance:
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


def _run_template_checks(
    checks: Iterable[tuple[str, str, Callable[..., Any]]], *values: Any
) -> None:
    with collect_failures() as check:
        for kind, label, predicate in checks:
            with check(label):
                if kind == "assert":
                    assert predicate(*values)
                else:
                    predicate(*values)


class TestDatasetTemplate:
    @pytest.mark.parametrize(
        "prepare,factory,source_factory,args,kwargs,error,error_kwargs",
        [
            pytest.param(
                None,
                AbateTrimestralDataset,
                None,
                ("aveia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="abate_trimestral.TestAbateTrimestralFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                BalancoDataset,
                None,
                ("aveia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="balanco.TestBalancoFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                CensoAgropecuarioDataset,
                None,
                ("aveia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="censo_agropecuario.TestCensoAgropecuarioFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                CensoAgropecuarioLegadoDataset,
                None,
                ("aveia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="censo_agropecuario_legado.TestCensoAgropecuarioLegadoFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                CondicaoLavourasDataset,
                lambda: make_source(_condicao_lavouras_make_df()),
                (),
                {"produto": "banana"},
                ValueError,
                {"match": "produto inválido: 'banana'. Valores válidos"},
                id="condicao_lavouras.TestCondicaoLavourasFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                CreditoRuralDataset,
                None,
                ("abacaxi",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="credito_rural.TestCreditoRuralFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                EstimativaSafraDataset,
                None,
                ("aveia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="estimativa_safra.TestEstimativaSafraFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                ExportacaoDataset,
                None,
                ("banana",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="exportacao.TestExportacaoFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                ExtrativsmoVegetalDataset,
                None,
                ("soja",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="extrativismo_vegetal.TestExtrativsmoVegetalFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                FertilizanteDataset,
                None,
                ("ureia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="fertilizante.TestFertilizanteFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                FuturosAgricolasDataset,
                None,
                ("banana",),
                {"data": "2025-03-05"},
                ValueError,
                {"match": "não suportado"},
                id="futuros_agricolas.TestFuturosValidation.test_invalid_produto",
            ),
            pytest.param(
                None,
                ImportacaoDataset,
                None,
                ("banana",),
                {"ano": 2024},
                ValueError,
                {"match": "Produto .* não suportado"},
                id="importacao.TestImportacaoFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                LeiteIndustrialDataset,
                None,
                ("aveia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="leite_industrial.TestLeiteIndustrialFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                PecuariaMunicipalDataset,
                None,
                ("aveia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="pecuaria_municipal.TestPecuariaMunicipalFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                PibAgroDataset,
                None,
                ("mineracao",),
                {},
                ValueError,
                {"match": "Produto .* não suportado"},
                id="pib_agro.TestPibAgroFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                PrecoDiarioDataset,
                None,
                ("aveia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="preco_diario.TestPrecoDiarioFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                _producao_anual_isolate_producao_sources,
                ProducaoAnualDataset,
                None,
                ("aveia",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="producao_anual.TestProducaoAnualFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                ProgressoSafraDataset,
                None,
                ("cana",),
                {},
                ValueError,
                {"match": "Produto .* não suportado"},
                id="progresso_safra.TestProgressoSafraFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                SerieHistoricaSafraDataset,
                None,
                ("banana",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="serie_historica_safra.TestSerieHistoricaSafraFetch.test_fetch_invalid_produto",
            ),
            pytest.param(
                None,
                SilviculturaDataset,
                None,
                ("soja",),
                {},
                ValueError,
                {"match": "não suportado"},
                id="silvicultura.TestSilviculturaFetch.test_fetch_invalid_produto",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_invalid_product_public_boundary(
        self, monkeypatch, prepare, factory, source_factory, args, kwargs, error, error_kwargs
    ):
        with fixture_instance(prepare, monkeypatch=monkeypatch) if prepare else nullcontext():
            dataset = factory()
            if source_factory is not None:
                dataset.info.sources[0].fetch_fn = source_factory()
            with pytest.raises(error, **error_kwargs):
                await dataset.fetch(*args, **kwargs)

    @pytest.mark.parametrize(
        "prepare,factory,source_factory,args,kwargs,checks",
        [
            pytest.param(
                None,
                AbateTrimestralDataset,
                lambda: make_source(amostra_abate_trimestral()),
                ("bovino",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'abate_trimestral'",
                        lambda _df, meta: meta.dataset == "abate_trimestral",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['ibge_abate']",
                        lambda _df, meta: meta.attempted_sources == ["ibge_abate"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'ibge_abate'",
                        lambda _df, meta: meta.selected_source == "ibge_abate",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="abate_trimestral.TestAbateTrimestralFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                BalancoDataset,
                lambda: make_source(_balanco_mock_df()),
                ("soja",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'balanco'",
                        lambda _df, meta: meta.dataset == "balanco",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.1'",
                        lambda _df, meta: meta.contract_version == "1.1",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['conab']",
                        lambda _df, meta: meta.attempted_sources == ["conab"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'conab'",
                        lambda _df, meta: meta.selected_source == "conab",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="balanco.TestBalancoFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                CadastroRuralDataset,
                lambda: AsyncMock(return_value=(amostra_cadastro_rural(), meta_cadastro_rural())),
                ("DF",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'cadastro_rural'",
                        lambda _df, meta: meta.dataset == "cadastro_rural",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.1'",
                        lambda _df, meta: meta.contract_version == "2.1",
                    ),
                    (
                        "assert",
                        "'sicar' in meta.attempted_sources",
                        lambda _df, meta: "sicar" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="cadastro_rural.TestCadastroRuralDataset.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                CensoAgropecuarioDataset,
                lambda: make_source(_censo_agropecuario_mock_df()),
                ("efetivo_rebanho",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'censo_agropecuario'",
                        lambda _df, meta: meta.dataset == "censo_agropecuario",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.2'",
                        lambda _df, meta: meta.contract_version == "1.2",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['ibge_censo_agro']",
                        lambda _df, meta: meta.attempted_sources == ["ibge_censo_agro"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'ibge_censo_agro'",
                        lambda _df, meta: meta.selected_source == "ibge_censo_agro",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="censo_agropecuario.TestCensoAgropecuarioFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                CensoAgropecuarioLegadoDataset,
                lambda: make_source(amostra_censo_agropecuario_legado()),
                ("tecnologia",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'censo_agropecuario_legado'",
                        lambda _df, meta: meta.dataset == "censo_agropecuario_legado",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.1'",
                        lambda _df, meta: meta.contract_version == "2.1",
                    ),
                    (
                        "assert",
                        "meta.schema_version == '2.1'",
                        lambda _df, meta: meta.schema_version == "2.1",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['ibge_censo_agro_legado']",
                        lambda _df, meta: meta.attempted_sources == ["ibge_censo_agro_legado"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'ibge_censo_agro_legado'",
                        lambda _df, meta: meta.selected_source == "ibge_censo_agro_legado",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="censo_agropecuario_legado.TestCensoAgropecuarioLegadoFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                ClimaDataset,
                lambda: _climamake_source(_clima_add_inmet_nullable_cols(_clima_mock_inmet_df())),
                ("SP",),
                {"ano": 2024, "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'clima'",
                        lambda _df, meta: meta.dataset == "clima",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '3.1'",
                        lambda _df, meta: meta.contract_version == "3.1",
                    ),
                    (
                        "assert",
                        "'inmet' in meta.attempted_sources",
                        lambda _df, meta: "inmet" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="clima.TestClimaFetchUF.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                CondicaoLavourasDataset,
                lambda: make_source(_condicao_lavouras_make_df()),
                (),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'condicao_lavouras'",
                        lambda _df, meta: meta.dataset == "condicao_lavouras",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "'deral' in meta.attempted_sources",
                        lambda _df, meta: "deral" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="condicao_lavouras.TestCondicaoLavourasFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                CreditoRuralDataset,
                lambda: make_source(_credito_rural_golden_df()),
                ("soja",),
                {"safra": "2023/24", "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'credito_rural'",
                        lambda _df, meta: meta.dataset == "credito_rural",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "'bcb' in meta.attempted_sources",
                        lambda _df, meta: "bcb" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="credito_rural.TestCreditoRuralFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                EstimativaSafraDataset,
                lambda: make_source(_estimativa_safra_mock_df()),
                ("soja",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'estimativa_safra'",
                        lambda _df, meta: meta.dataset == "estimativa_safra",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '3.1'",
                        lambda _df, meta: meta.contract_version == "3.1",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['conab']",
                        lambda _df, meta: meta.attempted_sources == ["conab"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'conab'",
                        lambda _df, meta: meta.selected_source == "conab",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="estimativa_safra.TestEstimativaSafraFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                ExportacaoDataset,
                lambda: make_source(_exportacao_mock_export_df()),
                ("soja",),
                {"ano": 2024, "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'exportacao'",
                        lambda _df, meta: meta.dataset == "exportacao",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.1'",
                        lambda _df, meta: meta.contract_version == "1.1",
                    ),
                    (
                        "assert",
                        "'comexstat' in meta.attempted_sources",
                        lambda _df, meta: "comexstat" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="exportacao.TestExportacaoFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                EmbarquesANECDataset,
                lambda: make_source(_exportacao_anec_mock_df()),
                (),
                {"ano": 2026, "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'embarques_anec'",
                        lambda _df, meta: meta.dataset == "embarques_anec",
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'anec'",
                        lambda _df, meta: meta.selected_source == "anec",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.1'",
                        lambda _df, meta: meta.contract_version == "1.1",
                    ),
                ],
                id="exportacao_anec.TestEmbarquesANEC.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                ExtrativsmoVegetalDataset,
                lambda: make_source(_extrativismo_vegetal_mock_df()),
                ("acai",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'extrativismo_vegetal'",
                        lambda _df, meta: meta.dataset == "extrativismo_vegetal",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.1'",
                        lambda _df, meta: meta.contract_version == "1.1",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['ibge_extracao_vegetal']",
                        lambda _df, meta: meta.attempted_sources == ["ibge_extracao_vegetal"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'ibge_extracao_vegetal'",
                        lambda _df, meta: meta.selected_source == "ibge_extracao_vegetal",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="extrativismo_vegetal.TestExtrativsmoVegetalFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                FertilizanteDataset,
                lambda: make_source(_fertilizante_mock_df()),
                ("total",),
                {"ano": 2024, "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'fertilizante'",
                        lambda _df, meta: meta.dataset == "fertilizante",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "'anda' in meta.attempted_sources",
                        lambda _df, meta: "anda" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="fertilizante.TestFertilizanteFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                FuturosAgricolasDataset,
                lambda: make_source(_futuros_agricolas_mock_ajustes_df()),
                ("boi",),
                {"data": "2025-03-05", "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'futuros_agricolas'",
                        lambda _df, meta: meta.dataset == "futuros_agricolas",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.0'",
                        lambda _df, meta: meta.contract_version == "1.0",
                    ),
                    (
                        "assert",
                        "'b3' in meta.attempted_sources",
                        lambda _df, meta: "b3" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="futuros_agricolas.TestFuturosFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                ImportacaoDataset,
                lambda: make_source(_importacao_make_df()),
                ("soja",),
                {"ano": 2024, "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'importacao'",
                        lambda _df, meta: meta.dataset == "importacao",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.2'",
                        lambda _df, meta: meta.contract_version == "1.2",
                    ),
                    (
                        "assert",
                        "'comexstat' in meta.attempted_sources",
                        lambda _df, meta: "comexstat" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="importacao.TestImportacaoFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                LeiteIndustrialDataset,
                lambda: make_source(amostra_leite_industrial()),
                ("leite",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'leite_industrial'",
                        lambda _df, meta: meta.dataset == "leite_industrial",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.0'",
                        lambda _df, meta: meta.contract_version == "1.0",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['ibge_leite_trimestral']",
                        lambda _df, meta: meta.attempted_sources == ["ibge_leite_trimestral"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'ibge_leite_trimestral'",
                        lambda _df, meta: meta.selected_source == "ibge_leite_trimestral",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="leite_industrial.TestLeiteIndustrialFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                MovimentacaoPortuariaDataset,
                lambda: make_source(_movimentacao_portuaria_make_df()),
                (),
                {"ano": 2024, "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'movimentacao_portuaria'",
                        lambda _df, meta: meta.dataset == "movimentacao_portuaria",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "'antaq' in meta.attempted_sources",
                        lambda _df, meta: "antaq" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="movimentacao_portuaria.TestMovimentacaoPortuariaFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                OfertaDemandaGlobalDataset,
                lambda: make_source(_oferta_demanda_global_make_df()),
                ("soja",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'oferta_demanda_global'",
                        lambda _df, meta: meta.dataset == "oferta_demanda_global",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "'usda' in meta.attempted_sources",
                        lambda _df, meta: "usda" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="oferta_demanda_global.TestOfertaDemandaGlobalFetch.test_return_meta",
            ),
            pytest.param(
                None,
                PecuariaMunicipalDataset,
                lambda: make_source(_pecuaria_municipal_mock_df()),
                ("bovino",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'pecuaria_municipal'",
                        lambda _df, meta: meta.dataset == "pecuaria_municipal",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.1'",
                        lambda _df, meta: meta.contract_version == "1.1",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['ibge_ppm']",
                        lambda _df, meta: meta.attempted_sources == ["ibge_ppm"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'ibge_ppm'",
                        lambda _df, meta: meta.selected_source == "ibge_ppm",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="pecuaria_municipal.TestPecuariaMunicipalFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                PibAgroDataset,
                lambda: make_source(_pib_agro_make_df()),
                ("agropecuaria",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'pib_agro'",
                        lambda _df, meta: meta.dataset == "pib_agro",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.0'",
                        lambda _df, meta: meta.contract_version == "1.0",
                    ),
                    (
                        "assert",
                        "'ibge' in meta.attempted_sources",
                        lambda _df, meta: "ibge" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="pib_agro.TestPibAgroFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                PosicionamentoFundosDataset,
                lambda: make_source(_posicionamento_fundos_make_df()),
                ("soja",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'posicionamento_fundos'",
                        lambda _df, meta: meta.dataset == "posicionamento_fundos",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "'cftc' in meta.attempted_sources",
                        lambda _df, meta: "cftc" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="posicionamento_fundos.TestPosicionamentoFundosFetch.test_return_meta",
            ),
            pytest.param(
                None,
                PrecoAtacadoDataset,
                lambda: make_source(amostra_preco_atacado()),
                (),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'preco_atacado'",
                        lambda _df, meta: meta.dataset == "preco_atacado",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="preco_atacado.TestPrecoAtacadoFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                PrecoDiarioDataset,
                lambda: make_source(_preco_diario_mock_df()),
                ("soja",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'preco_diario'",
                        lambda _df, meta: meta.dataset == "preco_diario",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.1'",
                        lambda _df, meta: meta.contract_version == "1.1",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['cepea']",
                        lambda _df, meta: meta.attempted_sources == ["cepea"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'cepea'",
                        lambda _df, meta: meta.selected_source == "cepea",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="preco_diario.TestPrecoDiarioFetch.test_fetch_return_meta",
            ),
            pytest.param(
                _producao_anual_isolate_producao_sources,
                ProducaoAnualDataset,
                lambda: make_source(_producao_anual_mock_df()),
                ("soja",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'producao_anual'",
                        lambda _df, meta: meta.dataset == "producao_anual",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.2'",
                        lambda _df, meta: meta.contract_version == "2.2",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['ibge_pam']",
                        lambda _df, meta: meta.attempted_sources == ["ibge_pam"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'ibge_pam'",
                        lambda _df, meta: meta.selected_source == "ibge_pam",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="producao_anual.TestProducaoAnualFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                ProgressoSafraDataset,
                lambda: make_source(_progresso_safra_make_df()),
                ("soja",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'progresso_safra'",
                        lambda _df, meta: meta.dataset == "progresso_safra",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "'conab' in meta.attempted_sources",
                        lambda _df, meta: "conab" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="progresso_safra.TestProgressoSafraFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                QueimadasDataset,
                lambda: make_source(_queimadas_make_df()),
                (),
                {"ano": 2024, "mes": 8, "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'queimadas'",
                        lambda _df, meta: meta.dataset == "queimadas",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.1'",
                        lambda _df, meta: meta.contract_version == "1.1",
                    ),
                    (
                        "assert",
                        "'inpe' in meta.attempted_sources",
                        lambda _df, meta: "inpe" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="queimadas.TestQueimadasFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                SerieHistoricaSafraDataset,
                lambda: make_source(_serie_historica_safra_mock_df()),
                ("soja",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'serie_historica_safra'",
                        lambda _df, meta: meta.dataset == "serie_historica_safra",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.1'",
                        lambda _df, meta: meta.contract_version == "1.1",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['conab_serie_historica']",
                        lambda _df, meta: meta.attempted_sources == ["conab_serie_historica"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'conab_serie_historica'",
                        lambda _df, meta: meta.selected_source == "conab_serie_historica",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="serie_historica_safra.TestSerieHistoricaSafraFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                SilviculturaDataset,
                lambda: make_source(_silvicultura_mock_df()),
                ("eucalipto_folha",),
                {"return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'silvicultura'",
                        lambda _df, meta: meta.dataset == "silvicultura",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '1.1'",
                        lambda _df, meta: meta.contract_version == "1.1",
                    ),
                    (
                        "assert",
                        "meta.attempted_sources == ['ibge_silvicultura']",
                        lambda _df, meta: meta.attempted_sources == ["ibge_silvicultura"],
                    ),
                    (
                        "assert",
                        "meta.selected_source == 'ibge_silvicultura'",
                        lambda _df, meta: meta.selected_source == "ibge_silvicultura",
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="silvicultura.TestSilviculturaFetch.test_fetch_return_meta",
            ),
            pytest.param(
                None,
                UsodoSoloDataset,
                lambda: make_source(_uso_do_solo_make_cobertura_df()),
                (),
                {"tipo": "cobertura", "return_meta": True},
                [
                    (
                        "assert",
                        "meta.dataset == 'uso_do_solo'",
                        lambda _df, meta: meta.dataset == "uso_do_solo",
                    ),
                    (
                        "assert",
                        "meta.contract_version == '2.0'",
                        lambda _df, meta: meta.contract_version == "2.0",
                    ),
                    (
                        "assert",
                        "'mapbiomas' in meta.attempted_sources",
                        lambda _df, meta: "mapbiomas" in meta.attempted_sources,
                    ),
                    (
                        "assert",
                        "meta.records_count == len(df)",
                        lambda df, meta: meta.records_count == len(df),
                    ),
                ],
                id="uso_do_solo.TestUsodoSoloFetch.test_fetch_return_meta",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_metadata_from_fetch(
        self, monkeypatch, prepare, factory, source_factory, args, kwargs, checks
    ):
        with fixture_instance(prepare, monkeypatch=monkeypatch) if prepare else nullcontext():
            dataset = factory()
            if source_factory is not None:
                dataset.info.sources[0].fetch_fn = source_factory()
            df, meta = await dataset.fetch(*args, **kwargs)
            _run_template_checks(checks, df, meta)

    @pytest.mark.parametrize(
        "prepare,factory,source_factory,args,kwargs,checks",
        [
            pytest.param(
                None,
                AbateTrimestralDataset,
                lambda: make_source(amostra_abate_trimestral()),
                ("bovino",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    (
                        "assert",
                        "'animais_abatidos' in df.columns",
                        lambda df: "animais_abatidos" in df.columns,
                    ),
                    (
                        "assert",
                        "'peso_carcacas' in df.columns",
                        lambda df: "peso_carcacas" in df.columns,
                    ),
                ],
                id="abate_trimestral.TestAbateTrimestralFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                BalancoDataset,
                lambda: make_source(_balanco_mock_df()),
                ("soja",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    (
                        "assert",
                        "'estoque_inicial' in df.columns",
                        lambda df: "estoque_inicial" in df.columns,
                    ),
                    ("assert", "'producao' in df.columns", lambda df: "producao" in df.columns),
                ],
                id="balanco.TestBalancoFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                CadastroRuralDataset,
                lambda: AsyncMock(return_value=(amostra_cadastro_rural(), meta_cadastro_rural())),
                ("DF",),
                {},
                [
                    ("assert", "len(df) == 2", lambda df: len(df) == 2),
                    ("assert", "'cod_imovel' in df.columns", lambda df: "cod_imovel" in df.columns),
                    ("assert", "'area_ha' in df.columns", lambda df: "area_ha" in df.columns),
                ],
                id="cadastro_rural.TestCadastroRuralDataset.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                CensoAgropecuarioDataset,
                lambda: make_source(_censo_agropecuario_mock_df()),
                ("efetivo_rebanho",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    ("assert", "'tema' in df.columns", lambda df: "tema" in df.columns),
                    ("assert", "'valor' in df.columns", lambda df: "valor" in df.columns),
                ],
                id="censo_agropecuario.TestCensoAgropecuarioFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                CensoAgropecuarioLegadoDataset,
                lambda: make_source(amostra_censo_agropecuario_legado()),
                ("tecnologia",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    ("assert", "'tema' in df.columns", lambda df: "tema" in df.columns),
                    ("assert", "'valor' in df.columns", lambda df: "valor" in df.columns),
                ],
                id="censo_agropecuario_legado.TestCensoAgropecuarioLegadoFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                ClimaDataset,
                lambda: _climamake_source(_clima_add_inmet_nullable_cols(_clima_mock_inmet_df())),
                ("SP",),
                {"ano": 2024},
                [
                    ("assert", "len(df) == 2", lambda df: len(df) == 2),
                    (
                        "assert",
                        "'precip_acum_mm' in df.columns",
                        lambda df: "precip_acum_mm" in df.columns,
                    ),
                    ("assert", "'fonte' in df.columns", lambda df: "fonte" in df.columns),
                ],
                id="clima.TestClimaFetchUF.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                CreditoRuralDataset,
                lambda: make_source(_credito_rural_golden_df()),
                ("soja",),
                {"safra": "2023/24"},
                [
                    ("assert", "not df.empty", lambda df: not df.empty),
                    ("assert", "'valor' in df.columns", lambda df: "valor" in df.columns),
                    (
                        "call",
                        "validate_dataset(df, 'credito_rural')",
                        lambda df: validate_dataset(df, "credito_rural"),
                    ),
                ],
                id="credito_rural.TestCreditoRuralFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                EstimativaSafraDataset,
                lambda: make_source(_estimativa_safra_mock_df()),
                ("soja",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    (
                        "assert",
                        "'produtividade' in df.columns",
                        lambda df: "produtividade" in df.columns,
                    ),
                    (
                        "assert",
                        "'levantamento' in df.columns",
                        lambda df: "levantamento" in df.columns,
                    ),
                ],
                id="estimativa_safra.TestEstimativaSafraFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                ExportacaoDataset,
                lambda: make_source(_exportacao_mock_export_df()),
                ("soja",),
                {"ano": 2024},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    ("assert", "'kg_liquido' in df.columns", lambda df: "kg_liquido" in df.columns),
                    (
                        "assert",
                        "df.iloc[0]['valor_fob_usd'] == 2500000000",
                        lambda df: df.iloc[0]["valor_fob_usd"] == 2500000000,
                    ),
                ],
                id="exportacao.TestExportacaoFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                EmbarquesANECDataset,
                lambda: make_source(_exportacao_anec_mock_df()),
                (),
                {"ano": 2026},
                [
                    ("assert", "len(df) == 2", lambda df: len(df) == 2),
                    ("assert", "'porto' in df.columns", lambda df: "porto" in df.columns),
                    ("assert", "'valor_ton' in df.columns", lambda df: "valor_ton" in df.columns),
                ],
                id="exportacao_anec.TestEmbarquesANEC.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                ExtrativsmoVegetalDataset,
                lambda: make_source(_extrativismo_vegetal_mock_df()),
                ("acai",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    ("assert", "'valor' in df.columns", lambda df: "valor" in df.columns),
                    ("assert", "'unidade' in df.columns", lambda df: "unidade" in df.columns),
                ],
                id="extrativismo_vegetal.TestExtrativsmoVegetalFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                FertilizanteDataset,
                lambda: make_source(_fertilizante_mock_df()),
                ("total",),
                {"ano": 2024},
                [
                    ("assert", "len(df) == 2", lambda df: len(df) == 2),
                    ("assert", "'volume_ton' in df.columns", lambda df: "volume_ton" in df.columns),
                    (
                        "assert",
                        "df.iloc[0]['volume_ton'] == 150000.0",
                        lambda df: df.iloc[0]["volume_ton"] == 150000.0,
                    ),
                ],
                id="fertilizante.TestFertilizanteFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                ImportacaoDataset,
                lambda: make_source(_importacao_make_df()),
                ("soja",),
                {"ano": 2024},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    ("assert", "'kg_liquido' in df.columns", lambda df: "kg_liquido" in df.columns),
                    (
                        "assert",
                        "df.iloc[0]['valor_fob_usd'] == 500000.0",
                        lambda df: df.iloc[0]["valor_fob_usd"] == 500000.0,
                    ),
                ],
                id="importacao.TestImportacaoFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                LeiteIndustrialDataset,
                lambda: make_source(amostra_leite_industrial()),
                ("leite",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    (
                        "assert",
                        "'leite_adquirido' in df.columns",
                        lambda df: "leite_adquirido" in df.columns,
                    ),
                    (
                        "assert",
                        "'preco_medio' in df.columns",
                        lambda df: "preco_medio" in df.columns,
                    ),
                ],
                id="leite_industrial.TestLeiteIndustrialFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                PecuariaMunicipalDataset,
                lambda: make_source(_pecuaria_municipal_mock_df()),
                ("bovino",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    ("assert", "'valor' in df.columns", lambda df: "valor" in df.columns),
                    ("assert", "'especie' in df.columns", lambda df: "especie" in df.columns),
                ],
                id="pecuaria_municipal.TestPecuariaMunicipalFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                PibAgroDataset,
                lambda: make_source(_pib_agro_make_df()),
                ("agropecuaria",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    ("assert", "'trimestre' in df.columns", lambda df: "trimestre" in df.columns),
                    (
                        "assert",
                        "df.iloc[0]['valor'] == 150000.0",
                        lambda df: df.iloc[0]["valor"] == 150000.0,
                    ),
                ],
                id="pib_agro.TestPibAgroFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                PrecoDiarioDataset,
                lambda: make_source(_preco_diario_mock_df()),
                ("soja",),
                {},
                [
                    ("assert", "len(df) == 2", lambda df: len(df) == 2),
                    ("assert", "'data' in df.columns", lambda df: "data" in df.columns),
                    ("assert", "'valor' in df.columns", lambda df: "valor" in df.columns),
                ],
                id="preco_diario.TestPrecoDiarioFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                _producao_anual_isolate_producao_sources,
                ProducaoAnualDataset,
                lambda: make_source(_producao_anual_mock_df()),
                ("soja",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    (
                        "assert",
                        "'area_plantada' in df.columns",
                        lambda df: "area_plantada" in df.columns,
                    ),
                    ("assert", "'rendimento' in df.columns", lambda df: "rendimento" in df.columns),
                ],
                id="producao_anual.TestProducaoAnualFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                ProgressoSafraDataset,
                lambda: make_source(_progresso_safra_make_df()),
                ("soja",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    ("assert", "'cultura' in df.columns", lambda df: "cultura" in df.columns),
                    (
                        "assert",
                        "df.iloc[0]['pct_semana_atual'] == 0.92",
                        lambda df: df.iloc[0]["pct_semana_atual"] == 0.92,
                    ),
                ],
                id="progresso_safra.TestProgressoSafraFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                SerieHistoricaSafraDataset,
                lambda: make_source(_serie_historica_safra_mock_df()),
                ("soja",),
                {},
                [
                    ("assert", "len(df) == 2", lambda df: len(df) == 2),
                    (
                        "assert",
                        "'area_plantada_mil_ha' in df.columns",
                        lambda df: "area_plantada_mil_ha" in df.columns,
                    ),
                    (
                        "assert",
                        "'producao_mil_ton' in df.columns",
                        lambda df: "producao_mil_ton" in df.columns,
                    ),
                    (
                        "assert",
                        "'produtividade_kg_ha' in df.columns",
                        lambda df: "produtividade_kg_ha" in df.columns,
                    ),
                ],
                id="serie_historica_safra.TestSerieHistoricaSafraFetch.test_fetch_returns_dataframe",
            ),
            pytest.param(
                None,
                SilviculturaDataset,
                lambda: make_source(_silvicultura_mock_df()),
                ("eucalipto_folha",),
                {},
                [
                    ("assert", "len(df) == 1", lambda df: len(df) == 1),
                    ("assert", "'valor' in df.columns", lambda df: "valor" in df.columns),
                    ("assert", "'unidade' in df.columns", lambda df: "unidade" in df.columns),
                ],
                id="silvicultura.TestSilviculturaFetch.test_fetch_returns_dataframe",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_dataframe_from_fetch(
        self, monkeypatch, prepare, factory, source_factory, args, kwargs, checks
    ):
        with fixture_instance(prepare, monkeypatch=monkeypatch) if prepare else nullcontext():
            dataset = factory()
            if source_factory is not None:
                dataset.info.sources[0].fetch_fn = source_factory()
            df = await dataset.fetch(*args, **kwargs)
            assert isinstance(df, pd.DataFrame), type(df)
            _run_template_checks(checks, df)


@pytest.mark.parametrize("dataset_name", DATASETS_WITH_PRODUCTS)
class TestDatasetValidation:
    def test_validate_produto_invalid(self, dataset_name):
        ds = registry.get_dataset(dataset_name)
        with pytest.raises(ValueError, match="banana_inexistente"):
            ds._validate_produto("banana_inexistente")
