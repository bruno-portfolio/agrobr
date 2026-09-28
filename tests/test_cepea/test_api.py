from __future__ import annotations

import warnings
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from agrobr import cepea, constants
from agrobr.cepea import api
from agrobr.cepea.client import FetchResult
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    StaleDataWarning,
)
from agrobr.models import Indicador
from agrobr.utils.warnings import warn_once_reset
from tests import helpers


def _make_indicador(
    produto: str = "soja",
    data: date | None = None,
    valor: Decimal | None = None,
    praca: str | None = None,
) -> Indicador:
    return Indicador(
        fonte=constants.Fonte.CEPEA,
        produto=produto,
        praca=praca,
        data=data or date.today() - timedelta(days=1),
        valor=valor or Decimal("145.50"),
        unidade="BRL/sc60kg",
    )


def _indicador_to_dict(ind: Indicador) -> dict:
    return {
        "produto": ind.produto,
        "praca": ind.praca,
        "data": ind.data,
        "valor": float(ind.valor),
        "unidade": ind.unidade,
        "fonte": ind.fonte.value,
        "metodologia": ind.metodologia,
        "variacao_percentual": None,
        "collected_at": datetime.utcnow(),
        "parser_version": ind.parser_version,
    }


class TestIndicador:
    @pytest.fixture(autouse=True)
    def _setup_mocks(self):
        warn_once_reset("cepea_license")
        self.mock_store = MagicMock()
        self.mock_store.indicadores_query.return_value = []
        self.mock_store.indicadores_upsert.return_value = 0
        self.mock_store.indicadores_ultima_coleta.return_value = None

        with patch("agrobr.cepea.api.get_store", return_value=self.mock_store):
            yield

        warn_once_reset("cepea_license")

    async def test_date_range_filters(self):
        today = date.today()
        ind_in = _make_indicador(data=today - timedelta(days=5))
        ind_out = _make_indicador(data=today - timedelta(days=100))
        dicts = [_indicador_to_dict(ind_in), _indicador_to_dict(ind_out)]
        self.mock_store.indicadores_query.return_value = dicts

        inicio = today - timedelta(days=10)
        fim = today
        df = await api.indicador("soja", inicio=inicio, fim=fim, offline=True)

        assert all(df["data"].dt.date >= inicio)
        assert all(df["data"].dt.date <= fim)

    @pytest.mark.parametrize("produto", ["banana", 123])
    async def test_invalid_product_raises_before_network(self, produto):
        with (
            patch(
                "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
            ) as mock_fetch,
            pytest.raises(InvalidParameterError, match="produto|Produto"),
        ):
            await api.indicador(produto)

        mock_fetch.assert_not_awaited()

    async def test_invalid_praca_raises_before_network(self):
        with pytest.raises(InvalidParameterError, match="Praça inválida"):
            await api.indicador("soja", praca="marte")

    @pytest.mark.parametrize(
        "scenario,parameters",
        [
            ("test_reverse_date_range_raises", {}),
            ("test_nat_rejected_before_cache", {"parameter": "inicio"}),
            ("test_nat_rejected_before_cache", {"parameter": "fim"}),
        ],
        ids=[
            "reverse_date_range_raises-0",
            "nat_rejected_before_cache-0",
            "nat_rejected_before_cache-1",
        ],
    )
    async def test_guardas_dos_limites_temporais(self, scenario: str, parameters: dict[str, Any]):
        with (
            helpers.collect_failures() as check,
            check((scenario, parameters)),
            helpers.isolated_dataset_case((scenario, parameters)),
        ):
            if scenario == "test_reverse_date_range_raises":
                with pytest.raises(InvalidParameterError, match="inicio"):
                    await api.indicador("soja", inicio="2025-02-01", fim="2025-01-01")
            elif scenario == "test_nat_rejected_before_cache":
                parameter = parameters["parameter"]
                with pytest.raises(InvalidParameterError, match="Datas"):
                    await api.indicador("soja", **{parameter: pd.NaT})
                self.mock_store.indicadores_query.assert_not_called()

    async def test_pagina_sem_indicadores_e_cache_vazio_levanta_parse_error(self):
        with (
            patch(
                "agrobr.cepea.api.client.fetch_indicador_page",
                new_callable=AsyncMock,
                return_value=FetchResult("<html>CEPEA</html>", "cepea"),
            ),
            patch(
                "agrobr.cepea.api.get_parser_with_fallback",
                new_callable=AsyncMock,
                return_value=(MagicMock(version=1), []),
            ),
            patch("agrobr.cepea.api.client.can_use_alternative_source", return_value=False),
            pytest.raises(ParseError, match="Nenhum indicador extraído"),
        ):
            await api.indicador("soja")

    async def test_empty_fetch_with_existing_cache_warns_stale(self):
        today = date.today()
        cached = _make_indicador(data=today - timedelta(days=1))
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(cached)]

        html = "<html>CEPEA data</html>"
        with (
            patch(
                "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
            ) as mock_fetch,
            patch(
                "agrobr.cepea.api.get_parser_with_fallback", new_callable=AsyncMock
            ) as mock_parser,
            warnings.catch_warnings(record=True) as w,
        ):
            warnings.simplefilter("always")
            mock_fetch.return_value = FetchResult(html, "cepea")
            mock_parser.return_value = (MagicMock(version=1), [])
            await api.indicador(
                "soja",
                inicio=today - timedelta(days=10),
                fim=today,
            )

        stale_warnings = [x for x in w if issubclass(x.category, StaleDataWarning)]
        assert len(stale_warnings) == 1
        assert "no data" in str(stale_warnings[0].message).lower()

    async def test_praca_filter(self):
        ind_paranagua = _make_indicador(praca="Paranaguá/PR")
        ind_parana = _make_indicador(praca="Paraná")
        dicts = [_indicador_to_dict(ind_paranagua), _indicador_to_dict(ind_parana)]
        self.mock_store.indicadores_query.return_value = dicts

        df = await api.indicador("soja", praca="paranagua", offline=True)

        assert df["praca"].tolist() == ["Paranaguá/PR"]


class TestUltimo:
    @pytest.fixture(autouse=True)
    def _setup_mocks(self):
        warn_once_reset("cepea_license")
        self.mock_store = MagicMock()
        self.mock_store.indicadores_query.return_value = []
        self.mock_store.indicadores_upsert.return_value = 0
        self.mock_store.indicadores_ultima_coleta.return_value = None

        with patch("agrobr.cepea.api.get_store", return_value=self.mock_store):
            yield

        warn_once_reset("cepea_license")

    async def test_returns_latest_indicador(self):
        today = date.today()
        old = _make_indicador(data=today - timedelta(days=5), valor=Decimal("140.00"))
        recent = _make_indicador(data=today - timedelta(days=1), valor=Decimal("150.00"))
        self.mock_store.indicadores_query.return_value = [
            _indicador_to_dict(old),
            _indicador_to_dict(recent),
        ]

        result = await api.ultimo("soja", offline=True)

        assert isinstance(result, Indicador)
        assert result.data == recent.data

    async def test_invalid_product_and_praca_raise_before_cache(self):
        with pytest.raises(InvalidParameterError, match="Produto inválido"):
            await api.ultimo("banana", offline=True)
        with pytest.raises(InvalidParameterError, match="Praça inválida"):
            await api.ultimo("soja", praca="marte", offline=True)

        self.mock_store.indicadores_query.assert_not_called()


class TestToDataframeVazio:
    def test_vazio_preserva_colunas_e_dtypes_do_contrato(self):
        from agrobr.contracts import validate_dataset

        df = api._to_dataframe([])

        assert df.empty
        assert list(df.columns)[:6] == ["data", "produto", "praca", "valor", "unidade", "fonte"]
        assert pd.api.types.is_datetime64_any_dtype(df["data"])
        assert pd.api.types.is_float_dtype(df["valor"])
        validate_dataset(df, "preco_diario")


async def test_pracas_bezerro():
    result = await cepea.pracas("bezerro")
    assert result == ["mato_grosso_do_sul"]


async def test_produtos_returns_list():
    result = await cepea.produtos()
    assert isinstance(result, list)
    assert len(result) == 22
    assert "soja" in result
    assert "milho" in result
    assert "bezerro" in result
    assert "cafe" in result
    assert "cafe_robusta" in result
