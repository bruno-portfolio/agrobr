"""Tests for CEPEA API."""

from __future__ import annotations

import warnings
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from agrobr import cepea, constants
from agrobr.cepea import api
from agrobr.cepea.client import FetchResult
from agrobr.exceptions import ParseError, SourceUnavailableError, StaleDataWarning
from agrobr.models import Indicador


@pytest.mark.integration
async def test_produtos_returns_list():
    result = await cepea.produtos()
    assert isinstance(result, list)
    assert "soja" in result
    assert "milho" in result
    assert "cafe" in result


@pytest.mark.integration
async def test_pracas_returns_list():
    result = await cepea.pracas("soja")
    assert isinstance(result, list)
    assert len(result) > 0


@pytest.mark.integration
async def test_pracas_unknown_product():
    result = await cepea.pracas("unknown_product")
    assert isinstance(result, list)
    assert len(result) == 0


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
        self.mock_store = MagicMock()
        self.mock_store.indicadores_query.return_value = []
        self.mock_store.indicadores_upsert.return_value = 0

        with patch("agrobr.cepea.api.get_store", return_value=self.mock_store):
            yield

    async def test_valid_product_returns_dataframe(self):
        ind = _make_indicador()
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(ind)]

        df = await api.indicador("soja", offline=True)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "data" in df.columns
        assert "valor" in df.columns
        assert "produto" in df.columns

    async def test_returns_empty_dataframe_when_no_data(self):
        df = await api.indicador("soja", offline=True)

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    async def test_return_meta_flag(self):
        ind = _make_indicador()
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(ind)]

        result = await api.indicador("soja", offline=True, return_meta=True)

        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert meta.records_count == len(df)

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

    async def test_string_date_params(self):
        today = date.today()
        ind = _make_indicador(data=today - timedelta(days=2))
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(ind)]

        inicio_str = (today - timedelta(days=10)).strftime("%Y-%m-%d")
        fim_str = today.strftime("%Y-%m-%d")
        df = await api.indicador("soja", inicio=inicio_str, fim=fim_str, offline=True)

        assert isinstance(df, pd.DataFrame)

    async def test_praca_filter(self):
        ind_sp = _make_indicador(praca="sao_paulo")
        ind_pr = _make_indicador(praca="parana")
        dicts = [_indicador_to_dict(ind_sp), _indicador_to_dict(ind_pr)]
        self.mock_store.indicadores_query.return_value = dicts

        df = await api.indicador("soja", praca="sao_paulo", offline=True)

        assert all(df["praca"].str.lower() == "sao_paulo")

    async def test_force_refresh_skips_cache_and_fetches(self):
        html = "<html><table class='indicador'>CEPEA</table></html>"
        new_ind = _make_indicador()

        with (
            patch(
                "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
            ) as mock_fetch,
            patch(
                "agrobr.cepea.api.get_parser_with_fallback", new_callable=AsyncMock
            ) as mock_parser,
        ):
            mock_fetch.return_value = FetchResult(html, "cepea")
            mock_parser.return_value = (MagicMock(version=1), [new_ind])

            await api.indicador("soja", force_refresh=True)

        mock_fetch.assert_awaited_once_with("soja")
        assert self.mock_store.indicadores_query.call_count == 0

    async def test_fetch_new_data_merges_with_cache(self):
        today = date.today()
        cached = _make_indicador(data=today - timedelta(days=5))
        fresh = _make_indicador(data=today - timedelta(days=1))
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(cached)]

        html = "<html>CEPEA data</html>"
        with (
            patch(
                "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
            ) as mock_fetch,
            patch(
                "agrobr.cepea.api.get_parser_with_fallback", new_callable=AsyncMock
            ) as mock_parser,
        ):
            mock_fetch.return_value = FetchResult(html, "cepea")
            mock_parser.return_value = (MagicMock(version=1), [fresh])

            df = await api.indicador(
                "soja",
                inicio=today - timedelta(days=10),
                fim=today,
            )

        assert len(df) == 2
        self.mock_store.indicadores_upsert.assert_called_once()

    async def test_noticias_agricolas_source_detected(self):
        today = date.today()
        ind = _make_indicador(data=today - timedelta(days=1))

        html = '<html><div class="cot-fisicas">noticiasagricolas data</div></html>'
        with (
            patch(
                "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
            ) as mock_fetch,
            patch("agrobr.noticias_agricolas.parser.parse_indicador") as mock_na_parse,
        ):
            mock_fetch.return_value = FetchResult(html, "noticias_agricolas")
            mock_na_parse.return_value = [ind]

            result = await api.indicador("soja", force_refresh=True, return_meta=True)

        df, meta = result
        assert meta.source == "noticias_agricolas"

    async def test_source_fetch_failure_falls_back_to_cache(self):
        today = date.today()
        cached = _make_indicador(data=today - timedelta(days=3))
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(cached)]

        with patch(
            "agrobr.cepea.api.client.fetch_indicador_page",
            new_callable=AsyncMock,
            side_effect=SourceUnavailableError(source="cepea", last_error="down"),
        ):
            df = await api.indicador(
                "soja",
                inicio=today - timedelta(days=10),
                fim=today,
                force_refresh=True,
            )

        assert not df.empty

    async def test_source_fetch_failure_no_cache_returns_empty(self):
        self.mock_store.indicadores_query.return_value = []

        with patch(
            "agrobr.cepea.api.client.fetch_indicador_page",
            new_callable=AsyncMock,
            side_effect=SourceUnavailableError(source="cepea", last_error="down"),
        ):
            df = await api.indicador("soja", force_refresh=True)

        assert df.empty

    async def test_offline_mode_never_fetches(self):
        with patch(
            "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
        ) as mock_fetch:
            df = await api.indicador("soja", offline=True)

        mock_fetch.assert_not_awaited()
        assert isinstance(df, pd.DataFrame)

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

    async def test_default_dates_when_none(self):
        df = await api.indicador("soja", offline=True)

        assert isinstance(df, pd.DataFrame)
        self.mock_store.indicadores_query.assert_called_once()
        call_kwargs = self.mock_store.indicadores_query.call_args
        assert call_kwargs.kwargs["produto"] == "soja"

    async def test_na_soft_block_falls_back_to_cache(self):
        """When NA returns a soft block (SourceUnavailableError), cached data is used."""
        today = date.today()
        cached = _make_indicador(data=today - timedelta(days=3))
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(cached)]

        with patch(
            "agrobr.cepea.api.client.fetch_indicador_page",
            new_callable=AsyncMock,
            side_effect=SourceUnavailableError(
                source="noticias_agricolas",
                last_error="Soft block detected",
            ),
        ):
            df = await api.indicador(
                "soja",
                inicio=today - timedelta(days=10),
                fim=today,
                force_refresh=True,
            )

        assert not df.empty

    async def test_cache_hit_sets_meta_source(self):
        ind = _make_indicador()
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(ind)]

        _, meta = await api.indicador("soja", offline=True, return_meta=True)

        assert meta.from_cache is True
        assert meta.source == "cache"


class TestUltimo:
    @pytest.fixture(autouse=True)
    def _setup_mocks(self):
        self.mock_store = MagicMock()
        self.mock_store.indicadores_query.return_value = []
        self.mock_store.indicadores_upsert.return_value = 0

        with patch("agrobr.cepea.api.get_store", return_value=self.mock_store):
            yield

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

    async def test_raises_parse_error_when_no_data(self):
        with pytest.raises(ParseError, match="No indicators found"):
            await api.ultimo("soja", offline=True)

    async def test_praca_filter(self):
        today = date.today()
        ind_sp = _make_indicador(data=today - timedelta(days=1), praca="sao_paulo")
        ind_pr = _make_indicador(data=today - timedelta(days=1), praca="parana")
        self.mock_store.indicadores_query.return_value = [
            _indicador_to_dict(ind_sp),
            _indicador_to_dict(ind_pr),
        ]

        result = await api.ultimo("soja", praca="sao_paulo", offline=True)

        assert result.praca == "sao_paulo"

    async def test_praca_filter_no_match_raises(self):
        today = date.today()
        ind = _make_indicador(data=today - timedelta(days=1), praca="parana")
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(ind)]

        with pytest.raises(ParseError, match="No indicators found"):
            await api.ultimo("soja", praca="campinas", offline=True)

    async def test_fetches_when_no_recent_data(self):
        today = date.today()
        old_ind = _make_indicador(data=today - timedelta(days=10))
        fresh_ind = _make_indicador(data=today - timedelta(days=1))
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(old_ind)]

        html = "<html>CEPEA data</html>"
        with (
            patch(
                "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
            ) as mock_fetch,
            patch(
                "agrobr.cepea.api.get_parser_with_fallback", new_callable=AsyncMock
            ) as mock_parser,
        ):
            mock_fetch.return_value = FetchResult(html, "cepea")
            mock_parser.return_value = (MagicMock(version=1), [fresh_ind])

            result = await api.ultimo("soja")

        assert result.data == fresh_ind.data
        self.mock_store.indicadores_upsert.assert_called_once()

    async def test_offline_never_fetches(self):
        today = date.today()
        ind = _make_indicador(data=today - timedelta(days=5))
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(ind)]

        with patch(
            "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
        ) as mock_fetch:
            result = await api.ultimo("soja", offline=True)

        mock_fetch.assert_not_awaited()
        assert isinstance(result, Indicador)

    async def test_fetch_failure_uses_cache(self):
        today = date.today()
        ind = _make_indicador(data=today - timedelta(days=10))
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(ind)]

        with patch(
            "agrobr.cepea.api.client.fetch_indicador_page",
            new_callable=AsyncMock,
            side_effect=SourceUnavailableError(source="cepea", last_error="down"),
        ):
            result = await api.ultimo("soja")

        assert result.data == ind.data

    async def test_fetch_failure_no_cache_raises(self):
        with (
            patch(
                "agrobr.cepea.api.client.fetch_indicador_page",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="cepea", last_error="down"),
            ),
            pytest.raises(ParseError, match="No indicators found"),
        ):
            await api.ultimo("soja")

    async def test_noticias_agricolas_source(self):
        today = date.today()
        ind = _make_indicador(data=today - timedelta(days=1))

        html = '<html><div class="cot-fisicas">NA</div></html>'
        with (
            patch(
                "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
            ) as mock_fetch,
            patch("agrobr.noticias_agricolas.parser.parse_indicador") as mock_na,
        ):
            mock_fetch.return_value = FetchResult(html, "noticias_agricolas")
            mock_na.return_value = [ind]

            result = await api.ultimo("soja")

        assert isinstance(result, Indicador)

    async def test_skips_fetch_when_recent_data_exists(self):
        today = date.today()
        ind = _make_indicador(data=today - timedelta(days=1))
        self.mock_store.indicadores_query.return_value = [_indicador_to_dict(ind)]

        with patch(
            "agrobr.cepea.api.client.fetch_indicador_page", new_callable=AsyncMock
        ) as mock_fetch:
            result = await api.ultimo("soja")

        mock_fetch.assert_not_awaited()
        assert result.data == ind.data
