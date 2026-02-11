"""Tests for CONAB API."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from agrobr import constants
from agrobr.conab import api
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.models import Safra


def _make_safra(
    produto: str = "soja",
    uf: str = "MT",
    safra: str = "2025/26",
    levantamento: int = 1,
) -> Safra:
    return Safra(
        fonte=constants.Fonte.CONAB,
        produto=produto,
        safra=safra,
        uf=uf,
        area_plantada=Decimal("10000.0"),
        producao=Decimal("30000.0"),
        produtividade=Decimal("3000.0"),
        levantamento=levantamento,
        data_publicacao=date.today(),
    )


def _mock_fetch_safra_xlsx(safra_list=None, suprimentos=None, totais=None):
    xlsx_bytes = BytesIO(b"fake-xlsx")
    metadata = {"safra": "2025/26", "levantamento": 1, "url": "https://conab.gov.br/test.xlsx"}

    mock_client = AsyncMock()
    mock_client.return_value = (xlsx_bytes, metadata)

    mock_parser = MagicMock()
    mock_parser.version = 1
    if safra_list is not None:
        mock_parser.parse_safra_produto.return_value = safra_list
    if suprimentos is not None:
        mock_parser.parse_suprimento.return_value = suprimentos
    if totais is not None:
        mock_parser.parse_brasil_total.return_value = totais

    return mock_client, mock_parser


class TestSafras:
    async def test_returns_dataframe_with_valid_data(self):
        safras = [_make_safra(uf="MT"), _make_safra(uf="PR")]
        mock_client, mock_parser = _mock_fetch_safra_xlsx(safra_list=safras)

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            df = await api.safras("soja")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "produto" in df.columns
        assert "uf" in df.columns

    async def test_returns_empty_dataframe_when_no_data(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(safra_list=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            df = await api.safras("soja")

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    async def test_uf_filter(self):
        safras = [_make_safra(uf="MT"), _make_safra(uf="PR"), _make_safra(uf="RS")]
        mock_client, mock_parser = _mock_fetch_safra_xlsx(safra_list=safras)

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            df = await api.safras("soja", uf="MT")

        assert len(df) == 1
        assert df.iloc[0]["uf"] == "MT"

    async def test_uf_filter_no_match_returns_empty(self):
        safras = [_make_safra(uf="MT")]
        mock_client, mock_parser = _mock_fetch_safra_xlsx(safra_list=safras)

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            df = await api.safras("soja", uf="AC")

        assert df.empty

    async def test_safra_param_forwarded_to_client(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(safra_list=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            await api.safras("soja", safra="2024/25")

        mock_client.assert_awaited_once_with(safra="2024/25", levantamento=None)

    async def test_levantamento_param_forwarded(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(safra_list=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            await api.safras("soja", levantamento=5)

        mock_client.assert_awaited_once_with(safra=None, levantamento=5)

    async def test_return_meta_flag(self):
        safras = [_make_safra()]
        mock_client, mock_parser = _mock_fetch_safra_xlsx(safra_list=safras)

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            result = await api.safras("soja", return_meta=True)

        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert meta.source == "conab"
        assert meta.records_count == len(df)

    async def test_return_meta_on_empty(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(safra_list=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            result = await api.safras("soja", return_meta=True)

        df, meta = result
        assert df.empty
        assert meta.records_count == 0

    async def test_parser_called_with_correct_produto(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(safra_list=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            await api.safras("milho", safra="2025/26")

        call_kwargs = mock_parser.parse_safra_produto.call_args.kwargs
        assert call_kwargs["produto"] == "milho"

    async def test_source_unavailable_propagates(self):
        with (
            patch(
                "agrobr.conab.api.client.fetch_safra_xlsx",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="conab", last_error="down"),
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await api.safras("soja")

    async def test_parse_error_propagates(self):
        mock_client = AsyncMock(return_value=(BytesIO(b"x"), {"safra": "2025/26"}))
        mock_parser = MagicMock()
        mock_parser.parse_safra_produto.side_effect = ParseError(
            source="conab", parser_version=1, reason="bad layout"
        )

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
            pytest.raises(ParseError, match="bad layout"),
        ):
            await api.safras("soja")


class TestBalanco:
    async def test_returns_dataframe(self):
        suprimentos = [
            {
                "produto": "SOJA",
                "safra": "2024/25",
                "producao": Decimal("150000"),
                "exportacao": Decimal("90000"),
                "consumo": Decimal("55000"),
            }
        ]
        mock_client, mock_parser = _mock_fetch_safra_xlsx(suprimentos=suprimentos)

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            df = await api.balanco("soja")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "produto" in df.columns

    async def test_returns_empty_dataframe_when_no_data(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(suprimentos=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            df = await api.balanco("soja")

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    async def test_produto_none_returns_all(self):
        suprimentos = [
            {"produto": "SOJA", "safra": "2024/25", "producao": Decimal("150000")},
            {"produto": "MILHO", "safra": "2024/25", "producao": Decimal("120000")},
        ]
        mock_client, mock_parser = _mock_fetch_safra_xlsx(suprimentos=suprimentos)

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            df = await api.balanco()

        assert len(df) == 2
        mock_parser.parse_suprimento.assert_called_once_with(
            xlsx=mock_client.return_value[0], produto=None
        )

    async def test_produto_filter_forwarded_to_parser(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(suprimentos=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            await api.balanco("milho")

        mock_parser.parse_suprimento.assert_called_once_with(
            xlsx=mock_client.return_value[0], produto="milho"
        )

    async def test_safra_param_forwarded_to_client(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(suprimentos=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            await api.balanco(safra="2024/25")

        mock_client.assert_awaited_once_with(safra="2024/25")

    async def test_source_unavailable_propagates(self):
        with (
            patch(
                "agrobr.conab.api.client.fetch_safra_xlsx",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="conab", last_error="down"),
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await api.balanco("soja")

    async def test_parse_error_propagates(self):
        mock_client = AsyncMock(return_value=(BytesIO(b"x"), {"safra": "2025/26"}))
        mock_parser = MagicMock()
        mock_parser.parse_suprimento.side_effect = ParseError(
            source="conab", parser_version=1, reason="bad suprimento"
        )

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
            pytest.raises(ParseError, match="bad suprimento"),
        ):
            await api.balanco("soja")


class TestBrasilTotal:
    async def test_returns_dataframe(self):
        totais = [
            {
                "produto": "Soja",
                "safra": "2025/26",
                "area_plantada": Decimal("45000"),
                "producao": Decimal("150000"),
                "produtividade": Decimal("3333"),
            }
        ]
        mock_client, mock_parser = _mock_fetch_safra_xlsx(totais=totais)

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            df = await api.brasil_total()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "produto" in df.columns

    async def test_returns_empty_dataframe_when_no_data(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(totais=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            df = await api.brasil_total()

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    async def test_safra_param_forwarded(self):
        mock_client, mock_parser = _mock_fetch_safra_xlsx(totais=[])

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
        ):
            await api.brasil_total(safra="2024/25")

        mock_client.assert_awaited_once_with(safra="2024/25")
        mock_parser.parse_brasil_total.assert_called_once_with(
            xlsx=mock_client.return_value[0], safra_ref="2024/25"
        )

    async def test_source_unavailable_propagates(self):
        with (
            patch(
                "agrobr.conab.api.client.fetch_safra_xlsx",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="conab", last_error="down"),
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await api.brasil_total()

    async def test_parse_error_propagates(self):
        mock_client = AsyncMock(return_value=(BytesIO(b"x"), {"safra": "2025/26"}))
        mock_parser = MagicMock()
        mock_parser.parse_brasil_total.side_effect = ParseError(
            source="conab", parser_version=1, reason="bad total"
        )

        with (
            patch("agrobr.conab.api.client.fetch_safra_xlsx", mock_client),
            patch("agrobr.conab.api.ConabParserV1", return_value=mock_parser),
            pytest.raises(ParseError, match="bad total"),
        ):
            await api.brasil_total()


class TestLevantamentos:
    async def test_returns_list(self):
        mock_data = [
            {"safra": "2025/26", "levantamento": 6, "url": "https://conab.gov.br/6.xlsx"},
            {"safra": "2025/26", "levantamento": 5, "url": "https://conab.gov.br/5.xlsx"},
        ]
        with patch(
            "agrobr.conab.api.client.list_levantamentos",
            new_callable=AsyncMock,
            return_value=mock_data,
        ):
            result = await api.levantamentos()

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["levantamento"] == 6

    async def test_returns_empty_list(self):
        with patch(
            "agrobr.conab.api.client.list_levantamentos",
            new_callable=AsyncMock,
            return_value=[],
        ):
            result = await api.levantamentos()

        assert result == []

    async def test_source_unavailable_propagates(self):
        with (
            patch(
                "agrobr.conab.api.client.list_levantamentos",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="conab", last_error="down"),
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await api.levantamentos()


class TestProdutos:
    async def test_returns_list_of_products(self):
        result = await api.produtos()

        assert isinstance(result, list)
        assert "soja" in result
        assert "milho" in result

    async def test_matches_constants(self):
        result = await api.produtos()

        assert set(result) == set(constants.CONAB_PRODUTOS.keys())


class TestUfs:
    async def test_returns_list_of_ufs(self):
        result = await api.ufs()

        assert isinstance(result, list)
        assert "MT" in result
        assert "PR" in result
        assert "SP" in result

    async def test_returns_copy_not_reference(self):
        result1 = await api.ufs()
        result2 = await api.ufs()

        assert result1 is not result2
