from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from agrobr.constants import Fonte
from agrobr.exceptions import FingerprintMismatchError, ParseError
from agrobr.models import Indicador


def _make_indicador():
    return Indicador(
        fonte=Fonte.CEPEA,
        produto="soja",
        data=date(2024, 1, 1),
        valor=Decimal("150.00"),
        unidade="BRL/sc60kg",
    )


class TestGetParserWithFallback:
    @pytest.mark.asyncio
    async def test_successful_parse(self):
        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        mock_parser = MagicMock()
        mock_parser.version = 1
        mock_parser.source = "cepea"
        mock_parser.valid_from = date(2020, 1, 1)
        mock_parser.valid_until = None
        mock_parser.can_parse.return_value = (True, 0.95)
        mock_parser.parse.return_value = [_make_indicador()]

        mock_parser_cls = MagicMock(return_value=mock_parser)

        with patch("agrobr.cepea.parsers.detector.PARSERS", [mock_parser_cls]):
            parser, results = await get_parser_with_fallback("<html>table</html>", "soja")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_no_parsers_raises(self):
        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        with (
            patch("agrobr.cepea.parsers.detector.PARSERS", []),
            pytest.raises(ParseError, match="No parsers"),
        ):
            await get_parser_with_fallback("<html>", "soja")

    @pytest.mark.asyncio
    async def test_all_parsers_fail(self):
        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        mock_parser_cls = MagicMock()
        mock_parser = MagicMock()
        mock_parser.version = 1
        mock_parser.source = "cepea"
        mock_parser.valid_from = date(2020, 1, 1)
        mock_parser.valid_until = None
        mock_parser.can_parse.return_value = (True, 0.95)
        mock_parser.parse.side_effect = Exception("parse error")
        mock_parser_cls.return_value = mock_parser

        with (
            patch("agrobr.cepea.parsers.detector.PARSERS", [mock_parser_cls]),
            pytest.raises(ParseError, match="All parsers failed"),
        ):
            await get_parser_with_fallback("<html>", "soja")

    @pytest.mark.asyncio
    async def test_low_confidence_strict_raises(self):
        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        mock_parser_cls = MagicMock()
        mock_parser = MagicMock()
        mock_parser.version = 1
        mock_parser.source = "cepea"
        mock_parser.valid_from = date(2020, 1, 1)
        mock_parser.valid_until = None
        mock_parser.can_parse.return_value = (True, 0.3)
        mock_parser_cls.return_value = mock_parser

        with (
            patch("agrobr.cepea.parsers.detector.PARSERS", [mock_parser_cls]),
            pytest.raises(FingerprintMismatchError),
        ):
            await get_parser_with_fallback("<html>", "soja", strict=True)

    @pytest.mark.asyncio
    async def test_parser_cannot_parse_skipped(self):
        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        mock_parser_cls = MagicMock()
        mock_parser = MagicMock()
        mock_parser.version = 1
        mock_parser.source = "cepea"
        mock_parser.valid_from = date(2020, 1, 1)
        mock_parser.valid_until = None
        mock_parser.can_parse.return_value = (False, 0.0)
        mock_parser_cls.return_value = mock_parser

        with (
            patch("agrobr.cepea.parsers.detector.PARSERS", [mock_parser_cls]),
            pytest.raises(ParseError),
        ):
            await get_parser_with_fallback("<html>", "soja")

    @pytest.mark.asyncio
    async def test_date_filtering(self):
        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        mock_parser_cls = MagicMock()
        mock_parser = MagicMock()
        mock_parser.version = 1
        mock_parser.source = "cepea"
        mock_parser.valid_from = date(2025, 1, 1)
        mock_parser.valid_until = None
        mock_parser.can_parse.return_value = (True, 0.95)
        mock_parser_cls.return_value = mock_parser

        with (
            patch("agrobr.cepea.parsers.detector.PARSERS", [mock_parser_cls]),
            pytest.raises(ParseError),
        ):
            await get_parser_with_fallback("<html>", "soja", data_referencia=date(2020, 1, 1))

    @pytest.mark.asyncio
    async def test_empty_results_tries_next(self):
        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        mock_parser_cls = MagicMock()
        mock_parser = MagicMock()
        mock_parser.version = 1
        mock_parser.source = "cepea"
        mock_parser.valid_from = date(2020, 1, 1)
        mock_parser.valid_until = None
        mock_parser.can_parse.return_value = (True, 0.95)
        mock_parser.parse.return_value = []
        mock_parser_cls.return_value = mock_parser

        with (
            patch("agrobr.cepea.parsers.detector.PARSERS", [mock_parser_cls]),
            pytest.raises(ParseError, match="All parsers failed"),
        ):
            await get_parser_with_fallback("<html>", "soja")

    @pytest.mark.asyncio
    async def test_medium_confidence_warning(self):
        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        mock_parser_cls = MagicMock()
        mock_parser = MagicMock()
        mock_parser.version = 1
        mock_parser.source = "cepea"
        mock_parser.valid_from = date(2020, 1, 1)
        mock_parser.valid_until = None
        mock_parser.can_parse.return_value = (True, 0.75)
        mock_parser.parse.return_value = [_make_indicador()]
        mock_parser_cls.return_value = mock_parser

        with patch("agrobr.cepea.parsers.detector.PARSERS", [mock_parser_cls]):
            parser, results = await get_parser_with_fallback("<html>", "soja")
        assert len(results) == 1
