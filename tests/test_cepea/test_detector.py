from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from agrobr.exceptions import FingerprintMismatchError, ParseError
from tests import helpers


class TestGetParserWithFallback:
    @pytest.mark.parametrize(
        "scenario,parameters",
        [
            ("test_low_confidence_strict_raises", {}),
            ("test_parser_cannot_parse_skipped", {}),
            ("test_date_filtering", {}),
            ("test_empty_results_tries_next", {}),
        ],
        ids=[
            "low_confidence_strict_raises-0",
            "parser_cannot_parse_skipped-0",
            "date_filtering-0",
            "empty_results_tries_next-0",
        ],
    )
    async def test_guardas_de_selecao_do_parser(self, scenario: str, parameters: dict[str, Any]):
        with (
            helpers.collect_failures() as check,
            check((scenario, parameters)),
            helpers.isolated_dataset_case((scenario, parameters)),
        ):
            if scenario == "test_low_confidence_strict_raises":
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
            elif scenario == "test_parser_cannot_parse_skipped":
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
            elif scenario == "test_date_filtering":
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
                    await get_parser_with_fallback(
                        "<html>", "soja", data_referencia=date(2020, 1, 1)
                    )
            elif scenario == "test_empty_results_tries_next":
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
            pytest.raises(ParseError, match="All parsers failed: v1: parse error"),
        ):
            await get_parser_with_fallback("<html>", "soja")

    @pytest.mark.asyncio
    async def test_no_parsers_raises(self):
        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        with (
            patch("agrobr.cepea.parsers.detector.PARSERS", []),
            pytest.raises(ParseError, match="Nenhum parser CEPEA registrado"),
        ):
            await get_parser_with_fallback("<html>", "soja")
