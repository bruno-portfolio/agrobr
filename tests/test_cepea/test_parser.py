"""Tests for CEPEA parser."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from agrobr.cepea.parsers.v1 import CepeaParserV1
from agrobr.exceptions import ParseError


class TestCepeaParserV1:
    """Tests for CepeaParserV1."""

    def setup_method(self):
        self.parser = CepeaParserV1()

    def test_can_parse_with_valid_html(self, sample_html_cepea):
        """Test can_parse returns True for valid CEPEA HTML."""
        can_parse, confidence = self.parser.can_parse(sample_html_cepea)
        assert can_parse is True
        assert confidence >= 0.4

    def test_can_parse_with_empty_html(self, sample_html_empty):
        """Test can_parse returns False for HTML without tables."""
        can_parse, confidence = self.parser.can_parse(sample_html_empty)
        assert can_parse is False or confidence < 0.4

    def test_parse_extracts_indicadores(self, sample_html_cepea):
        """Test parse extracts indicadores from HTML."""
        indicadores = self.parser.parse(sample_html_cepea, "soja")

        assert len(indicadores) == 2

        first = indicadores[0]
        assert first.data == date(2024, 2, 1)
        assert first.valor == Decimal("145.50")
        assert first.produto == "soja"
        assert first.unidade == "BRL/sc60kg"

    def test_parse_raises_on_empty_table(self, sample_html_empty):
        """Test parse raises ParseError for HTML without data."""
        with pytest.raises(ParseError) as exc_info:
            self.parser.parse(sample_html_empty, "soja")

        assert "No tables found" in str(exc_info.value)

    def test_parse_date_formats(self):
        """Test _parse_date handles various formats."""
        assert self.parser._parse_date("01/02/2024") == date(2024, 2, 1)
        assert self.parser._parse_date("01-02-2024") == date(2024, 2, 1)
        assert self.parser._parse_date("2024-02-01") == date(2024, 2, 1)
        assert self.parser._parse_date("invalid") is None

    def test_parse_decimal_formats(self):
        """Test _parse_decimal handles various formats."""
        assert self.parser._parse_decimal("145,50") == Decimal("145.50")
        assert self.parser._parse_decimal("1.234,56") == Decimal("1234.56")
        assert self.parser._parse_decimal("R$ 145,50") == Decimal("145.50")
        assert self.parser._parse_decimal("invalid") is None
        assert self.parser._parse_decimal("-10") is None

    def test_detect_unidade_soja(self):
        """Test unidade detection for soja."""
        unidade = self.parser._detect_unidade("soja", [])
        assert unidade == "BRL/sc60kg"

    def test_detect_unidade_boi(self):
        """Test unidade detection for boi."""
        unidade = self.parser._detect_unidade("boi", [])
        assert unidade == "BRL/@"

    def test_parser_metadata(self):
        """Test parser has correct metadata."""
        assert self.parser.version == 1
        assert self.parser.source == "cepea"
        assert self.parser.valid_from == date(2024, 1, 1)
        assert self.parser.valid_until is None
