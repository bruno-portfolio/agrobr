"""Tests for sanity validators."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from agrobr.constants import Fonte
from agrobr.models import Indicador
from agrobr.validators.sanity import PRICE_RULES, validate_indicador


class TestSanityValidation:
    """Tests for sanity validation of indicadores."""

    def test_valid_soja_indicador(self):
        """Test validation of valid soja indicador."""
        indicador = Indicador(
            fonte=Fonte.CEPEA,
            produto="soja",
            praca=None,
            data=date(2024, 2, 1),
            valor=Decimal("145.50"),
            unidade="BRL/sc60kg",
        )

        anomalies = validate_indicador(indicador)

        assert len(anomalies) == 0

    def test_soja_value_below_min(self):
        """Test detection of soja value below minimum."""
        indicador = Indicador(
            fonte=Fonte.CEPEA,
            produto="soja",
            praca=None,
            data=date(2024, 2, 1),
            valor=Decimal("10.00"),
            unidade="BRL/sc60kg",
        )

        anomalies = validate_indicador(indicador)

        assert len(anomalies) == 1
        assert anomalies[0].anomaly_type == "out_of_range"
        assert anomalies[0].severity == "critical"

    def test_soja_value_above_max(self):
        """Test detection of soja value above maximum."""
        indicador = Indicador(
            fonte=Fonte.CEPEA,
            produto="soja",
            praca=None,
            data=date(2024, 2, 1),
            valor=Decimal("500.00"),
            unidade="BRL/sc60kg",
        )

        anomalies = validate_indicador(indicador)

        assert len(anomalies) == 1
        assert anomalies[0].anomaly_type == "out_of_range"

    def test_excessive_daily_change(self):
        """Test detection of excessive daily price change."""
        indicador = Indicador(
            fonte=Fonte.CEPEA,
            produto="soja",
            praca=None,
            data=date(2024, 2, 1),
            valor=Decimal("150.00"),
            unidade="BRL/sc60kg",
        )

        valor_anterior = Decimal("100.00")
        anomalies = validate_indicador(indicador, valor_anterior)

        assert len(anomalies) == 1
        assert anomalies[0].anomaly_type == "excessive_change"

    def test_acceptable_daily_change(self):
        """Test that acceptable daily change passes validation."""
        indicador = Indicador(
            fonte=Fonte.CEPEA,
            produto="soja",
            praca=None,
            data=date(2024, 2, 1),
            valor=Decimal("148.00"),
            unidade="BRL/sc60kg",
        )

        valor_anterior = Decimal("145.00")
        anomalies = validate_indicador(indicador, valor_anterior)

        assert len(anomalies) == 0

    def test_unknown_product_no_rules(self):
        """Test that unknown products return no anomalies."""
        indicador = Indicador(
            fonte=Fonte.CEPEA,
            produto="unknown_product",
            praca=None,
            data=date(2024, 2, 1),
            valor=Decimal("1.00"),
            unidade="BRL/unit",
        )

        anomalies = validate_indicador(indicador)

        assert len(anomalies) == 0

    def test_price_rules_exist_for_main_products(self):
        """Test that price rules exist for main products."""
        assert "soja" in PRICE_RULES
        assert "milho" in PRICE_RULES
        assert "cafe" in PRICE_RULES
        assert "boi" in PRICE_RULES
