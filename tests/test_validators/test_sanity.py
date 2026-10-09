"""Tests for sanity validators."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from agrobr.constants import Fonte
from agrobr.models import Indicador
from agrobr.validators.sanity import (
    validate_indicador,
)


class TestSanityValidation:
    def test_acceptable_daily_change(self):
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
