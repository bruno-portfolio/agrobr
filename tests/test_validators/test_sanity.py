"""Tests for sanity validators."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from agrobr.constants import Fonte
from agrobr.models import Indicador, Safra
from agrobr.validators.sanity import (
    PRICE_RULES,
    validate_batch,
    validate_indicador,
    validate_safra,
)


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


class TestValidateSafra:
    def test_valid_soja_safra(self):
        safra = Safra(
            fonte=Fonte.CONAB,
            produto="soja",
            safra="2024/25",
            area_plantada=Decimal("35000"),
            producao=Decimal("120000"),
            produtividade=Decimal("3500"),
            unidade_area="mil_ha",
            unidade_producao="mil_ton",
            levantamento=1,
            data_publicacao=date(2024, 1, 1),
        )
        anomalies = validate_safra(safra)
        assert len(anomalies) == 0

    def test_area_below_min(self):
        safra = Safra(
            fonte=Fonte.CONAB,
            produto="soja",
            safra="2024/25",
            area_plantada=Decimal("100"),
            producao=Decimal("120000"),
            produtividade=Decimal("3500"),
            unidade_area="mil_ha",
            unidade_producao="mil_ton",
            levantamento=1,
            data_publicacao=date(2024, 1, 1),
        )
        anomalies = validate_safra(safra)
        assert len(anomalies) >= 1
        assert any(a.anomaly_type == "out_of_range" for a in anomalies)

    def test_producao_above_max(self):
        safra = Safra(
            fonte=Fonte.CONAB,
            produto="soja",
            safra="2024/25",
            area_plantada=Decimal("35000"),
            producao=Decimal("999999"),
            produtividade=Decimal("3500"),
            unidade_area="mil_ha",
            unidade_producao="mil_ton",
            levantamento=1,
            data_publicacao=date(2024, 1, 1),
        )
        anomalies = validate_safra(safra)
        assert len(anomalies) >= 1

    def test_none_values_skipped(self):
        safra = Safra(
            fonte=Fonte.CONAB,
            produto="soja",
            safra="2024/25",
            area_plantada=None,
            producao=None,
            produtividade=None,
            unidade_area="mil_ha",
            unidade_producao="mil_ton",
            levantamento=1,
            data_publicacao=date(2024, 1, 1),
        )
        anomalies = validate_safra(safra)
        assert len(anomalies) == 0

    def test_unknown_product_no_rules(self):
        safra = Safra(
            fonte=Fonte.CONAB,
            produto="quinoa",
            safra="2024/25",
            area_plantada=Decimal("1"),
            producao=Decimal("1"),
            unidade_area="mil_ha",
            unidade_producao="mil_ton",
            levantamento=1,
            data_publicacao=date(2024, 1, 1),
        )
        anomalies = validate_safra(safra)
        assert len(anomalies) == 0


class TestValidateBatch:
    @pytest.mark.asyncio
    async def test_valid_batch(self):
        indicadores = [
            Indicador(
                fonte=Fonte.CEPEA,
                produto="soja",
                data=date(2024, 1, i + 1),
                valor=Decimal("145.00") + i,
                unidade="BRL/sc60kg",
            )
            for i in range(3)
        ]
        sorted_inds, anomalies = await validate_batch(indicadores)
        assert len(sorted_inds) == 3
        assert len(anomalies) == 0

    @pytest.mark.asyncio
    async def test_batch_with_anomaly(self):
        indicadores = [
            Indicador(
                fonte=Fonte.CEPEA,
                produto="soja",
                data=date(2024, 1, 1),
                valor=Decimal("145.00"),
                unidade="BRL/sc60kg",
            ),
            Indicador(
                fonte=Fonte.CEPEA,
                produto="soja",
                data=date(2024, 1, 2),
                valor=Decimal("5.00"),
                unidade="BRL/sc60kg",
            ),
        ]
        sorted_inds, anomalies = await validate_batch(indicadores)
        assert len(anomalies) >= 1

    @pytest.mark.asyncio
    async def test_strict_raises_on_critical(self):
        from agrobr.exceptions import ValidationError

        indicadores = [
            Indicador(
                fonte=Fonte.CEPEA,
                produto="soja",
                data=date(2024, 1, 1),
                valor=Decimal("5.00"),
                unidade="BRL/sc60kg",
            ),
        ]
        with pytest.raises(ValidationError):
            await validate_batch(indicadores, strict=True)

    @pytest.mark.asyncio
    async def test_sorted_by_date(self):
        indicadores = [
            Indicador(
                fonte=Fonte.CEPEA,
                produto="soja",
                data=date(2024, 1, 3),
                valor=Decimal("145.00"),
                unidade="BRL/sc60kg",
            ),
            Indicador(
                fonte=Fonte.CEPEA,
                produto="soja",
                data=date(2024, 1, 1),
                valor=Decimal("143.00"),
                unidade="BRL/sc60kg",
            ),
        ]
        sorted_inds, _ = await validate_batch(indicadores)
        assert sorted_inds[0].data < sorted_inds[1].data
