from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

import structlog

from agrobr.exceptions import ValidationError
from agrobr.models import Indicador

logger = structlog.get_logger()


@dataclass
class SanityRule:
    field: str
    min_value: Decimal | None
    max_value: Decimal | None
    max_daily_change_pct: Decimal | None = None
    description: str = ""
    expected_unit: str | None = None


PRICE_RULES: dict[str, SanityRule] = {
    "soja": SanityRule(
        field="valor",
        min_value=Decimal("30"),
        max_value=Decimal("300"),
        max_daily_change_pct=Decimal("15"),
        description="Soja (BRL/sc60kg)",
        expected_unit="BRL/sc60kg",
    ),
    "milho": SanityRule(
        field="valor",
        min_value=Decimal("15"),
        max_value=Decimal("150"),
        max_daily_change_pct=Decimal("15"),
        description="Milho (BRL/sc60kg)",
        expected_unit="BRL/sc60kg",
    ),
    "cafe": SanityRule(
        field="valor",
        min_value=Decimal("200"),
        max_value=Decimal("3000"),
        max_daily_change_pct=Decimal("10"),
        description="Café Arábica (BRL/sc60kg)",
        expected_unit="BRL/sc60kg",
    ),
    "cafe_robusta": SanityRule(
        field="valor",
        min_value=Decimal("100"),
        max_value=Decimal("3000"),
        max_daily_change_pct=Decimal("10"),
        description="Café Robusta/Conilon (BRL/sc60kg)",
        expected_unit="BRL/sc60kg",
    ),
    "bezerro": SanityRule(
        field="valor",
        min_value=Decimal("800"),
        max_value=Decimal("8000"),
        max_daily_change_pct=Decimal("10"),
        description="Bezerro MS (BRL/cabeca)",
        expected_unit="BRL/cabeca",
    ),
    "boi": SanityRule(
        field="valor",
        min_value=Decimal("100"),
        max_value=Decimal("500"),
        max_daily_change_pct=Decimal("10"),
        description="Boi Gordo (BRL/@)",
        expected_unit="BRL/@",
    ),
    "boi_gordo": SanityRule(
        field="valor",
        min_value=Decimal("100"),
        max_value=Decimal("500"),
        max_daily_change_pct=Decimal("10"),
        description="Boi Gordo (BRL/@)",
        expected_unit="BRL/@",
    ),
    "trigo": SanityRule(
        field="valor",
        min_value=Decimal("20") * Decimal("1000") / Decimal("60"),
        max_value=Decimal("150") * Decimal("1000") / Decimal("60"),
        max_daily_change_pct=Decimal("15"),
        description="Trigo (BRL/ton)",
        expected_unit="BRL/ton",
    ),
    "algodao": SanityRule(
        field="valor",
        min_value=Decimal("50") * Decimal("100") * Decimal("0.45359237") / Decimal("15"),
        max_value=Decimal("250") * Decimal("100") * Decimal("0.45359237") / Decimal("15"),
        max_daily_change_pct=Decimal("10"),
        description="Algodão (cBRL/lb)",
        expected_unit="cBRL/lb",
    ),
    "arroz": SanityRule(
        field="valor",
        min_value=Decimal("8"),
        max_value=Decimal("300"),
        description="Arroz em casca RS, 58% de grãos inteiros (BRL/sc50kg)",
        expected_unit="BRL/sc50kg",
    ),
    "acucar": SanityRule(
        field="valor",
        min_value=Decimal("8"),
        max_value=Decimal("400"),
        description="Açúcar cristal branco SP (BRL/sc50kg)",
        expected_unit="BRL/sc50kg",
    ),
    "acucar_refinado": SanityRule(
        field="valor",
        min_value=Decimal("0.2"),
        max_value=Decimal("8"),
        description="Açúcar refinado amorfo SP (BRL/kg)",
        expected_unit="BRL/kg",
    ),
    "frango_congelado": SanityRule(
        field="valor",
        min_value=Decimal("0.6"),
        max_value=Decimal("20"),
        description="Frango congelado SP (BRL/kg)",
        expected_unit="BRL/kg",
    ),
    "frango_resfriado": SanityRule(
        field="valor",
        min_value=Decimal("0.6"),
        max_value=Decimal("20"),
        description="Frango resfriado SP (BRL/kg)",
        expected_unit="BRL/kg",
    ),
    "suino": SanityRule(
        field="valor",
        min_value=Decimal("0.8"),
        max_value=Decimal("30"),
        description="Suíno vivo, praças MG/PR/RS/SC/SP (BRL/kg)",
        expected_unit="BRL/kg",
    ),
    "etanol_hidratado": SanityRule(
        field="valor",
        min_value=Decimal("0.1"),
        max_value=Decimal("8"),
        description="Etanol hidratado combustível SP, semanal (BRL/L)",
        expected_unit="BRL/L",
    ),
    "etanol_anidro": SanityRule(
        field="valor",
        min_value=Decimal("0.1"),
        max_value=Decimal("10"),
        description="Etanol anidro SP, semanal (BRL/L)",
        expected_unit="BRL/L",
    ),
    "leite": SanityRule(
        field="valor",
        min_value=Decimal("0.1"),
        max_value=Decimal("8"),
        description="Leite ao produtor, preço líquido mensal (BRL/L)",
        expected_unit="BRL/L",
    ),
    "laranja_industria": SanityRule(
        field="valor",
        min_value=Decimal("4"),
        max_value=Decimal("300"),
        description="Laranja indústria SP, a prazo, posta na fábrica (BRL/cx40.8kg)",
        expected_unit="BRL/cx40.8kg",
    ),
    "laranja_in_natura": SanityRule(
        field="valor",
        min_value=Decimal("4"),
        max_value=Decimal("300"),
        description="Laranja pera in natura SP, a prazo, na árvore (BRL/cx40.8kg)",
        expected_unit="BRL/cx40.8kg",
    ),
}
PRICE_RULES["soja_parana"] = replace(PRICE_RULES["soja"], description="Soja Paraná (BRL/sc60kg)")
PRICE_RULES["cafe_arabica"] = replace(PRICE_RULES["cafe"])


@dataclass
class AnomalyReport:
    field: str
    value: Any
    expected_range: str
    anomaly_type: str
    severity: str
    details: dict[str, Any]


def validate_indicador(
    indicador: Indicador,
    valor_anterior: Decimal | None = None,
) -> list[AnomalyReport]:
    anomalies: list[AnomalyReport] = []
    rule = PRICE_RULES.get(indicador.produto.lower())

    if not rule:
        logger.debug("sanity_no_rules", produto=indicador.produto)
        return anomalies

    if rule.expected_unit is not None and indicador.unidade != rule.expected_unit:
        logger.warning(
            "sanity_anomalies_detected",
            produto=indicador.produto,
            count=1,
            types=["unit_mismatch"],
        )
        return [
            AnomalyReport(
                field="unidade",
                value=indicador.unidade,
                expected_range=rule.expected_unit,
                anomaly_type="unit_mismatch",
                severity="critical",
                details={"produto": indicador.produto, "rule": rule.description},
            )
        ]

    if rule.min_value and indicador.valor < rule.min_value:
        anomalies.append(
            AnomalyReport(
                field="valor",
                value=indicador.valor,
                expected_range=f"[{rule.min_value}, {rule.max_value}]",
                anomaly_type="out_of_range",
                severity="critical",
                details={
                    "produto": indicador.produto,
                    "rule": rule.description,
                    "below_min_by": float(rule.min_value - indicador.valor),
                },
            )
        )

    if rule.max_value and indicador.valor > rule.max_value:
        anomalies.append(
            AnomalyReport(
                field="valor",
                value=indicador.valor,
                expected_range=f"[{rule.min_value}, {rule.max_value}]",
                anomaly_type="out_of_range",
                severity="critical",
                details={
                    "produto": indicador.produto,
                    "rule": rule.description,
                    "above_max_by": float(indicador.valor - rule.max_value),
                },
            )
        )

    if valor_anterior and rule.max_daily_change_pct:
        change_pct = abs((indicador.valor - valor_anterior) / valor_anterior) * 100

        if change_pct > rule.max_daily_change_pct:
            severity = "critical" if change_pct > rule.max_daily_change_pct * 2 else "warning"
            anomalies.append(
                AnomalyReport(
                    field="valor",
                    value=indicador.valor,
                    expected_range=f"±{rule.max_daily_change_pct}% do dia anterior",
                    anomaly_type="excessive_change",
                    severity=severity,
                    details={
                        "produto": indicador.produto,
                        "valor_anterior": float(valor_anterior),
                        "change_pct": float(change_pct),
                        "max_allowed_pct": float(rule.max_daily_change_pct),
                    },
                )
            )

    if anomalies:
        logger.warning(
            "sanity_anomalies_detected",
            produto=indicador.produto,
            count=len(anomalies),
            types=[a.anomaly_type for a in anomalies],
        )
    else:
        logger.debug("sanity_check_passed", produto=indicador.produto)

    return anomalies


async def validate_batch(
    indicadores: list[Indicador],
    strict: bool = False,
) -> tuple[list[Indicador], list[AnomalyReport]]:
    all_anomalies: list[AnomalyReport] = []

    sorted_indicadores = sorted(indicadores, key=lambda x: x.data)

    previous_values: dict[tuple[str, str | None, str], Decimal] = {}
    for ind in sorted_indicadores:
        series = (ind.produto, ind.praca, ind.unidade)
        anomalies = validate_indicador(ind, previous_values.get(series))
        previous_values[series] = ind.valor

        if anomalies:
            ind.anomalies = [f"{a.anomaly_type}: {a.field}" for a in anomalies]
            all_anomalies.extend(anomalies)

            if strict and any(a.severity == "critical" for a in anomalies):
                raise ValidationError(
                    source=ind.fonte.value,
                    field=anomalies[0].field,
                    value=anomalies[0].value,
                    reason=anomalies[0].anomaly_type,
                )

    return sorted_indicadores, all_anomalies
