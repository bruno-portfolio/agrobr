"""Validação estatística de sanidade para dados agrícolas."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import structlog

from agrobr.exceptions import ValidationError
from agrobr.models import Indicador, Safra

logger = structlog.get_logger()


@dataclass
class SanityRule:
    """Regra de validação estatística."""

    field: str
    min_value: Decimal | None
    max_value: Decimal | None
    max_daily_change_pct: Decimal | None = None
    description: str = ""


PRICE_RULES: dict[str, SanityRule] = {
    "soja": SanityRule(
        field="valor",
        min_value=Decimal("30"),
        max_value=Decimal("300"),
        max_daily_change_pct=Decimal("15"),
        description="Soja (BRL/sc60kg)",
    ),
    "milho": SanityRule(
        field="valor",
        min_value=Decimal("15"),
        max_value=Decimal("150"),
        max_daily_change_pct=Decimal("15"),
        description="Milho (BRL/sc60kg)",
    ),
    "cafe": SanityRule(
        field="valor",
        min_value=Decimal("200"),
        max_value=Decimal("3000"),
        max_daily_change_pct=Decimal("10"),
        description="Café Arábica (BRL/sc60kg)",
    ),
    "boi": SanityRule(
        field="valor",
        min_value=Decimal("100"),
        max_value=Decimal("500"),
        max_daily_change_pct=Decimal("10"),
        description="Boi Gordo (BRL/@)",
    ),
    "boi_gordo": SanityRule(
        field="valor",
        min_value=Decimal("100"),
        max_value=Decimal("500"),
        max_daily_change_pct=Decimal("10"),
        description="Boi Gordo (BRL/@)",
    ),
    "trigo": SanityRule(
        field="valor",
        min_value=Decimal("20"),
        max_value=Decimal("150"),
        max_daily_change_pct=Decimal("15"),
        description="Trigo (BRL/sc60kg)",
    ),
    "algodao": SanityRule(
        field="valor",
        min_value=Decimal("50"),
        max_value=Decimal("250"),
        max_daily_change_pct=Decimal("10"),
        description="Algodão (BRL/@)",
    ),
}

SAFRA_RULES: dict[str, dict[str, SanityRule]] = {
    "soja": {
        "area_plantada": SanityRule(
            field="area_plantada",
            min_value=Decimal("20000"),
            max_value=Decimal("50000"),
            description="Área plantada soja Brasil (mil ha)",
        ),
        "producao": SanityRule(
            field="producao",
            min_value=Decimal("50000"),
            max_value=Decimal("200000"),
            description="Produção soja Brasil (mil ton)",
        ),
    },
    "milho": {
        "area_plantada": SanityRule(
            field="area_plantada",
            min_value=Decimal("10000"),
            max_value=Decimal("30000"),
            description="Área plantada milho Brasil (mil ha)",
        ),
        "producao": SanityRule(
            field="producao",
            min_value=Decimal("50000"),
            max_value=Decimal("150000"),
            description="Produção milho Brasil (mil ton)",
        ),
    },
}


@dataclass
class AnomalyReport:
    """Relatório de anomalia detectada."""

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
    """
    Valida indicador contra regras estatísticas.

    Args:
        indicador: Indicador a validar
        valor_anterior: Valor do dia anterior (para validar variação)

    Returns:
        Lista de anomalias detectadas (vazia se OK)
    """
    anomalies: list[AnomalyReport] = []
    rule = PRICE_RULES.get(indicador.produto.lower())

    if not rule:
        logger.debug("sanity_no_rules", produto=indicador.produto)
        return anomalies

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
            severity = (
                "critical" if change_pct > rule.max_daily_change_pct * 2 else "warning"
            )
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


def validate_safra(safra: Safra) -> list[AnomalyReport]:
    """Valida dados de safra contra regras estatísticas."""
    anomalies: list[AnomalyReport] = []
    rules = SAFRA_RULES.get(safra.produto.lower(), {})

    for field_name, rule in rules.items():
        value = getattr(safra, field_name)

        if value is None:
            continue

        if rule.min_value and value < rule.min_value:
            anomalies.append(
                AnomalyReport(
                    field=field_name,
                    value=value,
                    expected_range=f"[{rule.min_value}, {rule.max_value}]",
                    anomaly_type="out_of_range",
                    severity="critical",
                    details={"rule": rule.description},
                )
            )

        if rule.max_value and value > rule.max_value:
            anomalies.append(
                AnomalyReport(
                    field=field_name,
                    value=value,
                    expected_range=f"[{rule.min_value}, {rule.max_value}]",
                    anomaly_type="out_of_range",
                    severity="critical",
                    details={"rule": rule.description},
                )
            )

    return anomalies


async def validate_batch(
    indicadores: list[Indicador],
    strict: bool = False,
) -> tuple[list[Indicador], list[AnomalyReport]]:
    """
    Valida batch de indicadores.

    Args:
        indicadores: Lista de indicadores a validar
        strict: Se True, levanta exceção em anomalias críticas

    Returns:
        tuple: (indicadores com anomalies preenchidas, todas as anomalias)
    """
    all_anomalies: list[AnomalyReport] = []

    sorted_indicadores = sorted(indicadores, key=lambda x: x.data)

    for i, ind in enumerate(sorted_indicadores):
        valor_anterior = None
        if i > 0 and sorted_indicadores[i - 1].produto == ind.produto:
            valor_anterior = sorted_indicadores[i - 1].valor

        anomalies = validate_indicador(ind, valor_anterior)

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
