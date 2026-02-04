"""Validacao semantica avancada para dados agricolas."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import structlog

logger = structlog.get_logger()


@dataclass
class ValidationResult:
    """Resultado de uma validacao."""

    rule_name: str
    severity: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class SemanticRule(ABC):
    """Regra de validacao semantica base."""

    name: str
    description: str
    severity: str = "warning"

    @abstractmethod
    def check(self, df: pd.DataFrame, **kwargs: Any) -> list[ValidationResult]:
        """Executa verificacao. Retorna lista de resultados."""
        pass


@dataclass
class PricePositiveRule(SemanticRule):
    """Preco deve ser positivo."""

    name: str = "price_positive"
    description: str = "Prices must be positive"
    severity: str = "error"

    def check(self, df: pd.DataFrame, **_kwargs: Any) -> list[ValidationResult]:
        results: list[ValidationResult] = []

        if "valor" not in df.columns:
            return results

        negative_mask = df["valor"] <= 0
        if negative_mask.any():
            negative_rows = df[negative_mask]
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=False,
                    message=f"Found {len(negative_rows)} rows with non-positive prices",
                    details={
                        "count": len(negative_rows),
                        "sample": negative_rows.head(5).to_dict("records"),
                    },
                )
            )
        else:
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=True,
                    message="All prices are positive",
                )
            )

        return results


@dataclass
class ProductivityRangeRule(SemanticRule):
    """Produtividade deve estar em faixa razoavel."""

    name: str = "productivity_range"
    description: str = "Productivity must be within historical range"
    severity: str = "warning"
    ranges: dict[str, tuple[float, float]] = field(
        default_factory=lambda: {
            "soja": (1500, 5000),
            "milho": (2000, 12000),
            "arroz": (3000, 9000),
            "trigo": (1500, 5000),
            "feijao": (500, 3000),
            "algodao": (1000, 2500),
            "cafe": (1000, 4000),
        }
    )

    def check(self, df: pd.DataFrame, **_kwargs: Any) -> list[ValidationResult]:
        results: list[ValidationResult] = []

        if "produtividade" not in df.columns or "produto" not in df.columns:
            return results

        violations = []
        for _, row in df.iterrows():
            produto = str(row.get("produto", "")).lower()
            produtividade = row.get("produtividade")

            if pd.isna(produtividade) or produto not in self.ranges:
                continue

            min_val, max_val = self.ranges[produto]
            if produtividade < min_val or produtividade > max_val:
                violations.append(
                    {
                        "produto": produto,
                        "produtividade": produtividade,
                        "range": f"[{min_val}, {max_val}]",
                    }
                )

        if violations:
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=False,
                    message=f"Found {len(violations)} rows with productivity outside range",
                    details={"violations": violations[:10]},
                )
            )
        else:
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=True,
                    message="All productivity values within expected range",
                )
            )

        return results


@dataclass
class DailyVariationRule(SemanticRule):
    """Variacao diaria nao deve ser extrema."""

    name: str = "daily_variation"
    description: str = "Daily variation should not exceed threshold"
    severity: str = "warning"
    max_variation_pct: float = 20.0

    def check(self, df: pd.DataFrame, **_kwargs: Any) -> list[ValidationResult]:
        results: list[ValidationResult] = []

        if "valor" not in df.columns or "data" not in df.columns:
            return results

        df_sorted = df.sort_values("data")
        df_sorted["variacao_calc"] = df_sorted["valor"].pct_change() * 100

        extreme_mask = df_sorted["variacao_calc"].abs() > self.max_variation_pct
        if extreme_mask.any():
            extreme_rows = df_sorted[extreme_mask]
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=False,
                    message=f"Found {len(extreme_rows)} rows with extreme daily variation (>{self.max_variation_pct}%)",
                    details={
                        "count": len(extreme_rows),
                        "max_variation": float(df_sorted["variacao_calc"].abs().max()),
                        "sample": extreme_rows[["data", "valor", "variacao_calc"]]
                        .head(5)
                        .to_dict("records"),
                    },
                )
            )
        else:
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=True,
                    message=f"All daily variations within {self.max_variation_pct}% threshold",
                )
            )

        return results


@dataclass
class DateSequenceRule(SemanticRule):
    """Datas devem estar em sequencia logica."""

    name: str = "date_sequence"
    description: str = "Dates should be in logical sequence"
    severity: str = "warning"

    def check(self, df: pd.DataFrame, **_kwargs: Any) -> list[ValidationResult]:
        results: list[ValidationResult] = []

        if "data" not in df.columns:
            return results

        df_sorted = df.sort_values("data")

        if len(df_sorted) < 2:
            return results

        dates = pd.to_datetime(df_sorted["data"])
        gaps = dates.diff().dt.days

        large_gaps = gaps[gaps > 10].dropna()
        if len(large_gaps) > 0:
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=False,
                    message=f"Found {len(large_gaps)} gaps > 10 days in date sequence",
                    details={
                        "max_gap_days": int(gaps.max()),
                        "gap_count": len(large_gaps),
                    },
                )
            )
        else:
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=True,
                    message="Date sequence is continuous",
                )
            )

        return results


@dataclass
class AreaConsistencyRule(SemanticRule):
    """Area colhida nao pode ser maior que area plantada."""

    name: str = "area_consistency"
    description: str = "Harvested area cannot exceed planted area"
    severity: str = "error"

    def check(self, df: pd.DataFrame, **_kwargs: Any) -> list[ValidationResult]:
        results: list[ValidationResult] = []

        if "area_plantada" not in df.columns or "area_colhida" not in df.columns:
            return results

        mask = df["area_colhida"] > df["area_plantada"]
        mask = mask & df["area_plantada"].notna() & df["area_colhida"].notna()

        if mask.any():
            violations = df[mask]
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=False,
                    message=f"Found {len(violations)} rows where harvested area > planted area",
                    details={
                        "count": len(violations),
                        "sample": violations[["area_plantada", "area_colhida"]]
                        .head(5)
                        .to_dict("records"),
                    },
                )
            )
        else:
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=True,
                    message="Area consistency validated",
                )
            )

        return results


@dataclass
class SafraFormatRule(SemanticRule):
    """Safra deve estar no formato correto YYYY/YY."""

    name: str = "safra_format"
    description: str = "Safra must match format YYYY/YY"
    severity: str = "error"

    def check(self, df: pd.DataFrame, **_kwargs: Any) -> list[ValidationResult]:
        import re

        results: list[ValidationResult] = []

        if "safra" not in df.columns:
            return results

        pattern = re.compile(r"^\d{4}/\d{2}$")
        invalid = df[~df["safra"].astype(str).str.match(pattern)]

        if len(invalid) > 0:
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=False,
                    message=f"Found {len(invalid)} rows with invalid safra format",
                    details={
                        "count": len(invalid),
                        "invalid_values": invalid["safra"].unique().tolist()[:10],
                    },
                )
            )
        else:
            results.append(
                ValidationResult(
                    rule_name=self.name,
                    severity=self.severity,
                    passed=True,
                    message="All safra values match expected format",
                )
            )

        return results


DEFAULT_RULES: list[SemanticRule] = [
    PricePositiveRule(),
    ProductivityRangeRule(),
    DailyVariationRule(),
    DateSequenceRule(),
    AreaConsistencyRule(),
    SafraFormatRule(),
]


def validate_semantic(
    df: pd.DataFrame,
    rules: list[SemanticRule] | None = None,
    fail_on_error: bool = False,
) -> tuple[bool, list[ValidationResult]]:
    """
    Valida DataFrame semanticamente.

    Args:
        df: DataFrame a validar
        rules: Regras a aplicar (usa padrao se None)
        fail_on_error: Se True, levanta excecao em erros

    Returns:
        Tupla (valido, lista de resultados)
    """
    if rules is None:
        rules = DEFAULT_RULES

    all_results: list[ValidationResult] = []

    for rule in rules:
        try:
            results = rule.check(df)
            all_results.extend(results)

            for r in results:
                if not r.passed:
                    if r.severity == "error":
                        logger.error(
                            "semantic_validation",
                            rule=r.rule_name,
                            message=r.message,
                        )
                    else:
                        logger.warning(
                            "semantic_validation",
                            rule=r.rule_name,
                            message=r.message,
                        )
        except Exception as e:
            logger.error("semantic_rule_error", rule=rule.name, error=str(e))
            all_results.append(
                ValidationResult(
                    rule_name=rule.name,
                    severity="error",
                    passed=False,
                    message=f"Rule execution failed: {e}",
                )
            )

    errors = [r for r in all_results if r.severity == "error" and not r.passed]

    if errors and fail_on_error:
        from agrobr.exceptions import ValidationError

        raise ValidationError(
            source="semantic",
            field="multiple",
            value=None,
            reason=f"{len(errors)} semantic errors found: {[r.message for r in errors]}",
        )

    all_passed = all(r.passed for r in all_results)
    return all_passed, all_results


def get_validation_summary(results: list[ValidationResult]) -> dict[str, Any]:
    """Gera resumo das validacoes."""
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    errors = [r for r in results if r.severity == "error" and not r.passed]
    warnings = [r for r in results if r.severity == "warning" and not r.passed]

    return {
        "total_rules": total,
        "passed": passed,
        "failed": failed,
        "errors": len(errors),
        "warnings": len(warnings),
        "success_rate": passed / total if total > 0 else 1.0,
        "error_messages": [r.message for r in errors],
        "warning_messages": [r.message for r in warnings],
    }


__all__ = [
    "SemanticRule",
    "ValidationResult",
    "PricePositiveRule",
    "ProductivityRangeRule",
    "DailyVariationRule",
    "DateSequenceRule",
    "AreaConsistencyRule",
    "SafraFormatRule",
    "DEFAULT_RULES",
    "validate_semantic",
    "get_validation_summary",
]
