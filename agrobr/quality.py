"""Certificacao de qualidade de dados."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    import pandas as pd

logger = structlog.get_logger()


class QualityLevel(StrEnum):
    GOLD = "gold"
    SILVER = "silver"
    BRONZE = "bronze"
    UNCERTIFIED = "uncertified"


class CheckStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    WARNING = "warning"


@dataclass
class QualityCheck:
    name: str
    status: CheckStatus
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityCertificate:
    level: QualityLevel
    checks: list[QualityCheck]
    issued_at: datetime
    valid_until: datetime | None = None
    source: str = ""
    dataset: str = ""
    row_count: int = 0
    column_count: int = 0
    score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "level": self.level.value,
            "score": round(self.score, 2),
            "issued_at": self.issued_at.isoformat(),
            "valid_until": self.valid_until.isoformat() if self.valid_until else None,
            "source": self.source,
            "dataset": self.dataset,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "checks": [
                {
                    "name": c.name,
                    "status": c.status.value,
                    "message": c.message,
                }
                for c in self.checks
            ],
            "summary": {
                "passed": sum(1 for c in self.checks if c.status == CheckStatus.PASSED),
                "failed": sum(1 for c in self.checks if c.status == CheckStatus.FAILED),
                "warnings": sum(1 for c in self.checks if c.status == CheckStatus.WARNING),
                "skipped": sum(1 for c in self.checks if c.status == CheckStatus.SKIPPED),
            },
        }

    def is_valid(self) -> bool:
        if self.valid_until is None:
            return True
        return datetime.now() < self.valid_until


def _check_completeness(df: pd.DataFrame, threshold: float = 0.95) -> QualityCheck:
    total_cells = df.size
    non_null_cells = df.count().sum()
    completeness = non_null_cells / total_cells if total_cells > 0 else 0

    if completeness >= threshold:
        return QualityCheck(
            name="completeness",
            status=CheckStatus.PASSED,
            message=f"Completeness: {completeness:.1%}",
            details={"completeness": completeness, "threshold": threshold},
        )
    elif completeness >= threshold * 0.9:
        return QualityCheck(
            name="completeness",
            status=CheckStatus.WARNING,
            message=f"Completeness below threshold: {completeness:.1%}",
            details={"completeness": completeness, "threshold": threshold},
        )
    else:
        return QualityCheck(
            name="completeness",
            status=CheckStatus.FAILED,
            message=f"Low completeness: {completeness:.1%}",
            details={"completeness": completeness, "threshold": threshold},
        )


def _check_duplicates(df: pd.DataFrame, max_dup_pct: float = 0.01) -> QualityCheck:
    dup_count = df.duplicated().sum()
    dup_pct = dup_count / len(df) if len(df) > 0 else 0

    if dup_pct <= max_dup_pct:
        return QualityCheck(
            name="duplicates",
            status=CheckStatus.PASSED,
            message=f"Duplicates: {dup_count} ({dup_pct:.1%})",
            details={"duplicate_count": int(dup_count), "duplicate_pct": dup_pct},
        )
    else:
        return QualityCheck(
            name="duplicates",
            status=CheckStatus.FAILED,
            message=f"Too many duplicates: {dup_count} ({dup_pct:.1%})",
            details={"duplicate_count": int(dup_count), "duplicate_pct": dup_pct},
        )


def _check_schema(df: pd.DataFrame, expected_columns: list[str] | None = None) -> QualityCheck:
    if expected_columns is None:
        return QualityCheck(
            name="schema",
            status=CheckStatus.SKIPPED,
            message="No expected schema provided",
        )

    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    missing = expected_set - actual_columns
    extra = actual_columns - expected_set

    if not missing and not extra:
        return QualityCheck(
            name="schema",
            status=CheckStatus.PASSED,
            message="Schema matches expected columns",
            details={"columns": list(actual_columns)},
        )
    elif not missing:
        return QualityCheck(
            name="schema",
            status=CheckStatus.WARNING,
            message=f"Extra columns found: {extra}",
            details={"missing": list(missing), "extra": list(extra)},
        )
    else:
        return QualityCheck(
            name="schema",
            status=CheckStatus.FAILED,
            message=f"Missing columns: {missing}",
            details={"missing": list(missing), "extra": list(extra)},
        )


def _check_freshness(
    df: pd.DataFrame,
    date_column: str = "data",
    max_age_days: int = 7,
) -> QualityCheck:
    if date_column not in df.columns:
        return QualityCheck(
            name="freshness",
            status=CheckStatus.SKIPPED,
            message=f"Date column '{date_column}' not found",
        )

    import pandas

    df[date_column] = pandas.to_datetime(df[date_column])
    max_date = df[date_column].max()
    age_days = (datetime.now() - max_date).days

    if age_days <= max_age_days:
        return QualityCheck(
            name="freshness",
            status=CheckStatus.PASSED,
            message=f"Data age: {age_days} days",
            details={"max_date": max_date.isoformat(), "age_days": age_days},
        )
    elif age_days <= max_age_days * 2:
        return QualityCheck(
            name="freshness",
            status=CheckStatus.WARNING,
            message=f"Data slightly stale: {age_days} days",
            details={"max_date": max_date.isoformat(), "age_days": age_days},
        )
    else:
        return QualityCheck(
            name="freshness",
            status=CheckStatus.FAILED,
            message=f"Data too old: {age_days} days",
            details={"max_date": max_date.isoformat(), "age_days": age_days},
        )


def _check_value_ranges(
    df: pd.DataFrame,
    column: str,
    min_val: float | None = None,
    max_val: float | None = None,
) -> QualityCheck:
    if column not in df.columns:
        return QualityCheck(
            name=f"range_{column}",
            status=CheckStatus.SKIPPED,
            message=f"Column '{column}' not found",
        )

    values = df[column].dropna()
    if len(values) == 0:
        return QualityCheck(
            name=f"range_{column}",
            status=CheckStatus.WARNING,
            message=f"Column '{column}' is empty",
        )

    actual_min = values.min()
    actual_max = values.max()
    violations = 0

    if min_val is not None:
        violations += (values < min_val).sum()
    if max_val is not None:
        violations += (values > max_val).sum()

    if violations == 0:
        return QualityCheck(
            name=f"range_{column}",
            status=CheckStatus.PASSED,
            message=f"All values in range [{min_val}, {max_val}]",
            details={"min": float(actual_min), "max": float(actual_max)},
        )
    else:
        return QualityCheck(
            name=f"range_{column}",
            status=CheckStatus.FAILED,
            message=f"{violations} values out of range",
            details={
                "min": float(actual_min),
                "max": float(actual_max),
                "violations": int(violations),
            },
        )


def certify(
    df: pd.DataFrame,
    source: str = "",
    dataset: str = "",
    expected_columns: list[str] | None = None,
    date_column: str = "data",
    value_column: str = "valor",
    min_value: float | None = 0,
    max_value: float | None = None,
) -> QualityCertificate:
    checks = []

    checks.append(_check_completeness(df))
    checks.append(_check_duplicates(df))
    checks.append(_check_schema(df, expected_columns))
    checks.append(_check_freshness(df, date_column))

    if value_column in df.columns:
        checks.append(_check_value_ranges(df, value_column, min_value, max_value))

    passed = sum(1 for c in checks if c.status == CheckStatus.PASSED)
    failed = sum(1 for c in checks if c.status == CheckStatus.FAILED)
    warnings = sum(1 for c in checks if c.status == CheckStatus.WARNING)
    total = passed + failed + warnings

    score = (passed + warnings * 0.5) / total if total > 0 else 0

    if score >= 0.9 and failed == 0:
        level = QualityLevel.GOLD
    elif score >= 0.7 and failed <= 1:
        level = QualityLevel.SILVER
    elif score >= 0.5:
        level = QualityLevel.BRONZE
    else:
        level = QualityLevel.UNCERTIFIED

    return QualityCertificate(
        level=level,
        checks=checks,
        issued_at=datetime.now(),
        source=source,
        dataset=dataset,
        row_count=len(df),
        column_count=len(df.columns),
        score=score,
    )


def quick_check(df: pd.DataFrame) -> tuple[QualityLevel, float]:
    cert = certify(df)
    return cert.level, cert.score


__all__ = [
    "QualityLevel",
    "CheckStatus",
    "QualityCheck",
    "QualityCertificate",
    "certify",
    "quick_check",
]
