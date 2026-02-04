"""Health checks automatizados."""

from __future__ import annotations

from .checker import (
    CheckResult,
    CheckStatus,
    check_source,
    run_all_checks,
)
from .reporter import (
    HealthReport,
    generate_report,
)

__all__: list[str] = [
    "CheckResult",
    "CheckStatus",
    "check_source",
    "run_all_checks",
    "HealthReport",
    "generate_report",
]
