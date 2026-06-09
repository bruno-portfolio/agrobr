from __future__ import annotations

from .checker import (
    CheckResult,
    CheckStatus,
    check_source,
    run_all_checks,
    run_checks_with_state,
)
from .doctor import (
    CacheStats,
    DiagnosticsResult,
    SourceStatus,
    run_diagnostics,
)
from .registry import (
    HEALTH_REGISTRY,
    SOURCE_DATASET_MAP,
    SourceHealthConfig,
    get_affected_datasets,
)
from .reporter import (
    HealthReport,
    generate_report,
)
from .state import (
    get_consecutive_failures,
    get_last_success,
    record_check,
    should_send_alert,
)

__all__: list[str] = [
    "CheckResult",
    "CheckStatus",
    "check_source",
    "run_all_checks",
    "run_checks_with_state",
    "HealthReport",
    "generate_report",
    "DiagnosticsResult",
    "SourceStatus",
    "CacheStats",
    "run_diagnostics",
    "HEALTH_REGISTRY",
    "SOURCE_DATASET_MAP",
    "SourceHealthConfig",
    "get_affected_datasets",
    "record_check",
    "get_consecutive_failures",
    "get_last_success",
    "should_send_alert",
]
