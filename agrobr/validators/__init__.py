"""Validadores - sanity checks e validação estrutural."""

from __future__ import annotations

from .sanity import (
    AnomalyReport,
    SanityRule,
    validate_batch,
    validate_indicador,
)
from .structural import (
    StructuralValidationResult,
    compare_fingerprints,
    load_baseline,
    save_baseline,
    validate_against_baseline,
    validate_structure,
)

__all__: list[str] = [
    "AnomalyReport",
    "SanityRule",
    "validate_batch",
    "validate_indicador",
    "StructuralValidationResult",
    "validate_structure",
    "validate_against_baseline",
    "compare_fingerprints",
    "load_baseline",
    "save_baseline",
]
