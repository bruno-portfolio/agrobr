"""Service Level Agreement definitions per source."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import time
from enum import StrEnum
from typing import Any

from agrobr.constants import Fonte


class Tier(StrEnum):
    CRITICAL = "critical"
    STANDARD = "standard"
    BEST_EFFORT = "best_effort"


@dataclass
class FreshnessPolicy:
    update_frequency: str
    update_time: time | None = None
    timezone: str = "America/Sao_Paulo"
    weekends: bool = False
    holidays: bool = False


@dataclass
class LatencyTarget:
    p50_ms: int
    p95_ms: int
    p99_ms: int
    timeout_ms: int


@dataclass
class AvailabilityTarget:
    uptime_pct: float
    planned_maintenance_window: str | None = None
    degraded_mode_available: bool = True


@dataclass
class DataQualityTarget:
    completeness_pct: float = 99.0
    accuracy_checks: bool = True
    schema_validation: bool = True
    anomaly_detection: bool = True


@dataclass
class SourceSLA:
    source: Fonte
    tier: Tier
    freshness: FreshnessPolicy
    latency: LatencyTarget
    availability: AvailabilityTarget
    data_quality: DataQualityTarget
    fallback_sources: list[Fonte] = field(default_factory=list)
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source.value,
            "tier": self.tier.value,
            "freshness": {
                "update_frequency": self.freshness.update_frequency,
                "update_time": self.freshness.update_time.isoformat() if self.freshness.update_time else None,
                "timezone": self.freshness.timezone,
                "weekends": self.freshness.weekends,
                "holidays": self.freshness.holidays,
            },
            "latency": {
                "p50_ms": self.latency.p50_ms,
                "p95_ms": self.latency.p95_ms,
                "p99_ms": self.latency.p99_ms,
                "timeout_ms": self.latency.timeout_ms,
            },
            "availability": {
                "uptime_pct": self.availability.uptime_pct,
                "planned_maintenance_window": self.availability.planned_maintenance_window,
                "degraded_mode_available": self.availability.degraded_mode_available,
            },
            "data_quality": {
                "completeness_pct": self.data_quality.completeness_pct,
                "accuracy_checks": self.data_quality.accuracy_checks,
                "schema_validation": self.data_quality.schema_validation,
                "anomaly_detection": self.data_quality.anomaly_detection,
            },
            "fallback_sources": [f.value for f in self.fallback_sources],
            "notes": self.notes,
        }


CEPEA_SLA = SourceSLA(
    source=Fonte.CEPEA,
    tier=Tier.CRITICAL,
    freshness=FreshnessPolicy(
        update_frequency="daily",
        update_time=time(18, 0),
        weekends=False,
        holidays=False,
    ),
    latency=LatencyTarget(
        p50_ms=500,
        p95_ms=2000,
        p99_ms=5000,
        timeout_ms=30000,
    ),
    availability=AvailabilityTarget(
        uptime_pct=99.0,
        degraded_mode_available=True,
    ),
    data_quality=DataQualityTarget(
        completeness_pct=99.0,
        accuracy_checks=True,
        schema_validation=True,
        anomaly_detection=True,
    ),
    fallback_sources=[Fonte.NOTICIAS_AGRICOLAS],
    notes="CEPEA publica indicadores diarios as 18h. Cache expira as 18h do dia seguinte.",
)

CONAB_SLA = SourceSLA(
    source=Fonte.CONAB,
    tier=Tier.STANDARD,
    freshness=FreshnessPolicy(
        update_frequency="monthly",
        weekends=False,
        holidays=False,
    ),
    latency=LatencyTarget(
        p50_ms=1000,
        p95_ms=3000,
        p99_ms=10000,
        timeout_ms=60000,
    ),
    availability=AvailabilityTarget(
        uptime_pct=95.0,
        degraded_mode_available=True,
    ),
    data_quality=DataQualityTarget(
        completeness_pct=95.0,
        accuracy_checks=True,
        schema_validation=True,
        anomaly_detection=False,
    ),
    notes="CONAB publica boletins mensais. Dados de safra atualizados mensalmente.",
)

IBGE_SLA = SourceSLA(
    source=Fonte.IBGE,
    tier=Tier.STANDARD,
    freshness=FreshnessPolicy(
        update_frequency="monthly",
        weekends=False,
        holidays=False,
    ),
    latency=LatencyTarget(
        p50_ms=800,
        p95_ms=2500,
        p99_ms=8000,
        timeout_ms=45000,
    ),
    availability=AvailabilityTarget(
        uptime_pct=98.0,
        degraded_mode_available=True,
    ),
    data_quality=DataQualityTarget(
        completeness_pct=98.0,
        accuracy_checks=True,
        schema_validation=True,
        anomaly_detection=False,
    ),
    notes="IBGE SIDRA API. PAM anual, LSPA mensal.",
)

NOTICIAS_AGRICOLAS_SLA = SourceSLA(
    source=Fonte.NOTICIAS_AGRICOLAS,
    tier=Tier.BEST_EFFORT,
    freshness=FreshnessPolicy(
        update_frequency="daily",
        update_time=time(19, 0),
        weekends=False,
        holidays=False,
    ),
    latency=LatencyTarget(
        p50_ms=1500,
        p95_ms=5000,
        p99_ms=15000,
        timeout_ms=45000,
    ),
    availability=AvailabilityTarget(
        uptime_pct=90.0,
        degraded_mode_available=False,
    ),
    data_quality=DataQualityTarget(
        completeness_pct=90.0,
        accuracy_checks=False,
        schema_validation=True,
        anomaly_detection=False,
    ),
    notes="Fonte alternativa para CEPEA. Usado como fallback.",
)

_SLA_REGISTRY: dict[Fonte, SourceSLA] = {
    Fonte.CEPEA: CEPEA_SLA,
    Fonte.CONAB: CONAB_SLA,
    Fonte.IBGE: IBGE_SLA,
    Fonte.NOTICIAS_AGRICOLAS: NOTICIAS_AGRICOLAS_SLA,
}


def get_sla(source: Fonte) -> SourceSLA | None:
    return _SLA_REGISTRY.get(source)


def list_slas() -> list[SourceSLA]:
    return list(_SLA_REGISTRY.values())


def get_sla_summary() -> dict[str, Any]:
    return {
        "sources": [sla.to_dict() for sla in _SLA_REGISTRY.values()],
        "tiers": {
            "critical": "99%+ uptime, daily freshness, full validation",
            "standard": "95%+ uptime, monthly freshness, schema validation",
            "best_effort": "90%+ uptime, fallback source, basic validation",
        },
    }


__all__ = [
    "Tier",
    "FreshnessPolicy",
    "LatencyTarget",
    "AvailabilityTarget",
    "DataQualityTarget",
    "SourceSLA",
    "CEPEA_SLA",
    "CONAB_SLA",
    "IBGE_SLA",
    "NOTICIAS_AGRICOLAS_SLA",
    "get_sla",
    "list_slas",
    "get_sla_summary",
]
