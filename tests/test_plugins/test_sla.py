"""Tests for SLA module."""

from __future__ import annotations

from datetime import time

from agrobr.constants import Fonte
from agrobr.sla import (
    CEPEA_SLA,
    CONAB_SLA,
    IBGE_SLA,
    NOTICIAS_AGRICOLAS_SLA,
    AvailabilityTarget,
    DataQualityTarget,
    FreshnessPolicy,
    LatencyTarget,
    SourceSLA,
    Tier,
    get_sla,
    get_sla_summary,
    list_slas,
)


class TestTier:
    def test_tier_values(self):
        assert Tier.CRITICAL == "critical"
        assert Tier.STANDARD == "standard"
        assert Tier.BEST_EFFORT == "best_effort"


class TestFreshnessPolicy:
    def test_daily_freshness(self):
        policy = FreshnessPolicy(
            update_frequency="daily",
            update_time=time(18, 0),
            weekends=False,
        )
        assert policy.update_frequency == "daily"
        assert policy.update_time == time(18, 0)
        assert policy.weekends is False

    def test_monthly_freshness(self):
        policy = FreshnessPolicy(update_frequency="monthly")
        assert policy.update_frequency == "monthly"
        assert policy.update_time is None
        assert policy.timezone == "America/Sao_Paulo"


class TestLatencyTarget:
    def test_latency_creation(self):
        target = LatencyTarget(
            p50_ms=500,
            p95_ms=2000,
            p99_ms=5000,
            timeout_ms=30000,
        )
        assert target.p50_ms == 500
        assert target.p95_ms == 2000
        assert target.p99_ms == 5000
        assert target.timeout_ms == 30000


class TestAvailabilityTarget:
    def test_availability_creation(self):
        target = AvailabilityTarget(
            uptime_pct=99.0,
            degraded_mode_available=True,
        )
        assert target.uptime_pct == 99.0
        assert target.degraded_mode_available is True


class TestDataQualityTarget:
    def test_quality_defaults(self):
        target = DataQualityTarget()
        assert target.completeness_pct == 99.0
        assert target.accuracy_checks is True
        assert target.schema_validation is True
        assert target.anomaly_detection is True


class TestSourceSLA:
    def test_sla_to_dict(self):
        sla = SourceSLA(
            source=Fonte.CEPEA,
            tier=Tier.CRITICAL,
            freshness=FreshnessPolicy(update_frequency="daily"),
            latency=LatencyTarget(p50_ms=500, p95_ms=2000, p99_ms=5000, timeout_ms=30000),
            availability=AvailabilityTarget(uptime_pct=99.0),
            data_quality=DataQualityTarget(),
        )
        d = sla.to_dict()

        assert d["source"] == "cepea"
        assert d["tier"] == "critical"
        assert d["freshness"]["update_frequency"] == "daily"
        assert d["latency"]["p50_ms"] == 500
        assert d["availability"]["uptime_pct"] == 99.0


class TestPredefinedSLAs:
    def test_cepea_sla(self):
        assert CEPEA_SLA.source == Fonte.CEPEA
        assert CEPEA_SLA.tier == Tier.CRITICAL
        assert CEPEA_SLA.freshness.update_frequency == "daily"
        assert CEPEA_SLA.freshness.update_time == time(18, 0)
        assert Fonte.NOTICIAS_AGRICOLAS in CEPEA_SLA.fallback_sources

    def test_conab_sla(self):
        assert CONAB_SLA.source == Fonte.CONAB
        assert CONAB_SLA.tier == Tier.STANDARD
        assert CONAB_SLA.freshness.update_frequency == "monthly"

    def test_ibge_sla(self):
        assert IBGE_SLA.source == Fonte.IBGE
        assert IBGE_SLA.tier == Tier.STANDARD
        assert IBGE_SLA.availability.uptime_pct == 98.0

    def test_noticias_agricolas_sla(self):
        assert NOTICIAS_AGRICOLAS_SLA.source == Fonte.NOTICIAS_AGRICOLAS
        assert NOTICIAS_AGRICOLAS_SLA.tier == Tier.BEST_EFFORT
        assert NOTICIAS_AGRICOLAS_SLA.availability.uptime_pct == 90.0


class TestSLAFunctions:
    def test_get_sla(self):
        sla = get_sla(Fonte.CEPEA)
        assert sla is not None
        assert sla.source == Fonte.CEPEA

    def test_get_sla_not_found(self):
        sla = get_sla(Fonte.CEPEA)
        assert sla is not None

    def test_list_slas(self):
        slas = list_slas()
        assert len(slas) >= 4
        sources = [s.source for s in slas]
        assert Fonte.CEPEA in sources
        assert Fonte.CONAB in sources
        assert Fonte.IBGE in sources

    def test_get_sla_summary(self):
        summary = get_sla_summary()
        assert "sources" in summary
        assert "tiers" in summary
        assert len(summary["sources"]) >= 4
        assert "critical" in summary["tiers"]
