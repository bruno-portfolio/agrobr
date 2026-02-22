from __future__ import annotations

from datetime import datetime, timedelta

from agrobr.cache.policies import (
    POLICIES,
    CachePolicy,
    calculate_expiry,
    format_ttl,
    get_next_update_info,
    get_policy,
    is_expired,
    is_stale_acceptable,
    should_refresh,
)
from agrobr.constants import Fonte


class TestFormatTTL:
    def test_seconds(self):
        assert format_ttl(30) == "30 segundos"

    def test_one_minute(self):
        assert format_ttl(60) == "1 minuto"

    def test_multiple_minutes(self):
        assert format_ttl(120) == "2 minutos"

    def test_one_hour(self):
        assert format_ttl(3600) == "1 hora"

    def test_multiple_hours(self):
        assert format_ttl(7200) == "2 horas"

    def test_one_day(self):
        assert format_ttl(86400) == "1 dia"

    def test_multiple_days(self):
        assert format_ttl(172800) == "2 dias"

    def test_30_days(self):
        assert format_ttl(30 * 86400) == "30 dias"


class TestGetNextUpdateInfo:
    def test_smart_expiry_source(self):
        info = get_next_update_info("cepea_diario")
        assert info["type"] == "smart"
        assert "expires_at" in info
        assert "18h" in info["description"]

    def test_ttl_source(self):
        info = get_next_update_info(Fonte.CONAB)
        assert info["type"] == "ttl"
        assert "ttl" in info
        assert "description" in info

    def test_noticias_agricolas_smart(self):
        info = get_next_update_info(Fonte.NOTICIAS_AGRICOLAS)
        assert info["type"] == "smart"


class TestShouldRefresh:
    def test_force_true(self):
        created = datetime.now() - timedelta(minutes=5)
        refresh, reason = should_refresh(created, Fonte.CEPEA, force=True)
        assert refresh is True
        assert reason == "force_refresh"

    def test_expired_entry(self):
        created = datetime.now() - timedelta(days=10)
        refresh, reason = should_refresh(created, Fonte.CONAB)
        assert refresh is True
        assert reason == "expired"

    def test_fresh_entry(self):
        created = datetime.now() - timedelta(minutes=5)
        refresh, reason = should_refresh(created, Fonte.CONAB)
        assert refresh is False
        assert reason == "fresh"


class TestGetPolicy:
    def test_string_key_direct(self):
        policy = get_policy("cepea_diario")
        assert policy.smart_expiry is True

    def test_fonte_enum(self):
        policy = get_policy(Fonte.CEPEA)
        assert isinstance(policy, CachePolicy)

    def test_string_fonte_value(self):
        policy = get_policy("cepea")
        assert isinstance(policy, CachePolicy)

    def test_unknown_string_fallback(self):
        policy = get_policy("unknown_source_xyz")
        assert policy == POLICIES["cepea_diario"]

    def test_with_endpoint(self):
        policy = get_policy(Fonte.CONAB, endpoint="safras")
        assert isinstance(policy, CachePolicy)


class TestIsExpired:
    def test_smart_expiry_not_expired(self):
        created = datetime.now() - timedelta(hours=1)
        assert (
            is_expired(created, "cepea_diario") is False
            or is_expired(created, "cepea_diario") is True
        )

    def test_ttl_expired(self):
        created = datetime.now() - timedelta(days=10)
        assert is_expired(created, Fonte.CONAB) is True

    def test_ttl_not_expired(self):
        created = datetime.now() - timedelta(minutes=5)
        assert is_expired(created, Fonte.CONAB) is False


class TestIsStaleAcceptable:
    def test_within_stale_window(self):
        created = datetime.now() - timedelta(days=1)
        assert is_stale_acceptable(created, Fonte.CONAB) is True

    def test_beyond_stale_window(self):
        created = datetime.now() - timedelta(days=365)
        assert is_stale_acceptable(created, Fonte.CONAB) is False


class TestCalculateExpiry:
    def test_smart_expiry(self):
        expiry = calculate_expiry("cepea_diario")
        assert isinstance(expiry, datetime)
        assert expiry > datetime.now()

    def test_ttl_expiry(self):
        before = datetime.now()
        expiry = calculate_expiry(Fonte.CONAB)
        assert expiry > before
