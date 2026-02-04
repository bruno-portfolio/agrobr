"""Tests for quality module."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from agrobr.quality import (
    CheckStatus,
    QualityCertificate,
    QualityCheck,
    QualityLevel,
    certify,
    quick_check,
)


class TestQualityLevel:
    def test_level_values(self):
        assert QualityLevel.GOLD == "gold"
        assert QualityLevel.SILVER == "silver"
        assert QualityLevel.BRONZE == "bronze"
        assert QualityLevel.UNCERTIFIED == "uncertified"


class TestCheckStatus:
    def test_status_values(self):
        assert CheckStatus.PASSED == "passed"
        assert CheckStatus.FAILED == "failed"
        assert CheckStatus.SKIPPED == "skipped"
        assert CheckStatus.WARNING == "warning"


class TestQualityCheck:
    def test_check_creation(self):
        check = QualityCheck(
            name="test_check",
            status=CheckStatus.PASSED,
            message="All good",
        )
        assert check.name == "test_check"
        assert check.status == CheckStatus.PASSED
        assert check.details == {}


class TestQualityCertificate:
    def test_certificate_to_dict(self):
        cert = QualityCertificate(
            level=QualityLevel.GOLD,
            checks=[
                QualityCheck(name="check1", status=CheckStatus.PASSED),
                QualityCheck(name="check2", status=CheckStatus.PASSED),
            ],
            issued_at=datetime(2025, 1, 15, 10, 0),
            source="test",
            dataset="test_data",
            row_count=100,
            column_count=5,
            score=0.95,
        )
        d = cert.to_dict()

        assert d["level"] == "gold"
        assert d["score"] == 0.95
        assert d["row_count"] == 100
        assert d["summary"]["passed"] == 2

    def test_certificate_is_valid(self):
        cert = QualityCertificate(
            level=QualityLevel.GOLD,
            checks=[],
            issued_at=datetime.now(),
            valid_until=datetime.now() + timedelta(days=1),
            score=1.0,
        )
        assert cert.is_valid() is True

    def test_certificate_expired(self):
        cert = QualityCertificate(
            level=QualityLevel.GOLD,
            checks=[],
            issued_at=datetime.now() - timedelta(days=2),
            valid_until=datetime.now() - timedelta(days=1),
            score=1.0,
        )
        assert cert.is_valid() is False

    def test_certificate_no_expiry(self):
        cert = QualityCertificate(
            level=QualityLevel.GOLD,
            checks=[],
            issued_at=datetime.now(),
            valid_until=None,
            score=1.0,
        )
        assert cert.is_valid() is True


class TestCertify:
    def test_certify_gold(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range(datetime.now() - timedelta(days=5), periods=100),
                "valor": [100.0 + i for i in range(100)],
                "produto": ["soja"] * 100,
            }
        )
        cert = certify(df, source="test", dataset="test_data")

        assert cert.level in [QualityLevel.GOLD, QualityLevel.SILVER]
        assert cert.row_count == 100
        assert cert.score > 0.7

    def test_certify_with_nulls(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range(datetime.now() - timedelta(days=5), periods=10),
                "valor": [100.0, None, 102.0, None, 104.0, None, 106.0, None, 108.0, None],
                "produto": ["soja"] * 10,
            }
        )
        cert = certify(df)

        completeness_check = next((c for c in cert.checks if c.name == "completeness"), None)
        assert completeness_check is not None

    def test_certify_with_duplicates(self):
        df = pd.DataFrame(
            {
                "data": [datetime.now()] * 10,
                "valor": [100.0] * 10,
            }
        )
        cert = certify(df)

        dup_check = next((c for c in cert.checks if c.name == "duplicates"), None)
        assert dup_check is not None
        assert dup_check.status == CheckStatus.FAILED

    def test_certify_with_expected_schema(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range(datetime.now(), periods=5),
                "valor": [100.0] * 5,
                "produto": ["soja"] * 5,
            }
        )
        cert = certify(df, expected_columns=["data", "valor", "produto"])

        schema_check = next((c for c in cert.checks if c.name == "schema"), None)
        assert schema_check is not None
        assert schema_check.status == CheckStatus.PASSED

    def test_certify_missing_schema(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range(datetime.now(), periods=5),
                "valor": [100.0] * 5,
            }
        )
        cert = certify(df, expected_columns=["data", "valor", "produto", "praca"])

        schema_check = next((c for c in cert.checks if c.name == "schema"), None)
        assert schema_check is not None
        assert schema_check.status == CheckStatus.FAILED

    def test_certify_stale_data(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range(datetime.now() - timedelta(days=30), periods=5),
                "valor": [100.0] * 5,
            }
        )
        cert = certify(df)

        freshness_check = next((c for c in cert.checks if c.name == "freshness"), None)
        assert freshness_check is not None
        assert freshness_check.status in [CheckStatus.WARNING, CheckStatus.FAILED]

    def test_certify_negative_values(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range(datetime.now(), periods=5),
                "valor": [-10.0, 100.0, 150.0, 200.0, 250.0],
            }
        )
        cert = certify(df, min_value=0)

        range_check = next((c for c in cert.checks if c.name == "range_valor"), None)
        assert range_check is not None
        assert range_check.status == CheckStatus.FAILED


class TestQuickCheck:
    def test_quick_check(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range(datetime.now() - timedelta(days=3), periods=50),
                "valor": [100.0 + i for i in range(50)],
            }
        )
        level, score = quick_check(df)

        assert isinstance(level, QualityLevel)
        assert 0 <= score <= 1

    def test_quick_check_empty(self):
        df = pd.DataFrame()
        level, score = quick_check(df)

        assert level in [QualityLevel.UNCERTIFIED, QualityLevel.BRONZE]
        assert score >= 0
