"""Tests for semantic validation."""

from __future__ import annotations

import pandas as pd

from agrobr.validators.semantic import (
    AreaConsistencyRule,
    DailyVariationRule,
    DateSequenceRule,
    PricePositiveRule,
    ProductivityRangeRule,
    SafraFormatRule,
    get_validation_summary,
    validate_semantic,
)


class TestPricePositiveRule:
    def test_all_positive(self):
        rule = PricePositiveRule()
        df = pd.DataFrame({"valor": [100, 150, 200]})
        results = rule.check(df)
        assert len(results) == 1
        assert results[0].passed is True

    def test_negative_price(self):
        rule = PricePositiveRule()
        df = pd.DataFrame({"valor": [100, -50, 200]})
        results = rule.check(df)
        assert len(results) == 1
        assert results[0].passed is False
        assert "non-positive prices" in results[0].message

    def test_zero_price(self):
        rule = PricePositiveRule()
        df = pd.DataFrame({"valor": [100, 0, 200]})
        results = rule.check(df)
        assert results[0].passed is False

    def test_no_valor_column(self):
        rule = PricePositiveRule()
        df = pd.DataFrame({"preco": [100, 150]})
        results = rule.check(df)
        assert len(results) == 0


class TestProductivityRangeRule:
    def test_within_range(self):
        rule = ProductivityRangeRule()
        df = pd.DataFrame({"produto": ["soja", "soja"], "produtividade": [3000, 3500]})
        results = rule.check(df)
        assert len(results) == 1
        assert results[0].passed is True

    def test_outside_range(self):
        rule = ProductivityRangeRule()
        df = pd.DataFrame({"produto": ["soja", "soja"], "produtividade": [100, 10000]})
        results = rule.check(df)
        assert results[0].passed is False

    def test_unknown_product(self):
        rule = ProductivityRangeRule()
        df = pd.DataFrame(
            {"produto": ["unknown_product"], "produtividade": [100]}
        )
        results = rule.check(df)
        assert results[0].passed is True


class TestDailyVariationRule:
    def test_normal_variation(self):
        rule = DailyVariationRule()
        df = pd.DataFrame(
            {
                "data": pd.date_range("2024-01-01", periods=5),
                "valor": [100, 101, 102, 103, 104],
            }
        )
        results = rule.check(df)
        assert results[0].passed is True

    def test_extreme_variation(self):
        rule = DailyVariationRule(max_variation_pct=10.0)
        df = pd.DataFrame(
            {
                "data": pd.date_range("2024-01-01", periods=3),
                "valor": [100, 150, 160],
            }
        )
        results = rule.check(df)
        assert results[0].passed is False


class TestDateSequenceRule:
    def test_continuous_sequence(self):
        rule = DateSequenceRule()
        df = pd.DataFrame({"data": pd.date_range("2024-01-01", periods=10)})
        results = rule.check(df)
        assert results[0].passed is True

    def test_large_gap(self):
        rule = DateSequenceRule()
        df = pd.DataFrame(
            {"data": ["2024-01-01", "2024-01-05", "2024-01-25"]}
        )
        results = rule.check(df)
        assert results[0].passed is False
        assert "gaps" in results[0].message.lower()


class TestAreaConsistencyRule:
    def test_consistent_areas(self):
        rule = AreaConsistencyRule()
        df = pd.DataFrame({"area_plantada": [1000, 2000], "area_colhida": [950, 1900]})
        results = rule.check(df)
        assert results[0].passed is True

    def test_inconsistent_areas(self):
        rule = AreaConsistencyRule()
        df = pd.DataFrame({"area_plantada": [1000, 2000], "area_colhida": [1100, 1900]})
        results = rule.check(df)
        assert results[0].passed is False


class TestSafraFormatRule:
    def test_valid_format(self):
        rule = SafraFormatRule()
        df = pd.DataFrame({"safra": ["2024/25", "2023/24", "2022/23"]})
        results = rule.check(df)
        assert results[0].passed is True

    def test_invalid_format(self):
        rule = SafraFormatRule()
        df = pd.DataFrame({"safra": ["2024-25", "2024/2025", "24/25"]})
        results = rule.check(df)
        assert results[0].passed is False


class TestValidateSemantic:
    def test_validate_all_pass(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range("2024-01-01", periods=5),
                "valor": [100, 101, 102, 103, 104],
                "produto": ["soja"] * 5,
            }
        )
        passed, results = validate_semantic(df)
        errors = [r for r in results if r.severity == "error" and not r.passed]
        assert len(errors) == 0

    def test_validate_with_rules_subset(self):
        df = pd.DataFrame({"valor": [100, 150, 200]})
        passed, results = validate_semantic(df, rules=[PricePositiveRule()])
        assert len(results) == 1
        assert results[0].passed is True


class TestGetValidationSummary:
    def test_summary_all_passed(self):
        df = pd.DataFrame({"valor": [100, 150, 200]})
        _, results = validate_semantic(df, rules=[PricePositiveRule()])
        summary = get_validation_summary(results)
        assert summary["passed"] == 1
        assert summary["failed"] == 0
        assert summary["success_rate"] == 1.0

    def test_summary_with_failures(self):
        df = pd.DataFrame({"valor": [100, -50, 200]})
        _, results = validate_semantic(df, rules=[PricePositiveRule()])
        summary = get_validation_summary(results)
        assert summary["failed"] == 1
        assert summary["errors"] >= 1
