"""Tests for agrobr.health.reporter module."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.constants import Fonte
from agrobr.health.checker import CheckResult, CheckStatus
from agrobr.health.reporter import HealthReport, generate_report


def _make_result(
    source: Fonte = Fonte.CEPEA,
    status: CheckStatus = CheckStatus.OK,
    latency: float = 100.0,
    message: str = "ok",
) -> CheckResult:
    return CheckResult(
        source=source,
        status=status,
        latency_ms=latency,
        message=message,
        details={"test": True},
        timestamp=datetime(2024, 6, 15, 12, 0, 0),
    )


class TestHealthReport:
    def test_summary_all_ok(self):
        results = [
            _make_result(Fonte.CEPEA, CheckStatus.OK, 100),
            _make_result(Fonte.CONAB, CheckStatus.OK, 200),
            _make_result(Fonte.IBGE, CheckStatus.OK, 150),
        ]
        report = HealthReport(results)
        summary = report.summary

        assert summary["total_checks"] == 3
        assert summary["ok"] == 3
        assert summary["warnings"] == 0
        assert summary["failures"] == 0
        assert summary["success_rate"] == 1.0
        assert summary["all_passed"] is True
        assert summary["has_warnings"] is False

    def test_summary_with_failures(self):
        results = [
            _make_result(Fonte.CEPEA, CheckStatus.OK, 100),
            _make_result(Fonte.CONAB, CheckStatus.FAILED, 0, "timeout"),
            _make_result(Fonte.IBGE, CheckStatus.WARNING, 5000, "slow"),
        ]
        report = HealthReport(results)
        summary = report.summary

        assert summary["ok"] == 1
        assert summary["warnings"] == 1
        assert summary["failures"] == 1
        assert summary["all_passed"] is False
        assert summary["has_warnings"] is True

    def test_summary_cached(self):
        report = HealthReport([_make_result()])
        s1 = report.summary
        s2 = report.summary
        assert s1 is s2

    def test_summary_empty(self):
        report = HealthReport([])
        summary = report.summary
        assert summary["total_checks"] == 0
        assert summary["success_rate"] == 0
        assert summary["avg_latency_ms"] == 0

    def test_all_passed_true(self):
        report = HealthReport([_make_result(status=CheckStatus.OK)])
        assert report.all_passed is True

    def test_all_passed_false(self):
        report = HealthReport([_make_result(status=CheckStatus.FAILED)])
        assert report.all_passed is False

    def test_all_passed_warning_is_true(self):
        report = HealthReport([_make_result(status=CheckStatus.WARNING)])
        assert report.all_passed is True

    def test_failures_property(self):
        results = [
            _make_result(Fonte.CEPEA, CheckStatus.OK),
            _make_result(Fonte.CONAB, CheckStatus.FAILED, message="down"),
        ]
        report = HealthReport(results)
        failures = report.failures
        assert len(failures) == 1
        assert failures[0].source == Fonte.CONAB

    def test_warnings_property(self):
        results = [
            _make_result(Fonte.CEPEA, CheckStatus.WARNING, message="slow"),
            _make_result(Fonte.CONAB, CheckStatus.OK),
        ]
        report = HealthReport(results)
        warnings = report.warnings
        assert len(warnings) == 1
        assert warnings[0].source == Fonte.CEPEA

    def test_to_dict(self):
        report = HealthReport([_make_result()])
        d = report.to_dict()

        assert "timestamp" in d
        assert d["timestamp"].endswith("Z")
        assert "summary" in d
        assert "checks" in d
        assert len(d["checks"]) == 1
        assert d["checks"][0]["source"] == "cepea"
        assert d["checks"][0]["status"] == "ok"

    def test_to_json(self):
        report = HealthReport([_make_result()])
        j = report.to_json()
        data = json.loads(j)
        assert "summary" in data
        assert "checks" in data

    def test_to_json_indent(self):
        report = HealthReport([_make_result()])
        j4 = report.to_json(indent=4)
        assert "    " in j4

    def test_to_markdown(self):
        results = [
            _make_result(Fonte.CEPEA, CheckStatus.OK, 100),
            _make_result(Fonte.CONAB, CheckStatus.FAILED, 0, "down"),
        ]
        report = HealthReport(results)
        md = report.to_markdown()

        assert "# Health Check Report" in md
        assert "## Summary" in md
        assert "## Results" in md
        assert "| Source | Status |" in md
        assert "## Failures" in md
        assert "conab" in md
        assert "down" in md

    def test_to_markdown_no_failures(self):
        report = HealthReport([_make_result(status=CheckStatus.OK)])
        md = report.to_markdown()
        assert "## Failures" not in md

    def test_to_html(self):
        results = [
            _make_result(Fonte.CEPEA, CheckStatus.OK, 100),
            _make_result(Fonte.CONAB, CheckStatus.WARNING, 5000, "slow"),
        ]
        report = HealthReport(results)
        html = report.to_html()

        assert "<!DOCTYPE html>" in html
        assert "Health Check Report" in html
        assert "cepea" in html
        assert "conab" in html
        assert "#28a745" in html
        assert "#ffc107" in html

    def test_save_json(self, tmp_path):
        report = HealthReport([_make_result()])
        path = tmp_path / "report.json"
        report.save(path, format="json")

        assert path.exists()
        data = json.loads(path.read_text())
        assert "summary" in data

    def test_save_html(self, tmp_path):
        report = HealthReport([_make_result()])
        path = tmp_path / "report.html"
        report.save(path, format="html")

        assert path.exists()
        assert "<!DOCTYPE html>" in path.read_text()

    def test_save_markdown(self, tmp_path):
        report = HealthReport([_make_result()])
        path = tmp_path / "report.md"
        report.save(path, format="md")

        assert path.exists()
        assert "# Health Check Report" in path.read_text()

    def test_save_unsupported_format(self, tmp_path):
        report = HealthReport([_make_result()])
        with pytest.raises(ValueError, match="Formato"):
            report.save(tmp_path / "report.xml", format="xml")

    def test_save_creates_parent_dirs(self, tmp_path):
        report = HealthReport([_make_result()])
        path = tmp_path / "nested" / "deep" / "report.json"
        report.save(path, format="json")
        assert path.exists()

    def test_print_summary(self, capsys):
        results = [
            _make_result(Fonte.CEPEA, CheckStatus.OK, 100, "ok"),
            _make_result(Fonte.CONAB, CheckStatus.FAILED, 0, "down"),
            _make_result(Fonte.IBGE, CheckStatus.WARNING, 5000, "slow"),
        ]
        report = HealthReport(results)
        report.print_summary()

        captured = capsys.readouterr()
        assert "HEALTH CHECK REPORT" in captured.out
        assert "[OK]" in captured.out
        assert "[FAIL]" in captured.out
        assert "[WARN]" in captured.out
        assert "Total: 3" in captured.out
        assert "Failures: 1" in captured.out


class TestGenerateReport:
    @pytest.mark.asyncio
    async def test_generate_all_sources(self):
        mock_results = [
            _make_result(Fonte.CEPEA, CheckStatus.OK),
            _make_result(Fonte.CONAB, CheckStatus.OK),
            _make_result(Fonte.IBGE, CheckStatus.OK),
        ]
        with patch(
            "agrobr.health.reporter.run_all_checks",
            new_callable=AsyncMock,
            return_value=mock_results,
        ):
            report = await generate_report()

        assert len(report.results) == 3
        assert report.all_passed

    @pytest.mark.asyncio
    async def test_generate_filtered_sources(self):
        mock_results = [
            _make_result(Fonte.CEPEA, CheckStatus.OK),
            _make_result(Fonte.CONAB, CheckStatus.OK),
            _make_result(Fonte.IBGE, CheckStatus.OK),
        ]
        with patch(
            "agrobr.health.reporter.run_all_checks",
            new_callable=AsyncMock,
            return_value=mock_results,
        ):
            report = await generate_report(sources=[Fonte.CEPEA])

        assert len(report.results) == 1
        assert report.results[0].source == Fonte.CEPEA

    @pytest.mark.asyncio
    async def test_generate_and_save(self, tmp_path):
        mock_results = [_make_result()]
        save_path = tmp_path / "output.json"

        with patch(
            "agrobr.health.reporter.run_all_checks",
            new_callable=AsyncMock,
            return_value=mock_results,
        ):
            report = await generate_report(save_path=save_path, format="json")

        assert save_path.exists()
        assert isinstance(report, HealthReport)
