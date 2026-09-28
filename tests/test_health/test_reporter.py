"""Tests for agrobr.health.reporter module."""

from __future__ import annotations

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
    def test_all_passed_true(self):
        report = HealthReport([_make_result(status=CheckStatus.OK)])
        assert report.all_passed is True

    def test_warnings_property(self):
        results = [
            _make_result(Fonte.CEPEA, CheckStatus.WARNING, message="slow"),
            _make_result(Fonte.CONAB, CheckStatus.OK),
        ]
        report = HealthReport(results)
        warnings = report.warnings
        assert len(warnings) == 1
        assert warnings[0].source == Fonte.CEPEA

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

    def test_save_html(self, tmp_path):
        report = HealthReport([_make_result()])
        path = tmp_path / "report.html"
        report.save(path, format="html")

        assert path.exists()
        assert "<!DOCTYPE html>" in path.read_text(encoding="utf-8")

    def test_save_markdown(self, tmp_path):
        report = HealthReport([_make_result()])
        path = tmp_path / "report.md"
        report.save(path, format="md")

        assert path.exists()
        assert "# Health Check Report" in path.read_text(encoding="utf-8")

    def test_save_unsupported_format(self, tmp_path):
        report = HealthReport([_make_result()])
        with pytest.raises(ValueError, match="Formato"):
            report.save(tmp_path / "report.xml", format="xml")

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
