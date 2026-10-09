from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

from agrobr import _log

from ..constants import Fonte
from ..exceptions import InvalidParameterError
from ..utils.time import utcnow
from .checker import CheckResult, CheckStatus, run_all_checks

logger = _log.get_logger(__name__)

_FORMATOS = ("json", "html", "md")


def _validar_formato(format: str) -> None:
    if format not in _FORMATOS:
        raise InvalidParameterError(
            f"Formato não suportado: {format!r}. Valores válidos: {', '.join(_FORMATOS)}"
        )


class HealthReport:
    def __init__(self, results: list[CheckResult]):
        self.results = results
        self.timestamp = utcnow()
        self._summary: dict[str, Any] | None = None

    @property
    def summary(self) -> dict[str, Any]:
        if self._summary is None:
            self._summary = self._calculate_summary()
        return self._summary

    def _calculate_summary(self) -> dict[str, Any]:
        total = len(self.results)
        ok_count = sum(1 for r in self.results if r.status == CheckStatus.OK)
        warning_count = sum(1 for r in self.results if r.status == CheckStatus.WARNING)
        failed_count = sum(1 for r in self.results if r.status == CheckStatus.FAILED)
        not_verified = [
            r.source.value for r in self.results if r.status == CheckStatus.NOT_VERIFIED
        ]

        avg_latency = sum(r.latency_ms for r in self.results) / total if total > 0 else 0

        return {
            "total_checks": total,
            "ok": ok_count,
            "warnings": warning_count,
            "failures": failed_count,
            "success_rate": ok_count / total if total > 0 else 0,
            "avg_latency_ms": avg_latency,
            "all_passed": failed_count == 0,
            "has_warnings": warning_count > 0,
            "not_verified": not_verified,
        }

    @property
    def all_passed(self) -> bool:
        return bool(self.summary["all_passed"])

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == CheckStatus.FAILED]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == CheckStatus.WARNING]

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat() + "Z",
            "summary": self.summary,
            "checks": [
                {
                    "source": r.source.value,
                    "status": r.status.value,
                    "latency_ms": r.latency_ms,
                    "message": r.message,
                    "category": r.category,
                    "details": r.details,
                    "timestamp": r.timestamp.isoformat() + "Z",
                }
                for r in self.results
            ],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str, ensure_ascii=False)

    def save(self, path: str | Path, format: str = "json") -> None:
        _validar_formato(format)
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if format == "json":
            path.write_text(self.to_json(), encoding="utf-8")
        elif format == "html":
            path.write_text(self.to_html(), encoding="utf-8")
        else:
            path.write_text(self.to_markdown(), encoding="utf-8")

        logger.info("health_report_saved", path=str(path), format=format)

    def to_markdown(self) -> str:
        lines = [
            "# Health Check Report",
            "",
            f"**Timestamp:** {self.timestamp.isoformat()}Z",
            "",
            "## Summary",
            "",
            f"- Total checks: {self.summary['total_checks']}",
            f"- OK: {self.summary['ok']}",
            f"- Warnings: {self.summary['warnings']}",
            f"- Failures: {self.summary['failures']}",
            f"- Not verified: {', '.join(self.summary['not_verified']) or 'none'}",
            f"- Success rate: {self.summary['success_rate']:.1%}",
            f"- Average latency: {self.summary['avg_latency_ms']:.0f}ms",
            "",
            "## Results",
            "",
            "| Source | Status | Latency | Message |",
            "|--------|--------|---------|---------|",
        ]

        for r in self.results:
            status_emoji = {
                CheckStatus.OK: ":white_check_mark:",
                CheckStatus.WARNING: ":warning:",
                CheckStatus.FAILED: ":x:",
            }.get(r.status, "")

            lines.append(
                f"| {r.source.value} | {status_emoji} {r.status.value} | "
                f"{r.latency_ms:.0f}ms | {r.message} |"
            )

        if self.failures:
            lines.extend(
                [
                    "",
                    "## Failures",
                    "",
                ]
            )
            for r in self.failures:
                lines.extend(
                    [
                        f"### {r.source.value}",
                        "",
                        f"**Error:** {r.message}",
                        "",
                        "```json",
                        json.dumps(r.details, indent=2, default=str),
                        "```",
                        "",
                    ]
                )

        return "\n".join(lines)

    def to_html(self) -> str:
        status_colors = {
            CheckStatus.OK: "#28a745",
            CheckStatus.WARNING: "#ffc107",
            CheckStatus.FAILED: "#dc3545",
        }

        rows = []
        for r in self.results:
            color = status_colors.get(r.status, "#6c757d")
            rows.append(
                f"""
                <tr>
                    <td>{html.escape(r.source.value)}</td>
                    <td style="color: {color}; font-weight: bold;">{html.escape(r.status.value)}</td>
                    <td>{r.latency_ms:.0f}ms</td>
                    <td>{html.escape(r.message)}</td>
                </tr>
            """
            )

        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Health Check Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; }}
        h1 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f8f9fa; }}
        .summary {{ background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
        .summary-item {{ display: inline-block; margin-right: 30px; }}
        .summary-value {{ font-size: 24px; font-weight: bold; }}
        .summary-label {{ color: #666; }}
    </style>
</head>
<body>
    <h1>Health Check Report</h1>
    <p><strong>Timestamp:</strong> {self.timestamp.isoformat()}Z</p>

    <div class="summary">
        <div class="summary-item">
            <div class="summary-value">{self.summary["total_checks"]}</div>
            <div class="summary-label">Total</div>
        </div>
        <div class="summary-item">
            <div class="summary-value" style="color: #28a745;">{self.summary["ok"]}</div>
            <div class="summary-label">OK</div>
        </div>
        <div class="summary-item">
            <div class="summary-value" style="color: #ffc107;">{self.summary["warnings"]}</div>
            <div class="summary-label">Warnings</div>
        </div>
        <div class="summary-item">
            <div class="summary-value" style="color: #dc3545;">{self.summary["failures"]}</div>
            <div class="summary-label">Failures</div>
        </div>
        <div class="summary-item">
            <div class="summary-value">{self.summary["avg_latency_ms"]:.0f}ms</div>
            <div class="summary-label">Avg Latency</div>
        </div>
    </div>

    <h2>Results</h2>
    <table>
        <thead>
            <tr>
                <th>Source</th>
                <th>Status</th>
                <th>Latency</th>
                <th>Message</th>
            </tr>
        </thead>
        <tbody>
            {"".join(rows)}
        </tbody>
    </table>
</body>
</html>
"""

    def print_summary(self) -> None:
        print("\n" + "=" * 60)
        print("HEALTH CHECK REPORT")
        print("=" * 60)
        print(f"Timestamp: {self.timestamp.isoformat()}Z")
        print()

        for r in self.results:
            status_symbol = {
                CheckStatus.OK: "[OK]",
                CheckStatus.WARNING: "[WARN]",
                CheckStatus.FAILED: "[FAIL]",
                CheckStatus.NOT_VERIFIED: "[NOT VERIFIED]",
            }.get(r.status, "[?]")

            print(f"  {status_symbol} {r.source.value}: {r.message} ({r.latency_ms:.0f}ms)")

        print()
        print("-" * 60)
        print(
            f"Total: {self.summary['total_checks']} | "
            f"OK: {self.summary['ok']} | "
            f"Warnings: {self.summary['warnings']} | "
            f"Failures: {self.summary['failures']}"
        )
        print(
            f"Success Rate: {self.summary['success_rate']:.1%} | "
            f"Avg Latency: {self.summary['avg_latency_ms']:.0f}ms"
        )
        print("=" * 60 + "\n")


async def generate_report(
    sources: list[Fonte] | None = None,
    save_path: str | Path | None = None,
    format: str = "json",
) -> HealthReport:
    _validar_formato(format)
    results = await run_all_checks(sources)

    report = HealthReport(results)

    if save_path:
        report.save(save_path, format)

    return report
