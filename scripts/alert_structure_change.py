"""Envia alertas quando estrutura muda.

Uso:
    python scripts/alert_structure_change.py diff_report.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path


async def alert(report_path: str) -> None:
    """Envia alertas baseado no relatório de diff."""
    from agrobr.alerts.notifier import send_alert, AlertLevel

    report = json.loads(Path(report_path).read_text())

    drifted = [c for c in report["comparisons"] if c["status"] == "drift"]
    errors = [c for c in report["comparisons"] if c["status"] == "error"]

    if drifted:
        print(f"Sending drift alert for: {[d['source'] for d in drifted]}")
        await send_alert(
            level=AlertLevel.WARNING,
            title="Structure drift detected",
            details={
                "sources": [d["source"] for d in drifted],
                "similarities": {d["source"]: d["similarity"] for d in drifted},
                "threshold": report["threshold"],
            },
        )

    if errors:
        print(f"Sending error alert for: {[e['source'] for e in errors]}")
        await send_alert(
            level=AlertLevel.CRITICAL,
            title="Structure fetch failed",
            details={
                "sources": [e["source"] for e in errors],
                "errors": {e["source"]: e["error"] for e in errors},
            },
        )

    if not drifted and not errors:
        print("No alerts to send - all sources are OK")


def main():
    parser = argparse.ArgumentParser(description="Send alerts for structure changes")
    parser.add_argument("report_path", help="Path to diff report JSON file")
    args = parser.parse_args()

    asyncio.run(alert(args.report_path))


if __name__ == "__main__":
    main()
