"""Compara estruturas atuais com baseline.

Uso:
    python scripts/compare_structures.py --baseline .structures/baseline.json --current current_structures.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def compare(
    baseline_path: str,
    current_path: str,
    threshold: float,
    output_path: str,
) -> bool:
    """
    Compara fingerprints e retorna se há drift significativo.

    Returns:
        True se drift detectado
    """
    from agrobr.cepea.parsers.fingerprint import compare_fingerprints
    from agrobr.models import Fingerprint

    baseline_file = Path(baseline_path)
    current_file = Path(current_path)

    if not baseline_file.exists():
        print(f"Baseline file not found: {baseline_path}")
        print("Creating baseline from current structures...")
        import shutil

        shutil.copy(current_path, baseline_path)
        return False

    baseline = json.loads(baseline_file.read_text())
    current = json.loads(current_file.read_text())

    report = {
        "baseline_date": baseline.get("collected_at"),
        "current_date": current.get("collected_at"),
        "threshold": threshold,
        "comparisons": [],
        "drift_detected": False,
    }

    for source, current_data in current.get("sources", {}).items():
        if "error" in current_data:
            report["comparisons"].append(
                {
                    "source": source,
                    "status": "error",
                    "error": current_data["error"],
                }
            )
            report["drift_detected"] = True
            print(f"[ERROR] {source}: {current_data['error']}")
            continue

        baseline_data = baseline.get("sources", {}).get(source)
        if not baseline_data or "error" in baseline_data:
            report["comparisons"].append(
                {
                    "source": source,
                    "status": "no_baseline",
                }
            )
            print(f"[SKIP] {source}: No baseline available")
            continue

        current_fp = Fingerprint.model_validate(current_data)
        baseline_fp = Fingerprint.model_validate(baseline_data)

        similarity, diff = compare_fingerprints(current_fp, baseline_fp)

        status = "ok" if similarity >= threshold else "drift"
        comparison = {
            "source": source,
            "similarity": similarity,
            "threshold": threshold,
            "status": status,
            "diff": diff if similarity < threshold else None,
        }
        report["comparisons"].append(comparison)

        if similarity < threshold:
            report["drift_detected"] = True
            print(f"[DRIFT] {source}: {similarity:.1%} similarity (threshold: {threshold:.1%})")
            if diff:
                for key, value in diff.items():
                    print(f"        {key}: {value}")
        else:
            print(f"[OK] {source}: {similarity:.1%} similarity")

    Path(output_path).write_text(json.dumps(report, indent=2, default=str))

    if report["drift_detected"]:
        flag_path = Path("drift_detected.flag")
        flag_path.touch()
        print(f"\nDRIFT DETECTED! See {output_path}")
    else:
        print("\nNo significant drift detected")

    return report["drift_detected"]  # type: ignore[no-any-return]


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare structure fingerprints")
    parser.add_argument("--baseline", required=True, help="Baseline structures file")
    parser.add_argument("--current", required=True, help="Current structures file")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.85,
        help="Similarity threshold (default: 0.85)",
    )
    parser.add_argument(
        "--output",
        default="diff_report.json",
        help="Output report file",
    )
    args = parser.parse_args()

    drift = compare(args.baseline, args.current, args.threshold, args.output)
    exit(1 if drift else 0)


if __name__ == "__main__":
    main()
