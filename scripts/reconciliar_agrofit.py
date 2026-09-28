from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_r11_20260918/agrofit"
TAIL = re.compile(r"\(([^()]*)\)(?=\s*(?:\+|$))")


def concentration_label(text: str) -> tuple[str, str]:
    scientific = re.fullmatch(r"[0-9.,]+\s+x\s+10\^[+-]?[0-9]+\s+(.+)", text.strip())
    scalar = re.fullmatch(r"[0-9.,]+\s+(.+)", text.strip())
    found = scientific or scalar
    return ("published_suffix", found[1]) if found else ("unresolved_literal", text)


def inventory_csv(path: Path, resource: dict[str, Any]) -> dict[str, Any]:
    problems = []
    widths: Counter[int] = Counter()
    identities: Counter[str] = Counter()
    labels: Counter[tuple[str, str]] = Counter()
    seen_compositions = set()
    unknown_structure: list[int] = []
    rows = 0
    field = (
        "INGREDIENTE_ATIVO"
        if resource["original"]["family"] == "agrofit_formulados"
        else "INGREDIENTE_ATIVO(GRUPO_QUIMICI)(CONCENTRACAO)"
    )
    try:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream, delimiter=";", strict=True)
            headers = next(reader, [])
            if headers != resource["headers"] or len(headers) != len(set(headers)):
                problems.append("Cabeçalhos ausentes, duplicados ou sem decisão")
            for ordinal, row in enumerate(reader, 1):
                if not row:
                    continue
                rows += 1
                widths[len(row)] += 1
                if len(row) != len(headers):
                    continue
                values = dict(zip(headers, row, strict=True))
                identities[values.get(resource["identity_field"], "").strip()] += 1
                composition = values.get(field, "")
                if composition in seen_compositions:
                    continue
                seen_compositions.add(composition)
                groups = TAIL.findall(composition)
                if (not groups or composition.count("(") != composition.count(")")) and len(
                    unknown_structure
                ) < 5:
                    unknown_structure.append(ordinal)
                labels.update(concentration_label(value) for value in groups)
    except (csv.Error, UnicodeDecodeError) as error:
        problems.append(f"CSV ilegível: {type(error).__name__}")
    if not rows:
        problems.append("CSV sem registros")
    if set(widths) - {len(resource["headers"])}:
        problems.append("Largura CSV incompatível")
    if "" in identities:
        problems.append("Registro de produto vazio")
    if unknown_structure:
        problems.append(f"Estrutura de composição sem decisão nos registros {unknown_structure}")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "source_rows": rows,
        "unique_products": len(identities),
        "widths": dict(widths),
        "unique_compositions": len(seen_compositions),
        "concentration_labels": [
            {"kind": kind, "label": label, "count_in_unique_compositions": count}
            for (kind, label), count in sorted(labels.items())
        ],
    }


def compare_csv(path: Path, resource: dict[str, Any]) -> dict[str, Any]:
    result = inventory_csv(path, resource)
    expected = {(item["kind"], item["label"]) for item in resource["concentration_decisions"]}
    actual = {(item["kind"], item["label"]) for item in result["concentration_labels"]}
    unknown = sorted(actual - expected)
    if unknown:
        result["status"] = "mismatch"
        result["problems"].append(f"Rótulos/expressões de concentração sem decisão: {unknown}")
    return result


def compare_catalog(payload: Any, original: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("success") is not True:
        return {"status": "mismatch", "problems": ["CKAN sem success=true"]}
    result = payload.get("result")
    resources = result.get("resources") if isinstance(result, dict) else None
    if not isinstance(resources, list) or not all(isinstance(item, dict) for item in resources):
        return {"status": "mismatch", "problems": ["Lista de recursos CKAN inválida"]}
    expected = {item["id"]: item for item in original["result"]["resources"]}
    current = {item.get("id"): item for item in resources}
    problems = []
    if len(current) != len(resources) or set(current) != set(expected):
        problems.append("Recurso ausente, duplicado ou sem decisão")
    for identifier in current.keys() & expected.keys():
        if any(
            current[identifier].get(field) != expected[identifier].get(field)
            for field in ("name", "url", "format")
        ):
            problems.append(f"Identidade/formato do recurso {identifier} alterados")
    return {"status": "mismatch" if problems else "ok", "problems": problems}


def run(capture_dir: Path, output: Path) -> int:
    manifest = json.loads((GOLDEN / "manifest.json").read_bytes())
    receipts = json.loads((capture_dir / "receipts.json").read_bytes())
    by_family = {receipt["family"]: receipt for receipt in receipts}
    checks = []
    for resource in manifest["resources"]:
        receipt = by_family[resource["original"]["family"]]
        path = capture_dir / Path(receipt["file"]).name
        with path.open("rb") as stream:
            assert hashlib.file_digest(stream, "sha256").hexdigest() == receipt["sha256"]
        checks.append(
            {
                "file": path.name,
                "url": receipt["url"],
                "sha256": receipt["sha256"],
                "fetched_at": receipt["fetched_at"],
                **compare_csv(path, resource),
            }
        )
    receipt = by_family["agrofit_catalog"]
    body = (capture_dir / Path(receipt["file"]).name).read_bytes()
    assert hashlib.sha256(body).hexdigest() == receipt["sha256"]
    original = json.loads(gzip.decompress((GOLDEN / "agrofit_catalog_00.json.gz").read_bytes()))
    checks.append(
        {
            "file": Path(receipt["file"]).name,
            "url": receipt["url"],
            "sha256": receipt["sha256"],
            "fetched_at": receipt["fetched_at"],
            **compare_catalog(json.loads(body), original),
        }
    )
    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "mode": "Complete captured CSV/catalogue bodies; no new acquisition; unit labels preserve published spelling and unresolved expressions remain explicit",
        "checks": checks,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    failed = sum(item["status"] != "ok" for item in checks)
    print(f"{len(checks) - failed} ok / {failed} mismatch")
    return int(failed > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere estrutura, unidades e catálogo Agrofit capturados"
    )
    parser.add_argument("--capture-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    return run(arguments.capture_dir, arguments.output)


if __name__ == "__main__":
    raise SystemExit(main())
