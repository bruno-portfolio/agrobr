from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/zarc"
TEXT_CODES = (
    "Cod_Cultura",
    "Cod_Clima",
    "Cod_Outros_Manejos",
    "Cod_NM",
    "Cod_Munic",
    "Cod_Meso",
    "Cod_Micro",
)
RISKS = tuple(f"dec{i}" for i in range(1, 37))


def compare_catalog(payload: Any, decisions: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("success") is not True:
        return {"status": "mismatch", "problems": ["CKAN sem success=true"]}
    result = payload.get("result")
    resources = result.get("resources") if isinstance(result, dict) else None
    if not isinstance(resources, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("id"), str) for item in resources
    ):
        return {"status": "mismatch", "problems": ["Recursos CKAN inválidos"]}
    expected = {item["id"]: item for item in decisions}
    current = {item["id"]: item for item in resources}
    problems = []
    if len(current) != len(resources) or set(current) != set(expected):
        problems.append("Recurso ausente, duplicado ou sem decisão")
    for identifier in current.keys() & expected.keys():
        for field in ("url", "name", "format"):
            if current[identifier].get(field) != expected[identifier][field]:
                problems.append(f"Recurso {identifier}: {field} mudou")
    return {"status": "mismatch" if problems else "ok", "problems": problems}


def _invalid_fields(row: dict[str, str], resource: dict[str, Any]) -> list[str]:
    invalid = [field for field, values in resource["domains"].items() if row[field] not in values]
    invalid.extend(field for field in RISKS if row[field] not in {"", "0", "20", "30", "40", "50"})
    invalid.extend(field for field in TEXT_CODES if re.fullmatch(r"[0-9]*", row[field]) is None)
    if re.fullmatch(r"[0-9]{7}", row["geocodigo"]) is None:
        invalid.append("geocodigo")
    if not row["Portaria"].strip():
        invalid.append("Portaria")
    return invalid


def compare_csv(path: Path, resource: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    widths: Counter[int] = Counter()
    drift: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    rows = 0
    try:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream, delimiter=";", strict=True)
            headers = next(reader, [])
            if headers != resource["headers"]:
                return {
                    "status": "mismatch",
                    "problems": ["Cabeçalhos diferem das decisões"],
                    "headers": headers,
                }
            for ordinal, cells in enumerate(reader, 1):
                if not cells:
                    continue
                rows += 1
                widths[len(cells)] += 1
                if len(cells) != len(headers):
                    continue
                invalid = _invalid_fields(dict(zip(headers, cells, strict=True)), resource)
                drift.update(invalid)
                if invalid and len(examples) < 5:
                    examples.append({"record": ordinal, "fields": invalid})
    except (csv.Error, UnicodeError) as exc:
        problems.append(f"CSV inválido: {type(exc).__name__}")
    if set(widths) - {len(resource["headers"])}:
        problems.append("Registro com largura divergente")
    if drift:
        problems.append("Campos com valores fora das decisões registradas")
    if not rows:
        problems.append("CSV sem registros")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "rows": rows,
        "widths": dict(widths),
        "drift_by_field": dict(drift),
        "examples": examples,
    }


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run(capture_dir: Path, output: Path) -> int:
    manifest = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
    receipts = json.loads((capture_dir / "zarc_current_receipts.json").read_text(encoding="utf-8"))
    by_url = {receipt["requested_url"]: receipt for receipt in receipts}
    checks = []
    for resource in manifest["resources"]:
        receipt = by_url[resource["original"]["requested_url"]]
        path = capture_dir / Path(receipt["file"]).name
        digest = _digest(path)
        assert digest == receipt["sha256"]
        checks.append(
            {
                "url": receipt["requested_url"],
                "fetched_at": receipt["received_at"],
                "sha256": digest,
                **compare_csv(path, resource),
            }
        )
    other = json.loads((capture_dir / "receipts.json").read_text(encoding="utf-8"))
    catalog = next(receipt for receipt in other if receipt["family"] == "zarc:catalog")
    path = capture_dir / Path(catalog["file"]).name
    assert _digest(path) == catalog["sha256"]
    checks.append(
        {
            "url": catalog["requested_url"],
            "fetched_at": catalog["fetched_at"],
            **compare_catalog(json.loads(path.read_bytes()), manifest["catalog_decisions"]),
        }
    )
    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "mode": "Complete captured HTTP bodies; original UTC receipt times; no fresh network acquisition",
        "checks": checks,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    failed = sum(check["status"] != "ok" for check in checks)
    print(f"{len(checks) - failed} ok / {failed} mismatch")
    return int(failed > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere catálogo e estrutura completa das tábuas ZARC"
    )
    parser.add_argument("--capture-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    return run(arguments.capture_dir, arguments.output)


if __name__ == "__main__":
    raise SystemExit(main())
