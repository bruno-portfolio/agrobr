from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/psr"


def compare_catalog(payload: Any, golden: dict[str, Any]) -> dict[str, Any]:
    expected = {item["id"]: item for item in golden["result"]["resources"]}
    problems = []
    if not isinstance(payload, dict) or payload.get("success") is not True:
        return {"status": "mismatch", "problems": ["CKAN sem success=true"]}
    result = payload.get("result")
    resources = result.get("resources") if isinstance(result, dict) else None
    if not isinstance(resources, list) or any(not isinstance(item, dict) for item in resources):
        return {"status": "mismatch", "problems": ["Lista de recursos CKAN ausente ou inválida"]}
    current = {item.get("id"): item for item in resources}
    if len(current) != len(resources) or set(current) != set(expected):
        problems.append("Recursos ausentes, duplicados ou sem decisão")
    for identifier in current.keys() & expected.keys():
        for field in ("url", "format", "name"):
            if current[identifier].get(field) != expected[identifier].get(field):
                problems.append(f"Recurso {identifier}: {field} mudou")
    return {"status": "mismatch" if problems else "ok", "problems": problems}


def compare_csv(path: Path, resource: dict[str, Any], *, encoding: str = "utf-8") -> dict[str, Any]:
    problems: list[str] = []
    widths: Counter[int] = Counter()
    years: Counter[str] = Counter()
    invalid_years: list[int] = []
    rows = 0
    bounds = resource["original"]["family"].split(":", 1)[1].split("-")
    first_year, last_year = int(bounds[0]), int(bounds[-1])
    with path.open(encoding=encoding, newline="") as stream:
        reader = csv.reader(stream, delimiter=";", strict=True)
        headers = next(reader, [])
        if headers != resource["headers"]:
            problems.append("Cabeçalhos publicados diferentes das decisões registradas")
        year_column = headers.index("ANO_APOLICE") if "ANO_APOLICE" in headers else None
        for ordinal, row in enumerate(reader, 1):
            if not row:
                continue
            rows += 1
            widths[len(row)] += 1
            if year_column is None or len(row) != len(headers):
                continue
            year = row[year_column].strip()
            years[year] += 1
            try:
                number = Decimal(year)
                valid = number.is_finite() and number == number.to_integral_value()
                valid = valid and first_year <= number <= last_year
            except InvalidOperation:
                valid = False
            if not valid and len(invalid_years) < 5:
                invalid_years.append(ordinal)
    if set(widths) - {len(headers)}:
        problems.append("Registro com largura divergente do cabeçalho")
    if invalid_years:
        problems.append(f"Ano de apólice inválido nos registros {invalid_years}")
    if rows == 0:
        problems.append("Publicação sem registros")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "headers": headers,
        "rows": rows,
        "width_counts": dict(widths),
        "years": dict(years),
    }


def run(capture_dir: Path, output: Path) -> int:
    manifest = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
    receipts = json.loads((capture_dir / "receipts.json").read_text(encoding="utf-8"))
    by_url = {entry["requested_url"]: entry for entry in receipts}
    checks = []
    for resource in manifest["resources"]:
        receipt = by_url[resource["original"]["requested_url"]]
        path = capture_dir / Path(receipt["file"]).name
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        assert digest == receipt["sha256"]
        checks.append(
            {
                "url": receipt["requested_url"],
                "fetched_at": receipt["fetched_at"],
                "sha256": digest,
                **compare_csv(path, resource, encoding=resource["encoding"]),
            }
        )
    catalog = next(entry for entry in receipts if entry["family"] == "psr:catalog")
    body = (capture_dir / Path(catalog["file"]).name).read_bytes()
    assert hashlib.sha256(body).hexdigest() == catalog["sha256"]
    checks.append(
        {
            "url": catalog["requested_url"],
            "fetched_at": catalog["fetched_at"],
            **compare_catalog(
                json.loads(body), json.loads((GOLDEN / "catalog.json").read_text(encoding="utf-8"))
            ),
        }
    )
    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "mode": "Captured complete HTTP bodies; capture times retained; no new acquisition",
        "checks": checks,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    failed = sum(check["status"] != "ok" for check in checks)
    print(f"{len(checks) - failed} ok / {failed} mismatch")
    return int(failed > 0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Confere estrutura dos CSVs e catálogo PSR")
    parser.add_argument("--capture-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    return run(arguments.capture_dir, arguments.output)


if __name__ == "__main__":
    raise SystemExit(main())
