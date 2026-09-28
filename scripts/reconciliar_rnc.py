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
from urllib.parse import urljoin

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_r11_20260918/rnc"
DATE_COLUMNS = {
    "DATA DO REGISTRO",
    "DATA DE VALIDADE DO REGISTRO",
    "INÍCIO DA PROTEÇÃO",
    "TÉRMINO DA PROTEÇÃO",
}
CONDITIONAL_END = "até a emissão do certificado definitivo"


def compare_csv(path: Path, resource: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    widths: Counter[int] = Counter()
    keys: Counter[str] = Counter()
    invalid_dates: list[tuple[int, str]] = []
    rows = 0
    try:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream, delimiter=",", strict=True)
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
                keys[values.get(resource["identity_field"], "").strip()] += 1
                for column in DATE_COLUMNS & values.keys():
                    text = values[column].strip()
                    if not text or (column == "TÉRMINO DA PROTEÇÃO" and text == CONDITIONAL_END):
                        continue
                    try:
                        if re.fullmatch(r"[0-9]{2}/[0-9]{2}/[0-9]{4}", text) is None:
                            raise ValueError("Formato de data incompatível")
                        datetime.strptime(text, "%d/%m/%Y")
                    except ValueError:
                        if len(invalid_dates) < 5:
                            invalid_dates.append((ordinal, column))
    except (csv.Error, UnicodeDecodeError) as error:
        problems.append(f"CSV ilegível: {type(error).__name__}")
    if rows == 0:
        problems.append("CSV sem registros")
    if set(widths) - {len(resource["headers"])}:
        problems.append("Largura de registro incompatível")
    if "" in keys or any(count > 1 for count in keys.values()):
        problems.append("Identificador vazio ou repetido")
    if invalid_dates:
        problems.append(f"Data civil inválida nos registros/colunas {invalid_dates}")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "rows": rows,
        "widths": dict(widths),
        "unique_keys": len(keys),
    }


def html_inventory(data: bytes, base_url: str) -> dict[str, Any]:
    soup = BeautifulSoup(data, "lxml")
    forms = []
    for form in soup.find_all("form"):
        controls = []
        for item in form.find_all(["input", "select", "button", "textarea"]):
            name = item.get("name")
            if name:
                controls.append(
                    {
                        "tag": item.name,
                        "name": str(name),
                        "type": str(item.get("type", "")),
                        "export_format": str(item.get("value", "")) if name == "exportar" else None,
                    }
                )
        forms.append(
            {
                "method": str(form.get("method", "get")).lower(),
                "action": urljoin(base_url, str(form.get("action", ""))),
                "controls": controls,
            }
        )
    for element in soup.select("script,style"):
        element.decompose()
    text = " ".join(soup.stripped_strings)
    phrases = text.count("Sua pesquisa retornou")
    totals = [
        int(value.replace(".", ""))
        for value in re.findall(r"Sua pesquisa retornou\s+([0-9.]+)\s+registros\b", text)
    ]
    return {"forms": forms, "phrases": phrases, "totals": totals}


def compare_html(
    data: bytes,
    expected: bytes,
    base_url: str,
    *,
    csv_rows: int | None = None,
) -> dict[str, Any]:
    current = html_inventory(data, base_url)
    original = html_inventory(expected, base_url)
    problems = []
    if not current["forms"] or current["forms"] != original["forms"]:
        problems.append("Formulários, destinos ou controles diferentes do inventário")
    if current["phrases"] != len(current["totals"]) or len(set(current["totals"])) > 1:
        problems.append("Total publicado ausente, inválido ou conflitante")
    if bool(current["totals"]) != bool(original["totals"]):
        problems.append("Presença do total diferente da publicação inventariada")
    if csv_rows is not None and current["totals"] != [csv_rows]:
        problems.append("Total da pesquisa diferente da população CSV")
    return {"status": "mismatch" if problems else "ok", "problems": problems, "inventory": current}


def run(capture_dir: Path, output: Path) -> int:
    manifest = json.loads((GOLDEN / "manifest.json").read_bytes())
    receipts = json.loads((capture_dir / "receipts.json").read_bytes())
    by_file = {Path(item["file"]).name: item for item in receipts}
    checks = []
    populations = {}
    for resource in manifest["resources"]:
        receipt = by_file[Path(resource["original"]["file"]).name]
        path = capture_dir / Path(receipt["file"]).name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == receipt["sha256"]
        result = compare_csv(path, resource)
        populations[receipt["family"]] = result["rows"]
        checks.append(
            {
                "file": path.name,
                "url": receipt["requested_url"],
                "sha256": receipt["sha256"],
                "fetched_at": receipt["fetched_at"],
                **result,
            }
        )
    for item in manifest["files"]:
        if not item["original"]["file"].endswith(".html"):
            continue
        receipt = by_file[Path(item["original"]["file"]).name]
        body = (capture_dir / Path(receipt["file"]).name).read_bytes()
        assert hashlib.sha256(body).hexdigest() == receipt["sha256"]
        expected = gzip.decompress((GOLDEN / item["golden_file"]).read_bytes())
        csv_rows = populations[receipt["family"]] if receipt["method"] == "POST" else None
        checks.append(
            {
                "file": Path(receipt["file"]).name,
                "url": receipt["requested_url"],
                "sha256": receipt["sha256"],
                "fetched_at": receipt["fetched_at"],
                **compare_html(body, expected, receipt["requested_url"], csv_rows=csv_rows),
            }
        )
    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "mode": "Complete captured CSV/form/search bodies; no new network acquisition; token values excluded from form structure",
        "checks": checks,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    failed = sum(item["status"] != "ok" for item in checks)
    print(f"{len(checks) - failed} ok / {failed} mismatch")
    return int(failed > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere CSVs, formulários e totais RNC/SNPC capturados"
    )
    parser.add_argument("--capture-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    return run(arguments.capture_dir, arguments.output)


if __name__ == "__main__":
    raise SystemExit(main())
