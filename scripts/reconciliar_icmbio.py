from __future__ import annotations

import argparse
import csv
import io
import json
import math
import re
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_r11_20260918/icmbio"
SEQUENCE_ELEMENTS = (
    ".//{http://www.w3.org/2001/XMLSchema}sequence/{http://www.w3.org/2001/XMLSchema}element"
)


def compare_schema(data: bytes, expected: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    elements = []
    try:
        root = ET.fromstring(data)
        if root.tag != "{http://www.w3.org/2001/XMLSchema}schema":
            problems.append("Documento não é um schema XSD")
        if root.get("targetNamespace") != expected["target_namespace"]:
            problems.append("Namespace diferente da camada capturada")
        elements = [element.attrib for element in root.findall(SEQUENCE_ELEMENTS)]
        if not elements or elements != expected["elements"]:
            problems.append("Propriedades, tipos, ordem ou nulabilidade mudaram")
    except ET.ParseError:
        problems.append("XML inválido")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "properties": len(elements),
    }


def compare_csv(data: bytes, expected: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    keys: Counter[str] = Counter()
    invalid: list[int] = []
    count = 0
    try:
        reader = csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""), strict=True)
        header = next(reader, [])
        if header != expected["headers"] or len(set(header)) != len(header):
            problems.append("Colunas ausentes, duplicadas ou sem decisão")
        for count, row in enumerate(reader, 1):
            if len(row) != len(header):
                invalid.append(count)
                continue
            values = dict(zip(header, row, strict=True))
            keys[values.get("cnuc", "")] += 1
            try:
                area, year = values["areahaalb"], values["criacaoano"]
                if area and not math.isfinite(float(area)):
                    raise ValueError("Área não finita")
                if year and (
                    not re.fullmatch(r"[+-]?[0-9]+", year) or not -(2**63) <= int(year) <= 2**63 - 1
                ):
                    raise ValueError("Ano incompatível com Int64")
                if values["grupouc"].upper() not in {"PI", "US"}:
                    raise ValueError("Grupo sem decisão")
            except (KeyError, ValueError):
                invalid.append(count)
    except (UnicodeDecodeError, csv.Error) as error:
        problems.append(f"CSV inválido: {type(error).__name__}")
    if invalid:
        problems.append(f"Registros incompatíveis: {invalid[:10]}")
    if not count or "" in keys or any(value > 1 for value in keys.values()):
        problems.append("População vazia ou CNUC vazio/repetido")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "rows": count,
        "unique_codes": len(keys),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="N1 ICMBio sobre corpos CSV/XSD preservados")
    parser.add_argument("--input-dir", type=Path, default=GOLDEN)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((GOLDEN / "manifest.json").read_bytes())
    checks = [
        {
            "file": resource["file"],
            **compare_csv((args.input_dir / resource["file"]).read_bytes(), resource),
        }
        for resource in manifest["resources"]
    ]
    schema = manifest["schema"]
    checks.append(
        {
            "file": schema["file"],
            **compare_schema((args.input_dir / schema["file"]).read_bytes(), schema),
        }
    )
    result = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": "corpos locais; não é nova aquisição de rede",
        "checks": checks,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    failed = sum(check["status"] != "ok" for check in checks)
    print(f"{len(checks) - failed} ok / {failed} mismatch")
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
