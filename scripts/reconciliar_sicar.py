from __future__ import annotations

import argparse
import gzip
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts import reconciliar_icmbio as schema_reconciliation

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_r11_20260918/sicar"


def compare_page(data: bytes, schema: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    features: list[Any] = []
    expected = {
        name for name, decision in schema["decisions"].items() if decision["decision"] == "mapped"
    }
    seen: set[str] = set()
    invalid: list[int] = []
    try:
        payload = json.loads(data)
        features = payload["features"]
        if payload["type"] != "FeatureCollection" or payload["numberReturned"] != len(features):
            problems.append("Tipo ou contagem da página incompatível")
        for index, feature in enumerate(features):
            properties = feature["properties"]
            identifier = feature["id"]
            if identifier in seen:
                problems.append("Feature.id repetido")
            seen.add(identifier)
            if set(properties) != expected:
                invalid.append(index)
                continue
            try:
                for field in ("area", "m_fiscal"):
                    value = properties[field]
                    if value is not None and (
                        isinstance(value, bool)
                        or not math.isfinite(float(value))
                        or float(value) < 0
                    ):
                        raise ValueError("Medida inválida")
                for field in ("dat_criacao", "data_atualizacao"):
                    text = properties.get(field)
                    if text is not None and datetime.fromisoformat(text).utcoffset() is None:
                        raise ValueError("Instante sem fuso")
            except (TypeError, ValueError):
                invalid.append(index)
    except (KeyError, TypeError, ValueError) as error:
        problems.append(f"FeatureCollection incompatível: {type(error).__name__}")
    if invalid:
        problems.append(f"Propriedades sem decisão ou valores incompatíveis: {invalid[:10]}")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "features": len(features),
        "unique_feature_ids": len(seen),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="N1 SICAR sobre corpos WFS preservados")
    parser.add_argument("--input-dir", type=Path, default=GOLDEN)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((GOLDEN / "manifest.json").read_bytes())
    schemas = {schema["uf"]: schema for schema in manifest["schemas"]}
    checks = []
    for schema in schemas.values():
        body = gzip.decompress((args.input_dir / schema["file"]).read_bytes())
        checks.append(
            {
                "file": schema["file"],
                **schema_reconciliation.compare_schema(body, schema),
            }
        )
    for case in manifest["cases"]:
        resource = next(item for item in manifest["resources"] if item["id"] == case["resource"])
        for file in resource["pages"]:
            body = gzip.decompress((args.input_dir / file).read_bytes())
            checks.append({"file": file, **compare_page(body, schemas[case["query"]["uf"]])})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "checked_at": datetime.now(UTC).isoformat(),
                "scope": "corpos preservados; 27 XSDs e páginas das seleções atuais completas",
                "checks": checks,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    failed = sum(check["status"] != "ok" for check in checks)
    print(f"{len(checks) - failed} ok / {failed} mismatch")
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
