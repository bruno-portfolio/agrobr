from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import xlrd
from openpyxl.utils import cell
from xlrd import biffh, compdoc

from agrobr.conab._serie_historica import client, parser
from agrobr.exceptions import ParseError, SourceUnavailableError


def _published_period(value: Any) -> bool:
    text = str(int(value) if isinstance(value, float) and value.is_integer() else value).strip()
    return re.match(r"^(?:19|20)\d{2}(?:/\d{2,4})?", text) is not None


def _period_columns(sheet: Any) -> tuple[int | None, list[dict[str, Any]]]:
    for row in range(min(20, sheet.nrows)):
        decisions = parser.resolve_period_columns(sheet.row_values(row))
        columns = [
            {
                "column": column,
                "cell": f"{cell.get_column_letter(column + 1)}{row + 1}",
                "raw": value,
                "normalized": decisions[column].safra if column in decisions else None,
                "estado": decisions[column].estado if column in decisions else "desconhecida",
                "motivo": decisions[column].motivo
                if column in decisions
                else "periodo_sem_decisao",
            }
            for column, value in enumerate(sheet.row_values(row))
            if _published_period(value)
        ]
        if len(columns) >= 2:
            return row, columns
    return None, []


def _sheet_sample(
    sheet: Any, row: int | None, periods: list[dict[str, Any]]
) -> dict[str, Any] | None:
    if row is None:
        return None
    for period in reversed(periods):
        if period["estado"] != "mapeada":
            continue
        for index in range(row + 1, sheet.nrows):
            uf = str(sheet.cell_value(index, 0)).strip()
            value = sheet.cell_value(index, period["column"])
            if len(uf) == 2 and uf.isalpha() and isinstance(value, (int, float)) and value != 0:
                return {
                    "uf": uf,
                    "row_label_cell": f"A{index + 1}",
                    "cell": f"{cell.get_column_letter(period['column'] + 1)}{index + 1}",
                    "raw_period": period["raw"],
                    "normalized_period": period["normalized"],
                    "raw_value": value,
                }
    return None


def _period_policy(columns: list[dict[str, Any]]) -> tuple[list[tuple[str, Any]], list[str]]:
    states = sorted({(column["estado"], column["motivo"]) for column in columns})
    suffixes = []
    for column in columns:
        if column["estado"] != "ignorada":
            continue
        label = str(column["raw"]).strip()
        suffix = re.sub(r"^\d{4}(?:/\d{2,4})?", "", label).strip()
        suffixes.append(" ".join(suffix.casefold().split()))
    return states, suffixes


def _period_differences(
    sheet: dict[str, Any], reference: dict[str, Any], product: str
) -> list[str]:
    expected = reference["period_columns"].get(sheet["name"])
    if expected is None:
        return [f"period_reference_missing:{sheet['name']}"]
    actual = sheet["periods"]
    if product == reference["product"]:
        equal = actual == expected
    else:
        equal = _period_policy(actual) == _period_policy(expected)
    return [] if equal else [f"period_mapping_differs:{sheet['name']}"]


def inventory_workbook(raw: bytes) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    book = xlrd.open_workbook(file_contents=raw)
    structures = []
    samples = {}
    for sheet in book.sheets():
        header, periods = _period_columns(sheet)
        structures.append(
            {
                "name": sheet.name,
                "header_row": header + 1 if header is not None else None,
                "published_unit": str(sheet.cell_value(header - 1, 0))
                if header is not None and header > 0
                else None,
                "periods": periods,
            }
        )
        samples[sheet.name] = _sheet_sample(sheet, header, periods)
    return structures, samples


def _unit_difference(
    sheet: dict[str, Any], expected: str | None, exception: dict[str, Any] | None
) -> str | None:
    published = sheet["published_unit"]
    if exception is not None:
        if published != exception["published_unit"] or expected != exception["canonical_unit"]:
            return f"unit_exception_changed:{sheet['name']}"
    elif published != expected:
        return f"published_unit_differs:{sheet['name']}"
    return None


def reconcile_workbook(
    raw: bytes,
    product: str,
    reference: dict[str, Any],
    unit_exceptions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    structures, samples = inventory_workbook(raw)
    decisions = parser.resolve_sheets(product, [sheet["name"] for sheet in structures])
    mapped = {name: decision.model_dump() for name, decision in decisions.items()}
    differences = []
    if mapped != reference["sheets"]:
        differences.append("sheet_mapping_differs_from_reviewed_manifest")
    expected_units = {
        source["sheet"]: source["published_unit"]
        for sample in reference["samples"]
        for source in sample["raw_cells"]
    }
    annual = "/" not in reference["samples"][0]["key"]["safra"]
    exceptions = {
        item["sheet"]: item for item in unit_exceptions or [] if item["product"] == product
    }
    applied_exceptions = []
    for sheet in structures:
        decision = decisions[sheet["name"]]
        differences.extend(_period_differences(sheet, reference, product))
        if decision.estado == "desconhecida":
            differences.append(f"unknown_sheet:{sheet['name']}")
        if decision.estado != "mapeada":
            continue
        exception = exceptions.get(sheet["name"])
        unit_difference = _unit_difference(sheet, expected_units.get(sheet["name"]), exception)
        if unit_difference is not None:
            differences.append(unit_difference)
        elif exception is not None:
            applied_exceptions.append(exception)
        if any(("/" not in (period["normalized"] or "")) != annual for period in sheet["periods"]):
            differences.append(f"period_family_differs:{sheet['name']}")
        if (
            sheet["header_row"] is None
            or samples[sheet["name"]] is None
            or any(period["normalized"] is None for period in sheet["periods"])
        ):
            differences.append(f"unresolved_period_or_sample:{sheet['name']}")
    return {
        "status": "mismatch" if differences else "ok",
        "reference_case": reference["id"],
        "reference_layout": reference["layout"],
        "differences": differences,
        "applied_unit_exceptions": applied_exceptions,
        "structure": {"sheets": structures, "decisions": mapped},
        "values": {"samples": samples},
    }


async def sweep(
    products: list[str], manifest: dict[str, Any], output: Path, delay: float = 0.2
) -> dict[str, Any]:
    references = {case["product"]: case for case in manifest["cases"]}
    payload_dir = output.parent / f"{output.stem}_payloads"
    payload_dir.mkdir(parents=True, exist_ok=True)
    downloaded: dict[str, tuple[bytes, dict[str, Any]]] = {}
    results = []
    for product in products:
        result: dict[str, Any] = {"product": product}
        try:
            url = client.get_xls_url(product)
            result["url"] = url
            if url not in downloaded:
                stream, metadata = await client.download_xls(product)
                raw = stream.getvalue()
                path = payload_dir / f"{product}.xls"
                path.write_bytes(raw)
                receipt = {
                    "source_url": metadata["url"],
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "bytes": len(raw),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "payload": str(path),
                }
                downloaded[url] = (raw, receipt)
                if delay:
                    await asyncio.sleep(delay)
            raw, receipt = downloaded[url]
            result["acquisition"] = receipt
            reference = references.get(product, references["soja"])
            result.update(
                reconcile_workbook(raw, product, reference, manifest.get("unit_exceptions", []))
            )
        except (
            httpx.HTTPError,
            SourceUnavailableError,
            ParseError,
            OSError,
            biffh.XLRDError,
            compdoc.CompDocError,
        ) as exc:
            result.update(status="error", error_type=type(exc).__name__, error=str(exc))
        results.append(result)
        print(json.dumps({"product": product, "status": result["status"]}), flush=True)
    report = {
        "format_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "validation_boundary": "N1: raw xlrd workbook inventory and reviewed sheet mapping",
        "baseline_approved": False,
        "products_count": len(products),
        "downloaded_urls_count": len(downloaded),
        "failed_products": [result["product"] for result in results if result["status"] != "ok"],
        "unit_exception_products": [
            result["product"] for result in results if result.get("applied_unit_exceptions")
        ],
        "forecast_columns_count": sum(
            period["motivo"] == "previsao"
            for result in results
            for sheet in result.get("structure", {}).get("sheets", [])
            for period in sheet["periods"]
        ),
        "results": results,
    }
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    arguments = argparse.ArgumentParser(
        description="Reconcilia abas e períodos da série histórica CONAB"
    )
    arguments.add_argument("--output", type=Path)
    arguments.add_argument("--products", nargs="+", choices=sorted(client._PRODUCT_REGISTRY))
    arguments.add_argument("--delay", type=float, default=0.2)
    arguments.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "tests/golden_data/conab/serie_historica_20260917/manifest.json",
    )
    args = arguments.parse_args()
    if args.delay < 0:
        arguments.error("--delay deve ser não negativo")
    output = args.output or Path(
        f"reports/reconciliacao_serie_historica_{datetime.now(UTC):%Y%m%d}.json"
    )
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if manifest.get("format_version") != 1:
        arguments.error("manifesto deve usar format_version=1")
    products = list(dict.fromkeys(args.products or sorted(client._PRODUCT_REGISTRY)))
    report = asyncio.run(sweep(products, manifest, output, args.delay))
    return int(bool(report["failed_products"]))


if __name__ == "__main__":
    raise SystemExit(main())
