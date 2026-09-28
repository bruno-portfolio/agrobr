from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_r11_20260918/anp"


class _WorkbookLinks(HTMLParser):
    def __init__(self, base: str) -> None:
        super().__init__()
        self.base = base
        self.links: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for name, value in attrs:
            if name == "href" and value:
                url = urljoin(self.base, value)
                path = urlsplit(url).path.lower()
                if "/shlp/" in path and path.endswith((".xlsx", ".xls")):
                    self.links.add(url)


def compare_catalog(html: str, catalog: dict[str, Any]) -> dict[str, Any]:
    links = _WorkbookLinks(catalog["url"])
    links.feed(html)
    expected = {url for url in catalog["all_workbook_links"] if "/shlp/" in url}
    missing, unknown = sorted(expected - links.links), sorted(links.links - expected)
    return {
        "status": "mismatch" if missing or unknown else "ok",
        "missing_links": missing,
        "unclassified_links": unknown,
        "classified_links": len(expected & links.links),
    }


def workbook_profile(content: bytes) -> dict[str, Any]:
    profiles: list[dict[str, Any]] = []
    with pd.ExcelFile(BytesIO(content), engine="calamine") as book:
        for name in book.sheet_names:
            frame = book.parse(name, header=None, dtype=object, keep_default_na=False)
            header_row = next(
                (
                    index
                    for index, row in enumerate(frame.iloc[:30].itertuples(index=False, name=None))
                    if "DATA INICIAL" in row and "PRODUTO" in row
                ),
                None,
            )
            if header_row is None:
                profiles.append({"sheet": name, "header_row": None})
                continue
            headers = list(frame.iloc[header_row])
            while headers and headers[-1] == "":
                headers.pop()
            data = frame.iloc[header_row + 1 :, : len(headers)].copy()
            data.columns = headers
            data = data[
                [
                    any(value != "" for value in row)
                    for row in data.itertuples(index=False, name=None)
                ]
            ]
            products = data["PRODUTO"].astype(str).unique().tolist() if "PRODUTO" in data else []
            units = (
                data[["PRODUTO", "UNIDADE DE MEDIDA"]].drop_duplicates().values.tolist()
                if "UNIDADE DE MEDIDA" in data
                else []
            )
            profiles.append(
                {
                    "sheet": name,
                    "header_row": header_row + 1,
                    "headers": headers,
                    "rows": len(data),
                    "products": sorted(products),
                    "product_units": units,
                    "states": sorted(data["ESTADO"].astype(str).unique().tolist())
                    if "ESTADO" in data
                    else [],
                }
            )
    return {"sheets": profiles, "sha256": hashlib.sha256(content).hexdigest()}


def compare_workbook(content: bytes, resource: dict[str, Any]) -> dict[str, Any]:
    profile = workbook_profile(content)
    expected = resource["layout"]
    problems = []
    names = [sheet["sheet"] for sheet in profile["sheets"]]
    if names != [expected["sheet"]]:
        problems.append(f"abas publicadas sem correspondência: {names}")
    for sheet in profile["sheets"]:
        if sheet["sheet"] != expected["sheet"]:
            continue
        if sheet["header_row"] != expected["header_row"]:
            problems.append("posição do cabeçalho mudou ou cabeçalho ausente")
        headers = [value for value in expected["headers"] if value is not None]
        if sheet.get("headers") != headers:
            problems.append("colunas publicadas diferentes das decisões registradas")
        products = set(sheet.get("products", []))
        if products - set(expected["products"]):
            problems.append("produto publicado sem decisão")
        if not {"OLEO DIESEL", "OLEO DIESEL S10"} <= products:
            problems.append("família de diesel ausente")
        for product, unit in sheet.get("product_units", []):
            allowed = {"R$/l"}
            if product == "GLP":
                allowed = {"R$/13Kg", "R$/13kg"}
            elif product == "GNV":
                allowed = {"R$/m3", "R$/m³"}
            if unit not in allowed:
                problems.append(f"unidade publicada sem decisão: {product} = {unit}")
        if set(sheet.get("states", [])) - set(expected["states"]):
            problems.append("UF publicada sem decisão")
    return {"status": "mismatch" if problems else "ok", "problems": problems, **profile}


def run(capture_dir: Path, output: Path) -> int:
    manifest = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
    receipts = json.loads((capture_dir / "receipts.json").read_text(encoding="utf-8"))
    by_url = {receipt["requested_url"]: receipt for receipt in receipts}
    results = []
    for resource in manifest["resources"]:
        url = resource["original"]["requested_url"]
        receipt = by_url[url]
        path = capture_dir / Path(receipt["file"]).name
        content = path.read_bytes()
        assert hashlib.sha256(content).hexdigest() == receipt["sha256"]
        results.append(
            {"url": url, "fetched_at": receipt["fetched_at"], **compare_workbook(content, resource)}
        )
    catalog = manifest["catalog"]
    receipt = by_url[catalog["requested_url"]]
    body = (capture_dir / Path(receipt["file"]).name).read_bytes()
    assert hashlib.sha256(body).hexdigest() == receipt["sha256"]
    results.append(
        {
            "url": catalog["requested_url"],
            "fetched_at": receipt["fetched_at"],
            **compare_catalog(body.decode("utf-8"), catalog),
        }
    )
    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "mode": "captured HTTP bodies; capture times retained; no new network request",
        "checks": results,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    failed = sum(result["status"] != "ok" for result in results)
    print(f"{len(results) - failed} ok / {failed} mismatch")
    return int(failed > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere catálogo e layouts de preços semanais ANP"
    )
    parser.add_argument("--capture-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    return run(arguments.capture_dir, arguments.output)


if __name__ == "__main__":
    raise SystemExit(main())
