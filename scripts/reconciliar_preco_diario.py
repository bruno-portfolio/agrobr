from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from agrobr.cepea import client as cepea_client
from agrobr.noticias_agricolas import client as na_client

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "tests/golden_data/reconciliacao_r6_20260918/manifest.json"
PRODUTOS = ["soja", "milho", "boi", "bezerro", "cafe", "cafe_robusta", "trigo", "algodao"]


def _clean(text: str) -> str:
    return " ".join(text.split())


def _table(table: Any) -> tuple[list[str], list[list[str]]]:
    trs = table.find_all("tr")
    header = [_clean(c.get_text(" ", strip=True)) for c in trs[0].find_all(["th", "td"])]
    rows = [
        [_clean(c.get_text(" ", strip=True)) for c in tr.find_all(["td", "th"])] for tr in trs[1:]
    ]
    return header, [r for r in rows if len(r) >= 2]


def inventory_cepea(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    items = []
    for index, title in enumerate(soup.find_all("div", class_="imagenet-table-titulo")):
        header, rows = _table(title.find_next("table"))
        items.append(
            {
                "title": _clean(title.get_text(" ", strip=True)),
                "title_index": index,
                "headers": header,
                "rows": len(rows),
                "newest": rows[0][0] if rows else None,
                "oldest": rows[-1][0] if rows else None,
            }
        )
    return items


def inventory_na(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    items = []
    for index, table in enumerate(soup.find_all("table", class_="cot-fisicas")):
        header, rows = _table(table)
        cot = table.find_parent("div", class_="cotacao")
        fechamento = cot.find("div", class_="fechamento") if cot else None
        items.append(
            {
                "table_index": index,
                "fechamento": _clean(fechamento.get_text(" ", strip=True)) if fechamento else None,
                "headers": header,
                "rows": len(rows),
            }
        )
    return items


def compare_cepea(case: dict[str, Any], items: list[dict[str, Any]]) -> dict[str, Any]:
    problems = []
    counts = Counter(item["title"] for item in items)
    repeated = sorted(title for title, count in counts.items() if count > 1)
    if repeated:
        problems.append(f"título repetido, seleção ambígua: {repeated}")
    by_title = {item["title"]: item for item in items}
    for expected in case["structure"]:
        title = expected["locator"]["title"]
        live = by_title.get(title)
        if expected["estado"] == "mapeada":
            if live is None:
                problems.append(f"tabela mapeada ausente: {title}")
            elif live["headers"] != expected["headers"]:
                problems.append(
                    f"cabeçalho mudou em {title}: {live['headers']} != {expected['headers']}"
                )
        elif live is None:
            problems.append(f"tabela ignorada sumiu: {title}")
    unknown = sorted(set(by_title) - {item["locator"]["title"] for item in case["structure"]})
    if unknown:
        problems.append(f"tabelas novas sem decisão: {unknown}")
    return {"case": case["id"], "status": "mismatch" if problems else "ok", "problems": problems}


def compare_na(case: dict[str, Any], items: list[dict[str, Any]]) -> dict[str, Any]:
    accepted = {tuple(item["headers"]) for item in case["structure"] if item["estado"] == "mapeada"}
    problems = []
    if not items:
        problems.append("nenhuma tabela cot-fisicas na página")
    for item in items:
        if tuple(item["headers"]) not in accepted:
            problems.append(
                f"tabela {item['table_index']} com cabeçalho sem decisão: {item['headers']}"
            )
        if item["fechamento"] is None:
            problems.append(f"tabela {item['table_index']} sem fechamento")
    return {"case": case["id"], "status": "mismatch" if problems else "ok", "problems": problems}


async def run(produtos: list[str], output: Path) -> int:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    cases = {case["id"]: case for case in manifest["cases"]}
    report: dict[str, Any] = {
        "fetched_at": datetime.now(UTC).isoformat(),
        "structure": [],
        "values": [],
    }
    for produto in produtos:
        cepea_html = (await cepea_client.fetch_indicador_page(produto)).html
        items = inventory_cepea(cepea_html)
        report["structure"].append(
            compare_cepea(cases[f"cepea_{produto}"], items) | {"inventory": items}
        )
        report["values"].append(
            {
                "source": "cepea",
                "produto": produto,
                "sha256": hashlib.sha256(cepea_html.encode("utf-8")).hexdigest(),
                "newest": next((i["newest"] for i in items if i["newest"]), None),
            }
        )
        na_html = await na_client.fetch_indicador_page(produto)
        na_items = inventory_na(na_html)
        report["structure"].append(
            compare_na(cases[f"noticias_agricolas_{produto}"], na_items) | {"inventory": na_items}
        )
        report["values"].append(
            {
                "source": "noticias_agricolas",
                "produto": produto,
                "sha256": hashlib.sha256(na_html.encode("utf-8")).hexdigest(),
                "newest": na_items[0]["fechamento"] if na_items else None,
            }
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")
    mismatches = [entry for entry in report["structure"] if entry["status"] != "ok"]
    for entry in report["structure"]:
        print(entry["status"], entry["case"], "; ".join(entry["problems"]))
    print(
        f"{len(report['structure']) - len(mismatches)} ok / {len(mismatches)} mismatch -> {output}"
    )
    return 1 if mismatches else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inventário N1 live do preço diário (CEPEA e Notícias Agrícolas)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / f"reports/reconciliacao_preco_diario_{datetime.now(UTC):%Y%m%d}.json",
    )
    parser.add_argument("--products", nargs="+", choices=PRODUTOS, default=PRODUTOS)
    arguments = parser.parse_args()
    return asyncio.run(run(arguments.products, arguments.output))


if __name__ == "__main__":
    sys.exit(main())
