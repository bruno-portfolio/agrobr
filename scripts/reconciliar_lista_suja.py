from __future__ import annotations

import argparse
import csv
import gzip
import importlib
import io
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from html import parser as html_parser
from pathlib import Path
from typing import Any
from urllib import parse

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/mte"


class LinkInventory(html_parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        href = dict(attrs).get("href")
        if tag == "a" and href:
            self.links.append(href)


def compare_portal(data: bytes, expected: dict[str, Any]) -> dict[str, Any]:
    """Compara os links sem o fragmento vazio.

    O `urljoin` do 3.14 mantém o `#` de `href="#"`, e o do 3.11 ao 3.13 o descarta.
    """
    inventory = LinkInventory()
    inventory.feed(data.decode("utf-8"))
    links = [
        parse.urljoin(expected["portal_url"], href).removesuffix("#") for href in inventory.links
    ]
    recorded = [item["url"].removesuffix("#") for item in expected["html_links"]]
    return {"status": "ok" if links == recorded else "mismatch", "links": len(links)}


def read_csv(data: bytes) -> list[list[str]]:
    return list(
        csv.reader(io.StringIO(data.decode("cp1252"), newline=""), delimiter=";", strict=True)
    )


def compare_csv(data: bytes, resource: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    identifiers: set[str] = set()
    rows: list[list[str]] = []
    try:
        rows = read_csv(data)
        if not rows or rows[0] != resource["headers"]:
            problems.append("Cabeçalho com campo ausente, repetido ou sem decisão")
        for row in rows[1:]:
            if len(row) != len(resource["headers"]):
                problems.append("Quantidade de campos incompatível")
                continue
            identifier = row[0].strip()
            if not identifier.isascii() or not identifier.isdigit() or identifier in identifiers:
                problems.append("Identificador vazio, inválido ou repetido")
            identifiers.add(identifier)
            for index in (1, 6):
                value = row[index].strip()
                if value and (not re.fullmatch(r"[0-9]+", value) or int(value) > 2**63 - 1):
                    problems.append("Inteiro incompatível")
            if row[1].strip() and len(row[1].strip()) != 4:
                problems.append("Ano incompatível")
            inclusion = " ".join(row[9].split())
            pattern = r"[0-9]{2}/[0-9]{2}/[0-9]{4}"
            if not re.fullmatch(
                rf"{pattern}(?: a {pattern})?(?:, {pattern}(?: a {pattern})?)*", inclusion
            ):
                problems.append("Inclusão incompatível")
            for value in [row[8].strip(), *re.findall(pattern, inclusion)]:
                if value:
                    datetime.strptime(value, "%d/%m/%Y")
    except (UnicodeError, csv.Error, ValueError):
        problems.append("CSV ou data inválidos")
    if len(rows) <= 1:
        problems.append("População vazia")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "rows": max(len(rows) - 1, 0),
        "unique_ids": len(identifiers),
    }


def compare_companion(data: bytes, primary: bytes, expected: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    try:
        rows = list(
            csv.reader(io.StringIO(data.decode("cp1252"), newline=""), delimiter="\t", strict=True)
        )
        actual = {
            row[0].strip(): [cell.strip() for cell in row]
            for row in rows
            if len(row) == 10 and row[0].strip().isdigit()
        }
        records = {row[0].strip(): [cell.strip() for cell in row] for row in read_csv(primary)[1:]}
        if actual != records:
            problems.append("CSV/TXT com células ou conjuntos de IDs diferentes")
        headers = [
            [" ".join(cell.split()) for cell in row]
            for row in rows
            if row and row[0].strip() == "ID"
        ]
        if headers != [expected["resources"][0]["headers"]]:
            problems.append("Cabeçalho TXT incompatível")
        context = expected["publication"]
        text = data.decode("cp1252")
        updates = set(re.findall(r"Cadastro atualizado em ([0-9]{2}/[0-9]{2}/[0-9]{4})", text))
        actual_dates = {
            datetime.strptime(value, "%d/%m/%Y").date().isoformat() for value in updates
        }
        if actual_dates != {context["registry_updated_at"]} or context["title"] not in text:
            problems.append("Publicação diferente da captura aceita")
        notes = [row[0] for row in rows if row and row[0].startswith("(*")]
        if notes != context["notes"]:
            problems.append("Notas da publicação mudaram")
    except (UnicodeError, csv.Error, ValueError, IndexError):
        problems.append("TXT ou CSV inválido")
    return {"status": "mismatch" if problems else "ok", "problems": problems}


def _pdf_items(page: Any, layout: Any) -> tuple[list[Any], list[Any]]:
    pending, glyphs, rectangles = list(page), [], []
    while pending:
        item = pending.pop()
        if isinstance(item, layout.LTChar):
            glyphs.append(item)
        elif isinstance(item, layout.LTRect):
            rectangles.append(item)
        elif hasattr(item, "__iter__"):
            pending.extend(item)
    return glyphs, rectangles


def _pdf_cell(glyphs: list[Any], bounds: tuple[float, float, float, float]) -> str:
    left, bottom, right, top = bounds
    lines: dict[float, list[Any]] = defaultdict(list)
    for glyph in glyphs:
        if left < (glyph.x0 + glyph.x1) / 2 < right and bottom < (glyph.y0 + glyph.y1) / 2 < top:
            lines[round(glyph.y0, 1)].append(glyph)
    return "\n".join(
        "".join(glyph.get_text() for glyph in sorted(line, key=lambda item: item.x0)).strip()
        for _, line in sorted(lines.items(), reverse=True)
    )


def compare_pdf(data: bytes, expected: dict[str, Any]) -> dict[str, Any]:
    high_level = importlib.import_module("pdfminer.high_level")
    layout = importlib.import_module("pdfminer.layout")
    bounds = expected["column_boundaries"]
    pages = []
    for number, page in enumerate(high_level.extract_pages(io.BytesIO(data)), 1):
        glyphs, rectangles = _pdf_items(page, layout)
        lines = sorted(
            {
                (rect.y0 + rect.y1) / 2
                for rect in rectangles
                if abs(rect.x0 - bounds[0]) < 0.02
                and abs(rect.x1 - bounds[1]) < 0.02
                and 0 < rect.height < 1
            },
            reverse=True,
        )
        header = None
        count = 0
        for top, bottom in zip(lines, lines[1:]):
            cells = [
                _pdf_cell(glyphs, (left, bottom, right, top))
                for left, right in zip(bounds, bounds[1:])
            ]
            if cells[0] == "ID":
                header = cells
            elif cells[0].isdigit():
                count += 1
            elif any(cells):
                return {"status": "mismatch", "problems": [f"Linha sem decisão na página {number}"]}
        pages.append(
            {"page": number, "header": header, "records": count, "page_bbox": list(page.bbox)}
        )
    return {
        "status": "ok" if pages == expected["pages"] else "mismatch",
        "pages": len(pages),
        "rows": sum(page["records"] for page in pages),
    }


def main() -> int:
    arguments = argparse.ArgumentParser(
        description="N1 Lista Suja em corpos preservados; PDF exige o extra [pdf]"
    )
    arguments.add_argument("--input-dir", type=Path, default=GOLDEN)
    arguments.add_argument("--output", type=Path, required=True)
    args = arguments.parse_args()
    manifest = json.loads((GOLDEN / "manifest.json").read_bytes())
    bodies = {
        item["file"]: gzip.decompress((args.input_dir / item["file"]).read_bytes())
        for item in manifest["files"]
    }
    checks = []
    for name, body in bodies.items():
        if name.endswith(".html.gz"):
            result = compare_portal(body, manifest)
        elif name.endswith(".csv.gz"):
            result = compare_csv(body, manifest["resources"][0])
        elif name.endswith(".txt.gz"):
            result = compare_companion(
                body, bodies[manifest["resources"][0]["body_file"]], manifest
            )
        else:
            result = compare_pdf(body, manifest["resources"][1])
        checks.append({"file": name, **result})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "checked_at": datetime.now(UTC).isoformat(),
                "scope": "corpos locais preservados; sem nova aquisição",
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
