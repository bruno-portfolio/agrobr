from __future__ import annotations

import argparse
import asyncio
import bisect
import collections
import csv
import ctypes
import hashlib
import io
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pandas as pd

WFS = "https://cmr.funai.gov.br/geoserver/ows"
LAYER = "CMR-PUBLICO:lim_quilombolas_a"
PAGE = "https://www.gov.br/incra/pt-br/assuntos/governanca-fundiaria/quilombolas"
PROPERTIES = (
    "co_sr",
    "nu_processo",
    "no_comunidade",
    "no_municipio",
    "sg_uf",
    "dt_publica",
    "dt_public1",
    "nu_familia",
    "dt_titulo",
    "nu_area_ha",
    "no_responsavel",
    "no_esfera",
    "dt_cadastro",
    "cd_quilomb",
    "cd_sipra",
    "ds_descricao",
    "st_titulad",
    "dt_decreto",
    "tp_levanta",
    "nr_escalao",
    "ds_fase",
)
ALIASES = {
    "cd_quilomb": "codigo",
    "no_comunidade": "nome",
    "no_municipio": "municipio",
    "sg_uf": "uf",
    "nu_area_ha": "area_ha",
    "nu_familia": "familias",
    "ds_fase": "fase",
    "st_titulad": "titulado",
    "dt_publica": "data_publicacao",
    "dt_titulo": "data_titulo",
    "co_sr": "regional",
    "nu_processo": "processo",
    "dt_public1": "data_publicacao_2",
    "no_responsavel": "responsavel",
    "no_esfera": "esfera",
    "dt_cadastro": "data_cadastro",
    "cd_sipra": "codigo_sipra",
    "ds_descricao": "descricao",
    "dt_decreto": "data_decreto",
    "tp_levanta": "tipo_levantamento",
    "nr_escalao": "escala",
}
ADMINISTRATIVE = (
    "regional",
    "numero_publicado",
    "processo",
    "comunidade",
    "municipio",
    "area_ha_texto",
    "familias_texto",
    "edital_rtid_1",
    "edital_rtid_2",
    "retificacao_edital_1",
    "retificacao_edital_2",
    "portaria",
    "retificacao_portaria",
    "decreto",
    "titulo",
)
NUP = re.compile(r"(?<![0-9])[0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2}(?![0-9])")
RELATION = (
    "estado_vinculo",
    "referencia_literal",
    "perimetro_posicao",
    "perimetro_referencia_posicao",
    "administrativo_referencia_posicao",
    "administrativo_numero_publicado",
)


def wfs_url(output_format: str) -> str:
    return str(
        httpx.URL(
            WFS,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": LAYER,
                "outputFormat": output_format,
                "propertyName": ",".join(PROPERTIES),
                "count": "5000",
                "startIndex": "0",
                "sortBy": "cd_quilomb A,nu_processo A,no_comunidade A",
            },
        )
    )


def capture(directory: Path) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=True)
    receipts: dict[str, Any] = {}
    headers = {
        "User-Agent": "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
    }
    with httpx.Client(timeout=180, headers=headers, follow_redirects=False) as client:
        targets = [("wfs.json", wfs_url("application/json")), ("wfs.csv", wfs_url("csv"))]
        targets.append(("publisher.html", PAGE))
        for name, url in targets:
            receipts[name] = fetch(client, url, directory / name)
        links = sorted(
            set(
                re.findall(
                    r'href="([^"]*andamento_dos_processos_quilombolas[^"]*)"',
                    (directory / "publisher.html").read_text(encoding="utf-8", errors="replace"),
                )
            )
        )
        receipts["links"] = links
        if len(links) == 1:
            receipts["publication.pdf"] = fetch(
                client, str(httpx.URL(PAGE).join(links[0])), directory / "publication.pdf"
            )
    return receipts


def fetch(client: httpx.Client, url: str, path: Path) -> dict[str, Any]:
    requested = datetime.now(UTC).isoformat()
    response = client.get(url)
    path.write_bytes(response.content)
    return {
        "url": url,
        "status": response.status_code,
        "bytes": len(response.content),
        "sha256": hashlib.sha256(response.content).hexdigest(),
        "requested_at": requested,
        "received_at": datetime.now(UTC).isoformat(),
    }


def wfs_features(body: bytes) -> list[dict[str, Any]]:
    envelope = json.loads(body)
    features: list[dict[str, Any]] = envelope["features"]
    if envelope.get("numberMatched") != len(features):
        raise ValueError("Captura JSON não trouxe toda a população")
    return features


def csv_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return repr(value)
    return str(value)


def compare_json_csv(features: list[dict[str, Any]], body: bytes) -> dict[str, Any]:
    rows = list(csv.DictReader(io.StringIO(body.decode("utf-8-sig"), newline="")))
    by_id = {row["FID"]: row for row in rows}
    problems = []
    if len(rows) != len(features) or set(by_id) != {feature["id"] for feature in features}:
        problems.append("JSON e CSV com identificadores diferentes")
    cells = 0
    for feature in features:
        row = by_id.get(feature["id"], {})
        for name in PROPERTIES:
            value = feature["properties"][name]
            expected = csv_text(value)
            observed = row.get(name, "")
            if name == "dt_cadastro":
                expected = expected.removesuffix("Z")
            if expected == "0001-01-01":
                expected = "-0002-12-29"
            if name == "nu_area_ha" and value is not None and observed:
                equal = abs(float(observed) - value) <= 5e-5
            else:
                equal = observed == expected
            cells += 1
            if not equal and len(problems) < 20:
                problems.append(f"{feature['id']} {name}: JSON {expected!r} × CSV {observed!r}")
    return {"status": "mismatch" if problems else "ok", "cells": cells, "problems": problems}


def source_date(value: str | None, *, timestamp: bool = False) -> pd.Timestamp | None:
    if not value or value == "0001-01-01":
        return None
    formats = (
        (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        )
        if timestamp
        else ("%Y-%m-%d",)
    )
    for date_format in formats:
        try:
            parsed = datetime.strptime(value, date_format)
        except ValueError:
            continue
        if timestamp:
            return pd.Timestamp(
                parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
            )
        return pd.Timestamp(parsed) if 1900 <= parsed.year <= 2099 else None
    return None


def compare_quilombolas(frame: pd.DataFrame, features: list[dict[str, Any]]) -> dict[str, Any]:
    by_id = {feature["id"]: feature for feature in features}
    problems = []
    if len(frame) != len(features) or set(frame["feature_id"]) != set(by_id):
        problems.append(f"agrobr {len(frame)} linhas × fonte {len(features)}")
    cells = 0
    for record in frame.to_dict("records"):
        feature = by_id.get(record["feature_id"])
        if feature is None:
            continue
        for raw, column in ALIASES.items():
            expected = feature["properties"][raw]
            if raw in {"dt_publica", "dt_public1", "dt_titulo", "dt_decreto", "dt_cadastro"}:
                expected = source_date(expected, timestamp=raw == "dt_cadastro")
            value = record[column]
            observed = None if pd.isna(value) else value
            cells += 1
            if observed != expected and len(problems) < 20:
                problems.append(f"{record['feature_id']} {column}: {observed!r} × {expected!r}")
    return {"status": "mismatch" if problems else "ok", "cells": cells, "problems": problems}


def _utf16(getter: Any, *args: Any) -> str:
    size = getter(*args, None, 0)
    if size <= 2:
        return ""
    buffer = ctypes.create_string_buffer(size)
    getter(*args, ctypes.cast(buffer, getter.argtypes[len(args)]), size)
    return buffer.raw[: size - 2].decode("utf-16-le")


def _children(raw: Any, element: Any) -> list[Any]:
    found = []
    for index in range(raw.FPDF_StructElement_CountChildren(element)):
        child = raw.FPDF_StructElement_GetChildAtIndex(element, index)
        if child:
            found.append(child)
    return found


def _mcids(raw: Any, element: Any) -> list[int]:
    own = [
        raw.FPDF_StructElement_GetMarkedContentIdAtIndex(element, index)
        for index in range(raw.FPDF_StructElement_GetMarkedContentIdCount(element))
    ]
    nested = [mcid for child in _children(raw, element) for mcid in _mcids(raw, child)]
    return [mcid for mcid in own + nested if mcid >= 0]


def _tables(raw: Any, element: Any) -> list[Any]:
    if _utf16(raw.FPDF_StructElement_GetType, element) == "Table":
        return [element]
    return [table for child in _children(raw, element) for table in _tables(raw, child)]


def _cell_text(objects: list[dict[str, Any]]) -> str:
    lines: list[list[Any]] = []
    for item in sorted(objects, key=lambda obj: (-obj["bounds"][3], obj["bounds"][0])):
        bottom, top = item["bounds"][1], item["bounds"][3]
        if lines:
            line_bottom, line_top = lines[-1][0]
            if min(top, line_top) - max(bottom, line_bottom) >= 0.5 * min(
                top - bottom, line_top - line_bottom
            ):
                lines[-1][0] = (min(bottom, line_bottom), max(top, line_top))
                lines[-1][1].append(item)
                continue
        lines.append([(bottom, top), [item]])
    rendered = []
    for _, items in lines:
        text, previous = "", None
        for obj in sorted(items, key=lambda obj: obj["bounds"][0]):
            if previous is not None and not text[-1:].isspace() and obj["bounds"][0] - previous > 1:
                text += " "
            text += obj["text"]
            previous = obj["bounds"][2]
        text = " ".join(text.split())
        if text:
            rendered.append(text)
    return "\n".join(rendered)


def pdfium_publication(content: bytes) -> dict[str, Any]:
    import pypdfium2 as pdfium
    import pypdfium2.raw as raw

    pdf = pdfium.PdfDocument(content)
    borders: list[float] = []
    records: list[dict[str, Any]] = []
    declared: list[int] = []
    regional = None
    last_text = ""
    for page in pdf:
        lines: collections.Counter[float] = collections.Counter()
        texts: dict[int, list[dict[str, Any]]] = collections.defaultdict(list)
        textpage = page.get_textpage()
        for obj in page.get_objects():
            left, bottom, right, top = obj.get_bounds()
            if obj.type == raw.FPDF_PAGEOBJ_PATH and right - left < 2 and top - bottom > 5:
                lines[round((left + right) / 2, 1)] += 1
            elif obj.type == raw.FPDF_PAGEOBJ_TEXT:
                texts[raw.FPDFPageObj_GetMarkedContentID(obj.raw)].append(
                    {
                        "text": _utf16(raw.FPDFTextObj_GetText, obj.raw, textpage.raw),
                        "bounds": (left, bottom, right, top),
                    }
                )
        if not borders:
            borders = sorted(x for x, count in lines.items() if count >= 3)
            if len(borders) != 16:
                raise ValueError(f"Grade sem 16 bordas: {borders}")
        last_text = textpage.get_text_range()
        tree = raw.FPDF_StructTree_GetForPage(page.raw)
        tables = [
            table
            for index in range(raw.FPDF_StructTree_CountChildren(tree))
            for table in _tables(raw, raw.FPDF_StructTree_GetChildAtIndex(tree, index))
        ]
        for row in _children(raw, tables[0]) if tables else []:
            cells: dict[int, str] = {}
            ambiguous = False
            for cell in _children(raw, row):
                objects = [obj for mcid in _mcids(raw, cell) for obj in texts.get(mcid, [])]
                columns = sorted(
                    {
                        bisect.bisect_right(borders, (obj["bounds"][0] + obj["bounds"][2]) / 2) - 1
                        for obj in objects
                    }
                )
                if columns:
                    ambiguous = ambiguous or len(columns) > 1 or columns[0] in cells
                    cells.setdefault(columns[0], _cell_text(objects))
            label = cells.get(0, "")
            if re.fullmatch(r"SR\([0-9]+\)[A-Z]+", label):
                regional = label
            if label == "TOTAL":
                declared = [
                    int(match[1])
                    for value in cells.values()
                    if (match := re.fullmatch(r"([0-9]+) processos com algum tipo.*", value))
                ]
                continue
            if not re.fullmatch(r"[0-9]+", cells.get(1, "")):
                continue
            if ambiguous:
                raise ValueError(f"Registro {cells[1]} com célula fora de uma coluna única")
            record: dict[str, Any] = {
                name: cells.get(index, "") for index, name in enumerate(ADMINISTRATIVE)
            }
            record.update(regional=regional, numero_publicado=int(cells[1]))
            records.append(record)
    before_source = last_text[: last_text.find("Fonte: INCRA")].splitlines()[-3:]
    dates = sorted(
        {date for line in before_source for date in re.findall(r"\d{2}/\d{2}/\d{4}", line)}
    )
    return {"records": records, "declared_total": declared, "edition_dates": dates}


def compare_andamento(frame: pd.DataFrame, oracle: dict[str, Any], edition: str) -> dict[str, Any]:
    problems = []
    expected = oracle["records"]
    if oracle["declared_total"] != [len(expected)]:
        problems.append(f"Total declarado {oracle['declared_total']} × {len(expected)} registros")
    if [
        datetime.strptime(date, "%d/%m/%Y").date().isoformat() for date in oracle["edition_dates"]
    ] != [edition]:
        problems.append(f"Edição agrobr {edition} × PDF {oracle['edition_dates']}")
    observed = [
        {
            name: (int(value) if name == "numero_publicado" else str(value))
            for name, value in row.items()
        }
        for row in frame.to_dict("records")
    ]
    if len(observed) != len(expected):
        problems.append(f"agrobr {len(observed)} × oráculo {len(expected)} registros")
    for actual, wanted in zip(observed, expected, strict=False):
        for name in ADMINISTRATIVE:
            if actual[name] != wanted[name] and len(problems) < 20:
                problems.append(
                    f"nº {wanted['numero_publicado']} {name}: {actual[name]!r} × {wanted[name]!r}"
                )
    return {
        "status": "mismatch" if problems else "ok",
        "cells": len(expected) * len(ADMINISTRATIVE),
        "problems": problems,
    }


def relation(left: list[str | None], right: list[tuple[str, int]]) -> list[tuple[Any, ...]]:
    left_tokens: dict[str, int] = collections.Counter()
    right_tokens: dict[str, list[tuple[int, int]]] = collections.defaultdict(list)
    for text in left:
        for token in NUP.findall(text or ""):
            left_tokens[token] += 1
    for number, (text, _) in enumerate(right, 1):
        for ordinal, token in enumerate(NUP.findall(text or ""), 1):
            right_tokens[token].append((number, ordinal))
    rows: list[tuple[Any, ...]] = []
    for position, text in enumerate(left, 1):
        tokens = NUP.findall(text or "")
        if not tokens:
            state = (
                "referencia_ausente" if not (text or "").strip() else "referencia_nao_reconhecida"
            )
            rows.append((state, text, position, None, None, None))
        for ordinal, token in enumerate(tokens, 1):
            matches = right_tokens.get(token, [])
            if not matches:
                rows.append(("sem_referencia_administrativa", token, position, ordinal, None, None))
            for number, other in matches:
                rows.append(
                    ("vinculo_exato", token, position, ordinal, other, right[number - 1][1])
                )
    for text, published in right:
        tokens = NUP.findall(text or "")
        if not tokens:
            state = (
                "referencia_ausente" if not (text or "").strip() else "referencia_nao_reconhecida"
            )
            rows.append((state, text, None, None, None, published))
        for ordinal, token in enumerate(tokens, 1):
            if token not in left_tokens:
                rows.append(("sem_referencia_geografica", token, None, None, ordinal, published))
    return rows


def compare_vinculos(frame: pd.DataFrame, expected: list[tuple[Any, ...]]) -> dict[str, Any]:
    observed = [
        tuple(None if pd.isna(value) else value for value in row)
        for row in frame[list(RELATION)].itertuples(index=False, name=None)
    ]
    problems = []
    if collections.Counter(observed) != collections.Counter(expected):
        missing = collections.Counter(expected) - collections.Counter(observed)
        extra = collections.Counter(observed) - collections.Counter(expected)
        problems.append(f"{sum(missing.values())} arestas ausentes, {sum(extra.values())} a mais")
        problems.extend(f"ausente {item}" for item in list(missing)[:5])
        problems.extend(f"a mais {item}" for item in list(extra)[:5])
    return {
        "status": "mismatch" if problems else "ok",
        "rows": len(observed),
        "states": dict(collections.Counter(row[0] for row in observed)),
        "problems": problems,
    }


async def agrobr_outputs() -> tuple[pd.DataFrame, pd.DataFrame, str, pd.DataFrame]:
    from agrobr import incra

    geographical = await incra.quilombolas(max_registros=None)
    administrative, meta = cast(
        "tuple[pd.DataFrame, Any]", await incra.andamento_quilombola(return_meta=True)
    )
    relation_frame = await incra.vinculos_quilombolas()
    return (
        geographical,
        administrative,
        meta.source_details["query"]["resolved_edition"],
        relation_frame,
    )


def run(directory: Path, output: Path) -> int:
    geographical, administrative, edition, relation_frame = asyncio.run(agrobr_outputs())
    receipts = capture(directory)
    features = wfs_features((directory / "wfs.json").read_bytes())
    checks: dict[str, Any] = {
        "wfs_json_x_csv": compare_json_csv(features, (directory / "wfs.csv").read_bytes()),
        "quilombolas": compare_quilombolas(geographical, features),
    }
    if (directory / "publication.pdf").exists():
        oracle = pdfium_publication((directory / "publication.pdf").read_bytes())
        checks["andamento"] = compare_andamento(administrative, oracle, edition)
        left = [feature["properties"]["nu_processo"] for feature in features]
        right = [(record["processo"], record["numero_publicado"]) for record in oracle["records"]]
        checks["vinculos"] = compare_vinculos(relation_frame, relation(left, right))
    else:
        checks["andamento"] = {"status": "mismatch", "problems": [f"links: {receipts['links']}"]}
    result = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": "captura ao vivo independente (httpx direto, json/csv puros, PDFium) × saída pública do agrobr",
        "population": len(features),
        "receipts": receipts,
        "checks": checks,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    failed = sum(check["status"] != "ok" for check in checks.values())
    print(f"{len(checks) - failed} ok / {failed} mismatch")
    return int(failed > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere ao vivo INCRA (WFS, andamento e vínculos) contra leitura independente"
    )
    parser.add_argument("--capture-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    return run(arguments.capture_dir, arguments.output)


if __name__ == "__main__":
    raise SystemExit(main())
