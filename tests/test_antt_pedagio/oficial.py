from __future__ import annotations

import collections
import csv
import hashlib
import io
import json
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/antt_pedagio/veracidade20260922"
COUNT = re.compile(r"[+]?[0-9]+(?:[,.]0+)?")


def _entry(name: str) -> dict[str, object]:
    manifest = json.loads((GOLDEN / "manifest.json").read_bytes())
    return next(item for item in manifest["files"] if item["file"] == name)


def load(name: str) -> bytes:
    entry = _entry(name)
    body = (GOLDEN / name).read_bytes()
    assert hashlib.sha256(body).hexdigest() == entry["sha256"]
    assert len(body) == entry["size_bytes"]
    return body


def subset(name: str, *labels: str) -> bytes:
    source_lines = _entry(name)["source_lines"]
    assert isinstance(source_lines, dict) and set(labels) <= set(source_lines.values())
    lines = load(name).split(b"\r\n")
    chosen = [
        line
        for line, label in zip(lines[1:-1], source_lines.values(), strict=True)
        if label in labels
    ]
    return b"\r\n".join([lines[0], *chosen, b""])


def rows(body: bytes) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(body.decode("cp1252"), newline=""), delimiter=";"))


def countable(volume: str) -> bool:
    return COUNT.fullmatch(volume) is not None


def month(reference: str) -> date:
    pieces = [int(piece) for piece in reference.split("/")]
    return date(pieces[-1], pieces[-2], 1)


def oracle(body: bytes) -> dict[tuple[object, ...], int]:
    blocks: dict[tuple[str, date], collections.Counter[tuple[tuple[str, str], ...]]] = (
        collections.defaultdict(collections.Counter)
    )
    for row in rows(body):
        if countable(row["volume_total"]):
            blocks[row["concessionaria"], month(row["mes_ano"])][tuple(row.items())] += 1
    totals: dict[tuple[object, ...], int] = collections.defaultdict(int)
    for occurrences in blocks.values():
        copies = 2 if all(count % 2 == 0 for count in occurrences.values()) else 1
        for items, count in occurrences.items():
            row = dict(items)
            key = (
                month(row["mes_ano"]),
                row["concessionaria"],
                row["praca"],
                row["sentido"],
                row.get("categoria_eixo", row.get("categoria")),
                row["tipo_cobranca"],
                row["tipo_de_veiculo"],
            )
            totals[key] += int(Decimal(row["volume_total"].replace(",", "."))) * count // copies
    return dict(totals)


def published(frame: pd.DataFrame) -> dict[tuple[object, ...], int]:
    names = (
        "concessionaria",
        "praca",
        "sentido",
        "categoria_eixo",
        "tipo_cobranca",
        "tipo_veiculo",
    )
    return {
        (
            row.data.date(),
            *(None if pd.isna(getattr(row, name)) else getattr(row, name) for name in names),
        ): int(row.volume)
        for row in frame.itertuples()
    }


def plazas(body: bytes) -> list[dict[str, object]]:
    aliases = {"latitude": "lat", "longitude": "lon", "praca": "praca_de_pedagio"}
    result = []
    for raw in rows(body):
        row: dict[str, object] = {aliases.get(name, name): value for name, value in raw.items()}
        row["uf"] = raw["uf"].strip().upper() or None
        for name in ("lat", "lon"):
            text = str(row[name])
            row[name] = float(Decimal(text.replace(",", "."))) if text else None
        row.setdefault("municipio", raw.get("municipal"))
        result.append(row)
    return result


def records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return [
        {name: None if pd.isna(value) else value for name, value in row.items()}
        for row in frame.to_dict("records")
    ]
