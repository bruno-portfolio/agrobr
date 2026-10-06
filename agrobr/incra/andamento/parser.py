from __future__ import annotations

import bisect
import hashlib
import importlib
import io
import json
import math
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any

import pandas as pd
from pydantic import ValidationError

from agrobr import constants
from agrobr.exceptions import ParseError

from . import models


def check_pdf() -> Any:
    try:
        return importlib.import_module("pdfplumber")
    except ImportError:
        raise ImportError("pdfplumber é necessário. Instale com: pip install agrobr[pdf]") from None


def _text(value: str | None) -> str:
    return " ".join((value or "").split())


def _header(table: Any) -> list[float]:
    if "ANDAMENTO DOS PROCESSOS" not in _text(table.extract()[0][0]):
        raise ValueError("Título administrativo não reconhecido")
    header = table.extract()[1]
    if tuple(_text(value) for value in header) != constants.INCRA_ANDAMENTO_HEADERS:
        raise ValueError("Cabeçalho administrativo divergente")
    boxes = table.rows[1].cells
    if len(boxes) != 15 or any(box is None for box in boxes):
        raise ValueError("Cabeçalho administrativo sem 15 caixas")
    borders = [box[0] for box in boxes] + [boxes[-1][2]]
    if not all(math.isfinite(value) for value in borders) or any(
        a >= b for a, b in zip(borders, borders[1:], strict=False)
    ):
        raise ValueError("Colunas administrativas sem ordem geométrica")
    return borders


def _table_rows(
    table: Any, page_number: int, borders: list[float]
) -> tuple[list[models.OrdinalCell], int | None, list[Any]]:
    result = []
    total = None
    footer = []
    for raw, row in zip(table.extract(), table.rows, strict=True):
        if len(raw) != 15:
            raise ValueError("Largura da tabela administrativa divergente")
        if raw[1] is not None and re.fullmatch(r"[0-9]+", raw[1]):
            if total is not None:
                raise ValueError("Registros administrativos após total declarado")
            box = row.cells[1]
            if box is None or abs(box[0] - borders[1]) > 1 or abs(box[2] - borders[2]) > 1:
                raise ValueError("Caixa ordinal fora da coluna declarada")
            result.append(
                models.OrdinalCell(
                    ordinal=int(raw[1]), page=page_number, top=float(box[1]), bottom=float(box[3])
                )
            )
        elif _text(raw[0]) == "TOTAL":
            match = re.fullmatch(
                r"([0-9]+) processos com algum tipo de andamento no INCRA", _text(raw[1])
            )
            if match is None or total is not None:
                raise ValueError("Total declarado administrativo inválido")
            total, footer = int(match[1]), raw
        elif _text(raw[0]) == "SR":
            if tuple(_text(value) for value in raw) != constants.INCRA_ANDAMENTO_HEADERS:
                raise ValueError("Cabeçalho repetido divergente")
            continue
        elif "ANDAMENTO DOS PROCESSOS" in _text(raw[0]):
            continue
        else:
            raise ValueError("Linha administrativa sem ordinal ou total reconhecido")
    return result, total, footer


def _regional_paths(device: Any, height: float, borders: list[float]) -> list[dict[str, Any]]:
    middle = (borders[0] + borders[1]) / 2
    selected = []
    for item in device.black_segments:
        a, b, c, d = item["segment"]
        if abs(b - d) < 0.01 and min(a, c) <= middle <= max(a, c):
            selected.append({**item, "top_y": height - (b + d) / 2})
    return selected


def _supplementary_tables(tables: list[Any], total: int | None) -> list[dict[str, Any]]:
    widths = {
        "OBSERVAÇÕES:": 1,
        "Resultado Anual": 10,
        "Condensado Geral - Área e Famílias": 2,
        "Processos abertos": 2,
    }
    found: set[str] = set()
    result = []
    for table in tables[1:]:
        rows = table.extract()
        title = _text(rows[0][0]) if rows and rows[0] else ""
        if rows and len(rows[0]) == 2 and _text(rows[0][1]) == "Processos abertos":
            title = "Processos abertos"
        if (
            total is None
            or title not in widths
            or title in found
            or any(len(row) != widths[title] for row in rows)
            or table.bbox[1] < tables[0].bbox[3]
        ):
            raise ValueError("Página de dados com tabelas adicionais não suportadas")
        found.add(title)
        result.append({"title": title, "bbox": list(table.bbox), "cells": rows})
    return result


def _extract_rows(
    device: Any,
    row_boxes: list[models.OrdinalCell],
    height: float,
    borders: list[float],
    pdfplumber: Any,
    pdf_backend: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    slots: Any = defaultdict(list)
    labels: Any = defaultdict(list)
    tops = [row.top for row in row_boxes]
    clipped = 0
    for glyph in device.glyphs:
        box = glyph["bbox"]
        visible = pdf_backend.intersection(box, glyph["clip"])
        hidden = visible[2] <= visible[0] or visible[3] <= visible[1]
        if hidden:
            clip = glyph["clip"]
            if clip is not None and bisect.bisect_right(borders, (clip[0] + clip[2]) / 2) - 1 == 0:
                continue
            y_clip = (
                height - (clip[1] + clip[3]) / 2
                if clip is not None
                else height - (box[1] + box[3]) / 2
            )
            candidate = bisect.bisect_right(tops, y_clip) - 1
            if 0 <= candidate < len(row_boxes) and y_clip < row_boxes[candidate].bottom:
                raise ValueError("Glifo administrativo totalmente cortado não suportado")
            continue
        x, y = (visible[0] + visible[2]) / 2, height - (visible[1] + visible[3]) / 2
        column = bisect.bisect_right(borders, x) - 1
        row = bisect.bisect_right(tops, y) - 1
        if column == 0:
            labels[glyph["text_object"]].append((glyph, y))
        if not (1 <= column < 15 and 0 <= row < len(row_boxes) and y < row_boxes[row].bottom):
            continue
        clipped += int(tuple(visible) != tuple(box))
        slots[row, column].append(
            {
                "text": glyph["text"],
                "x0": box[0],
                "x1": box[2],
                "top": height - box[3],
                "bottom": height - box[1],
                "doctop": height - box[3],
                "upright": glyph["upright"],
                "height": box[3] - box[1],
                "width": box[2] - box[0],
            }
        )
    values = []
    for index, ordinal_cell in enumerate(row_boxes):
        cells: dict[str, Any] = {
            name: pdfplumber.utils.extract_text(slots[index, column], x_tolerance=1, y_tolerance=1)
            if slots[index, column]
            else ""
            for column, name in enumerate(models.columns()[1:], 1)
        }
        if cells["numero_publicado"] != str(ordinal_cell.ordinal):
            raise ValueError("Ordinal textual diverge da grade")
        cells["numero_publicado"] = ordinal_cell.ordinal
        values.append(cells)
    regional = []
    for obj, glyphs in labels.items():
        label = "".join(
            glyph["text"] for glyph, _ in sorted(glyphs, key=lambda pair: pair[0]["bbox"][0])
        )
        if label.startswith("SR("):
            if re.fullmatch(r"SR\([0-9]+\)[A-Z]+", label) is None:
                raise ValueError("Rótulo regional não reconhecido")
            regional.append(
                {"label": label, "text_object": obj, "y": sum(y for _, y in glyphs) / len(glyphs)}
            )
    return values, regional, clipped


class Groups:
    def __init__(self) -> None:
        self.pending: list[tuple[models.OrdinalCell, dict[str, Any]]] = []
        self.labels: list[dict[str, Any]] = []
        self.groups: list[models.RegionalGroup] = []
        self.rows: list[models.AndamentoRow] = []

    def add_page(
        self,
        boxes: list[models.OrdinalCell],
        cells: list[dict[str, Any]],
        labels: list[dict[str, Any]],
        paths: list[dict[str, Any]],
    ) -> None:
        if (
            self.pending
            and boxes
            and any(abs(path["top_y"] - boxes[0].top) <= 0.7 for path in paths)
        ):
            raise ValueError("Continuação regional com divisória de abertura")
        for box, cell in zip(boxes, cells, strict=True):
            self.pending.append((box, cell))
            self.labels.extend(
                {**label, "page": box.page} for label in labels if box.top < label["y"] < box.bottom
            )
            closing = [path for path in paths if abs(path["top_y"] - box.bottom) <= 0.7]
            if closing:
                self.close(closing)

    def close(self, paths: list[dict[str, Any]]) -> None:
        if len(self.labels) != 1:
            raise ValueError("Grupo regional deve ter exatamente um rótulo")
        label = self.labels[0]
        self.groups.append(
            models.RegionalGroup(
                first=self.pending[0][0].ordinal,
                last=self.pending[-1][0].ordinal,
                label=label["label"],
                pages=sorted({row.page for row, _ in self.pending}),
                label_page=label["page"],
                label_text_object=label["text_object"],
                closing_paths=paths,
            )
        )
        self.rows.extend(
            models.AndamentoRow(regional=label["label"], **cells) for _, cells in self.pending
        )
        self.pending, self.labels = [], []


def _edition(last_text: str) -> date:
    candidates = re.findall(r"(?m)^\s*(\d{2}/\d{2}/\d{4})(?:\s|$)", last_text)
    if (
        len(set(candidates)) != 1
        or "Fonte: INCRA" not in last_text
        or "Autorizada a reprodução" not in last_text
    ):
        raise ValueError("Edição e proveniência interna do PDF não reconhecidas")
    return datetime.strptime(candidates[0], "%d/%m/%Y").date()


def _read(content: bytes, pdfplumber: Any, backend: Any) -> models.ParsedPublication:
    grouped = Groups()
    declared = None
    notes = []
    clipping = 0
    borders: list[float] = []
    last_text = ""
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        if not 1 <= len(pdf.pages) <= constants.INCRA_ANDAMENTO_MAX_PAGES:
            raise ValueError("Orçamento local de páginas PDF excedido")
        page_count = len(pdf.pages)
        for page_number, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            last_text = text
            if declared is not None:
                if any(
                    len(row) == 15 and row[1] is not None and re.fullmatch(r"[0-9]+", row[1])
                    for table in page.find_tables()
                    for row in table.extract()
                ):
                    raise ValueError("Página com registros após encerramento declarado")
                notes.append({"page": page_number, "text": text})
                page.close()
                continue
            tables = page.find_tables()
            if not tables:
                raise ValueError("Página de dados sem tabela reconhecida")
            if not borders:
                borders = _header(tables[0])
            boxes, total, footer = _table_rows(tables[0], page_number, borders)
            supplementary = _supplementary_tables(tables, total)
            if not boxes and total != 0:
                raise ValueError("Página de dados sem ordinais")
            device = backend.collect(page.page_obj)
            cells, labels, clipped = _extract_rows(
                device, boxes, float(page.height), borders, pdfplumber, backend
            )
            grouped.add_page(
                boxes, cells, labels, _regional_paths(device, float(page.height), borders)
            )
            clipping += clipped
            if total is not None:
                declared = total
                notes.append(
                    {
                        "page": page_number,
                        "declared_footer_cells": footer,
                        "supplementary_tables": supplementary,
                        "text": text[text.find("OBSERVAÇÕES:") :] if "OBSERVAÇÕES:" in text else "",
                    }
                )
            page.close()
    if grouped.pending or grouped.labels:
        raise ValueError("Grupo regional final sem fechamento")
    if (
        declared is None
        or len(grouped.rows) != declared
        or any(row.numero_publicado != index for index, row in enumerate(grouped.rows, 1))
    ):
        raise ValueError("População/ordinais divergem do total declarado")
    edition = _edition(last_text)
    frame = pd.DataFrame([row.model_dump() for row in grouped.rows], columns=models.columns())
    for name in models.columns():
        frame[name] = frame[name].astype(
            "Int64" if name == "numero_publicado" else pd.Series([""]).dtype
        )
    fingerprint = {
        "kind": "incra_andamento_pdf_rectangular_clipping_and_painted_regional_groups",
        "header": list(constants.INCRA_ANDAMENTO_HEADERS),
        "column_bounds": borders,
        "parser_version": constants.INCRA_ANDAMENTO_PARSER_VERSION,
    }
    fingerprint["sha256"] = hashlib.sha256(
        json.dumps(fingerprint, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return models.ParsedPublication(
        frame=frame,
        edition=edition,
        declared_total=declared,
        page_count=page_count,
        regional_groups=grouped.groups,
        notes=notes,
        fingerprint=fingerprint,
        diagnostics={
            "partially_clipped_glyphs": clipping,
            "empty_text_counts": {
                name: int(frame[name].eq("").sum())
                for name in models.columns()
                if name != "numero_publicado"
            },
            "regional_assignment": "painted_black_boundaries_with_unique_literal_label",
            "source_text_visibility": "underlying_partially_clipped_text_preserved",
            "uf_inferred": False,
        },
        source_sha256=hashlib.sha256(content).hexdigest(),
    )


def parse_publication(content: bytes) -> models.ParsedPublication:
    pdfplumber = check_pdf()
    backend = importlib.import_module("agrobr.incra.andamento._pdf")
    from pdfminer.pdfexceptions import PDFException

    if not isinstance(content, bytes) or not content.startswith(b"%PDF"):
        raise ParseError(
            "incra",
            constants.INCRA_ANDAMENTO_PARSER_VERSION,
            "Publicação administrativa sem assinatura PDF",
        )
    try:
        return _read(content, pdfplumber, backend)
    except (
        ValueError,
        TypeError,
        IndexError,
        KeyError,
        ValidationError,
        PDFException,
        pdfplumber.utils.exceptions.PdfminerException,
    ) as exc:
        raise ParseError(
            "incra",
            constants.INCRA_ANDAMENTO_PARSER_VERSION,
            f"Layout administrativo não suportado: {exc}",
        ) from exc
