from __future__ import annotations

import re
import struct
from typing import Any

import xlrd
from pydantic import BaseModel
from xlrd import compdoc


class TextBox(BaseModel):
    name: str
    text: str
    column_start: float
    column_end: float
    row_start: float


def clean_text(text: str) -> str:
    return " ".join(re.sub(r"(?<=\w)-\s*\n\s*(?=\w)", "", text).split())


def read_text_boxes(data: bytes) -> list[TextBox]:
    document = compdoc.CompDoc(data)
    stream = document.get_named_stream("Book") or document.get_named_stream("Workbook")
    if stream is None or len(stream) < 6:
        return []
    if struct.unpack_from("<H", stream, 4)[0] == 0x0600:
        return _read_biff8_boxes(_biff_records(stream))
    boxes = []
    offset = 0
    while offset + 4 <= len(stream):
        code, length = struct.unpack_from("<HH", stream, offset)
        body = stream[offset + 4 : offset + 4 + length]
        offset += length + 4
        if code != 0x005D or len(body) < 72 or struct.unpack_from("<H", body, 4)[0] != 6:
            continue
        name_length = body[70]
        name_bytes = body[71 : 71 + name_length]
        if not name_bytes.startswith((b"TitPag", b"TitTab", b"Indic", b"Box")):
            continue
        name = name_bytes.decode("cp1252")
        text_length = struct.unpack_from("<H", body, 44)[0]
        text_start = ((71 + name_length + 1) & ~1) + struct.unpack_from("<H", body, 26)[0]
        text = body[text_start : text_start + text_length].decode("cp1252")
        left, dx_left, top, dy_top, right, dx_right, _, _ = struct.unpack_from("<8H", body, 10)
        boxes.append(
            TextBox(
                name=name,
                text=clean_text(text),
                column_start=left + dx_left / 1024,
                column_end=right + dx_right / 1024,
                row_start=top + dy_top / 256,
            )
        )
    return boxes


def _biff_records(stream: bytes) -> list[tuple[int, bytes]]:
    records = []
    offset = 0
    while offset + 4 <= len(stream):
        code, length = struct.unpack_from("<HH", stream, offset)
        end = offset + 4 + length
        if end > len(stream):
            raise ValueError("Registro BIFF truncado")
        records.append((code, stream[offset + 4 : end]))
        offset = end
    return records


def _shape_name(properties: bytes, count: int) -> str:
    complex_offset = count * 6
    for offset in range(0, count * 6, 6):
        code, value = struct.unpack_from("<HI", properties, offset)
        if not code & 0x8000:
            continue
        data = properties[complex_offset : complex_offset + value]
        if code & 0x3FFF == 0x0380:
            return data.decode("utf-16-le").rstrip("\0")
        complex_offset += value
    return ""


def _drawing_shape(data: bytes) -> tuple[str, tuple[int, ...] | None]:
    name = ""
    anchor = None
    offset = 0
    while offset + 8 <= len(data):
        options, code, length = struct.unpack_from("<HHI", data, offset)
        offset += 8
        if options & 0xF == 0xF:
            continue
        body = data[offset : offset + length]
        if len(body) != length:
            raise ValueError("Registro OfficeArt truncado")
        if code == 0xF00B:
            name = _shape_name(body, options >> 4)
        elif code == 0xF010 and len(body) == 18:
            anchor = struct.unpack_from("<8H", body, 2)
        offset += length
    return name, anchor


def _continued_text(records: list[tuple[int, bytes]], start: int, count: int) -> str:
    parts = []
    remaining = count
    for code, body in records[start:]:
        if code != 0x003C or not body:
            break
        width = 2 if body[0] & 1 else 1
        characters = min(remaining, (len(body) - 1) // width)
        encoding = "utf-16-le" if width == 2 else "latin1"
        parts.append(body[1 : 1 + characters * width].decode(encoding))
        remaining -= characters
        if remaining == 0:
            return "".join(parts)
    raise ValueError("Texto TxO truncado")


def _read_biff8_boxes(records: list[tuple[int, bytes]]) -> list[TextBox]:
    boxes = []
    name = ""
    anchor = None
    for index, (code, body) in enumerate(records):
        if code == 0x00EC:
            shape_name, shape_anchor = _drawing_shape(body)
            if shape_name or shape_anchor is not None:
                name, anchor = shape_name, shape_anchor
        elif code == 0x01B6 and len(body) >= 14 and anchor is not None:
            count = struct.unpack_from("<H", body, 10)[0]
            if not count or not name.startswith(("TitPag", "TitTab", "Indic", "Box")):
                continue
            text = _continued_text(records, index + 1, count)
            left, dx_left, top, dy_top, right, dx_right, _, _ = anchor
            boxes.append(
                TextBox(
                    name=name,
                    text=clean_text(text),
                    column_start=left + dx_left / 1024,
                    column_end=right + dx_right / 1024,
                    row_start=top + dy_top / 256,
                )
            )
    return boxes


def column_headers(boxes: list[TextBox], column: int) -> list[str]:
    matched = [
        box
        for box in boxes
        if box.name.startswith("Box") and box.column_start <= column + 0.5 <= box.column_end
    ]
    ordered = sorted(matched, key=lambda box: (round(box.row_start * 256), box.column_start))
    headers = {round(box.row_start * 256): box.text for box in ordered}
    return list(dict.fromkeys(headers.values()))


def read_rows(data: bytes) -> list[list[Any]]:
    book = xlrd.open_workbook(file_contents=data, formatting_info=True)
    sheet = book.sheet_by_index(0)
    rows = []
    for row in range(sheet.nrows):
        values = []
        for column in range(sheet.ncols):
            cell = sheet.cell(row, column)
            value = cell.value
            if cell.ctype == xlrd.XL_CELL_NUMBER:
                format_key = book.xf_list[sheet.cell_xf_index(row, column)].format_key
                number_format = book.format_map[format_key].format_str.split(";", 1)[0]
                number_format = re.sub(r'"[^"]*"|\\.|_.|\*.', "", number_format)
                scale = re.search(r"[0#?](,+)[^0#?.,]*$", number_format)
                if scale:
                    value /= 1000 ** len(scale.group(1))
            values.append(value)
        rows.append(values)
    return rows
