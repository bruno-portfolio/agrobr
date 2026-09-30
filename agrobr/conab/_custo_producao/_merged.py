from __future__ import annotations

import posixpath
import zipfile
from io import BytesIO
from xml.etree import ElementTree

from openpyxl.utils.cell import range_boundaries


def xlsx_header_merges(raw: bytes) -> dict[str, dict[tuple[int, int], tuple[int, int]]]:
    result = {}
    with zipfile.ZipFile(BytesIO(raw)) as archive:
        relationships = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        targets = {item.attrib["Id"]: item.attrib["Target"] for item in relationships}
        workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
        for sheet in workbook.findall("{*}sheets/{*}sheet"):
            relation = next(value for key, value in sheet.attrib.items() if key.endswith("}id"))
            target = targets[relation]
            path = (
                target.lstrip("/")
                if target.startswith("/")
                else posixpath.normpath(posixpath.join("xl", target))
            )
            document = ElementTree.fromstring(archive.read(path))
            ranges = [
                range_boundaries(item.attrib["ref"])
                for item in document.findall("{*}mergeCells/{*}mergeCell")
            ]
            result[sheet.attrib["name"]] = {
                (row - 1, col - 1): (col - 1, end_col)
                for col, row, end_col, _end_row in ranges
                if row <= 30
            }
    return result
