from __future__ import annotations

import sys
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from types import SimpleNamespace
from typing import Any
from xml.etree import ElementTree

import xlrd

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.utils.io import open_excel_safe

from . import _merged


@dataclass
class Aba:
    nome: str
    linhas: list[list[Any]]
    formatos: dict[tuple[int, int], str]
    mesclas_cabecalho: dict[tuple[int, int], tuple[int, int]] = field(default_factory=dict)


class Workbook:
    def __init__(self, content: bytes | BytesIO) -> None:
        raw = content.getvalue() if isinstance(content, BytesIO) else content
        if len(raw) > constants.CONAB_CUSTOS_MAX_BODY_BYTES:
            raise ParseError(
                source="conab_custo",
                parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                reason="Workbook excede orçamento",
            )
        if raw.startswith(b"PK"):
            try:
                with zipfile.ZipFile(BytesIO(raw)) as archive:
                    if (
                        sum(item.file_size for item in archive.infolist())
                        > constants.CONAB_CUSTOS_MAX_EXPANDED_BYTES
                    ):
                        raise ParseError(
                            source="conab_custo",
                            parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                            reason="Expansão XLSX excede orçamento",
                        )
            except zipfile.BadZipFile as error:
                raise ParseError(
                    source="conab_custo",
                    parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                    reason=f"XLSX inválido: {error}",
                ) from error
        self.excel: Any
        if raw.startswith(b"\xd0\xcf\x11\xe0"):
            try:
                book = xlrd.open_workbook(file_contents=raw, on_demand=True, formatting_info=True)
            except (xlrd.XLRDError, ValueError) as error:
                raise ParseError(
                    source="conab_custo",
                    parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                    reason=f"XLS inválido: {error}",
                ) from error
            self.excel = SimpleNamespace(
                book=book,
                engine="xlrd",
                sheet_names=book.sheet_names(),
                close=book.release_resources,
            )
        else:
            self.excel = open_excel_safe(
                content, source="conab_custo", parser_version=constants.CONAB_CUSTOS_PARSER_VERSION
            )
            if self.excel.engine != "openpyxl":
                self.close()
                raise ParseError(
                    source="conab_custo",
                    parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                    reason="Leitor alternativo não preserva os formatos de células dos custos CONAB",
                )
        self.names = [str(name) for name in self.excel.sheet_names]
        if len(self.names) > constants.CONAB_CUSTOS_MAX_SHEETS:
            self.close()
            raise ParseError(
                source="conab_custo",
                parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                reason="Mais de 1000 abas",
            )

        try:
            self._header_merges = _merged.xlsx_header_merges(raw) if raw.startswith(b"PK") else {}
        except (
            zipfile.BadZipFile,
            KeyError,
            ElementTree.ParseError,
            ValueError,
            StopIteration,
        ) as error:
            self.close()
            raise ParseError(
                source="conab_custo",
                parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                reason=f"Mesclas dos cabeçalhos ilegíveis: {error}",
            ) from error

    def close(self) -> None:
        primary = sys.exception()
        try:
            self.excel.close()
        except OSError as error:
            if primary is None:
                raise
            primary.__dict__["conab_workbook_close_error"] = repr(error)

    def read(self, name: str, *, head: bool = False) -> Aba:
        if self.excel.engine == "xlrd":
            sheet = self.excel.book.sheet_by_name(name)
            if (
                sheet.nrows > constants.CONAB_CUSTOS_MAX_SHEET_ROWS
                or sheet.nrows * sheet.ncols > constants.CONAB_CUSTOS_MAX_SHEET_CELLS
            ):
                raise ParseError(
                    source="conab_custo",
                    parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                    reason="Limite de células/linhas da aba",
                )
            rows = []
            formats = {}
            for row_index in range(min(sheet.nrows, 30) if head else sheet.nrows):
                row = []
                for col in range(sheet.ncols):
                    cell = sheet.cell(row_index, col)
                    value = cell.value
                    if cell.ctype == xlrd.XL_CELL_DATE:
                        value = xlrd.xldate_as_datetime(value, self.excel.book.datemode)
                    elif cell.ctype == xlrd.XL_CELL_ERROR:
                        value = {"excel_error": int(value)}
                    elif cell.ctype == xlrd.XL_CELL_BOOLEAN:
                        value = bool(value)
                    row.append(value)
                    xf = self.excel.book.xf_list[sheet.cell_xf_index(row_index, col)]
                    formats[row_index, col] = self.excel.book.format_map[xf.format_key].format_str
                rows.append(row)
            merges = {(r, c): (c, end) for r, _, c, end in sheet.merged_cells if r < 30}
            return Aba(name, rows, formats, merges)
        sheet = self.excel.book[name]
        if (
            sheet.max_row > constants.CONAB_CUSTOS_MAX_SHEET_ROWS
            or sheet.max_row * sheet.max_column > constants.CONAB_CUSTOS_MAX_SHEET_CELLS
        ):
            raise ParseError(
                source="conab_custo",
                parser_version=constants.CONAB_CUSTOS_PARSER_VERSION,
                reason="Limite de células/linhas da aba",
            )
        rows = []
        formats = {}
        for row_index, cells in enumerate(
            sheet.iter_rows(max_row=min(sheet.max_row, 30) if head else sheet.max_row)
        ):
            row = []
            for col, cell in enumerate(cells):
                value = cell.value
                if cell.data_type == "e":
                    value = {"excel_error": str(value)}
                if isinstance(value, datetime):
                    value = value.replace(tzinfo=None)
                row.append("" if value is None else value)
                formats[row_index, col] = cell.number_format
            rows.append(row)
        return Aba(name, rows, formats, self._header_merges.get(name, {}))
