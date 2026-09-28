from __future__ import annotations

import io
from typing import Any

from . import _parsing, models


def pdfplumber_module() -> Any:
    try:
        import pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber is required for lista_suja PDF. Install with: pip install agrobr[pdf]"
        ) from None
    return pdfplumber


def _table_records(table: list[list[str | None]], page_number: int) -> list[list[str]]:
    found = False
    records = []
    for position, row in enumerate(table, 1):
        if not row or not any(value and value.strip() for value in row):
            continue
        cells = [value or "" for value in row]
        if _parsing.compact(cells[0]) == "ID":
            _parsing.header(cells)
            found = True
        elif found and len(cells) == len(models.SOURCE_COLUMNS) and cells[0].strip().isdigit():
            records.append(cells)
        elif sum(bool(value.strip()) for value in cells) != 1 or not _parsing.known_decoration(
            " ".join(cells)
        ):
            raise _parsing.fail(
                f"PDF página {page_number}, linha {position}: estrutura incompatível"
            )
    if not found:
        raise _parsing.fail(f"PDF página {page_number}: tabela sem cabeçalho")
    return records


def read_pdf(data: bytes) -> tuple[list[list[str]], dict[str, Any], int]:
    library = pdfplumber_module()
    from pdfminer import pdfparser, psparser
    from pdfplumber.utils import exceptions

    if not data.lstrip().startswith(b"%PDF-"):
        raise _parsing.fail("Conteúdo não é PDF")
    records = []
    texts = []
    try:
        with library.open(io.BytesIO(data)) as document:
            page_count = len(document.pages)
            if not page_count:
                raise _parsing.fail("PDF sem páginas")
            for page_number, page in enumerate(document.pages, 1):
                texts.append(page.extract_text() or "")
                tables = page.extract_tables()
                if not tables:
                    raise _parsing.fail(f"PDF página {page_number}: nenhuma tabela encontrada")
                for table in tables:
                    records.extend(_table_records(table, page_number))
                page.close()
    except (
        OSError,
        ValueError,
        TypeError,
        EOFError,
        pdfparser.PDFSyntaxError,
        psparser.PSEOF,
        exceptions.PdfminerException,
    ) as exc:
        raise _parsing.fail("PDF inválido ou ilegível") from exc
    if not records:
        raise _parsing.fail("PDF sem registros")
    return records, _parsing.publication("\n".join(texts)), page_count
