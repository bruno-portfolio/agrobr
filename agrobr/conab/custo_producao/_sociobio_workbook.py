from __future__ import annotations

from agrobr import constants
from agrobr.exceptions import ParseError

from . import _workbook

AbaSociobio = _workbook.Aba
WorkbookSociobio = _workbook.Workbook


def fail(sheet: str, reason: str) -> ParseError:
    return ParseError(
        source="conab_sociobio",
        parser_version=constants.CONAB_SOCIOBIO_PARSER_VERSION,
        reason=f"{sheet}: {reason}",
    )
