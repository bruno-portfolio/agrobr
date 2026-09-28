from __future__ import annotations

import pytest

from agrobr.exceptions import ParseError
from agrobr.incra.andamento import parser
from tests.test_incra import replay


@pytest.fixture(scope="session")
def june_publication():
    try:
        return parser.parse_publication((replay.JUNE / "publication.pdf").read_bytes())
    except ParseError as error:
        return error


@pytest.fixture(scope="session")
def september_publication():
    try:
        return parser.parse_publication((replay.SEPTEMBER / "publication.pdf").read_bytes())
    except ParseError as error:
        return error
