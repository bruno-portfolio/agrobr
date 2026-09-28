from __future__ import annotations

from typing import Any

from agrobr.utils import json as json_utils
from agrobr.utils.json import RawNumber as RawNumber
from agrobr.utils.json import numeric_identifier as numeric_identifier
from agrobr.utils.json import plain_numbers as plain_numbers

as_float = json_utils.floating


def as_int(value: Any) -> int:
    return json_utils.integer(value, bits=None, token_only=True)


def canonical(value: Any) -> Any:
    return json_utils.canonical(value, preserve_signed_zero=False)


def decode(content: bytes) -> Any:
    return json_utils.decode(content, encoding=None, number_type=RawNumber)
