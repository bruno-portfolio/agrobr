from __future__ import annotations

from typing import Any

from agrobr.utils import json as json_utils
from agrobr.utils.json import Number as Number
from agrobr.utils.json import canonical as canonical
from agrobr.utils.json import decimal as decimal
from agrobr.utils.json import floating as floating


def decode(content: bytes) -> Any:
    return json_utils.decode(content, encoding=None)


def integer(value: Any, bits: int | None = None) -> int:
    return json_utils.integer(value, bits, token_only=True)
