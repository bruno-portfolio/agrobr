from __future__ import annotations

import sys
from typing import Any

import pandas as pd
from pydantic import BaseModel

from agrobr import constants
from agrobr.exceptions import ResourceLimitError


def retained_size(value: Any, seen: set[int] | None = None) -> int:
    seen = set() if seen is None else seen
    if id(value) in seen:
        return 0
    seen.add(id(value))
    if isinstance(value, pd.DataFrame):
        return int(value.memory_usage(index=True, deep=True).sum())
    size = sys.getsizeof(value)
    if isinstance(value, BaseModel):
        return size + retained_size(value.__dict__, seen)
    if isinstance(value, dict):
        return size + sum(
            retained_size(key, seen) + retained_size(item, seen) for key, item in value.items()
        )
    if isinstance(value, (tuple, list, set)):
        return size + sum(retained_size(item, seen) for item in value)
    return size


def check(estimated: int) -> int:
    if estimated > constants.INCRA_VINCULOS_MAX_RETAINED_BYTES:
        raise ResourceLimitError(
            "incra_vinculos", "Orçamento local de retenção estimada do vínculo excedido"
        )
    return estimated
