from __future__ import annotations

import sys
from typing import Any

from pydantic import BaseModel


def deep_size(value: Any, seen: set[int] | None = None) -> int:
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, BaseModel):
        return (
            size + deep_size(value.__dict__, seen) + deep_size(value.__pydantic_fields_set__, seen)
        )
    if isinstance(value, dict):
        return size + sum(
            deep_size(key, seen) + deep_size(item, seen) for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set)):
        return size + sum(deep_size(item, seen) for item in value)
    return size
