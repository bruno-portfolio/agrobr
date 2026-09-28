from __future__ import annotations

import sys
from collections.abc import Sequence

import pandas as pd

from agrobr.exceptions import ResourceLimitError


class Retention:
    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self.pool: dict[str, str] = {}
        self.text_bytes = 0
        self.resident = 0
        self.peak = 0
        self.guard(0)

    def guard(self, retained: int, *, transient: int = 0) -> None:
        estimate = retained + transient + 8 * 1024**2
        self.peak = max(self.peak, estimate)
        if estimate > self.max_bytes:
            raise ResourceLimitError(
                source="comexstat",
                reason=f"Retenção estimada excede max_memoria_bytes: {estimate}>{self.max_bytes}",
            )

    def intern(self, value: str) -> str:
        known = self.pool.get(value)
        if known is not None:
            return known
        size = sys.getsizeof(value)
        self.guard(self.resident + size + 256 + sys.getsizeof(self.pool) * 3)
        self.pool[value] = value
        self.text_bytes += size
        self.resident += size + 128
        return value

    def reserve(self, size: int) -> None:
        self.guard(self.resident + size)
        self.resident += size

    def build(
        self, rows: Sequence[tuple[object, ...]], columns: tuple[str, ...], dtypes: dict[str, str]
    ) -> pd.DataFrame:
        count = len(rows)
        buffers = count * len(columns) * 9 + 32768 + len(columns) * 4096
        self.guard(self.resident + buffers, transient=count * 96)
        frame = pd.DataFrame(index=pd.RangeIndex(count))
        for index, column in enumerate(columns):
            values = [row[index] for row in rows]
            if dtypes[column] == "string":
                frame[column] = pd.Series(
                    values, dtype=pd.StringDtype(storage="python"), index=frame.index
                )
            else:
                frame[column] = pd.Series(values, dtype=dtypes[column], index=frame.index)
            del values
        return frame

    def frame_bytes(self, frame: pd.DataFrame) -> int:
        buffers = int(frame.memory_usage(index=True, deep=False).sum())
        return buffers + self.text_bytes + 32768 + len(frame.columns) * 4096
