from __future__ import annotations

import sys
from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd

from agrobr.exceptions import ResourceLimitError


@dataclass(frozen=True)
class FrameUsage:
    resident_bytes: int
    buffer_bytes: int
    accounting_peak_bytes: int


def frame_usage(frames: Iterable[pd.DataFrame], *, max_bytes: int) -> FrameUsage:
    materialized = tuple(frames)
    buffers = sum(int(frame.memory_usage(index=True, deep=False).sum()) for frame in materialized)
    resident = buffers + len(materialized) * 16384
    seen: set[int] = set()
    identity_bytes = 0
    peak = resident + sys.getsizeof(seen) + sys.getsizeof(materialized) + 2048

    def guard(estimated: int) -> None:
        if estimated > max_bytes:
            raise ResourceLimitError(
                source="antt_pedagio",
                reason=f"Limite de retenção durante contabilidade de quadros: {estimated}>{max_bytes} bytes",
            )

    guard(peak)
    for frame in materialized:
        for name in frame.columns:
            column = frame[name]
            if not (isinstance(column.dtype, pd.StringDtype) or column.dtype == object):
                continue
            for value in column:
                identity = id(value)
                if identity not in seen:
                    allocation = (
                        resident
                        + sys.getsizeof(value)
                        + sys.getsizeof(seen) * 4
                        + identity_bytes
                        + sys.getsizeof(identity)
                        + sys.getsizeof(materialized)
                        + 2048
                    )
                    peak = max(peak, allocation)
                    guard(peak)
                    seen.add(identity)
                    identity_bytes += sys.getsizeof(identity)
                    resident += sys.getsizeof(value)
                    peak = max(
                        peak,
                        (
                            resident
                            + sys.getsizeof(seen)
                            + identity_bytes
                            + sys.getsizeof(materialized)
                            + 2048
                        ),
                    )
                    guard(peak)
    return FrameUsage(resident_bytes=resident, buffer_bytes=buffers, accounting_peak_bytes=peak)
