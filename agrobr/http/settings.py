from __future__ import annotations

import httpx

from agrobr.constants import HTTPSettings


def get_timeout(
    settings: HTTPSettings | None = None,
    *,
    read: float | None = None,
) -> httpx.Timeout:
    """``read`` é o mínimo do cliente; ``AGROBR_HTTP_TIMEOUT_READ`` maior prevalece."""
    s = settings or HTTPSettings()
    return httpx.Timeout(
        connect=s.timeout_connect,
        read=s.timeout_read if read is None else max(read, s.timeout_read),
        write=s.timeout_write,
        pool=s.timeout_pool,
    )
