from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import httpx

from agrobr import constants
from agrobr.http import settings, wfs_transport

from . import acquisition

TIMEOUT = settings.get_timeout(read=120.0)
Role = Literal["count_before", "page", "count_after"]


class Transport(wfs_transport.Transport[acquisition.IncraResource, Role]):
    def __init__(self) -> None:
        super().__init__(
            source="incra",
            timeout=TIMEOUT,
            initial_role="count_before",
            resource_factory=acquisition.IncraResource.model_validate,
            body_limit=lambda: constants.INCRA_MAX_BODY_BYTES,
            total_limit=lambda: constants.INCRA_MAX_TOTAL_BODY_BYTES,
        )

    def _request_fields(self) -> dict[str, Any]:
        return {"started_at": datetime.now(UTC)}

    def _finished(self, resource: acquisition.IncraResource) -> None:
        resource.finished_at = datetime.now(UTC)

    def _close_failure(self, resource: acquisition.IncraResource, exc: httpx.HTTPError) -> None:
        resource.close_error_type = type(exc).__name__
