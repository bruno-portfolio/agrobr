from __future__ import annotations

import ssl
from datetime import UTC, datetime
from typing import Any, Literal

from agrobr import constants
from agrobr.http import settings, wfs_transport

from . import _tls, acquisition

TIMEOUT = settings.get_timeout(read=120.0)
Role = Literal["count_before", "page", "count_after"]


class Transport(wfs_transport.Transport[acquisition.FunaiResource, Role]):
    def __init__(self) -> None:
        super().__init__(
            source="funai",
            timeout=TIMEOUT,
            initial_role="count_before",
            resource_factory=acquisition.FunaiResource.model_validate,
            body_limit=lambda: constants.FUNAI_MAX_BODY_BYTES,
            total_limit=lambda: constants.FUNAI_MAX_TOTAL_BODY_BYTES,
        )

    def _request_fields(self) -> dict[str, Any]:
        return {"started_at": datetime.now(UTC)}

    def _finished(self, resource: acquisition.FunaiResource) -> None:
        resource.finished_at = datetime.now(UTC)

    def _verify(self) -> ssl.SSLContext:
        return _tls.build_context()
