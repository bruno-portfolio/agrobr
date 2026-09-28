from __future__ import annotations

from typing import Literal

from agrobr import constants
from agrobr.http import settings, wfs_transport

from . import acquisition

TIMEOUT = settings.get_timeout(read=180.0)
Role = Literal["hits_before", "page", "hits_after"]


class Transport(wfs_transport.Transport[acquisition.SolosResource, Role]):
    def __init__(self) -> None:
        super().__init__(
            source="embrapa_solos",
            timeout=TIMEOUT,
            initial_role="hits_before",
            resource_factory=acquisition.SolosResource.model_validate,
            body_limit=lambda: constants.EMBRAPA_SOLOS_MAX_BODY_BYTES,
            total_limit=lambda: constants.EMBRAPA_SOLOS_MAX_TOTAL_BODY_BYTES,
        )
