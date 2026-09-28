from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any

import httpx
import pydantic


class HTTPRedirect(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    url: str
    status_code: int
    location: str | None = None


class HTTPResource(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    requested_url: str
    final_url: str
    fetched_at: pydantic.AwareDatetime
    status_code: int
    sha256: str = pydantic.Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = pydantic.Field(ge=0)
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    redirects: list[HTTPRedirect] = pydantic.Field(default_factory=list)

    @classmethod
    def from_response(cls, response: httpx.Response, requested_url: str) -> HTTPResource:
        return cls(
            requested_url=requested_url,
            final_url=str(response.url),
            fetched_at=datetime.now(UTC),
            status_code=response.status_code,
            sha256=hashlib.sha256(response.content).hexdigest(),
            size_bytes=len(response.content),
            content_type=response.headers.get("content-type"),
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
            redirects=[
                HTTPRedirect(
                    url=str(item.url),
                    status_code=item.status_code,
                    location=item.headers.get("location"),
                )
                for item in response.history
            ],
        )


class WorkbookMember(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    name: str
    sha256: str = pydantic.Field(pattern=r"^[0-9a-f]{64}$")
    crc32: str = pydantic.Field(pattern=r"^[0-9a-f]{8}$")
    compressed_size_bytes: int = pydantic.Field(ge=0)
    size_bytes: int = pydantic.Field(ge=0)


class WorkbookAcquisition(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    content: bytes = pydantic.Field(exclude=True, repr=False)
    source_url: str
    resource: HTTPResource
    confirmation: HTTPResource | None = None
    member: WorkbookMember | None = None

    def provenance(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)
