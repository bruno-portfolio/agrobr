from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import httpx
import pydantic

from agrobr import constants


class HTTPResource(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)

    requested_url: str
    final_url: str
    status_code: int = pydantic.Field(ge=200, le=299)
    received_at: datetime
    sha256: str = pydantic.Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = pydantic.Field(ge=0)
    published_size_bytes: int | None = pydantic.Field(default=None, ge=0)
    headers: dict[str, str]

    @pydantic.field_validator("received_at")
    @classmethod
    def utc_time(cls, value: datetime) -> datetime:
        if value.utcoffset() != UTC.utcoffset(None):
            raise ValueError("Aquisição requer horário UTC explícito")
        return value


class HTTPAcquisition(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)

    resource: HTTPResource
    content: bytes = pydantic.Field(exclude=True, repr=False)

    @pydantic.model_validator(mode="after")
    def check_content(self) -> HTTPAcquisition:
        if len(self.content) != self.resource.size_bytes:
            raise ValueError("Tamanho do corpo incompatível")
        if hashlib.sha256(self.content).hexdigest() != self.resource.sha256:
            raise ValueError("Hash do corpo incompatível")
        return self


def from_response(
    response: httpx.Response, requested_url: str, *, published_size_bytes: int | None = None
) -> HTTPAcquisition:
    content = response.content
    return HTTPAcquisition(
        content=content,
        resource=HTTPResource(
            requested_url=requested_url,
            final_url=str(response.url),
            status_code=response.status_code,
            received_at=datetime.now(UTC),
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
            published_size_bytes=published_size_bytes,
            headers={
                name: response.headers[name]
                for name in constants.ZARC_PUBLIC_RESPONSE_HEADERS
                if name in response.headers
            },
        ),
    )
