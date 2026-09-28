from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, Literal

import httpx
import pydantic

from agrobr import constants

Family = Literal["registradas", "protegidas"]


class HTTPResource(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    requested_url: str = pydantic.Field(strict=True)
    url: str = pydantic.Field(strict=True)
    method: Literal["POST"]
    status_code: int = pydantic.Field(strict=True, ge=200, le=299)
    received_at: datetime
    sha256: str = pydantic.Field(strict=True, pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = pydantic.Field(strict=True, gt=0)
    headers: dict[str, str]

    @pydantic.field_validator("received_at")
    @classmethod
    def validate_utc(cls, value: datetime) -> datetime:
        if value.utcoffset() != UTC.utcoffset(None):
            raise ValueError("Horário da aquisição deve ser UTC com fuso explícito")
        return value

    @pydantic.field_validator("headers")
    @classmethod
    def validate_headers(cls, value: dict[str, str]) -> dict[str, str]:
        if set(value) - set(constants.RNC_PUBLIC_RESPONSE_HEADERS):
            raise ValueError("Headers incompatíveis com a proveniência pública")
        return value


class SearchResource(HTTPResource):
    reported_total: int | None = pydantic.Field(default=None, strict=True, ge=0)


class AcquisitionInfo(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    kind: Family
    resource: HTTPResource
    search: SearchResource

    @pydantic.model_validator(mode="after")
    def validate_family(self) -> AcquisitionInfo:
        expected = httpx.URL(constants.RNC_PUBLIC_URLS[self.kind])
        for resource in (self.resource, self.search):
            for value in (resource.requested_url, resource.url):
                try:
                    actual = httpx.URL(value)
                except httpx.InvalidURL as error:
                    raise ValueError("URL pública RNC/SNPC inválida") from error
                if (
                    actual.scheme != expected.scheme
                    or actual.host != expected.host
                    or actual.port != expected.port
                    or actual.path != expected.path
                    or actual.userinfo
                    or actual.fragment
                ):
                    raise ValueError("URL incompatível com a família RNC/SNPC")
        if self.search.received_at > self.resource.received_at:
            raise ValueError("Pesquisa posterior à exportação na mesma aquisição")
        return self


class CSVAcquisition(AcquisitionInfo):
    content: bytes = pydantic.Field(strict=True, exclude=True, repr=False)

    @pydantic.model_validator(mode="after")
    def validate_content(self) -> CSVAcquisition:
        if len(self.content) != self.resource.size_bytes:
            raise ValueError("Tamanho do CSV incompatível com a aquisição")
        if hashlib.sha256(self.content).hexdigest() != self.resource.sha256:
            raise ValueError("Hash do CSV incompatível com a aquisição")
        return self

    def provenance(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def describe_response(response: httpx.Response) -> HTTPResource:
    return HTTPResource.model_validate(
        {
            "requested_url": str(response.request.url),
            "url": str(response.url),
            "method": response.request.method,
            "status_code": response.status_code,
            "received_at": datetime.now(UTC),
            "sha256": hashlib.sha256(response.content).hexdigest(),
            "size_bytes": len(response.content),
            "headers": {
                name: response.headers[name]
                for name in constants.RNC_PUBLIC_RESPONSE_HEADERS
                if name in response.headers
            },
        }
    )
