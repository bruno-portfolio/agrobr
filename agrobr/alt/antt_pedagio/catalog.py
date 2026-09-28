from __future__ import annotations

import re
import unicodedata
from typing import Literal
from urllib.parse import unquote, urlsplit

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, model_validator

from agrobr.exceptions import SourceUnavailableError

Frequency = Literal["mensal", "diaria"]


class CatalogResource(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    id: StrictStr
    name: StrictStr
    url: StrictStr
    format: StrictStr
    size: StrictInt | None = Field(default=None, ge=0)
    created: StrictStr | None = None
    last_modified: StrictStr | None = None
    revision_id: StrictStr | None = None
    datastore_active: StrictBool | None = None


class CatalogPackage(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    id: StrictStr
    name: StrictStr
    resources: list[CatalogResource]
    license_id: StrictStr | None = None
    metadata_created: StrictStr | None = None
    metadata_modified: StrictStr | None = None

    @model_validator(mode="after")
    def unique_resource_ids(self) -> CatalogPackage:
        identifiers = [resource.id for resource in self.resources]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("CKAN resource IDs duplicados")
        return self


class CatalogEnvelope(BaseModel):
    model_config = ConfigDict(extra="allow")

    success: StrictBool
    result: CatalogPackage

    @model_validator(mode="after")
    def successful(self) -> CatalogEnvelope:
        if not self.success:
            raise ValueError("CKAN success=false")
        return self


def resource_text(resource: CatalogResource) -> str:
    filename = unquote(urlsplit(resource.url).path.rsplit("/", 1)[-1])
    text = f"{resource.name} {filename}"
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()


def is_csv(resource: CatalogResource) -> bool:
    fmt = resource.format.strip().upper()
    return bool(resource.url) and (
        fmt == "CSV" or (not fmt and unquote(urlsplit(resource.url).path).lower().endswith(".csv"))
    )


def priority(resource: CatalogResource, year: int, frequency: Frequency) -> int | None:
    if not is_csv(resource):
        return None
    text = resource_text(resource)
    years = set(re.findall(r"(?<![a-z0-9])(?:19|20)\d{2}(?![a-z0-9])", text))
    if years != {str(year)}:
        return None
    tokens = set(re.findall(r"[a-z0-9]+", text))
    if {"mensal", "diario"} <= tokens:
        return None
    if frequency == "diaria":
        return 1 if "diario" in tokens else None
    if "diario" in tokens:
        return None
    if "mensal" in tokens:
        return 3 if "consolidado" in tokens else 2
    return 1 if year < 2024 else None


def select_traffic(
    resources: list[CatalogResource], year: int, frequency: Frequency
) -> CatalogResource:
    candidates = [
        (rank, resource)
        for resource in resources
        if (rank := priority(resource, year, frequency)) is not None
    ]
    if not candidates:
        raise SourceUnavailableError(
            source="antt_pedagio",
            last_error=f"Recurso CSV ausente: ano={year}, frequencia={frequency}",
        )
    rank = max(item[0] for item in candidates)
    selected = [resource for value, resource in candidates if value == rank]
    if len(selected) != 1:
        raise SourceUnavailableError(
            source="antt_pedagio",
            last_error=f"Recurso CSV ambíguo: ano={year}, frequencia={frequency}, ids={[item.id for item in selected]}",
        )
    return selected[0]


def select_plazas(resources: list[CatalogResource]) -> CatalogResource:
    candidates = [resource for resource in resources if is_csv(resource)]
    if len(candidates) != 1:
        raise SourceUnavailableError(
            source="antt_pedagio",
            last_error=f"Cadastro CSV ausente ou ambíguo: ids={[item.id for item in candidates]}",
        )
    return candidates[0]


def validate_download_url(url: str) -> None:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "dados.antt.gov.br"
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise SourceUnavailableError(
            source="antt_pedagio", url=url, last_error="URL fora da origem HTTPS oficial ANTT"
        )
