from __future__ import annotations

import hashlib
import json
from typing import Any

import pydantic

from agrobr.exceptions import InvalidParameterError, ParseError

from . import acquisition, models


class CatalogResource(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow", frozen=True, strict=True)

    id: str = pydantic.Field(min_length=1)
    name: str
    url: str
    format: str
    created: str | None = None
    last_modified: str | None = None
    metadata_modified: str | None = None
    hash: str | None = None

    @pydantic.field_validator("url")
    @classmethod
    def http_url(cls, value: str) -> str:
        parsed = pydantic.TypeAdapter(pydantic.AnyHttpUrl).validate_python(value)
        if parsed.username or parsed.password or parsed.fragment:
            raise ValueError("URL do recurso incompatível")
        return value

    def legacy(self) -> dict[str, str]:
        return {name: getattr(self, name) for name in ("id", "name", "url", "format")}

    def revision_key(self) -> str:
        payload = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


class CatalogResult(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow", strict=True)
    resources: list[CatalogResource]


class CatalogEnvelope(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow", strict=True)
    success: bool
    result: CatalogResult

    @pydantic.field_validator("success")
    @classmethod
    def success_required(cls, value: bool) -> bool:
        if not value:
            raise ValueError("CKAN não confirmou sucesso")
        return value


class Catalog(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True, strict=True, extra="forbid")
    acquisition: acquisition.HTTPResource
    publication: CatalogEnvelope

    def resources(self) -> list[dict[str, str]]:
        return [resource.legacy() for resource in self.publication.result.resources]

    def select(self, safra: str | None) -> tuple[str, CatalogResource]:
        available = models.extract_safras(self.resources())
        selected = safra
        if selected is None:
            selected = next((item for item in reversed(available) if item != "perene"), None)
        if selected is None:
            raise InvalidParameterError("Catálogo sem safra anual reconhecida")
        candidates = [
            resource
            for resource in self.publication.result.resources
            if selected in models.extract_safras([resource.legacy()])
        ]
        if not candidates:
            raise InvalidParameterError(
                f"Safra '{selected}' não encontrada. Disponíveis: {available}"
            )
        if len(candidates) != 1:
            raise ParseError(
                source="zarc", parser_version=2, reason=f"Catálogo ambíguo para safra {selected}"
            )
        return selected, candidates[0]


def parse_catalog(payload: Any, resource: acquisition.HTTPResource) -> Catalog:
    try:
        envelope = CatalogEnvelope.model_validate(payload)
        return Catalog(acquisition=resource, publication=envelope)
    except pydantic.ValidationError as exc:
        raise ParseError(
            source="zarc", parser_version=2, reason=f"Catálogo CKAN inválido: {exc}"
        ) from exc
