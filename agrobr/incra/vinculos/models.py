from __future__ import annotations

from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator


class Span(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    start: int = Field(ge=0)
    end: int = Field(ge=0)
    text: str

    @model_validator(mode="after")
    def text_extent(self) -> Span:
        if self.end - self.start != len(self.text):
            raise ValueError("Extensão textual inconsistente")
        return self


class Reference(Span):
    ordinal: int = Field(ge=1)


class CellEvidence(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    position: int = Field(ge=1)
    original: str | None
    references: list[Reference]
    residues: list[Span]
    state: Literal["recognized", "unrecognized", "absent"]

    @model_validator(mode="after")
    def preserved_intervals(self) -> CellEvidence:
        text = self.original or ""
        intervals = sorted([*self.references, *self.residues], key=lambda item: item.start)
        cursor = 0
        for item in intervals:
            if (
                item.start < cursor
                or text[item.start : item.end] != item.text
                or text[cursor : item.start].strip()
            ):
                raise ValueError("Intervalos não preservam célula")
            cursor = item.end
        if text[cursor:].strip():
            raise ValueError("Resíduo textual não representado")
        if [item.ordinal for item in self.references] != list(range(1, len(self.references) + 1)):
            raise ValueError("Ordem das referências inconsistente")
        expected = "recognized" if self.references else "unrecognized" if text.strip() else "absent"
        if self.state != expected:
            raise ValueError("Estado da célula inconsistente")
        return self


class RelationResult(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", arbitrary_types_allowed=True)

    frame: pd.DataFrame = Field(exclude=True)
    evidence: dict[str, list[CellEvidence]]
    counts: dict[str, Any]
    retained_bytes_estimate: int = Field(ge=0)


class MaterializedRelation(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", arbitrary_types_allowed=True)

    frame: pd.DataFrame = Field(exclude=True)
    states: dict[str, int]
    geographical_rows_represented: int = Field(ge=0)
    administrative_rows_represented: int = Field(ge=0)
    geographical_rows_linked: int = Field(ge=0)
    administrative_rows_linked: int = Field(ge=0)
    retained_bytes_estimate: int = Field(ge=0)
