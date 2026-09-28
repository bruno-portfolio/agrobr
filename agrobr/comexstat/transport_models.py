from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, BinaryIO, Literal

from pydantic import BaseModel, ConfigDict, Field

Flow = Literal["exportacao", "importacao"]
Dictionary = Literal["unidades", "paises", "vias", "urfs"]


class ResourceSpec(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    kind: Literal["annual", "dictionary"]
    url: str
    fluxo: Flow | None = None
    ano: int | None = None
    tabela: Dictionary | None = None


class DownloadReceipt(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    index: int
    attempt: int
    url: str
    started_at: datetime
    finished_at: datetime | None = None
    status: int | None = None
    headers: list[tuple[str, str]] = Field(default_factory=list)
    received_bytes: int = 0
    size_bytes: int = 0
    sha256: str | None = None
    saved_sha256: str | None = None
    complete_body: bool = False
    closed: bool = False
    error_type: str | None = None
    error_message: str | None = None
    close_error_type: str | None = None
    close_error_message: str | None = None


class CloseFailure(BaseModel):
    scope: Literal["client", "spool"]
    error_type: str
    error_message: str
    at: datetime


@dataclass
class DownloadedResource:
    resource: ResourceSpec
    _file: BinaryIO | None = field(default=None, repr=False)
    receipts: list[DownloadReceipt] = field(default_factory=list)
    resource_index: int | None = None
    size_bytes: int = 0
    sha256: str | None = None
    transfer_bytes: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    fetched_at: datetime | None = None
    finished_at: datetime | None = None
    complete: bool = False
    size_check: str | None = None
    client_closed: bool | None = None
    spool_closed: bool | None = None
    close_errors: list[CloseFailure] = field(default_factory=list)
    tls: dict[str, Any] = field(default_factory=dict)
    budgets: dict[str, int] = field(default_factory=dict)

    @property
    def file(self) -> BinaryIO:
        if self._file is None:
            raise RuntimeError("Recurso temporário ainda não foi aberto")
        return self._file

    def details(self) -> dict[str, Any]:
        return {
            "resource": self.resource.model_dump(mode="json"),
            "receipts": [item.model_dump(mode="json") for item in self.receipts],
            "resource_index": self.resource_index,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "transfer_bytes": self.transfer_bytes,
            "started_at": self.started_at.isoformat(),
            "fetched_at": self.fetched_at.isoformat() if self.fetched_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "complete": self.complete,
            "size_check": self.size_check,
            "client_closed": self.client_closed,
            "spool_closed": self.spool_closed,
            "close_errors": [item.model_dump(mode="json") for item in self.close_errors],
            "tls": deepcopy(self.tls),
            "budgets": dict(self.budgets),
            "hash_kind": "raw_identity_sha256",
            "transfer_basis": "all_received_attempt_bytes_including_retries_and_redirects",
            "source_revision_snapshot": False,
        }
