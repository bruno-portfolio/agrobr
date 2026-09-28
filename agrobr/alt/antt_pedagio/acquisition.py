from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, BinaryIO, Literal

from pydantic import BaseModel, ConfigDict, Field

from agrobr import constants

from . import catalog


class AttemptReceipt(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    role: Literal["catalog", "trafego", "pracas"]
    logical_index: int
    attempt: int
    url: str
    resource_id: str | None = None
    year: int | None = None
    started_at: datetime
    finished_at: datetime | None = None
    status: int | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    size_bytes: int = 0
    sha256: str | None = None
    complete_body: bool = False
    closed: bool = False
    error_type: str | None = None
    error_message: str | None = None
    close_error_type: str | None = None
    close_error_message: str | None = None


@dataclass
class DownloadedCSV:
    ano: int | None
    frequencia: catalog.Frequency | None
    resource: catalog.CatalogResource
    file: BinaryIO
    size_bytes: int
    sha256: str
    resource_index: int

    def details(self) -> dict[str, Any]:
        return {
            "ano": self.ano,
            "frequencia": self.frequencia,
            "resource": self.resource.model_dump(mode="json"),
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "resource_index": self.resource_index,
        }


@dataclass
class TrafegoAcquisition:
    requested_years: list[int]
    frequencia: catalog.Frequency | None
    files: list[DownloadedCSV] = field(default_factory=list)
    catalog: catalog.CatalogPackage | None = None
    catalog_sha256: str | None = None
    catalog_size_bytes: int = 0
    catalog_resource_index: int | None = None
    attempts: list[AttemptReceipt] = field(default_factory=list)
    selected_resources: list[dict[str, Any]] = field(default_factory=list)
    transfer_bytes: int = 0
    spool_bytes: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    complete: bool = False
    spool_close_errors: list[dict[str, Any]] = field(default_factory=list)

    def details(self) -> dict[str, Any]:
        return {
            "requested_years": list(self.requested_years),
            "frequencia": self.frequencia,
            "catalog": self.catalog.model_dump(mode="json") if self.catalog is not None else None,
            "catalog_sha256": self.catalog_sha256,
            "catalog_size_bytes": self.catalog_size_bytes,
            "catalog_resource_index": self.catalog_resource_index,
            "files": [item.details() for item in self.files],
            "attempts": [item.model_dump(mode="json") for item in self.attempts],
            "selected_resources": deepcopy(self.selected_resources),
            "transfer_bytes": self.transfer_bytes,
            "spool_bytes": self.spool_bytes,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "complete": self.complete,
            "spool_close_errors": deepcopy(self.spool_close_errors),
            "source_revision_snapshot": False,
            "hash_kind": "decoded_body_sha256",
            "resource_index_semantics": "zero_based_successful_attempt_index",
            "budgets": {
                "catalog_bytes": constants.ANTT_MAX_CATALOG_BYTES,
                "csv_bytes": constants.ANTT_MAX_CSV_BYTES,
                "pracas_bytes": constants.ANTT_MAX_PRACAS_BYTES,
                "spool_bytes": constants.ANTT_MAX_SPOOL_BYTES,
                "transfer_bytes": constants.ANTT_MAX_TRANSFER_BYTES,
                "disk_write_chunk_bytes": constants.ANTT_STREAM_CHUNK_BYTES,
                "attempts_per_logical_request": constants.ANTT_MAX_ATTEMPTS,
            },
        }

    def close(self) -> None:
        first: OSError | None = None
        for index, item in enumerate(self.files):
            try:
                item.file.close()
            except OSError as exc:
                self.spool_close_errors.append(
                    {
                        "file_index": index,
                        "resource_id": item.resource.id,
                        "ano": item.ano,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "at": datetime.now(UTC).isoformat(),
                    }
                )
                if first is None:
                    first = exc
        if first is not None:
            raise first
