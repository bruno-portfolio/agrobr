from __future__ import annotations

import asyncio
import json
import threading
import zipfile
import zlib
from datetime import UTC, datetime
from pathlib import Path
from weakref import WeakKeyDictionary, WeakValueDictionary

import pydantic

from agrobr import _log, constants
from agrobr.utils.atomic import atomic_output

from . import acquisition, parser

logger = _log.get_logger(__name__)

_LOCKS: WeakKeyDictionary[asyncio.AbstractEventLoop, WeakValueDictionary[str, asyncio.Lock]] = (
    WeakKeyDictionary()
)
_LOCKS_GUARD = threading.Lock()


def cache_dir() -> Path:
    directory = constants.CacheSettings().cache_dir / "rnc"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


class _Manifest(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    format_version: int = pydantic.Field(strict=True)
    parser_version: int = pydantic.Field(strict=True)
    schema_version: str = pydantic.Field(strict=True)
    acquisition: acquisition.AcquisitionInfo

    @pydantic.model_validator(mode="after")
    def validate_versions(self) -> _Manifest:
        if self.format_version != constants.RNC_CACHE_FORMAT_VERSION:
            raise ValueError("Versão incompatível do cache RNC/SNPC")
        if self.parser_version != parser.PARSER_VERSION or self.schema_version != "1.0":
            raise ValueError("Parser ou contrato incompatível do cache RNC/SNPC")
        return self


def _validate_kind(kind: str) -> None:
    if kind not in constants.RNC_PUBLIC_URLS:
        raise ValueError(f"Família RNC/SNPC desconhecida: {kind}")


def acquisition_lock(kind: str) -> asyncio.Lock:
    _validate_kind(kind)
    loop = asyncio.get_running_loop()
    with _LOCKS_GUARD:
        locks = _LOCKS.setdefault(loop, WeakValueDictionary())
        lock = locks.get(kind)
        if lock is None:
            lock = asyncio.Lock()
            locks[kind] = lock
        return lock


def _read_bundle(bundle: zipfile.ZipFile, kind: str) -> acquisition.CSVAcquisition | None:
    expected = {"manifest.json", "source.csv"}
    members = bundle.infolist()
    if len(members) != len(expected) or {item.filename for item in members} != expected:
        raise ValueError("Membros incompatíveis do cache RNC/SNPC")
    if any(item.flag_bits & 1 for item in members):
        raise ValueError("Membro criptografado no cache RNC/SNPC")
    manifest = _Manifest.model_validate_json(bundle.read("manifest.json"))
    if manifest.acquisition.kind != kind:
        raise ValueError("Aquisição de outra família no cache RNC/SNPC")
    age = (datetime.now(UTC) - manifest.acquisition.resource.received_at).total_seconds()
    if not 0 <= age < constants.RNC_CACHE_TTL_SECONDS:
        logger.info("rnc_snapshot_stale", kind=kind)
        return None
    return acquisition.CSVAcquisition.model_validate(
        {**manifest.acquisition.model_dump(), "content": bundle.read("source.csv")}
    )


def read_acquisition(kind: str) -> acquisition.CSVAcquisition | None:
    _validate_kind(kind)
    try:
        path = cache_dir() / f"{kind}.acquisition.v1.zip"
        if not path.exists():
            return None
        with zipfile.ZipFile(path) as bundle:
            captured = _read_bundle(bundle, kind)
    except (
        OSError,
        UnicodeError,
        ValueError,
        TypeError,
        KeyError,
        OverflowError,
        EOFError,
        zipfile.BadZipFile,
        NotImplementedError,
        zlib.error,
    ) as error:
        logger.warning("rnc_snapshot_invalid", kind=kind, error=type(error).__name__)
        return None
    if captured is not None:
        logger.info("rnc_snapshot_hit", kind=kind)
    return captured


def write_acquisition(captured: acquisition.CSVAcquisition) -> None:
    manifest = _Manifest(
        format_version=constants.RNC_CACHE_FORMAT_VERSION,
        parser_version=parser.PARSER_VERSION,
        schema_version="1.0",
        acquisition=acquisition.AcquisitionInfo.model_validate(captured.model_dump()),
    )
    path = cache_dir() / f"{captured.kind}.acquisition.v1.zip"
    with (
        atomic_output(path) as temporary,
        zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as bundle,
    ):
        bundle.writestr("source.csv", captured.content)
        bundle.writestr("manifest.json", json.dumps(manifest.model_dump(mode="json")))
    logger.info("rnc_snapshot_write", kind=captured.kind, size_bytes=len(captured.content))
