from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pydantic

from agrobr import constants

from . import _buffers, acquisition, cache
from . import query as query_models

_lock = threading.RLock()


class StoredDetails(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, strict=True)

    resource: acquisition.HTTPResource
    parser: dict[str, Any]


class Entry(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, strict=True)

    revision_key: str
    raw_sha256: str
    safra_recurso: str
    received_at: datetime
    details: StoredDetails

    @pydantic.model_validator(mode="after")
    def matching_receipt(self) -> Entry:
        resource = self.details.resource
        if self.raw_sha256 != resource.sha256 or self.received_at != resource.received_at:
            raise ValueError("Metadados do cache ZARC divergem do recibo original")
        return self


def path() -> Path:
    return constants.CacheSettings().cache_dir / constants.ZARC_STORE_FILENAME


@contextmanager
def _connection() -> Iterator[duckdb.DuckDBPyConnection]:
    location = path()
    location.parent.mkdir(parents=True, exist_ok=True)
    with _lock, duckdb.connect(str(location)) as connection:
        fields = ", ".join(
            f'"{name}" {"BIGINT" if name in constants.ZARC_INTEGER_COLUMNS else "VARCHAR"}'
            for name in constants.ZARC_OUTPUT_COLUMNS
        )
        connection.execute(
            f"CREATE TABLE IF NOT EXISTS tabuas ({fields}, revision_key VARCHAR, "
            "raw_sha256 VARCHAR, safra_recurso VARCHAR, received_at TIMESTAMPTZ)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS tabuas_detalhes (revision_key VARCHAR PRIMARY KEY, "
            "raw_sha256 VARCHAR, safra_recurso VARCHAR, details_json VARCHAR, "
            "received_at TIMESTAMPTZ)"
        )
        yield connection


def _entry(connection: duckdb.DuckDBPyConnection, revision_key: str) -> Entry | None:
    row = connection.execute(
        "SELECT revision_key, raw_sha256, safra_recurso, "
        "received_at AT TIME ZONE 'UTC', details_json "
        "FROM tabuas_detalhes WHERE revision_key = ?",
        [revision_key],
    ).fetchone()
    if row is None:
        return None
    received_at = row[3].replace(tzinfo=UTC)
    if cache.now() >= cache.expires_at(received_at):
        return None
    details = StoredDetails.model_validate_json(row[4])
    return Entry(
        revision_key=row[0],
        raw_sha256=row[1],
        safra_recurso=row[2],
        received_at=received_at,
        details=details,
    )


def lookup(revision_key: str) -> Entry | None:
    if not path().is_file():
        return None
    with _connection() as connection:
        return _entry(connection, revision_key)


def _evict(connection: duckdb.DuckDBPyConnection) -> None:
    keep = (
        "SELECT revision_key FROM tabuas_detalhes ORDER BY received_at DESC, revision_key LIMIT ?"
    )
    for table in ("tabuas", "tabuas_detalhes"):
        connection.execute(
            f"DELETE FROM {table} WHERE revision_key NOT IN ({keep})",
            [constants.ZARC_STORE_MAX_REVISIONS],
        )


def store(
    revision_key: str,
    sha256: str,
    safra: str,
    frame: pd.DataFrame,
    details: StoredDetails,
) -> None:
    if sha256 != details.resource.sha256:
        raise ValueError("SHA do cache ZARC difere da aquisição")
    if list(frame.columns) != list(constants.ZARC_OUTPUT_COLUMNS):
        raise ValueError("Colunas do cache ZARC incompatíveis")
    received_at = details.resource.received_at
    with _connection() as connection:
        connection.execute("BEGIN TRANSACTION")
        previous = connection.execute(
            "SELECT received_at AT TIME ZONE 'UTC' FROM tabuas_detalhes WHERE revision_key = ?",
            [revision_key],
        ).fetchone()
        if previous is not None and previous[0].replace(tzinfo=UTC) > received_at:
            connection.execute("ROLLBACK")
            return
        for table in ("tabuas", "tabuas_detalhes"):
            connection.execute(f"DELETE FROM {table} WHERE revision_key = ?", [revision_key])
        for start in range(0, len(frame), constants.ZARC_STORE_BATCH_ROWS):
            connection.register(
                "zarc_batch", frame.iloc[start : start + constants.ZARC_STORE_BATCH_ROWS]
            )
            connection.execute(
                "INSERT INTO tabuas SELECT *, ?, ?, ?, ? FROM zarc_batch",
                [revision_key, sha256, safra, received_at],
            )
            connection.unregister("zarc_batch")
        connection.execute(
            "INSERT INTO tabuas_detalhes VALUES (?, ?, ?, ?, ?)",
            [revision_key, sha256, safra, details.model_dump_json(), received_at],
        )
        _evict(connection)
        connection.execute("COMMIT")


def query(revision_key: str, selected: query_models.ZarcQuery) -> pd.DataFrame:
    predicates = ["revision_key = ?"]
    values: list[Any] = [revision_key]
    for name, value in [
        ("cultura", selected.cultura),
        ("uf", selected.uf),
        ("solo_codigo", selected.solo),
        ("ciclo_codigo", selected.ciclo),
    ]:
        if value is not None:
            predicates.append(f'"{name}" = ?')
            values.append(value)
    if selected.municipio is not None:
        predicates.append("geocodigo = ?")
        values.append(selected.municipio)
    projection = ", ".join(f'"{name}"' for name in constants.ZARC_OUTPUT_COLUMNS)
    with _connection() as connection:
        if _entry(connection, revision_key) is None:
            raise KeyError("Revisão ZARC ausente ou expirada")
        frame = connection.execute(
            f"SELECT {projection} FROM tabuas WHERE {' AND '.join(predicates)} "
            "ORDER BY registro_origem",
            values,
        ).fetchdf()
    for name in constants.ZARC_OUTPUT_COLUMNS:
        frame[name] = frame[name].astype(
            "Int64" if name in constants.ZARC_INTEGER_COLUMNS else _buffers.TEXTO
        )
    return frame


def clear() -> None:
    if path().is_file():
        with _connection() as connection:
            connection.execute("BEGIN TRANSACTION")
            connection.execute("DELETE FROM tabuas")
            connection.execute("DELETE FROM tabuas_detalhes")
            connection.execute("COMMIT")
