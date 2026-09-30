from __future__ import annotations

import asyncio
import hashlib
import io
import threading
import zipfile
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4
from weakref import WeakKeyDictionary, WeakValueDictionary

import pandas as pd
import pydantic

from agrobr import _log, constants, models
from agrobr.utils.atomic import atomic_output

from . import cache, parser
from . import models as source_models

logger = _log.get_logger(__name__)

_TABLES = {
    "formulados": ("formulados", "autorizacoes", "composicao"),
    "tecnicos": ("tecnicos", "composicao"),
}
_META_ADAPTER = pydantic.TypeAdapter(models.MetaInfo)
_LOCKS: WeakKeyDictionary[asyncio.AbstractEventLoop, WeakValueDictionary[str, asyncio.Lock]] = (
    WeakKeyDictionary()
)
_LOCKS_GUARD = threading.Lock()


@dataclass
class Snapshot:
    tables: dict[str, pd.DataFrame]
    meta: models.MetaInfo


class _TableManifest(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    schema_version: Literal["1.0", "1.1"]
    columns: list[str]
    dtypes: dict[str, str]
    rows: int = pydantic.Field(ge=0, strict=True)
    sha256: str = pydantic.Field(pattern=r"^[0-9a-f]{64}$")

    @pydantic.model_validator(mode="after")
    def validate_layout(self) -> _TableManifest:
        if not self.columns or len(self.columns) != len(set(self.columns)):
            raise ValueError("Colunas ausentes ou duplicadas no snapshot")
        if set(self.dtypes) != set(self.columns):
            raise ValueError("Tipos incompatíveis no snapshot")
        numeric = {"concentracao_valor": "Float64", "ordem_componente": "Int64"}
        for column, dtype in self.dtypes.items():
            expected = {numeric[column]} if column in numeric else {"object", "string", "str"}
            if dtype not in expected:
                raise ValueError(f"Tipo incompatível para {column}")
        return self


class _Manifest(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    format_version: int = pydantic.Field(strict=True)
    parser_version: int = pydantic.Field(strict=True)
    kind: Literal["formulados", "tecnicos"]
    fetched_at: datetime
    null_token: str = pydantic.Field(pattern=r"^__agrobr_null_[0-9a-f]{32}__$")
    tables: dict[str, _TableManifest]
    meta: dict[str, Any]

    @pydantic.model_validator(mode="after")
    def validate_acquisition(self) -> _Manifest:
        if self.format_version != constants.DEFENSIVOS_CACHE_FORMAT_VERSION:
            raise ValueError("Versão de formato incompatível no snapshot")
        if self.parser_version != parser.PARSER_VERSION:
            raise ValueError("Versão de parser incompatível no snapshot")
        if set(self.tables) != set(_TABLES[self.kind]):
            raise ValueError("Tabelas incompatíveis com o acervo")
        for name, table in self.tables.items():
            _validate_columns(name, table.columns)
            if table.schema_version != ("1.0" if name == "composicao" else "1.1"):
                raise ValueError("Schema incompatível no snapshot")
        meta = _META_ADAPTER.validate_python(self.meta)
        if meta.source != "defensivos" or meta.parser_version != self.parser_version:
            raise ValueError("Proveniência incompatível no snapshot")
        if meta.schema_version not in {"1.0", "1.1"}:
            raise ValueError("Schema de metadados incompatível no snapshot")
        if meta.fetched_at != self.fetched_at:
            raise ValueError("Timestamp de aquisição incompatível")
        if self.fetched_at.utcoffset() != UTC.utcoffset(None):
            raise ValueError("Timestamp de aquisição incompatível ou sem UTC")
        digest = meta.raw_content_hash
        if (
            not digest
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise ValueError("Hash da aquisição inválido")
        models.MetaInfo.from_dict(self.meta)
        self.meta = meta.to_dict()
        return self


def _table_names(kind: str) -> tuple[str, ...]:
    if kind not in _TABLES:
        raise ValueError(f"Acervo de defensivos desconhecido: {kind}")
    return _TABLES[kind]


def _validate_columns(name: str, columns: list[str]) -> None:
    expected = {
        "formulados": source_models.FORMULADOS_PRODUCT_COLS,
        "autorizacoes": source_models.AUTORIZACOES_COLS,
        "tecnicos": source_models.TECNICOS_COLS,
        "composicao": source_models.COMPOSICAO_COLS,
    }[name]
    if set(columns) != set(expected):
        raise ValueError(f"Colunas incompatíveis com o schema de {name}")


def _validate_relationships(kind: str, tables: dict[str, pd.DataFrame]) -> None:
    if not tables["composicao"]["tipo"].eq(kind).fillna(False).all():
        raise ValueError("Família de composição incompatível com o acervo")
    parents = tables[kind]["nr_registro"]
    for name in _table_names(kind):
        if name != kind and not tables[name]["nr_registro"].isin(parents).all():
            raise ValueError(f"Registros sem produto correspondente em {name}")


def acquisition_lock(kind: str) -> asyncio.Lock:
    _table_names(kind)
    loop = asyncio.get_running_loop()
    with _LOCKS_GUARD:
        locks = _LOCKS.setdefault(loop, WeakValueDictionary())
        lock = locks.get(kind)
        if lock is None:
            lock = asyncio.Lock()
            locks[kind] = lock
        return lock


def _serialize_table(frame: pd.DataFrame, null_token: str) -> bytes:
    for column in frame.columns:
        series = frame[column]
        if str(series.dtype) in {"object", "string", "str"}:
            values = series.dropna()
            if not values.map(lambda value: isinstance(value, str)).all():
                raise ValueError(f"Valores não textuais em {column}")
    return frame.astype(object).to_csv(sep=";", index=False, na_rep=null_token).encode("utf-8")


def _prepare_snapshot(
    kind: str, tables: dict[str, pd.DataFrame], meta: models.MetaInfo
) -> tuple[_Manifest, dict[str, bytes]]:
    names = _table_names(kind)
    if set(tables) != set(names):
        raise ValueError("Tabelas incompatíveis com o acervo")
    null_token = f"__agrobr_null_{uuid4().hex}__"
    table_manifests = {}
    contents = {}
    for name in names:
        frame = tables[name]
        table = _TableManifest(
            schema_version="1.0" if name == "composicao" else "1.1",
            columns=frame.columns.tolist(),
            dtypes={str(column): str(dtype) for column, dtype in frame.dtypes.items()},
            rows=len(frame),
            sha256="0" * 64,
        )
        content = _serialize_table(frame, null_token)
        table.sha256 = hashlib.sha256(content).hexdigest()
        table_manifests[name] = table
        contents[name] = content
    metadata = meta.to_dict()
    manifest = _Manifest.model_validate(
        {
            "format_version": constants.DEFENSIVOS_CACHE_FORMAT_VERSION,
            "parser_version": parser.PARSER_VERSION,
            "kind": kind,
            "fetched_at": metadata["fetched_at"],
            "null_token": null_token,
            "tables": table_manifests,
            "meta": metadata,
        }
    )
    _validate_relationships(kind, tables)
    return manifest, contents


def write_snapshot(kind: str, tables: dict[str, pd.DataFrame], meta: models.MetaInfo) -> None:
    manifest, contents = _prepare_snapshot(kind, tables, meta)
    path = cache.snapshot_path(kind)
    with (
        atomic_output(path) as temporary,
        zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as bundle,
    ):
        for name, content in contents.items():
            bundle.writestr(f"{name}.csv", content)
        bundle.writestr("manifest.json", manifest.model_dump_json())
    logger.info("defensivos_snapshot_write", kind=kind, tables=list(contents))


def _read_table(content: bytes, table: _TableManifest, null_token: str) -> pd.DataFrame:
    if hashlib.sha256(content).hexdigest() != table.sha256:
        raise ValueError("Hash de tabela incompatível")
    frame = pd.read_csv(
        io.BytesIO(content), sep=";", dtype=object, keep_default_na=False, na_filter=False
    )
    if frame.columns.tolist() != table.columns or len(frame) != table.rows:
        raise ValueError("Layout ou contagem de tabela incompatível")
    for column, dtype in table.dtypes.items():
        series = frame[column].astype(object).mask(frame[column].eq(null_token), pd.NA)
        if dtype == "str" and int(pd.__version__.split(".")[0]) < 3:
            dtype = "object"
        frame[column] = series.astype(pd.api.types.pandas_dtype(dtype))
    return frame


def _read_bundle(bundle: zipfile.ZipFile, kind: str) -> Snapshot | None:
    names = _table_names(kind)
    expected = {"manifest.json", *(f"{name}.csv" for name in names)}
    if len(bundle.namelist()) != len(expected) or set(bundle.namelist()) != expected:
        raise ValueError("Membros incompatíveis no snapshot")
    if any(member.flag_bits & 1 for member in bundle.infolist()):
        raise ValueError("Membros criptografados incompatíveis no snapshot")
    manifest = _Manifest.model_validate_json(bundle.read("manifest.json"))
    if manifest.kind != kind:
        raise ValueError("Acervo incompatível no snapshot")
    age = (datetime.now(UTC) - manifest.fetched_at).total_seconds()
    if not 0 <= age < constants.DEFENSIVOS_CACHE_TTL_SECONDS:
        logger.info("defensivos_snapshot_stale", kind=kind)
        return None
    frames = {
        name: _read_table(bundle.read(f"{name}.csv"), manifest.tables[name], manifest.null_token)
        for name in names
    }
    _validate_relationships(kind, frames)
    return Snapshot(tables=frames, meta=models.MetaInfo.from_dict(manifest.meta))


def read_snapshot(kind: str) -> Snapshot | None:
    _table_names(kind)
    try:
        path = cache.snapshot_path(kind)
        if not path.exists():
            return None
        with zipfile.ZipFile(path) as bundle:
            result = _read_bundle(bundle, kind)
    except (
        OSError,
        UnicodeError,
        ValueError,
        TypeError,
        KeyError,
        OverflowError,
        zipfile.BadZipFile,
        NotImplementedError,
        zlib.error,
        pd.errors.ParserError,
        pd.errors.EmptyDataError,
    ) as exc:
        logger.warning("defensivos_snapshot_read_failed", kind=kind, error=type(exc).__name__)
        return None
    if result is not None:
        logger.info("defensivos_snapshot_hit", kind=kind)
    return result
