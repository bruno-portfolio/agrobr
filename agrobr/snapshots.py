from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from agrobr import _log
from agrobr.config import get_config
from agrobr.exceptions import SnapshotError
from agrobr.utils import atomic
from agrobr.utils import time as time_utils

if TYPE_CHECKING:
    from agrobr.models import MetaInfo

logger = _log.get_logger(__name__)

_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_\-\.]*$")
_WINDOWS_RESERVED = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{n}" for n in range(1, 10)),
        *(f"LPT{n}" for n in range(1, 10)),
    }
)


def _validate_path_component(value: str, label: str) -> None:
    if not value or not _SAFE_NAME_RE.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    if value.endswith(".") or value.split(".", 1)[0].upper() in _WINDOWS_RESERVED:
        raise ValueError(
            f"Invalid {label}: {value!r} (nome reservado do Windows ou terminado em ponto; "
            "o snapshot tem de abrir em qualquer sistema)"
        )


@dataclass
class SnapshotManifest:
    name: str
    created_at: datetime
    agrobr_version: str
    sources: list[str] = field(default_factory=list)
    files: dict[str, dict[str, Any]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "created_at": self.created_at.isoformat(),
            "agrobr_version": self.agrobr_version,
            "sources": self.sources,
            "files": self.files,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SnapshotManifest:
        data = data.copy()
        if isinstance(data.get("created_at"), str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        return cls(**data)


@dataclass
class SnapshotInfo:
    name: str
    path: Path
    created_at: datetime
    size_bytes: int
    sources: list[str]
    file_count: int
    errors: dict[str, list[str]] = field(default_factory=dict)


def get_snapshots_dir() -> Path:
    config = get_config()
    return config.get_snapshot_dir()


def list_snapshots() -> list[SnapshotInfo]:
    snapshots_dir = get_snapshots_dir()

    if not snapshots_dir.exists():
        return []

    snapshots = []
    for path in sorted(snapshots_dir.iterdir()):
        if not path.is_dir() or path.name.startswith("."):
            continue

        manifest_path = path / "manifest.json"
        if not manifest_path.exists():
            continue

        try:
            with open(manifest_path, encoding="utf-8") as f:
                manifest = SnapshotManifest.from_dict(json.load(f))

            size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
            file_count = len(list(path.rglob("*.parquet")))

            snapshots.append(
                SnapshotInfo(
                    name=manifest.name,
                    path=path,
                    created_at=manifest.created_at,
                    size_bytes=size,
                    sources=manifest.sources,
                    file_count=file_count,
                )
            )
        except Exception as e:
            logger.warning("snapshot_read_error", path=str(path), error=str(e))

    return snapshots


def get_snapshot(name: str) -> SnapshotInfo | None:
    for snapshot in list_snapshots():
        if snapshot.name == name:
            return snapshot
    return None


async def create_snapshot(
    name: str | None = None,
    sources: list[str] | None = None,
) -> SnapshotInfo:
    import agrobr

    if name is None:
        name = date.today().isoformat()

    _validate_path_component(name, "snapshot name")

    if sources is None:
        sources = ["cepea", "conab", "ibge"]
    if any(source not in {"cepea", "conab", "ibge"} for source in sources):
        raise ValueError("Fontes inválidas para snapshot. Válidas: cepea, conab, ibge")
    sources = list(dict.fromkeys(sources))

    if not any(
        importlib.util.find_spec(engine) is not None for engine in ("pyarrow", "fastparquet")
    ):
        raise ImportError(
            'pyarrow é necessário para snapshots. Instale com: pip install "pyarrow>=14.0.1"'
        )

    snapshots_dir = get_snapshots_dir()
    snapshot_path = snapshots_dir / name

    if not snapshot_path.resolve().is_relative_to(snapshots_dir.resolve()):
        raise ValueError(f"Invalid snapshot name: {name!r}")

    if snapshot_path.exists():
        raise ValueError(f"Snapshot '{name}' already exists")

    snapshots_dir.mkdir(parents=True, exist_ok=True)

    manifest = SnapshotManifest(
        name=name,
        created_at=time_utils.utcnow_aware(),
        agrobr_version=getattr(agrobr, "__version__", "unknown"),
        sources=sources,
    )

    with tempfile.TemporaryDirectory(prefix=f".{name}-", dir=snapshots_dir) as temporary:
        staging = Path(temporary)
        await _collect_snapshot(staging, manifest)
        if not manifest.files:
            raise SnapshotError(manifest.metadata.get("errors", {}))
        try:
            for filename, info in manifest.files.items():
                info["sha256"] = _file_digest(staging / filename)
            manifest.metadata["integrity"] = "sha256"
            with open(staging / "manifest.json", "w", encoding="utf-8") as stream:
                json.dump(manifest.to_dict(), stream, indent=2, ensure_ascii=False)
            if snapshot_path.exists():
                raise ValueError(f"Snapshot '{name}' already exists")
            atomic.replace_with_retry(staging, snapshot_path)
        except OSError as exc:
            raise SnapshotError(
                {"snapshot": [str(exc)]},
                message="Falha ao publicar snapshot.",
            ) from exc

    logger.info("snapshot_created", name=name, path=str(snapshot_path))
    return SnapshotInfo(
        name=name,
        path=snapshot_path,
        created_at=manifest.created_at,
        size_bytes=sum(p.stat().st_size for p in snapshot_path.rglob("*") if p.is_file()),
        sources=sources,
        file_count=len(manifest.files),
        errors=manifest.metadata.get("errors", {}),
    )


async def _collect_snapshot(path: Path, manifest: SnapshotManifest) -> None:
    for source in manifest.sources:
        source_path = path / source
        source_path.mkdir()
        try:
            if source == "cepea":
                await _snapshot_cepea(source_path, manifest)
            elif source == "conab":
                await _snapshot_conab(source_path, manifest)
            elif source == "ibge":
                await _snapshot_ibge(source_path, manifest)
        except Exception as exc:
            logger.error("snapshot_source_error", source=source, error=str(exc))
            _record_error(manifest, source, str(exc))
        if not any(filename.startswith(f"{source}/") for filename in manifest.files):
            _record_error(manifest, source, "Nenhum conjunto de dados disponível")


def _file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _record_error(manifest: SnapshotManifest, source: str, message: str) -> None:
    manifest.metadata.setdefault("errors", {}).setdefault(source, []).append(message)


def _registrar(
    manifest: SnapshotManifest,
    chave: str,
    df: pd.DataFrame,
    meta: MetaInfo,
    parametros: dict[str, Any],
) -> None:
    manifest.files[chave] = {
        "rows": len(df),
        "columns": df.columns.tolist(),
        "source": meta.source,
        "selected_source": meta.selected_source,
        "source_url": meta.source_url,
        "fetch_timestamp": meta.fetch_timestamp.isoformat() if meta.fetch_timestamp else None,
        "parametros": parametros,
    }


async def _snapshot_cepea(path: Path, manifest: SnapshotManifest) -> None:
    from agrobr import cepea

    produtos = await cepea.produtos()

    for produto in produtos:
        try:
            df, meta = await cepea.indicador(
                produto, inicio=date.min, offline=True, return_meta=True
            )
            if not df.empty:
                df.to_parquet(path / f"{produto}.parquet", index=False)
                datas = pd.to_datetime(df["data"])
                periodo = [datas.min().date().isoformat(), datas.max().date().isoformat()]
                _registrar(
                    manifest,
                    f"cepea/{produto}.parquet",
                    df,
                    meta,
                    {"produto": produto, "offline": True, "periodo": periodo},
                )
        except Exception as e:
            logger.warning("snapshot_produto_error", produto=produto, error=str(e))
            _record_error(manifest, "cepea", f"{produto}: {e}")


async def _snapshot_conab(path: Path, manifest: SnapshotManifest) -> None:
    from agrobr import conab

    try:
        df, meta = await conab.safras(produto="soja", return_meta=True)
        if not df.empty:
            df.to_parquet(path / "safras.parquet", index=False)
            _registrar(manifest, "conab/safras.parquet", df, meta, {"produto": "soja"})
    except Exception as e:
        logger.warning("snapshot_conab_safras_error", error=str(e))
        _record_error(manifest, "conab", f"safras: {e}")

    try:
        df, meta = await conab.balanco(return_meta=True)
        if not df.empty:
            df.to_parquet(path / "balanco.parquet", index=False)
            _registrar(manifest, "conab/balanco.parquet", df, meta, {})
    except Exception as e:
        logger.warning("snapshot_conab_balanco_error", error=str(e))
        _record_error(manifest, "conab", f"balanco: {e}")


async def _snapshot_ibge(path: Path, manifest: SnapshotManifest) -> None:
    from agrobr import ibge

    try:
        df, meta = await ibge.pam(produto="soja", return_meta=True)
        if not df.empty:
            df.to_parquet(path / "pam.parquet", index=False)
            _registrar(manifest, "ibge/pam.parquet", df, meta, {"produto": "soja"})
    except Exception as e:
        logger.warning("snapshot_ibge_pam_error", error=str(e))
        _record_error(manifest, "ibge", f"pam: {e}")

    try:
        df, meta = await ibge.lspa(produto="soja", return_meta=True)
        if not df.empty:
            df.to_parquet(path / "lspa.parquet", index=False)
            _registrar(manifest, "ibge/lspa.parquet", df, meta, {"produto": "soja"})
    except Exception as e:
        logger.warning("snapshot_ibge_lspa_error", error=str(e))
        _record_error(manifest, "ibge", f"lspa: {e}")


def _require_safe_pyarrow() -> None:
    try:
        version = importlib.metadata.version("pyarrow")
    except importlib.metadata.PackageNotFoundError:
        return
    if tuple(int(part) for part in re.findall(r"\d+", version)[:3]) < (14, 0, 1):
        raise ImportError(
            f"pyarrow {version} executa código ao ler Parquet malicioso (CVE-2023-47248); "
            'atualize antes de carregar o snapshot: pip install "pyarrow>=14.0.1"'
        )


def load_from_snapshot(
    source: str,
    dataset: str,
    snapshot_name: str | None = None,
) -> pd.DataFrame | None:
    config = get_config()

    if snapshot_name is None:
        if config.snapshot_date:
            snapshot_name = config.snapshot_date.isoformat()
        else:
            raise ValueError("No snapshot specified and no snapshot_date in config")

    _validate_path_component(snapshot_name, "snapshot name")
    _validate_path_component(source, "source")
    _validate_path_component(dataset, "dataset")

    snapshots_dir = get_snapshots_dir()
    snapshot_path = snapshots_dir / snapshot_name / source / f"{dataset}.parquet"

    if not snapshot_path.resolve().is_relative_to(snapshots_dir.resolve()):
        raise ValueError(f"Invalid path components: {snapshot_name!r}/{source!r}/{dataset!r}")

    if not snapshot_path.exists():
        logger.warning(
            "snapshot_file_not_found",
            source=source,
            dataset=dataset,
            path=str(snapshot_path),
        )
        return None

    manifest_path = snapshots_dir / snapshot_name / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = SnapshotManifest.from_dict(
                json.loads(manifest_path.read_text(encoding="utf-8"))
            )
            chave = f"{source}/{dataset}.parquet"
            registrada = next(
                (nome for nome in manifest.files if nome.casefold() == chave.casefold()), chave
            )
            expected = manifest.files.get(registrada, {}).get("sha256")
            if (expected or manifest.metadata.get("integrity") == "sha256") and (
                expected != _file_digest(snapshot_path)
            ):
                raise SnapshotError(
                    {source: [f"SHA-256 divergente: {dataset}.parquet"]},
                    message="Integridade do snapshot comprometida.",
                )
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise SnapshotError(
                {source: [str(exc)]},
                message="Não foi possível verificar o snapshot.",
            ) from exc

    _require_safe_pyarrow()
    return pd.read_parquet(snapshot_path)


def delete_snapshot(name: str) -> bool:
    _validate_path_component(name, "snapshot name")

    snapshots_dir = get_snapshots_dir()
    snapshot_path = snapshots_dir / name

    if not snapshot_path.resolve().is_relative_to(snapshots_dir.resolve()):
        raise ValueError(f"Invalid snapshot name: {name!r}")

    if not snapshot_path.exists():
        return False

    shutil.rmtree(snapshot_path)
    logger.info("snapshot_deleted", name=name)
    return True


__all__ = [
    "SnapshotManifest",
    "SnapshotInfo",
    "list_snapshots",
    "get_snapshot",
    "create_snapshot",
    "load_from_snapshot",
    "delete_snapshot",
]
