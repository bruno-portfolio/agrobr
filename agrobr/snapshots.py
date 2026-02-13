"""Gerenciamento de snapshots para modo deterministico."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from agrobr.config import get_config

logger = structlog.get_logger()


@dataclass
class SnapshotManifest:
    """Manifesto de um snapshot."""

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
    """Informacoes resumidas de um snapshot."""

    name: str
    path: Path
    created_at: datetime
    size_bytes: int
    sources: list[str]
    file_count: int


def get_snapshots_dir() -> Path:
    config = get_config()
    return config.get_snapshot_dir()


def list_snapshots() -> list[SnapshotInfo]:
    snapshots_dir = get_snapshots_dir()

    if not snapshots_dir.exists():
        return []

    snapshots = []
    for path in sorted(snapshots_dir.iterdir()):
        if not path.is_dir():
            continue

        manifest_path = path / "manifest.json"
        if not manifest_path.exists():
            continue

        try:
            with open(manifest_path) as f:
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
    _include_cache: bool = True,
) -> SnapshotInfo:
    import agrobr

    if name is None:
        name = date.today().isoformat()

    if sources is None:
        sources = ["cepea", "conab", "ibge"]

    snapshots_dir = get_snapshots_dir()
    snapshot_path = snapshots_dir / name

    if snapshot_path.exists():
        raise ValueError(f"Snapshot '{name}' already exists")

    snapshot_path.mkdir(parents=True, exist_ok=True)

    manifest = SnapshotManifest(
        name=name,
        created_at=datetime.now(),
        agrobr_version=getattr(agrobr, "__version__", "unknown"),
        sources=sources,
    )

    for source in sources:
        source_path = snapshot_path / source
        source_path.mkdir(exist_ok=True)

        try:
            if source == "cepea":
                await _snapshot_cepea(source_path, manifest)
            elif source == "conab":
                await _snapshot_conab(source_path, manifest)
            elif source == "ibge":
                await _snapshot_ibge(source_path, manifest)
        except Exception as e:
            logger.error("snapshot_source_error", source=source, error=str(e))

    with open(snapshot_path / "manifest.json", "w") as f:
        json.dump(manifest.to_dict(), f, indent=2)

    logger.info("snapshot_created", name=name, path=str(snapshot_path))

    return get_snapshot(name)  # type: ignore


async def _snapshot_cepea(path: Path, manifest: SnapshotManifest) -> None:
    from agrobr import cepea

    produtos = await cepea.produtos()

    for produto in produtos:
        try:
            df = await cepea.indicador(produto, offline=True)
            if df is not None and not df.empty:
                file_path = path / f"{produto}.parquet"
                df.to_parquet(file_path, index=False)
                manifest.files[f"cepea/{produto}.parquet"] = {
                    "rows": len(df),
                    "columns": df.columns.tolist(),
                }
        except Exception as e:
            logger.warning("snapshot_produto_error", produto=produto, error=str(e))


async def _snapshot_conab(path: Path, manifest: SnapshotManifest) -> None:
    from agrobr import conab

    try:
        df = await conab.safras(produto="soja")
        if df is not None and not df.empty:
            file_path = path / "safras.parquet"
            df.to_parquet(file_path, index=False)
            manifest.files["conab/safras.parquet"] = {
                "rows": len(df),
                "columns": df.columns.tolist(),
            }
    except Exception as e:
        logger.warning("snapshot_conab_safras_error", error=str(e))

    try:
        df = await conab.balanco()
        if df is not None and not df.empty:
            file_path = path / "balanco.parquet"
            df.to_parquet(file_path, index=False)
            manifest.files["conab/balanco.parquet"] = {
                "rows": len(df),
                "columns": df.columns.tolist(),
            }
    except Exception as e:
        logger.warning("snapshot_conab_balanco_error", error=str(e))


async def _snapshot_ibge(path: Path, manifest: SnapshotManifest) -> None:
    from agrobr import ibge

    try:
        df = await ibge.pam(produto="soja")
        if df is not None and not df.empty:
            file_path = path / "pam.parquet"
            df.to_parquet(file_path, index=False)
            manifest.files["ibge/pam.parquet"] = {
                "rows": len(df),
                "columns": df.columns.tolist(),
            }
    except Exception as e:
        logger.warning("snapshot_ibge_pam_error", error=str(e))

    try:
        df = await ibge.lspa(produto="soja")
        if df is not None and not df.empty:
            file_path = path / "lspa.parquet"
            df.to_parquet(file_path, index=False)
            manifest.files["ibge/lspa.parquet"] = {
                "rows": len(df),
                "columns": df.columns.tolist(),
            }
    except Exception as e:
        logger.warning("snapshot_ibge_lspa_error", error=str(e))


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

    snapshot_path = get_snapshots_dir() / snapshot_name / source / f"{dataset}.parquet"

    if not snapshot_path.exists():
        logger.warning(
            "snapshot_file_not_found",
            source=source,
            dataset=dataset,
            path=str(snapshot_path),
        )
        return None

    return pd.read_parquet(snapshot_path)


def delete_snapshot(name: str) -> bool:
    snapshot_path = get_snapshots_dir() / name

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
