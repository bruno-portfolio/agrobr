"""Export para formatos auditaveis."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    import pandas as pd

    from agrobr.models import MetaInfo

logger = structlog.get_logger()


def export_parquet(
    df: pd.DataFrame,
    path: str | Path,
    meta: MetaInfo | None = None,
    compression: str = "snappy",
) -> Path:
    """
    Exporta DataFrame para Parquet com metadados.

    Args:
        df: DataFrame a exportar
        path: Caminho do arquivo
        meta: Metadados opcionais
        compression: Compressao (snappy, gzip, zstd)

    Returns:
        Path do arquivo criado
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df)

    metadata = {
        b"agrobr_version": _get_version().encode(),
        b"export_timestamp": datetime.now().isoformat().encode(),
        b"row_count": str(len(df)).encode(),
    }

    if meta:
        metadata[b"source"] = meta.source.encode()
        metadata[b"source_url"] = meta.source_url.encode()
        metadata[b"fetched_at"] = meta.fetched_at.isoformat().encode()
        if meta.raw_content_hash:
            metadata[b"content_hash"] = meta.raw_content_hash.encode()

    existing_meta = table.schema.metadata or {}
    table = table.replace_schema_metadata({**existing_meta, **metadata})

    pq.write_table(table, path, compression=compression)

    logger.info("export_parquet", path=str(path), rows=len(df))
    return path


def export_csv(
    df: pd.DataFrame,
    path: str | Path,
    meta: MetaInfo | None = None,
    include_header: bool = True,
    include_sidecar: bool = True,
) -> tuple[Path, Path | None]:
    """
    Exporta DataFrame para CSV com arquivo sidecar de metadados.

    Args:
        df: DataFrame a exportar
        path: Caminho do arquivo
        meta: Metadados opcionais
        include_header: Incluir linha de cabecalho
        include_sidecar: Criar arquivo .meta.json

    Returns:
        Tupla (path_csv, path_sidecar ou None)
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(path, index=False, header=include_header, quoting=csv.QUOTE_NONNUMERIC)

    sidecar_path = None
    if include_sidecar:
        sidecar_path = path.with_suffix(".meta.json")
        sidecar_data = _create_sidecar(df, meta)
        with open(sidecar_path, "w") as f:
            json.dump(sidecar_data, f, indent=2, ensure_ascii=False)

    logger.info("export_csv", path=str(path), rows=len(df))
    return path, sidecar_path


def export_json(
    df: pd.DataFrame,
    path: str | Path,
    meta: MetaInfo | None = None,
    orient: str = "records",
    include_metadata: bool = True,
) -> Path:
    """
    Exporta DataFrame para JSON com metadados embutidos.

    Args:
        df: DataFrame a exportar
        path: Caminho do arquivo
        meta: Metadados opcionais
        orient: Orientacao do JSON (records, split, index, etc)
        include_metadata: Incluir metadados no JSON

    Returns:
        Path do arquivo criado
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if include_metadata:
        output = {
            "metadata": _create_sidecar(df, meta),
            "data": json.loads(df.to_json(orient=orient, date_format="iso")),
        }
        with open(path, "w") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
    else:
        df.to_json(path, orient=orient, date_format="iso", indent=2)

    logger.info("export_json", path=str(path), rows=len(df))
    return path


def _create_sidecar(df: pd.DataFrame, meta: MetaInfo | None = None) -> dict[str, Any]:
    """Cria metadados para arquivo sidecar."""
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    content_hash = hashlib.sha256(csv_bytes).hexdigest()

    sidecar: dict[str, Any] = {
        "agrobr_version": _get_version(),
        "export_timestamp": datetime.now().isoformat(),
        "file_info": {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": df.columns.tolist(),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "content_hash": f"sha256:{content_hash}",
        },
    }

    if meta:
        sidecar["provenance"] = {
            "source": meta.source,
            "source_url": meta.source_url,
            "source_method": meta.source_method,
            "fetched_at": meta.fetched_at.isoformat(),
            "from_cache": meta.from_cache,
            "original_hash": meta.raw_content_hash,
        }

    return sidecar


def verify_export(path: str | Path, expected_hash: str | None = None) -> dict[str, Any]:
    """
    Verifica integridade de um arquivo exportado.

    Args:
        path: Caminho do arquivo
        expected_hash: Hash esperado (opcional)

    Returns:
        Dict com status da verificacao
    """
    path = Path(path)

    if not path.exists():
        return {"valid": False, "error": "File not found"}

    result: dict[str, Any] = {
        "valid": True,
        "path": str(path),
        "size_bytes": path.stat().st_size,
    }

    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        try:
            table = pq.read_table(path)
            result["row_count"] = table.num_rows
            result["columns"] = table.schema.names

            metadata = table.schema.metadata or {}
            if b"content_hash" in metadata:
                result["stored_hash"] = metadata[b"content_hash"].decode()
        except Exception as e:
            result["valid"] = False
            result["error"] = str(e)

    elif path.suffix == ".csv":
        import pandas as pd

        try:
            df = pd.read_csv(path)
            result["row_count"] = len(df)
            result["columns"] = df.columns.tolist()

            csv_bytes = df.to_csv(index=False).encode("utf-8")
            result["computed_hash"] = f"sha256:{hashlib.sha256(csv_bytes).hexdigest()}"

            sidecar_path = path.with_suffix(".meta.json")
            if sidecar_path.exists():
                with open(sidecar_path) as f:
                    sidecar = json.load(f)
                    result["stored_hash"] = sidecar.get("file_info", {}).get("content_hash")
        except Exception as e:
            result["valid"] = False
            result["error"] = str(e)

    if expected_hash and result.get("computed_hash"):
        result["hash_match"] = result["computed_hash"] == expected_hash

    return result


def _get_version() -> str:
    """Retorna versao do agrobr."""
    try:
        import agrobr

        return getattr(agrobr, "__version__", "unknown")
    except ImportError:
        return "unknown"


__all__ = [
    "export_parquet",
    "export_csv",
    "export_json",
    "verify_export",
]
