from __future__ import annotations

import io
import zipfile
from typing import Any, Literal

import pandas as pd
import structlog

from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.normalize.encoding import detect_encoding_chain

_ExcelEngine = Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"]

logger = structlog.get_logger()

_DOWNLOAD_SIGNATURES = {
    "zip": b"PK\x03\x04",
    "xlsx": b"PK\x03\x04",
    "xls": b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",
    "pdf": b"%PDF",
}


def validate_download(
    content: bytes,
    *,
    kinds: tuple[str, ...],
    source: str,
    url: str,
    min_size: int,
) -> None:
    preview = repr(content[:60])
    if len(content) < min_size:
        raise SourceUnavailableError(
            source=source,
            url=url,
            last_error=(
                f"Download muito pequeno: {len(content)} bytes (mínimo {min_size}); "
                f"início={preview}"
            ),
        )

    valid = False
    for kind in kinds:
        if kind == "csv":
            prefix = content[:512].lstrip().lower()
            valid = not (prefix.startswith(b"<") or b"<html" in prefix or b"<!doctype" in prefix)
        elif kind in _DOWNLOAD_SIGNATURES:
            valid = content.startswith(_DOWNLOAD_SIGNATURES[kind])
        else:
            raise ValueError(f"Tipo de download desconhecido: {kind!r}")
        if valid:
            return

    expected = ", ".join(kinds)
    raise SourceUnavailableError(
        source=source,
        url=url,
        last_error=f"Assinatura inválida para {expected}; início={preview}",
    )


def _extract_bytes(data: bytes | io.BytesIO) -> bytes:
    if isinstance(data, io.BytesIO):
        return data.getvalue()
    return data


def open_excel_safe(
    data: bytes | io.BytesIO,
    *,
    source: str,
    parser_version: int = 1,
    engine: _ExcelEngine | None = None,
) -> pd.ExcelFile:
    raw = _extract_bytes(data)
    try:
        return pd.ExcelFile(io.BytesIO(raw), engine=engine)
    except Exception as primary_err:
        fallback_engine: _ExcelEngine = "openpyxl" if engine == "calamine" else "calamine"
        logger.warning(
            "excel_engine_fallback",
            source=source,
            primary_engine=engine or "auto",
            fallback_engine=fallback_engine,
            primary_error=str(primary_err),
        )
        try:
            return pd.ExcelFile(io.BytesIO(raw), engine=fallback_engine)
        except Exception as fallback_err:
            raise ParseError(
                source=source,
                parser_version=parser_version,
                reason=(
                    f"Erro ao abrir Excel (primary: {primary_err}, "
                    f"{fallback_engine}: {fallback_err})"
                ),
            ) from fallback_err


def read_excel_safe(
    data: bytes | io.BytesIO,
    *,
    source: str,
    parser_version: int = 1,
    label: str = "Excel",
    **kwargs: Any,
) -> pd.DataFrame:
    raw = _extract_bytes(data)
    try:
        df: pd.DataFrame = pd.read_excel(io.BytesIO(raw), **kwargs)
        return df
    except Exception as primary_err:
        primary_engine = kwargs.get("engine")
        fallback_engine: _ExcelEngine = "openpyxl" if primary_engine == "calamine" else "calamine"
        logger.warning(
            "excel_engine_fallback",
            source=source,
            label=label,
            primary_engine=primary_engine or "auto",
            fallback_engine=fallback_engine,
            primary_error=str(primary_err),
        )
        fallback_kwargs = {**kwargs, "engine": fallback_engine}
        try:
            df = pd.read_excel(io.BytesIO(raw), **fallback_kwargs)
            return df
        except Exception as fallback_err:
            raise ParseError(
                source=source,
                parser_version=parser_version,
                reason=(
                    f"Erro ao ler {label} (primary: {primary_err}, "
                    f"{fallback_engine}: {fallback_err})"
                ),
            ) from fallback_err


def read_csv_safe(
    data: bytes,
    *,
    source: str,
    parser_version: int = 1,
    label: str = "CSV",
    **kwargs: Any,
) -> pd.DataFrame:
    encoding = detect_encoding_chain(data)
    try:
        df: pd.DataFrame = pd.read_csv(io.BytesIO(data), encoding=encoding, **kwargs)
        return df
    except Exception as e:
        raise ParseError(
            source=source,
            parser_version=parser_version,
            reason=f"Erro ao ler {label} ({encoding}): {e}",
        ) from e


def concat_csv_pages(
    pages: list[bytes],
    *,
    source: str,
    parser_version: int,
    empty_columns: list[str],
) -> pd.DataFrame:
    if not pages:
        return pd.DataFrame(columns=empty_columns)

    dfs: list[pd.DataFrame] = []
    for i, data in enumerate(pages):
        df = read_csv_safe(
            data, source=source, parser_version=parser_version, label=f"CSV pagina {i}"
        )
        if not df.empty:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame(columns=empty_columns)

    return pd.concat(dfs, ignore_index=True)


def extract_csv_from_zip(data: bytes, *, source: str, url: str) -> bytes:
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise SourceUnavailableError(
                    source=source,
                    url=url,
                    last_error="ZIP não contém arquivo CSV",
                )
            return zf.read(csv_names[0])
    except zipfile.BadZipFile as e:
        raise SourceUnavailableError(
            source=source,
            url=url,
            last_error=f"Resposta não é um ZIP válido: {e}",
        ) from e
