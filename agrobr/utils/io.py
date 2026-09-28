from __future__ import annotations

import io
import zipfile
import zlib
from typing import IO, Any, Literal

import pandas as pd
import structlog

from agrobr import constants
from agrobr.exceptions import ParseError, ResourceLimitError, SourceUnavailableError
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


def _expansion_limit(source: str) -> int:
    return constants.MAX_EXPANDED_BYTES.get(source, constants.MAX_EXPANDED_BYTES_DEFAULT)


def open_zip_member(
    archive: zipfile.ZipFile, member: str | zipfile.ZipInfo, *, source: str, url: str = ""
) -> IO[bytes]:
    """Abre 1 membro do ZIP se o tamanho expandido declarado cabe no teto da fonte.

    O ``zipfile`` não entrega mais do que o tamanho declarado: o que o membro expandir além dele falha no CRC
    (``zipfile.BadZipFile``). Por isso conferir o declarado basta para o ``zipfile``.
    """
    info = member if isinstance(member, zipfile.ZipInfo) else archive.getinfo(member)
    limit = _expansion_limit(source)
    if info.file_size > limit:
        raise ResourceLimitError(
            source,
            f"o membro {info.filename} do ZIP expande para {info.file_size} bytes, "
            f"acima do teto de {limit}",
            url=url,
        )
    return archive.open(info)


def read_zip_member(
    archive: zipfile.ZipFile, member: str | zipfile.ZipInfo, *, source: str, url: str = ""
) -> bytes:
    with open_zip_member(archive, member, source=source, url=url) as stream:
        return stream.read()


def check_xlsx_expansion(raw: bytes, *, source: str, url: str = "") -> None:
    """Recusa, antes do leitor de planilha, o XLSX que expande além do teto da fonte.

    Soma o tamanho declarado dos membros e confere o CRC de cada um em stream. O calamine não respeita o tamanho
    declarado: um membro que expande além dele só aparece no CRC. Arquivo que não abre como ZIP segue para o leitor,
    que dá o erro dele.
    """
    if not raw.startswith(b"PK"):
        return
    limit = _expansion_limit(source)
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            total = sum(info.file_size for info in archive.infolist())
            if total > limit:
                raise ResourceLimitError(
                    source,
                    f"o XLSX expande para {total} bytes, acima do teto de {limit}",
                    url=url,
                )
            wrong = archive.testzip()
    except (zipfile.BadZipFile, zlib.error, EOFError):
        return
    if wrong is not None:
        raise ResourceLimitError(
            source,
            f"o membro {wrong} do XLSX não confere com o tamanho e o CRC declarados; "
            "a expansão real não é conhecida",
            url=url,
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
    check_xlsx_expansion(raw, source=source)
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
    check_xlsx_expansion(raw, source=source)
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
            return read_zip_member(zf, csv_names[0], source=source, url=url)
    except zipfile.BadZipFile as e:
        raise SourceUnavailableError(
            source=source,
            url=url,
            last_error=f"Resposta não é um ZIP válido: {e}",
        ) from e
