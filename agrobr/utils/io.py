from __future__ import annotations

import inspect
import io
import struct
import zipfile
import zlib
from collections.abc import Callable
from typing import IO, Any, Literal

import pandas as pd

from agrobr import _log, constants
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.normalize.encoding import detect_encoding_chain

_ExcelEngine = Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"]

logger = _log.get_logger(__name__)

_DOWNLOAD_SIGNATURES = {
    "zip": b"PK\x03\x04",
    "xlsx": b"PK\x03\x04",
    "xls": b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",
    "pdf": b"%PDF",
}
_INFLATE_CHUNK = 16 * 1024


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


def _real_expansion(raw: bytes, info: zipfile.ZipInfo, budget: int) -> int:
    """Bytes que um leitor tira do membro; no deflate, a conta para em ``budget + 1``.

    O deflate vai do cabeçalho local até o fim do stream, sem parar no tamanho comprimido nem no expandido declarados.
    """
    signature, name_size, extra_size = struct.unpack_from("<4s22xHH", raw, info.header_offset)
    if signature != _DOWNLOAD_SIGNATURES["zip"]:
        raise zipfile.BadZipFile(f"o membro {info.filename} não tem cabeçalho local")
    start: int = info.header_offset + 30 + name_size + extra_size
    if info.compress_type == zipfile.ZIP_STORED:
        return max(0, min(info.compress_size, len(raw) - start))
    inflater = zlib.decompressobj(-15)
    total = 0
    for offset in range(start, len(raw), _INFLATE_CHUNK):
        total += len(inflater.decompress(raw[offset : offset + _INFLATE_CHUNK], budget - total + 1))
        if inflater.eof or total > budget:
            break
    return total


def check_xlsx_expansion(raw: bytes, *, source: str, url: str = "") -> None:
    """Recusa, antes do leitor de planilha, o XLSX que expande além do teto da fonte.

    Soma primeiro o tamanho declarado dos membros e depois mede a expansão real de cada um, com orçamento cumulativo:
    descomprime o deflate a partir do cabeçalho local até o fim do stream e para assim que o total passa do teto.
    O calamine não respeita o tamanho declarado e o CRC é escolhido por quem gera o arquivo; por isso nenhum dos dois
    serve de prova. Membro guardado conta o que cabe no arquivo; outro método de compressão é recusado. Decide pelo
    diretório central, não pelo início: o ``zipfile`` e o calamine abrem ZIP com dado antes do ``PK``. Arquivo sem
    diretório central segue para o leitor, que dá o erro dele; ZIP ou deflate ilegível é recusado, porque a expansão
    real não é conhecida.
    """
    if not zipfile.is_zipfile(io.BytesIO(raw)):
        return
    limit = _expansion_limit(source)
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            members = archive.infolist()
        declared = sum(info.file_size for info in members)
        if declared > limit:
            raise ResourceLimitError(
                source,
                f"o XLSX expande para {declared} bytes, acima do teto de {limit}",
                url=url,
            )
        total = 0
        for info in members:
            if info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                raise ResourceLimitError(
                    source,
                    f"o membro {info.filename} do XLSX usa a compressão {info.compress_type}; "
                    "a expansão real não é conhecida",
                    url=url,
                )
            total += _real_expansion(raw, info, limit - total)
            if total > limit:
                raise ResourceLimitError(
                    source,
                    f"o XLSX expande de fato para mais de {limit} bytes, acima do teto de {limit}",
                    url=url,
                )
    except (zipfile.BadZipFile, zlib.error, struct.error, NotImplementedError) as exc:
        raise ResourceLimitError(
            source,
            f"o ZIP do XLSX está ilegível ({type(exc).__name__}: {exc}); "
            "a expansão real não é conhecida",
            url=url,
        ) from exc


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


def _conferir_argumentos(leitor: Callable[..., Any], **kwargs: Any) -> None:
    """Levanta o ``TypeError`` da chamada (argumento que o pandas não aceita) antes de ler, para não
    virar ``ParseError`` nem abrir o motor alternativo."""
    inspect.signature(leitor).bind(None, **kwargs)


def read_excel_safe(
    data: bytes | io.BytesIO,
    *,
    source: str,
    parser_version: int = 1,
    label: str = "Excel",
    **kwargs: Any,
) -> pd.DataFrame:
    sheet_name = kwargs.get("sheet_name", 0)
    if sheet_name is None or isinstance(sheet_name, list | tuple):
        raise InvalidParameterError(
            f"read_excel_safe lê uma aba por vez; sheet_name deve ser nome ou índice: {sheet_name!r}"
        )
    _conferir_argumentos(pd.read_excel, **kwargs)
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
    if kwargs.get("chunksize") is not None or kwargs.get("iterator"):
        raise InvalidParameterError(
            "read_csv_safe lê o arquivo inteiro; chunksize e iterator não são aceitos"
        )
    encoding = detect_encoding_chain(data)
    _conferir_argumentos(pd.read_csv, encoding=encoding, **kwargs)
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
