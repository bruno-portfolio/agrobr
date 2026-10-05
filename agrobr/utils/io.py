from __future__ import annotations

import inspect
import io
import itertools
import re
import struct
import zipfile
import zlib
from collections.abc import Callable, Coroutine, Iterator
from typing import IO, Any, Literal
from urllib.parse import urlsplit
from xml.parsers import expat

import httpx
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
_XLSX_CELL_REF = re.compile(r"\s*\$?([A-Za-z]{1,7})\$?([0-9]{1,10})\s*")
_XLSX_SHEET_MARKS = tuple(
    "sheetData".encode(codec) for codec in ("utf-8", "utf-16-le", "utf-16-be")
)
_XLSX_ODD = re.compile(
    rb'<(?:\?xml(?! version="1\.0"(?: encoding="[Uu][Tt][Ff]-8")?(?: standalone="yes")?\?>)|!DOCTYPE|!ENTITY'
    rb'|c(?:[\t\n\r/>]| (?!r="[A-Z]{1,3}[0-9]{1,7}"))'
    rb'|row(?:[\t\n\r/>]| (?!r="[0-9]{1,7}"))'
    rb'|dimension(?:[\t\n\r/>]| (?!ref="[A-Z]{1,3}[0-9]{1,7}(?::[A-Z]{1,3}[0-9]{1,7})?"))'
    rb"|[^\s<>/!?:]+:(?:c|row|dimension)[\s/>])"
)
_XLSX_COLUMNS = re.compile(rb'<c r="([A-Z]{1,3})[0-9]')
_XLSX_ROWS = re.compile(rb'<(?:c r="[A-Z]{1,3}|row r=")([0-9]{1,7})"')
_XLSX_DIMENSION = re.compile(rb'<dimension ref="([A-Z0-9:]+)"')


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


def validate_download_url(url: str, *, base_url: str, source: str) -> None:
    """Recusa a URL lida da resposta que não seja ``https`` no host de ``base_url``, na porta padrão."""
    parts = urlsplit(url)
    try:
        port = parts.port
    except ValueError:
        port = -1
    if (
        parts.scheme != "https"
        or parts.hostname != urlsplit(base_url).hostname
        or port not in (None, 443)
        or parts.username is not None
        or parts.password is not None
    ):
        raise SourceUnavailableError(
            source=source,
            url=url,
            last_error="URL lida da resposta fora da origem HTTPS oficial da fonte",
        )


def download_url_hook(
    *, base_url: str, source: str
) -> Callable[[httpx.Request], Coroutine[Any, Any, None]]:
    """Gancho ``request`` do httpx que aplica ``validate_download_url`` a todo pedido, redirecionamento incluído,
    antes do envio."""

    async def conferir(request: httpx.Request) -> None:
        validate_download_url(str(request.url), base_url=base_url, source=source)

    return conferir


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


def read_zip_members(
    archive: zipfile.ZipFile, members: list[zipfile.ZipInfo], *, source: str, url: str = ""
) -> list[tuple[str, bytes]]:
    """Lê os membros se a soma dos tamanhos declarados cabe no teto da fonte, além do teto por membro."""
    limit = _expansion_limit(source)
    total = sum(info.file_size for info in members)
    if total > limit:
        raise ResourceLimitError(
            source,
            f"os membros do ZIP expandem para {total} bytes, acima do teto de {limit}",
            url=url,
        )
    return [
        (info.filename, read_zip_member(archive, info, source=source, url=url)) for info in members
    ]


def _member_start(stream: IO[bytes], info: zipfile.ZipInfo) -> int:
    stream.seek(max(info.header_offset, 0))
    signature, name_size, extra_size = struct.unpack("<4s22xHH", stream.read(30))
    if info.header_offset < 0 or signature != _DOWNLOAD_SIGNATURES["zip"]:
        raise zipfile.BadZipFile(f"o membro {info.filename} não tem cabeçalho local")
    start: int = info.header_offset + 30 + name_size + extra_size
    return start


def _member_chunks(stream: IO[bytes], info: zipfile.ZipInfo) -> Iterator[bytes]:
    """O conteúdo do membro em pedaços, como ``_real_expansion`` o mede."""
    start = _member_start(stream, info)
    stream.seek(start)
    if info.compress_type == zipfile.ZIP_STORED:
        remaining = info.compress_size
        while remaining > 0 and (chunk := stream.read(min(_INFLATE_CHUNK, remaining))):
            remaining -= len(chunk)
            yield chunk
        return
    inflater = zlib.decompressobj(-15)
    while not inflater.eof and (chunk := stream.read(_INFLATE_CHUNK)):
        while not inflater.eof:
            piece = inflater.decompress(chunk, _INFLATE_CHUNK)
            yield piece
            chunk = inflater.unconsumed_tail
            if not chunk and len(piece) < _INFLATE_CHUNK:
                break


def _real_expansion(stream: IO[bytes], info: zipfile.ZipInfo, budget: int) -> int:
    """Bytes que um leitor tira do membro; no deflate, a conta para em ``budget + 1``.

    O deflate vai do cabeçalho local até o fim do stream, sem parar no tamanho comprimido nem no expandido declarados.
    """
    start = _member_start(stream, info)
    if info.compress_type == zipfile.ZIP_STORED:
        return max(0, min(info.compress_size, stream.seek(0, io.SEEK_END) - start))
    stream.seek(start)
    inflater = zlib.decompressobj(-15)
    total = 0
    while chunk := stream.read(_INFLATE_CHUNK):
        total += len(inflater.decompress(chunk, budget - total + 1))
        if inflater.eof or total > budget:
            break
    return total


def check_zip_expansion(
    archive: IO[bytes], *, source: str, limit: int, label: str, url: str = ""
) -> None:
    """Recusa, antes do leitor, o ZIP que expande além de ``limit``.

    Soma primeiro o tamanho declarado dos membros e depois mede a expansão real de cada um, com orçamento cumulativo:
    descomprime o deflate a partir do cabeçalho local até o fim do stream e para assim que o total passa do teto.
    O calamine e o GDAL não respeitam o tamanho declarado e o CRC é escolhido por quem gera o arquivo; por isso nenhum
    dos dois serve de prova. Membro guardado conta o que cabe no arquivo; outro método de compressão é recusado. ZIP
    ou deflate ilegível é recusado, porque a expansão real não é conhecida.
    """
    try:
        with zipfile.ZipFile(archive) as zip_file:
            members = zip_file.infolist()
        declared = sum(info.file_size for info in members)
        if declared > limit:
            raise ResourceLimitError(
                source,
                f"o {label} expande para {declared} bytes, acima do teto de {limit}",
                url=url,
            )
        total = 0
        for info in members:
            if info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                raise ResourceLimitError(
                    source,
                    f"o membro {info.filename} do {label} usa a compressão {info.compress_type}; "
                    "a expansão real não é conhecida",
                    url=url,
                )
            total += _real_expansion(archive, info, limit - total)
            if total > limit:
                raise ResourceLimitError(
                    source,
                    f"o {label} expande de fato para mais de {limit} bytes, acima do teto de {limit}",
                    url=url,
                )
    except (
        zipfile.BadZipFile,
        zlib.error,
        struct.error,
        NotImplementedError,
        UnicodeDecodeError,
    ) as exc:
        raise ResourceLimitError(
            source,
            f"o ZIP do {label} está ilegível ({type(exc).__name__}: {exc}); "
            "a expansão real não é conhecida",
            url=url,
        ) from exc


def _column_number(letters: str) -> int:
    column = 0
    for letter in letters.upper():
        column = column * 26 + ord(letter) - 64
    return column


def _xlsx_cell(ref: str) -> tuple[int, int]:
    match = _XLSX_CELL_REF.fullmatch(ref)
    if match is None:
        raise ValueError(f"referência de célula ilegível: {ref[:40]!r}")
    letters, row = match.groups()
    return int(row), _column_number(letters)


def _xlsx_row(ref: str) -> int:
    """Número da linha como o openpyxl o lê, que aceita ``r`` em ponto flutuante inteiro."""
    try:
        return int(ref)
    except ValueError:
        value = float(ref)
    if not value.is_integer():
        raise ValueError(f"linha ilegível: {ref[:40]!r}")
    return int(value)


def _xlsx_declared(ref: str) -> int:
    corners = [_xlsx_cell(corner) for corner in ref.split(":")]
    return max(row for row, _ in corners) * max(column for _, column in corners)


class _SheetExtent:
    """Retângulo que o leitor monta a partir de A1: maior linha × maior coluna das células e das linhas, ou o
    ``<dimension>`` declarado, se for maior. ``row`` e ``c`` sem ``r`` seguem o anterior, como no openpyxl e no
    calamine."""

    def __init__(self) -> None:
        self.row = self.column = self.rows = self.columns = self.declared = 0

    @property
    def cells(self) -> int:
        return max(self.rows * max(self.columns, 1), self.declared)

    def start(self, name: str, attrs: dict[str, str]) -> None:
        tag = name.rpartition(":")[2]
        if tag == "c":
            ref = attrs.get("r")
            if ref is None:
                row, self.column = self.row, self.column + 1
            else:
                row, self.column = _xlsx_cell(ref)
            self.rows = max(self.rows, row, 1)
            self.columns = max(self.columns, self.column)
        elif tag == "row":
            ref = attrs.get("r")
            self.row = self.row + 1 if ref is None else _xlsx_row(ref)
            self.column = 0
            self.rows = max(self.rows, self.row)
        elif tag == "dimension":
            self.declared = max(self.declared, _xlsx_declared(attrs.get("ref", "")))

    @staticmethod
    def doctype(*_args: object) -> None:
        raise ValueError("DOCTYPE em membro do XLSX")


def _fast_cells(stream: IO[bytes], info: zipfile.ZipInfo, limit: int) -> int | None:
    """Células do membro em que toda ``c``, ``row`` e ``dimension`` está na forma que o Excel grava, com ``r``/``ref``
    primeiro e por extenso, em UTF-8 sem DTD; ``None`` quando aparece outra forma. Para assim que passa de ``limit``."""
    rows = columns = declared = 0
    carry = b""
    for piece in itertools.chain(_member_chunks(stream, info), [None]):
        window = carry + (piece or b"")
        cut = -1 if piece is None else window.rfind(b"<", max(0, len(window) - 64))
        if cut < 0:
            cut = len(window)
        window, carry = window[:cut], window[cut:]
        if b"\x00" in window or _XLSX_ODD.search(window):
            return None
        letters = set(_XLSX_COLUMNS.findall(window))
        columns = max(columns, *(_column_number(item.decode()) for item in letters), 0)
        rows = max(rows, *map(int, set(_XLSX_ROWS.findall(window))), 1 if columns else 0)
        for ref in _XLSX_DIMENSION.findall(window):
            declared = max(declared, _xlsx_declared(ref.decode()))
        cells = max(rows * max(columns, 1), declared)
        if cells > limit:
            return cells
    return max(rows * max(columns, 1), declared)


def _exact_cells(stream: IO[bytes], info: zipfile.ZipInfo, limit: int) -> int:
    """Células do membro pelo expat (``_SheetExtent``). Membro que não é XML bem formado conta 0, a não ser que traga
    ``sheetData``, por onde o calamine lê as células: aí é ilegível. O openpyxl recusa XML mal formado por conta
    própria."""
    extent = _SheetExtent()
    parser = expat.ParserCreate()
    parser.StartElementHandler = extent.start
    parser.StartDoctypeDeclHandler = _SheetExtent.doctype
    malformed: expat.ExpatError | None = None
    sheet = False
    tail = b""
    for piece, final in itertools.chain(
        ((piece, False) for piece in _member_chunks(stream, info)), [(b"", True)]
    ):
        if not sheet:
            window = tail + piece
            sheet = any(mark in window for mark in _XLSX_SHEET_MARKS)
            tail = window[-32:]
        if malformed is None:
            try:
                parser.Parse(piece, final)
            except expat.ExpatError as exc:
                malformed = exc
        if extent.cells > limit:
            return extent.cells
    if malformed is not None and sheet:
        raise ValueError(f"não é XML bem formado: {malformed}")
    return extent.cells if malformed is None else 0


def _check_xlsx_cells(raw: bytes, *, source: str, url: str) -> None:
    """Recusa o XLSX em que algum membro monta mais de ``MAX_XLSX_CELLS`` células.

    O calamine e o openpyxl montam a planilha densa, de A1 até a maior linha e a maior coluna: poucas células
    espalhadas esgotam a memória sem exceção capturável. Cada membro é lido como o calamine o lê (ver
    ``_member_chunks``), sem abrir o leitor: primeiro por ``_fast_cells``; se aparecer forma fora do padrão do Excel,
    de novo por ``_exact_cells``. O ZIP que não é XLSX (ODS, XLSB, sem ``xl/workbook.xml``) é recusado antes: o calamine
    o abriria como outra planilha, que a contagem não lê. Os nomes são comparados como o pandas os compara. O XLS da
    CONAB (assinatura CFB) traz um pacote de tema ou desenho no fim, que o ``zipfile`` acha: sem ``xl/workbook.xml``, é
    lido como XLS.
    """
    limit = constants.MAX_XLSX_CELLS
    stream = io.BytesIO(raw)
    with zipfile.ZipFile(stream) as zip_file:
        members = zip_file.infolist()
    nomes = {info.filename.replace("\\", "/").lower() for info in members}
    outros = sorted(nomes & {"xl/workbook.bin", "content.xml"})
    xls = raw.startswith(_DOWNLOAD_SIGNATURES["xls"])
    if outros or ("xl/workbook.xml" not in nomes and not xls):
        motivo = f"com {', '.join(outros)}" if outros else "sem xl/workbook.xml"
        raise ResourceLimitError(
            source,
            f"o arquivo não é XLSX ({motivo}); o tamanho da planilha não é conhecido",
            url=url,
        )
    for info in members:
        try:
            cells = _fast_cells(stream, info, limit)
            if cells is None:
                cells = _exact_cells(stream, info, limit)
        except ValueError as exc:
            raise ResourceLimitError(
                source,
                f"a planilha {info.filename} do XLSX está ilegível ({exc}); o tamanho não é conhecido",
                url=url,
            ) from exc
        if cells > limit:
            raise ResourceLimitError(
                source,
                f"a planilha {info.filename} do XLSX monta {cells} células, acima do teto de {limit}",
                url=url,
            )


def check_xlsx_expansion(raw: bytes, *, source: str, url: str = "") -> None:
    """Recusa, antes do leitor de planilha, o XLSX que expande além do teto da fonte (ver ``check_zip_expansion``)
    ou cuja planilha passa do teto de células (ver ``_check_xlsx_cells``).

    Decide pelo diretório central, não pelo início: o ``zipfile`` e o calamine abrem ZIP com dado antes do ``PK``.
    Arquivo sem diretório central segue para o leitor, que dá o erro dele.
    """
    if not zipfile.is_zipfile(io.BytesIO(raw)):
        return
    check_zip_expansion(
        io.BytesIO(raw), source=source, limit=_expansion_limit(source), label="XLSX", url=url
    )
    _check_xlsx_cells(raw, source=source, url=url)


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
