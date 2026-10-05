from __future__ import annotations

import codecs
import csv
import io
import json
import re
from collections.abc import Generator, Iterator
from contextlib import closing
from typing import IO, Any
from urllib.parse import urlsplit

import pandas as pd
import pydantic

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize import regions
from agrobr.normalize.municipalities import MunicipioInfo
from agrobr.normalize.numeric import parse_numeric_br
from agrobr.normalize.regions import remover_acentos

from . import models

logger = _log.get_logger(__name__)

PARSER_VERSION = 4


_CSV_DO_CATALOGO = re.compile(r"dados_abertos_psr_(\d{4})(?:a(\d{4}))?csv\.csv", re.I)


def parse_catalogo(corpo: bytes) -> dict[str, str]:
    """Os CSV do pacote do PSR no CKAN do MAPA, por período (``"2025"`` ou ``"2016-2024"``).

    Só entra recurso servido pelo próprio portal (https em ``dados.agricultura.gov.br``, sem porta nem credencial).
    """
    try:
        pacote: Any = json.loads(corpo)
        recursos = pacote["result"]["resources"] if pacote["success"] is True else None
    except (ValueError, KeyError, TypeError):
        recursos = None
    if not isinstance(recursos, list):
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason="Catálogo do PSR sem a lista de recursos do pacote",
        )
    catalogo: dict[str, str] = {}
    for recurso in recursos:
        url = recurso.get("url") if isinstance(recurso, dict) else None
        if not isinstance(url, str):
            continue
        partes = urlsplit(url)
        achado = _CSV_DO_CATALOGO.fullmatch(partes.path.rsplit("/", 1)[-1])
        if achado and partes.scheme == "https" and partes.netloc == "dados.agricultura.gov.br":
            catalogo[achado[1] if achado[2] is None else f"{achado[1]}-{achado[2]}"] = url
    return catalogo


def _detect_separator(text: str) -> str:
    first_line = text.split("\n", 1)[0]
    if first_line.count(";") > first_line.count(","):
        return ";"
    return ","


def _normalize_column_name(col: str) -> str:
    return remover_acentos(col.strip())


def _detect_file_encoding(stream: IO[bytes]) -> str:
    stream.seek(0)
    bom = stream.read(3) == codecs.BOM_UTF8
    for encoding in ("utf-8-sig",) if bom else ("utf-8", "windows-1252", "iso-8859-1"):
        stream.seek(0)
        decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
        try:
            while block := stream.read(65536):
                decoder.decode(block)
            decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            continue
        stream.seek(0)
        return encoding
    raise UnicodeError("Nenhum encoding válido para o CSV")


def _iter_apolices_csv(stream: IO[bytes], chunk_size: int) -> Generator[pd.DataFrame, None, None]:
    try:
        encoding = _detect_file_encoding(stream)
        sep = _detect_separator(stream.readline().decode(encoding))
        stream.seek(0)
        _validate_csv_records(stream, encoding, sep)
        found_rows = False
        with pd.read_csv(
            stream,
            encoding=encoding,
            sep=sep,
            dtype=str,
            keep_default_na=False,
            on_bad_lines="error",
            engine="python",
            chunksize=chunk_size,
        ) as reader:
            for df in reader:
                if not df.empty:
                    found_rows = True
                    yield df
        if not found_rows:
            raise ParseError(source="mapa_psr", parser_version=PARSER_VERSION, reason="CSV vazio")
    except (ValueError, LookupError, csv.Error) as e:
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler CSV: {e}",
        ) from e


def _csv_header(reader: Iterator[list[str]]) -> list[str]:
    header = next(reader, None)
    if not header:
        raise ParseError(source="mapa_psr", parser_version=PARSER_VERSION, reason="CSV vazio")
    normalized = [_normalize_column_name(name).upper() for name in header]
    if len(normalized) != len(set(normalized)):
        raise ParseError(
            source="mapa_psr", parser_version=PARSER_VERSION, reason="Cabeçalho CSV duplicado"
        )
    missing = {"ANO_APOLICE", "SG_UF_PROPRIEDADE", "NM_CULTURA_GLOBAL"} - set(normalized)
    if missing:
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason=f"Colunas criticas faltando: {sorted(missing)}",
        )
    return normalized


def _validate_csv_records(stream: IO[bytes], encoding: str, sep: str) -> None:
    text = io.TextIOWrapper(stream, encoding=encoding, newline="")
    try:
        reader = csv.reader(text, delimiter=sep, strict=True)
        header = _csv_header(reader)
        year_index = header.index("ANO_APOLICE")
        valid_years: set[str] = set()
        for position, row in enumerate(reader, 1):
            if not row:
                continue
            location = f"Registro {position}, linha física {reader.line_num}"
            if len(row) != len(header):
                raise ParseError(
                    source="mapa_psr",
                    parser_version=PARSER_VERSION,
                    reason=f"{location}: largura {len(row)}, esperada {len(header)}",
                )
            year = row[year_index]
            if year not in valid_years:
                try:
                    models.AnoApolice.model_validate({"ano_apolice": year})
                except pydantic.ValidationError as error:
                    raise ParseError(
                        source="mapa_psr",
                        parser_version=PARSER_VERSION,
                        reason=f"{location}: ANO_APOLICE inválido",
                    ) from error
                valid_years.add(year)
    finally:
        text.detach()
        stream.seek(0)


def _drop_ignored_columns(df: pd.DataFrame) -> pd.DataFrame:
    from agrobr.alt.mapa_psr.models import COLUNAS_DROP

    df.columns = [_normalize_column_name(c) for c in df.columns]
    upper_cols = {c.upper(): c for c in df.columns}
    drop_cols = [upper_cols[d.upper()] for d in COLUNAS_DROP if d.upper() in upper_cols]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df


def _build_rename_map(df: pd.DataFrame) -> dict[str, str]:
    from agrobr.alt.mapa_psr.models import COLUNAS_CSV

    upper_cols = {c.upper(): c for c in df.columns}
    rename_map: dict[str, str] = {}
    for csv_col, df_col in COLUNAS_CSV.items():
        if csv_col in df.columns:
            rename_map[csv_col] = df_col
        else:
            upper = csv_col.upper()
            if upper in upper_cols and upper_cols[upper] in df.columns:
                rename_map[upper_cols[upper]] = df_col
    return rename_map


def _normalize_apolices_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = _drop_ignored_columns(df)
    return df.rename(columns=_build_rename_map(df))


def _normalize_apolices_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("uf", "municipio", "cultura", "classificacao"):
        if col in df.columns:
            df[col] = df[col].str.strip().str.upper()

    if "nr_apolice" in df.columns:
        df["nr_apolice"] = df["nr_apolice"].fillna("").str.strip()

    if "cd_ibge" in df.columns:
        codigo = df["cd_ibge"].str.strip()
        df["cd_ibge"] = codigo.where(codigo.ne("-") & codigo.ne(""))
    df["cod_municipio"] = (
        regions.cod_municipio(df["cd_ibge"])
        if "cd_ibge" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="Int64")
    )

    if "evento" in df.columns:
        df["evento"] = df["evento"].fillna("").str.strip().str.lower()

    if "seguradora" in df.columns:
        df["seguradora"] = df["seguradora"].str.strip()

    return df


def _convert_apolices_numbers(df: pd.DataFrame) -> pd.DataFrame:
    from agrobr.alt.mapa_psr.models import COLUNAS_FLOAT

    for col in COLUNAS_FLOAT:
        if col in df.columns:
            df[col] = df[col].apply(parse_numeric_br).astype("float64")

    return df


def _chave_municipio(nome: str) -> str:
    return " ".join(remover_acentos(nome).upper().split())


def _mesmo_municipio(df: pd.DataFrame, municipio: MunicipioInfo) -> pd.Series:
    mesmo_nome = df["municipio"].map(_chave_municipio, na_action="ignore").eq(
        _chave_municipio(municipio["nome"])
    ) & df["uf"].eq(municipio["uf"])
    if "cd_ibge" not in df.columns:
        return mesmo_nome
    return df["cd_ibge"].eq(f"{municipio['codigo_ibge']:07d}") | (df["cd_ibge"].isna() & mesmo_nome)


def _filter_apolices(
    df: pd.DataFrame,
    cultura: str | None,
    uf: str | None,
    ano: int | None,
    municipio: MunicipioInfo | None,
) -> pd.DataFrame:
    if uf:
        df = df[df["uf"] == uf.upper()]

    if cultura:
        cultura_norm = remover_acentos(cultura.upper())
        mask = (
            df["cultura"]
            .apply(
                lambda x, cn=cultura_norm: (
                    cn in remover_acentos(str(x).upper()) if pd.notna(x) else False
                )
            )
            .astype(bool)
        )
        df = df[mask]

    if ano and "ano_apolice" in df.columns:
        df = df[df["ano_apolice"] == ano]

    if municipio is not None:
        df = df[_mesmo_municipio(df, municipio)]

    return df


def iter_apolices(
    stream: IO[bytes],
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    municipio: MunicipioInfo | None = None,
    *,
    sinistros: bool = False,
    evento: str | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    chunk_size: int = 10000,
) -> Generator[pd.DataFrame, None, None]:
    from agrobr.alt.mapa_psr.models import COLUNAS_APOLICES, COLUNAS_SINISTROS

    with closing(_iter_apolices_csv(stream, chunk_size)) as chunks:
        for df in chunks:
            df = _normalize_apolices_columns(df)
            df["ano_apolice"] = pd.to_numeric(df["ano_apolice"], errors="raise")
            df["ano_apolice"] = df["ano_apolice"].astype("Int64")
            df = _normalize_apolices_strings(df)
            df = _filter_apolices(df, cultura=cultura, uf=uf, ano=ano, municipio=municipio)
            if ano_inicio is not None:
                df = df[df["ano_apolice"] >= ano_inicio]
            if ano_fim is not None:
                df = df[df["ano_apolice"] <= ano_fim]
            df = _convert_apolices_numbers(df.copy())
            if sinistros:
                df = _filter_sinistros(df, evento)
            elif "valor_indenizacao" not in df.columns:
                df["valor_indenizacao"] = pd.Series(float("nan"), index=df.index)
            columns = COLUNAS_SINISTROS if sinistros else COLUNAS_APOLICES
            yield df[[c for c in columns if c in df.columns]]


def _collect_frames(frames: Iterator[pd.DataFrame]) -> pd.DataFrame:
    result = pd.concat(list(frames), ignore_index=True)
    return result.sort_values("ano_apolice").reset_index(drop=True)


def parse_apolices(
    content: bytes,
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    municipio: MunicipioInfo | None = None,
) -> pd.DataFrame:
    return _collect_frames(
        iter_apolices(io.BytesIO(content), cultura=cultura, uf=uf, ano=ano, municipio=municipio)
    )


def parse_sinistros(
    content: bytes,
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    municipio: MunicipioInfo | None = None,
    evento: str | None = None,
) -> pd.DataFrame:
    return _collect_frames(
        iter_apolices(
            io.BytesIO(content),
            cultura=cultura,
            uf=uf,
            ano=ano,
            municipio=municipio,
            sinistros=True,
            evento=evento,
        )
    )


def _filter_sinistros(df: pd.DataFrame, evento: str | None) -> pd.DataFrame:
    if "valor_indenizacao" not in df.columns:
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason="Coluna valor_indenizacao obrigatória para identificar sinistros",
        )
    if evento and "evento" not in df.columns:
        raise ParseError(
            source="mapa_psr",
            parser_version=PARSER_VERSION,
            reason=f"Arquivo sem a coluna de evento; o filtro evento={evento!r} não pode ser aplicado",
        )
    df = df[df["valor_indenizacao"].fillna(0) > 0]

    if "evento" in df.columns:
        mask_evento = df["evento"].fillna("").str.strip().ne("")
        df = df[mask_evento]

    if evento and "evento" in df.columns:
        mask = df["evento"].str.contains(evento.lower(), na=False, regex=False)
        df = df[mask]

    return df
