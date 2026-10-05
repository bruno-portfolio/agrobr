from __future__ import annotations

import contextlib
import hashlib
import time
import warnings
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.geo import check_geopandas
from agrobr.utils.result import (
    ATRIBUTO_AVISOS,
    DataFrameResult,
    GeoDataFrameResult,
    build_source_meta,
    finalize_result,
)
from agrobr.utils.time import hoje, utcnow
from agrobr.utils.validation import validate_bioma, validate_uf

from . import client, parser

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)


def _require_int(name: str, value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidParameterError(f"{name} deve ser inteiro")
    return value


_LIMITE_DIARIO = timedelta(hours=12)
_RELOGIO_DIARIO = timedelta(hours=13, minutes=5)
_LIMITE_MENSAL = timedelta(hours=23, minutes=50)
_RELOGIO_MENSAL = timedelta(days=1, minutes=56)


def _arquivo_parcial(
    df: pd.DataFrame,
    inicio: datetime,
    limite: datetime,
    relogio_ate: datetime,
    last_modified: str | None,
    *,
    diario: bool,
) -> dict[str, object]:
    """Detecta o arquivo que o INPE ainda atualiza (o do mês ou o do dia).

    O arquivo é parcial quando o `Last-Modified` é anterior ao `limite`, ou, sem o cabeçalho,
    quando o relógio está entre o `inicio` do período e o `relogio_ate`. Os limites vêm do
    fechamento observado: o diário fecha em D+1 às 12:05 GMT (os 25 diários de set/2026), e o
    mensal, no dia 1 do mês seguinte às 23:56 GMT (jul a dez/2025, jul e ago/2026). O limite fica
    alguns minutos antes, e o relógio, 1 h depois. O aviso vai para `df.attrs`, de onde o
    `build_source_meta` o leva ao `MetaInfo`, e o detalhe volta para o `source_details`.
    """
    atualizado = None
    if last_modified:
        with contextlib.suppress(TypeError, ValueError):
            atualizado = parsedate_to_datetime(last_modified).astimezone(UTC).replace(tzinfo=None)
    parcial = atualizado < limite if atualizado else inicio <= utcnow() < relogio_ate
    if not parcial:
        return {}
    validos = df.dropna(subset=["data", "hora_gmt"])
    ultimo = (
        (validos["data"].astype(str) + " " + validos["hora_gmt"]).max() if len(validos) else None
    )
    arquivo, periodo = (
        (f"diário de {inicio:%Y-%m-%d}", "dia") if diario else (f"mensal de {inicio:%Y-%m}", "mês")
    )
    aviso = (
        f"queimadas: o arquivo {arquivo} ainda está sendo atualizado pelo INPE "
        f"(Last-Modified {last_modified or 'ausente'}); o resultado vai até o foco de {ultimo} GMT e muda "
        f"durante o {periodo}."
    )
    df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
    warnings.warn(aviso, UserWarning, stacklevel=3)
    chave = "dia_parcial" if diario else "mes_parcial"
    return {chave: True, "ultimo_foco": ultimo, "last_modified": last_modified}


def _validate_period(ano: object, mes: object, dia: object | None) -> tuple[int, int, int | None]:
    ano_int = _require_int("ano", ano)
    mes_int = _require_int("mes", mes)
    dia_int = _require_int("dia", dia) if dia is not None else None
    corrente = hoje().year
    if ano_int > corrente:
        raise InvalidParameterError(f"ano não pode ser posterior a {corrente}")
    if not 1 <= mes_int <= 12:
        raise InvalidParameterError("mes deve estar entre 1 e 12")
    try:
        date(ano_int, mes_int, 1 if dia_int is None else dia_int)
    except ValueError as exc:
        raise InvalidParameterError(
            f"data inválida: ano={ano_int}, mes={mes_int}, dia={dia_int}"
        ) from exc
    return ano_int, mes_int, dia_int


def _satelite_publicado(df: pd.DataFrame, satelite: str, periodo: str) -> str:
    publicados = {str(nome).casefold(): str(nome) for nome in df["satelite"].dropna().unique()}
    nome = publicados.get(satelite.strip().casefold())
    if nome is None:
        raise InvalidParameterError(
            f"satelite={satelite!r} não aparece no arquivo de {periodo}; "
            f"satélites publicados: {', '.join(sorted(publicados.values()))}"
        )
    return nome


@overload
async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Focos INPE no período; meses sem arquivo mensal usam o ZIP anual.

    O CSV anual pode ocupar centenas de MB após descompactar: em 2020,
    o ZIP tem cerca de 81 MB e o CSV, 584 MB.
    Os esquemas atual e legado dos CSVs são normalizados para a mesma saída.
    """
    ano, mes, dia = _validate_period(ano, mes, dia)
    uf = validate_uf(uf)
    bioma = validate_bioma(bioma)
    if satelite is not None and (not isinstance(satelite, str) or not satelite.strip()):
        raise InvalidParameterError(f"satelite deve ser texto não vazio: {satelite!r}")
    logger.info(
        "queimadas_focos",
        ano=ano,
        mes=mes,
        dia=dia,
        uf=uf,
        bioma=bioma,
        satelite=satelite,
    )

    t0 = time.monotonic()

    if dia is not None:
        data_str = f"{ano:04d}{mes:02d}{dia:02d}"
        csv_bytes, source_url, last_modified = await client.fetch_focos_diario(data_str)
        corpo = csv_bytes
    else:
        csv_bytes, source_url, corpo, last_modified = await client.fetch_focos_mensal(ano, mes)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_focos_csv(csv_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if dia is None and "anual" in source_url:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df = df[df["data"].dt.month == mes].reset_index(drop=True)

    inicio = datetime(ano, mes, dia or 1)
    fim = inicio + timedelta(days=1) if dia else datetime(ano + mes // 12, mes % 12 + 1, 1)
    limite, relogio_ate = (
        (_LIMITE_DIARIO, _RELOGIO_DIARIO) if dia else (_LIMITE_MENSAL, _RELOGIO_MENSAL)
    )
    parcial = _arquivo_parcial(
        df, inicio, fim + limite, fim + relogio_ate, last_modified, diario=dia is not None
    )

    if satelite is not None:
        periodo = f"{ano:04d}-{mes:02d}-{dia:02d}" if dia else f"{ano:04d}-{mes:02d}"
        satelite = _satelite_publicado(df, satelite, periodo)

    if uf is not None:
        df = df[df["uf"] == uf].reset_index(drop=True)

    if bioma is not None:
        df = df[df["bioma"] == bioma].reset_index(drop=True)

    if satelite is not None:
        df = df[df["satelite"] == satelite].reset_index(drop=True)

    df, detalhes = parser.tratar_publicacao(df)

    meta = build_source_meta(
        "queimadas",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        raw_content_hash=hashlib.sha256(corpo).hexdigest(),
        raw_content_size=len(corpo),
        source_details={**parcial, **detalhes},
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def focos_geo(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def focos_geo(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def focos_geo(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    gpd_mod = check_geopandas()

    if return_meta:
        df, meta = await focos(
            ano=ano,
            mes=mes,
            dia=dia,
            uf=uf,
            bioma=bioma,
            satelite=satelite,
            return_meta=True,
        )
    else:
        df = await focos(
            ano=ano,
            mes=mes,
            dia=dia,
            uf=uf,
            bioma=bioma,
            satelite=satelite,
        )

    geometry = gpd_mod.points_from_xy(df["lon"], df["lat"])
    gdf = gpd_mod.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    if return_meta:
        meta.columns = list(gdf.columns)
        return gdf, meta
    return gdf
