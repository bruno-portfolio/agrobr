from __future__ import annotations

import hashlib
import importlib
import re
import time
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

from agrobr import _log, normalize
from agrobr.exceptions import InvalidParameterError, ParseError, ResourceLimitError
from agrobr.models import MetaInfo
from agrobr.normalize import municipalities
from agrobr.normalize.regions import remover_acentos
from agrobr.utils.geo import avisar_geometrias_invalidas, check_geopandas, validate_bbox
from agrobr.utils.result import (
    ATRIBUTO_AVISOS,
    DataFrameResult,
    GeoDataFrameResult,
    build_source_meta,
)
from agrobr.utils.validation import validate_bioma, validate_uf

from . import client, models, parser

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl

logger = _log.get_logger(__name__)

_MUNICIPIO_PUBLICADO = re.compile(r"(.+) \(([A-Z]{2})\)")


def _chave(texto: str) -> str:
    return remover_acentos(" ".join(texto.lower().split()))


_CATEGORIAS = {_chave(categoria): categoria for categoria in models.CATEGORIAS}


@dataclass(frozen=True)
class Consulta:
    filtro: client.FiltroServidor
    municipio: municipalities.MunicipioInfo | None
    bioma: str | None
    max_registros: int | None

    @property
    def filtro_local(self) -> bool:
        return self.filtro.uf is not None or self.municipio is not None or self.bioma is not None

    def resumo(self) -> dict[str, Any]:
        return {
            "uf": self.filtro.uf,
            "municipio": None if self.municipio is None else self.municipio["codigo_ibge"],
            "esfera": self.filtro.esfera,
            "categoria": self.filtro.categoria,
            "grupo": self.filtro.grupo,
            "bioma": self.bioma,
            "bbox": None if self.filtro.bbox is None else list(self.filtro.bbox),
            "max_registros": self.max_registros,
        }


def _validate_output(*, as_polars: bool, return_meta: bool) -> None:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError("Instale agrobr[polars] para usar as_polars=True") from None


def _dominio(rotulo: str, valor: object, validos: dict[str, str]) -> str:
    chave = _chave(valor) if isinstance(valor, str) else None
    if chave not in validos:
        raise InvalidParameterError(
            f"{rotulo}: {valor!r}. Valores válidos: {', '.join(sorted(validos.values()))}"
        )
    return validos[chave]


def _validate_max_registros(max_registros: object) -> int | None:
    if max_registros is not None and (type(max_registros) is not int or max_registros < 1):
        raise InvalidParameterError(
            f"max_registros deve ser inteiro positivo ou None, recebeu {max_registros!r}"
        )
    return max_registros


def _consulta(
    *,
    uf: str | None,
    municipio: str | int | None,
    esfera: str | None,
    categoria: str | None,
    grupo: str | None,
    bioma: str | None,
    bbox: tuple[float, float, float, float] | None,
    max_registros: int | None,
) -> Consulta:
    uf = validate_uf(uf)
    info = None if municipio is None else normalize.resolver_municipio(municipio, uf=uf)
    if esfera is not None:
        esfera = _dominio("Esfera inválida", esfera, {chave: chave for chave in models.ESFERAS})
    if categoria is not None:
        categoria = _dominio("Categoria inválida", categoria, _CATEGORIAS)
    if grupo is not None:
        grupo = _dominio("Grupo inválido", grupo, {sigla.lower(): sigla for sigla in models.GRUPOS})
    filtro = client.FiltroServidor(
        uf=info["uf"] if info is not None else uf,
        esfera=esfera,
        categoria=categoria,
        grupo=grupo,
        bbox=validate_bbox(bbox),
    )
    return Consulta(
        filtro=filtro,
        municipio=info,
        bioma=validate_bioma(bioma),
        max_registros=_validate_max_registros(max_registros),
    )


def _filtrar_municipio(
    frame: Any, municipio: municipalities.MunicipioInfo
) -> tuple[Any, list[str]]:
    uf, alvo = municipio["uf"], municipio["codigo_ibge"]
    casadas: list[int] = []
    cortadas: list[str] = []
    fora_do_ibge: dict[str, list[str]] = {}
    for posicao, (codigo, lista) in enumerate(zip(frame["codigo"], frame["municipios"])):
        achou = False
        for item in lista.split(","):
            publicado = _MUNICIPIO_PUBLICADO.fullmatch(item.strip().removesuffix("..."))
            if publicado is None or publicado[2] != uf:
                continue
            try:
                achou |= normalize.resolver_municipio(publicado[1], uf=uf)["codigo_ibge"] == alvo
            except InvalidParameterError:
                fora_do_ibge.setdefault(publicado[0], []).append(codigo)
        if achou:
            casadas.append(posicao)
        elif lista.endswith("..."):
            cortadas.append(codigo)
    avisos = []
    if cortadas:
        avisos.append(
            f"cnuc: {len(cortadas)} UC(s) de {uf} com a lista de municípios cortada pela fonte, "
            f"que pode incluir {municipio['nome']}, ficaram fora: {', '.join(cortadas)}"
        )
    if fora_do_ibge:
        grafias = "; ".join(
            f"{nome} em {', '.join(codigos)}" for nome, codigos in fora_do_ibge.items()
        )
        avisos.append(
            f"cnuc: grafia de município fora do cadastro do IBGE, não comparada com "
            f"{municipio['nome']}: {grafias}"
        )
    return frame.iloc[casadas].reset_index(drop=True), avisos


def _filtrar_local(frame: Any, consulta: Consulta) -> tuple[Any, list[str]]:
    avisos: list[str] = []
    if frame.empty:
        return frame, avisos
    if consulta.filtro.uf is not None:
        frame = frame[
            frame["uf"].str.split("/").map(lambda siglas: consulta.filtro.uf in siglas)
        ].reset_index(drop=True)
    if consulta.municipio is not None:
        frame, avisos = _filtrar_municipio(frame, consulta.municipio)
    if consulta.bioma is not None:
        frame = frame[
            frame["bioma"]
            .fillna("")
            .str.split("/")
            .map(lambda biomas: consulta.bioma in biomas)
            .astype(bool)
        ].reset_index(drop=True)
    return frame, avisos


def _vazio(*, geo: bool) -> Any:
    frame = parser.empty_frame()
    if not geo:
        return frame
    gpd = check_geopandas()
    return gpd.GeoDataFrame(
        frame.assign(geometry=gpd.GeoSeries([], crs="EPSG:4326")),
        geometry="geometry",
        crs="EPSG:4326",
    )


async def _adquirir(consulta: Consulta, *, geo: bool) -> tuple[Any, MetaInfo]:
    t0 = time.monotonic()
    count_body, count_url = await client.fetch_count(consulta.filtro)
    expected = parser.parse_feature_count(count_body)
    count = None if consulta.filtro_local else consulta.max_registros
    a_baixar = expected if count is None else min(expected, count)
    limite = models.MAX_FEATURES_GEO if geo else models.MAX_FEATURES_TABULAR
    if a_baixar > limite:
        filtros = (
            "uf, esfera, categoria, grupo ou bbox"
            if consulta.filtro_local
            else "uf, esfera, categoria, grupo, bbox ou max_registros"
        )
        raise ResourceLimitError(
            "cnuc",
            f"Seleção de {a_baixar} UCs excede o limite de {limite}"
            f"{' com geometria' if geo else ''}; reduza com {filtros}",
            url=count_url,
        )
    body = b""
    source_url = count_url
    if a_baixar:
        body, source_url = await client.fetch_ucs(consulta.filtro, geo=geo, count=count)
    acquired_at = datetime.now(UTC)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    if not a_baixar:
        frame = _vazio(geo=geo)
    else:
        frame = parser.parse_ucs_geo(body) if geo else parser.parse_ucs(body)
    if len(frame) != a_baixar:
        raise ParseError(
            source="cnuc",
            parser_version=parser.PARSER_VERSION,
            reason=f"Contagem divergente: {a_baixar} feições anunciadas, {len(frame)} recebidas; "
            "camada alterada ou coleta incompleta",
        )
    frame, avisos = _filtrar_local(frame, consulta)
    truncado = consulta.max_registros is not None and len(frame) > consulta.max_registros
    if truncado:
        frame = frame.iloc[: consulta.max_registros].reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    for aviso in avisos:
        warnings.warn(aviso, UserWarning, stacklevel=4)
    frame.attrs[ATRIBUTO_AVISOS] = avisos
    meta = build_source_meta(
        "cnuc",
        source_url,
        "httpx+wfs+gml",
        fetch_ms,
        parse_ms,
        frame,
        parser.PARSER_VERSION,
        attempted_sources=["cnuc_wfs_geo" if geo else "cnuc_wfs"],
        selected_source="cnuc_wfs_geo" if geo else "cnuc_wfs",
        raw_content_hash=hashlib.sha256(body).hexdigest() if body else None,
        raw_content_size=len(body),
        source_details={
            "layer": models.TYPENAME,
            "mapfile": models.MAPFILE,
            "query": consulta.resumo(),
            "coverage": {
                "expected": expected,
                "downloaded": a_baixar,
                "returned": len(frame),
                "status": "count_reconciled",
                "limit": limite,
                "truncated": truncado or (count is not None and expected > count),
                "transactional_snapshot": False,
            },
            "count": {
                "url": count_url,
                "sha256": hashlib.sha256(count_body).hexdigest(),
                "bytes": len(count_body),
                "order": "before_feature_acquisition",
            },
            "excluded": "zonas de amortecimento (limite='za')",
            "edition": None,
            "temporal_scope": "current_layer",
        },
    )
    meta.fetched_at = acquired_at
    meta.fetch_timestamp = acquired_at
    meta.timestamp = datetime.now(UTC)
    if geo:
        avisar_geometrias_invalidas(frame, "CNUC", meta)
    return frame, meta


def _to_polars(frame: pd.DataFrame) -> Any:
    pl = importlib.import_module("polars")
    tipos = {"area_ha": pl.Float64, "data_criacao": pl.Datetime("ns")}
    return pl.DataFrame(
        {
            name: pl.Series(
                name,
                [
                    None
                    if pd.isna(value)
                    else value.to_pydatetime()
                    if isinstance(value, pd.Timestamp)
                    else value
                    for value in frame[name]
                ],
                dtype=tipos.get(name, pl.Utf8),
                strict=True,
            )
            for name in frame.columns
        }
    )


@overload
async def ucs(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ucs(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def ucs(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def ucs(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def ucs(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def ucs(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    _validate_output(as_polars=as_polars, return_meta=return_meta)
    consulta = _consulta(
        uf=uf,
        municipio=municipio,
        esfera=esfera,
        categoria=categoria,
        grupo=grupo,
        bioma=bioma,
        bbox=bbox,
        max_registros=max_registros,
    )
    logger.info("cnuc_ucs", **consulta.resumo())
    frame, meta = await _adquirir(consulta, geo=False)
    result = _to_polars(frame) if as_polars else frame
    return (result, meta) if return_meta else result


@overload
async def ucs_geo(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def ucs_geo(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def ucs_geo(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def ucs_geo(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    esfera: str | None = None,
    categoria: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    if not isinstance(return_meta, bool):
        raise InvalidParameterError("return_meta deve ser booleano")
    consulta = _consulta(
        uf=uf,
        municipio=municipio,
        esfera=esfera,
        categoria=categoria,
        grupo=grupo,
        bioma=bioma,
        bbox=bbox,
        max_registros=max_registros,
    )
    check_geopandas()
    logger.info("cnuc_ucs_geo", **consulta.resumo())
    gdf, meta = await _adquirir(consulta, geo=True)
    return (gdf, meta) if return_meta else gdf
