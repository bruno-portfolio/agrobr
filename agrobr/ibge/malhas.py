from __future__ import annotations

import hashlib
import importlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, overload
from urllib import parse

import pandas as pd
from pydantic import (
    BaseModel,
    Field,
    FiniteFloat,
    ValidationError,
    field_validator,
    model_validator,
)

from agrobr import _log, normalize
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import InvalidParameterError, ParseError, ResourceLimitError
from agrobr.http.settings import get_timeout
from agrobr.models import MetaInfo
from agrobr.normalize import municipalities
from agrobr.normalize.regions import UFS
from agrobr.utils.geo import check_geopandas, fetch_wfs, parse_wfs_hits, validate_bbox
from agrobr.utils.result import (
    DataFrameResult,
    GeoDataFrameResult,
    build_source_meta,
    finalize_result,
)
from agrobr.utils.validation import validate_uf

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl

logger = _log.get_logger(__name__)

PARSER_VERSION = 1
TIMEOUT = get_timeout(read=120.0)
WFS_VERSION = "2.0.0"
GEOM_COLUMN = "geom"
CRS_URN_4326 = "urn:ogc:def:crs:EPSG::4326"
TIPOS_GEOMETRIA = ("Polygon", "MultiPolygon")
_TEXTO = pd.Series([""]).dtype


class Municipio(BaseModel):
    cd_uf: str = Field(pattern=r"^[0-9]{2}$")
    sigla_uf: str
    cd_mun: str = Field(pattern=r"^[0-9]{7}$")
    nm_mun: str = Field(min_length=1)
    area_km2: FiniteFloat | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def uf_coerente(self) -> Municipio:
        uf = UFS.get(self.sigla_uf)
        if uf is None or f"{uf['ibge']:02d}" != self.cd_uf or self.cd_mun[:2] != self.cd_uf:
            raise ValueError(
                f"UF incoerente: sigla_uf={self.sigla_uf!r}, cd_uf={self.cd_uf!r}, "
                f"cd_mun={self.cd_mun!r}"
            )
        return self

    def saida(self) -> dict[str, Any]:
        return {
            "uf": self.sigla_uf,
            "cod_uf": self.cd_uf,
            "cod_municipio": int(self.cd_mun),
            "municipio": self.nm_mun,
            "area_km2": self.area_km2,
        }


class AreaUrbanizada(BaseModel):
    fid: str = Field(min_length=1)
    densidade: str | None = Field(default=None, alias="Densidade")
    tipo: str | None = Field(default=None, alias="Tipo")
    comparacao: str | None = Field(default=None, alias="Comparacao")
    data_imagem: datetime | None = Field(default=None, alias="DataImagem")
    area_ha: FiniteFloat | None = Field(default=None, ge=0, alias="Area_ha")
    area_km2: FiniteFloat | None = Field(default=None, ge=0, alias="Area_km2")

    @field_validator("densidade", "tipo", "comparacao", mode="before")
    @classmethod
    def vazio_vira_nulo(cls, value: Any) -> Any:
        return None if isinstance(value, str) and not value.strip() else value

    @field_validator("data_imagem", mode="before")
    @classmethod
    def ano_mes(cls, value: Any) -> Any:
        if value is None or value == "":
            return None
        if not isinstance(value, str):
            raise ValueError(f"DataImagem fora do formato AAAA/MM: {value!r}")
        return datetime.strptime(value, "%Y/%m")

    def saida(self) -> dict[str, Any]:
        return {
            "id": self.fid,
            "densidade": self.densidade,
            "tipo": self.tipo,
            "comparacao": self.comparacao,
            "data_imagem": self.data_imagem,
            "area_ha": self.area_ha,
            "area_km2": self.area_km2,
        }


@dataclass(frozen=True)
class Camada:
    nome: str
    url: str
    typename: str
    edicao: int
    ordem: str
    propriedades: tuple[str, ...]
    modelo: type[Municipio] | type[AreaUrbanizada]
    dtypes: dict[str, Any]
    max_tabular: int
    max_geo: int
    rotulo: str
    refino: str


MALHA_MUNICIPAL = Camada(
    nome="malha_municipal",
    url=URLS[Fonte.IBGE]["wfs_malha_municipal"],
    typename="CGMAT:qg_2025_030_munic",
    edicao=2025,
    ordem="cd_mun",
    propriedades=("cd_uf", "sigla_uf", "cd_mun", "nm_mun", "area_km2"),
    modelo=Municipio,
    dtypes={
        "uf": _TEXTO,
        "cod_uf": _TEXTO,
        "cod_municipio": "Int64",
        "municipio": _TEXTO,
        "area_km2": "float64",
    },
    max_tabular=10_000,
    max_geo=900,
    rotulo="municípios",
    refino="uf, municipio, bbox ou max_registros",
)

AREAS_URBANIZADAS = Camada(
    nome="areas_urbanizadas",
    url=URLS[Fonte.IBGE]["wfs_areas_urbanizadas"],
    typename="CGEO:AU_2026_AreasUrbanizadas2022_Brasil",
    edicao=2022,
    ordem="fid",
    propriedades=("fid", "Densidade", "Tipo", "Comparacao", "DataImagem", "Area_ha", "Area_km2"),
    modelo=AreaUrbanizada,
    dtypes={
        "id": _TEXTO,
        "densidade": _TEXTO,
        "tipo": _TEXTO,
        "comparacao": _TEXTO,
        "data_imagem": "datetime64[ns]",
        "area_ha": "float64",
        "area_km2": "float64",
    },
    max_tabular=50_000,
    max_geo=10_000,
    rotulo="áreas urbanizadas",
    refino="um bbox menor ou max_registros",
)


@dataclass(frozen=True)
class Consulta:
    camada: Camada
    uf: str | None = None
    municipio: municipalities.MunicipioInfo | None = None
    bbox: tuple[float, float, float, float] | None = None
    max_registros: int | None = None
    bbox_crs: str = "EPSG:4326"

    def filtro_cql(self) -> str | None:
        condicoes = []
        if self.municipio is not None:
            condicoes.append(f"cd_mun='{self.municipio['codigo_ibge']}'")
        elif self.uf is not None:
            condicoes.append(f"sigla_uf='{self.uf}'")
        if self.bbox is not None:
            minlon, minlat, maxlon, maxlat = (float(valor) for valor in self.bbox)
            condicoes.append(
                f"BBOX({GEOM_COLUMN},{minlon!r},{minlat!r},{maxlon!r},{maxlat!r},'{self.bbox_crs}')"
            )
        return " AND ".join(condicoes) or None

    def _url(self, params: dict[str, str]) -> str:
        base = {
            "service": "WFS",
            "version": WFS_VERSION,
            "request": "GetFeature",
            "typeNames": self.camada.typename,
        }
        cql = self.filtro_cql()
        filtro = {} if cql is None else {"CQL_FILTER": cql}
        query = parse.urlencode(base | params | filtro, quote_via=parse.quote, safe=",:")
        return f"{self.camada.url}?{query}"

    def url_contagem(self) -> str:
        return self._url({"resultType": "hits"})

    def url_feicoes(self, *, geo: bool) -> str:
        propriedades = (GEOM_COLUMN, *self.camada.propriedades) if geo else self.camada.propriedades
        params = {
            "outputFormat": "application/json",
            "propertyName": ",".join(propriedades),
            "sortBy": self.camada.ordem,
        }
        if self.max_registros is not None:
            params["count"] = str(self.max_registros)
        if geo:
            params["srsName"] = "EPSG:4326"
        return self._url(params)

    def resumo(self) -> dict[str, Any]:
        return {
            "uf": self.uf,
            "municipio": None if self.municipio is None else self.municipio["codigo_ibge"],
            "bbox": None if self.bbox is None else list(self.bbox),
            "max_registros": self.max_registros,
        }


def _erro(reason: str) -> ParseError:
    return ParseError(source="ibge", parser_version=PARSER_VERSION, reason=reason)


def _crs_declarado(colecao: dict[str, Any]) -> object:
    try:
        return colecao["crs"]["properties"]["name"]
    except (KeyError, TypeError):
        return None


def _colecao(data: bytes, *, geo: bool) -> list[Any]:
    try:
        colecao = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _erro(f"GeoJSON inválido: {exc}") from exc
    if (
        not isinstance(colecao, dict)
        or colecao.get("type") != "FeatureCollection"
        or not isinstance(colecao.get("features"), list)
    ):
        raise _erro("Resposta WFS em GeoJSON FeatureCollection esperada")
    feicoes: list[Any] = colecao["features"]
    if colecao.get("numberReturned") != len(feicoes):
        raise _erro(
            f"numberReturned={colecao.get('numberReturned')!r} diverge de "
            f"{len(feicoes)} feições recebidas"
        )
    if geo and _crs_declarado(colecao) != CRS_URN_4326:
        raise _erro(f"CRS {_crs_declarado(colecao)!r} diferente de {CRS_URN_4326}")
    return feicoes


def vazio(camada: Camada) -> pd.DataFrame:
    return pd.DataFrame({nome: pd.Series(dtype=dtype) for nome, dtype in camada.dtypes.items()})


def _tabela(feicoes: list[Any], camada: Camada) -> pd.DataFrame:
    linhas = []
    for posicao, feicao in enumerate(feicoes):
        propriedades = feicao.get("properties") if isinstance(feicao, dict) else None
        if not isinstance(propriedades, dict):
            raise _erro(f"Feição {posicao} sem properties")
        try:
            linhas.append(camada.modelo.model_validate(propriedades).saida())
        except ValidationError as exc:
            raise _erro(f"Feição inválida na posição {posicao} de {camada.nome}: {exc}") from exc
    if not linhas:
        return vazio(camada)
    return pd.DataFrame(linhas, columns=list(camada.dtypes)).astype(camada.dtypes)


def _geoframe(frame: pd.DataFrame, geometrias: list[Any]) -> Any:
    gpd = check_geopandas()
    if not geometrias:
        serie = gpd.GeoSeries([], crs="EPSG:4326")
    else:
        recortes = ({"type": "Feature", "properties": {}, "geometry": g} for g in geometrias)
        erro_shapely = importlib.import_module("shapely.errors").ShapelyError
        try:
            serie = gpd.GeoDataFrame.from_features(recortes, crs="EPSG:4326").geometry.values
        except (KeyError, TypeError, ValueError, erro_shapely) as exc:
            raise _erro(f"Geometria GeoJSON ilegível: {exc!r}") from exc
    return gpd.GeoDataFrame(frame.assign(geometry=serie), geometry="geometry", crs="EPSG:4326")


def parse_geojson(data: bytes, camada: Camada, *, geo: bool) -> Any:
    feicoes = _colecao(data, geo=geo)
    frame = _tabela(feicoes, camada)
    if not geo:
        return frame
    geometrias = [feicao.get("geometry") for feicao in feicoes]
    fora = [
        posicao
        for posicao, geometria in enumerate(geometrias)
        if not isinstance(geometria, dict) or geometria.get("type") not in TIPOS_GEOMETRIA
    ]
    if fora:
        raise _erro(
            f"{len(fora)} feição(ões) de {camada.nome} sem geometria {'/'.join(TIPOS_GEOMETRIA)} "
            f"no GeoJSON, a partir da posição {fora[0]}"
        )
    return _geoframe(frame, geometrias)


def _meta(
    consulta: Consulta,
    frame: Any,
    *,
    geo: bool,
    source_url: str,
    body: bytes,
    contagem: tuple[str, bytes, int],
    tempos: tuple[int, int],
) -> MetaInfo:
    camada = consulta.camada
    count_url, count_body, expected = contagem
    origem = f"ibge_{camada.nome}_wfs{'_geo' if geo else ''}"
    return build_source_meta(
        "ibge",
        source_url,
        "httpx+wfs+geojson",
        tempos[0],
        tempos[1],
        frame,
        PARSER_VERSION,
        attempted_sources=[origem],
        selected_source=origem,
        raw_content_hash=hashlib.sha256(body).hexdigest() if body else None,
        raw_content_size=len(body),
        source_details={
            "layer": camada.typename,
            "edition": camada.edicao,
            "temporal_scope": "current_layer",
            "query": consulta.resumo(),
            "coverage": {
                "expected": expected,
                "downloaded": len(frame),
                "returned": len(frame),
                "status": "count_reconciled",
                "limit": camada.max_geo if geo else camada.max_tabular,
                "truncated": expected > len(frame),
                "transactional_snapshot": False,
            },
            "count": {
                "url": count_url,
                "sha256": hashlib.sha256(count_body).hexdigest(),
                "bytes": len(count_body),
                "order": "before_feature_acquisition",
            },
        },
    )


async def _adquirir(consulta: Consulta, *, geo: bool) -> tuple[Any, MetaInfo]:
    camada = consulta.camada
    t0 = time.monotonic()
    count_url = consulta.url_contagem()
    count_body = await fetch_wfs(count_url, source="ibge", timeout=TIMEOUT)
    expected = parse_wfs_hits(count_body, source="ibge")
    a_baixar = expected if consulta.max_registros is None else min(expected, consulta.max_registros)
    limite = camada.max_geo if geo else camada.max_tabular
    if a_baixar > limite:
        raise ResourceLimitError(
            "ibge",
            f"Seleção de {a_baixar} {camada.rotulo} excede o limite de {limite}"
            f"{' com geometria' if geo else ''}; refine com {camada.refino}",
            url=count_url,
        )
    body, source_url = b"", count_url
    if a_baixar:
        source_url = consulta.url_feicoes(geo=geo)
        body = await fetch_wfs(source_url, source="ibge", timeout=TIMEOUT)
        logger.info(f"ibge_{camada.nome}_geojson", geo=geo, size=len(body))
    acquired_at = datetime.now(UTC)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    if a_baixar:
        frame = parse_geojson(body, camada, geo=geo)
    else:
        frame = _geoframe(vazio(camada), []) if geo else vazio(camada)
    if len(frame) != a_baixar:
        raise _erro(
            f"Contagem divergente: {a_baixar} feições anunciadas, {len(frame)} recebidas; "
            "camada alterada ou coleta incompleta"
        )
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _meta(
        consulta,
        frame,
        geo=geo,
        source_url=source_url,
        body=body,
        contagem=(count_url, count_body, expected),
        tempos=(fetch_ms, parse_ms),
    )
    meta.fetched_at = acquired_at
    meta.fetch_timestamp = acquired_at
    meta.timestamp = datetime.now(UTC)
    return frame, meta


def _validar_saida(*, as_polars: bool, return_meta: bool) -> None:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError("Instale agrobr[polars] para usar as_polars=True") from None


def _validar_max_registros(max_registros: object) -> int | None:
    if max_registros is not None and (type(max_registros) is not int or max_registros < 1):
        raise InvalidParameterError(
            f"max_registros deve ser inteiro positivo ou None, recebeu {max_registros!r}"
        )
    return max_registros


def _consulta_malha(
    *,
    uf: str | None,
    municipio: str | int | None,
    bbox: tuple[float, float, float, float] | None,
    max_registros: int | None,
) -> Consulta:
    uf = validate_uf(uf)
    info = None if municipio is None else normalize.resolver_municipio(municipio, uf=uf)
    return Consulta(
        MALHA_MUNICIPAL,
        uf=uf,
        municipio=info,
        bbox=validate_bbox(bbox),
        max_registros=_validar_max_registros(max_registros),
    )


def _consulta_areas(
    *, bbox: tuple[float, float, float, float] | None, max_registros: int | None
) -> Consulta:
    if bbox is None:
        raise InvalidParameterError(
            "bbox é obrigatório: a camada de áreas urbanizadas não tem UF nem município. "
            "Para um município, use ibge.malha_municipal_geo(municipio=...).total_bounds"
        )
    return Consulta(
        AREAS_URBANIZADAS,
        bbox=validate_bbox(bbox),
        max_registros=_validar_max_registros(max_registros),
    )


def _string_columns(camada: Camada) -> tuple[str, ...]:
    return tuple(nome for nome, dtype in camada.dtypes.items() if dtype is _TEXTO)


@overload
async def malha_municipal(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def malha_municipal(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def malha_municipal(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def malha_municipal(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def malha_municipal(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def malha_municipal(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    _validar_saida(as_polars=as_polars, return_meta=return_meta)
    consulta = _consulta_malha(uf=uf, municipio=municipio, bbox=None, max_registros=max_registros)
    logger.info("ibge_malha_municipal", **consulta.resumo())
    frame, meta = await _adquirir(consulta, geo=False)
    return finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=_string_columns(MALHA_MUNICIPAL),
    )


@overload
async def malha_municipal_geo(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def malha_municipal_geo(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def malha_municipal_geo(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def malha_municipal_geo(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    if not isinstance(return_meta, bool):
        raise InvalidParameterError("return_meta deve ser booleano")
    consulta = _consulta_malha(uf=uf, municipio=municipio, bbox=bbox, max_registros=max_registros)
    check_geopandas()
    logger.info("ibge_malha_municipal_geo", **consulta.resumo())
    gdf, meta = await _adquirir(consulta, geo=True)
    return (gdf, meta) if return_meta else gdf


@overload
async def areas_urbanizadas(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def areas_urbanizadas(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def areas_urbanizadas(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def areas_urbanizadas(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def areas_urbanizadas(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def areas_urbanizadas(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    _validar_saida(as_polars=as_polars, return_meta=return_meta)
    consulta = _consulta_areas(bbox=bbox, max_registros=max_registros)
    logger.info("ibge_areas_urbanizadas", **consulta.resumo())
    frame, meta = await _adquirir(consulta, geo=False)
    return finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=_string_columns(AREAS_URBANIZADAS),
    )


@overload
async def areas_urbanizadas_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def areas_urbanizadas_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def areas_urbanizadas_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def areas_urbanizadas_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    if not isinstance(return_meta, bool):
        raise InvalidParameterError("return_meta deve ser booleano")
    consulta = _consulta_areas(bbox=bbox, max_registros=max_registros)
    check_geopandas()
    logger.info("ibge_areas_urbanizadas_geo", **consulta.resumo())
    gdf, meta = await _adquirir(consulta, geo=True)
    return (gdf, meta) if return_meta else gdf
