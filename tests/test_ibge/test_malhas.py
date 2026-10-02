from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import parse

import httpx
import pandas as pd
import pytest

from agrobr import ibge
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.ibge import malhas
from tests.helpers import levanta_exatamente

GOLDEN = Path(__file__).parents[1] / "golden_data" / "ibge"
MALHA = GOLDEN / "malha_df_20261001"
AREAS = GOLDEN / "areas_urbanizadas_df_20261001"
BBOX_PLANALTINA = (-47.68, -15.64, -47.62, -15.58)
WAF = (
    b"<html><head><title>Request Rejected</title></head><body>The requested URL was rejected. "
    b"Please consult with your administrator.</body></html>"
)


def hits(total: int) -> bytes:
    return (
        b'<?xml version="1.0" encoding="UTF-8"?><wfs:FeatureCollection '
        b'xmlns:wfs="http://www.opengis.net/wfs/2.0" numberMatched="%d" numberReturned="0" '
        b'timeStamp="2026-10-01T00:00:00Z"/>' % total
    )


def primeiras(corpo: bytes, quantidade: int) -> bytes:
    colecao = json.loads(corpo)
    colecao["features"] = colecao["features"][:quantidade]
    colecao["numberReturned"] = len(colecao["features"])
    return json.dumps(colecao).encode()


def alterar(corpo: bytes, posicao: int, **propriedades: Any) -> bytes:
    colecao = json.loads(corpo)
    colecao["features"][posicao]["properties"].update(propriedades)
    return json.dumps(colecao).encode()


def instalar(
    monkeypatch: pytest.MonkeyPatch,
    pasta: Path = MALHA,
    *,
    contagem: bytes | None = None,
    tabular: bytes | None = None,
    geo: bytes | None = None,
) -> list[str]:
    corpos = {
        "hits": (pasta / "hits.xml").read_bytes() if contagem is None else contagem,
        "tabular": (pasta / "tabular.json").read_bytes() if tabular is None else tabular,
        "geo": (pasta / "geo.json").read_bytes() if geo is None else geo,
    }
    urls: list[str] = []

    async def send(_client: httpx.AsyncClient, request: httpx.Request, **_kwargs: Any):
        url = str(request.url)
        urls.append(url)
        query = dict(parse.parse_qsl(request.url.query.decode()))
        tipo = (
            "hits"
            if query.get("resultType") == "hits"
            else "geo"
            if "srsName" in query
            else "tabular"
        )
        return httpx.Response(200, content=corpos[tipo], request=request)

    monkeypatch.setattr(httpx.AsyncClient, "send", send)
    return urls


def consulta(url: str) -> dict[str, str]:
    return dict(parse.parse_qsl(parse.urlsplit(url).query))


@pytest.mark.parametrize("pasta", [MALHA, AREAS], ids=lambda pasta: pasta.name)
def test_golden_confere_com_o_manifesto_e_com_as_urls_do_client(pasta):
    manifesto = json.loads((pasta / "manifest.json").read_bytes())
    alvo = (
        malhas.Consulta(malhas.MALHA_MUNICIPAL, uf="DF")
        if pasta == MALHA
        else malhas.Consulta(malhas.AREAS_URBANIZADAS, bbox=BBOX_PLANALTINA)
    )
    urls = {
        "hits.xml": alvo.url_contagem(),
        "tabular.json": alvo.url_feicoes(geo=False),
        "geo.json": alvo.url_feicoes(geo=True),
    }
    assert set(manifesto["files"]) == set(urls)
    for nome, arquivo in manifesto["files"].items():
        corpo = (pasta / nome).read_bytes()
        assert arquivo["url"] == urls[nome]
        assert arquivo["bytes"] == len(corpo)
        assert arquivo["sha256"] == hashlib.sha256(corpo).hexdigest()


@pytest.mark.parametrize(
    ("funcao", "kwargs", "mensagem"),
    [
        (ibge.malha_municipal, {"uf": "XX"}, "UF inválida"),
        (ibge.malha_municipal, {"municipio": "Atlantida"}, "Município"),
        (ibge.malha_municipal, {"municipio": "Brasília", "uf": "GO"}, "Município"),
        (ibge.malha_municipal, {"municipio": 4300001}, "4300001"),
        (ibge.malha_municipal, {"max_registros": 0}, "max_registros"),
        (ibge.malha_municipal, {"max_registros": True}, "max_registros"),
        (ibge.malha_municipal, {"as_polars": 1}, "booleanos"),
        (ibge.malha_municipal, {"return_meta": "sim"}, "booleanos"),
        (ibge.malha_municipal_geo, {"bbox": (1, 2, 0, 3)}, "BBOX"),
        (ibge.malha_municipal_geo, {"uf": "XX"}, "UF inválida"),
        (ibge.malha_municipal_geo, {"return_meta": 1}, "return_meta"),
        (ibge.areas_urbanizadas, {"bbox": None}, "bbox é obrigatório"),
        (ibge.areas_urbanizadas, {"bbox": (-47, -15)}, "BBOX"),
        (ibge.areas_urbanizadas, {"bbox": BBOX_PLANALTINA, "max_registros": -1}, "max_registros"),
        (ibge.areas_urbanizadas_geo, {"bbox": None}, "malha_municipal_geo"),
        (ibge.areas_urbanizadas_geo, {"bbox": BBOX_PLANALTINA, "return_meta": None}, "return_meta"),
    ],
    ids=lambda valor: getattr(valor, "__name__", str(valor)),
)
async def test_parametro_invalido_antes_da_rede(monkeypatch, funcao, kwargs, mensagem):
    urls = instalar(monkeypatch)
    with levanta_exatamente(InvalidParameterError, mensagem):
        await funcao(**kwargs)
    assert urls == []


@pytest.mark.parametrize("funcao", [ibge.areas_urbanizadas, ibge.areas_urbanizadas_geo])
async def test_areas_sem_bbox_e_type_error(funcao):
    with levanta_exatamente(TypeError, "bbox"):
        await funcao()


async def test_malha_municipal_do_df_com_meta(monkeypatch):
    urls = instalar(monkeypatch)
    frame, meta = await ibge.malha_municipal(uf="df", return_meta=True)
    assert frame.to_dict("records") == [
        {
            "uf": "DF",
            "cod_uf": "53",
            "cod_municipio": 5300108,
            "municipio": "Brasília",
            "area_km2": 5760.785,
        }
    ]
    assert frame.dtypes.equals(malhas.vazio(malhas.MALHA_MUNICIPAL).dtypes)
    assert [consulta(url).get("resultType") for url in urls] == ["hits", None]
    assert consulta(urls[1])["CQL_FILTER"] == "sigla_uf='DF'"
    assert consulta(urls[1])["sortBy"] == "cd_mun"
    assert "count" not in consulta(urls[1])
    assert meta.source == "ibge"
    assert meta.selected_source == "ibge_malha_municipal_wfs"
    assert meta.source_url == urls[1]
    assert meta.records_count == 1
    assert (
        meta.raw_content_hash == hashlib.sha256((MALHA / "tabular.json").read_bytes()).hexdigest()
    )
    detalhes = meta.source_details
    assert detalhes["layer"] == "CGMAT:qg_2025_030_munic"
    assert detalhes["edition"] == 2025
    assert detalhes["query"] == {"uf": "DF", "municipio": None, "bbox": None, "max_registros": None}
    assert detalhes["coverage"] == {
        "expected": 1,
        "downloaded": 1,
        "returned": 1,
        "status": "count_reconciled",
        "limit": 10_000,
        "truncated": False,
        "transactional_snapshot": False,
    }
    assert detalhes["count"]["url"] == urls[0]
    assert detalhes["count"]["bytes"] == len((MALHA / "hits.xml").read_bytes())


async def test_municipio_resolvido_vira_codigo_no_servidor(monkeypatch):
    urls = instalar(monkeypatch)
    frame, meta = await ibge.malha_municipal(municipio="brasilia", uf="DF", return_meta=True)
    assert frame["cod_municipio"].tolist() == [5300108]
    assert consulta(urls[0])["CQL_FILTER"] == "cd_mun='5300108'"
    assert meta.source_details["query"]["municipio"] == 5300108


async def test_selecao_vazia_nao_baixa_feicoes(monkeypatch):
    urls = instalar(monkeypatch, contagem=hits(0))
    frame = await ibge.malha_municipal(uf="DF")
    assert frame.empty
    assert frame.dtypes.equals(malhas.vazio(malhas.MALHA_MUNICIPAL).dtypes)
    assert len(urls) == 1


async def test_contagem_divergente(monkeypatch):
    instalar(monkeypatch, contagem=hits(2))
    with levanta_exatamente(ParseError, "Contagem divergente: 2 feições anunciadas, 1 recebidas"):
        await ibge.malha_municipal(uf="DF")


async def test_limite_tabular_antes_do_download(monkeypatch):
    urls = instalar(monkeypatch, AREAS, contagem=hits(50_001))
    with levanta_exatamente(
        ResourceLimitError, "50001 áreas urbanizadas excede o limite de 50000; refine com um bbox"
    ):
        await ibge.areas_urbanizadas(bbox=BBOX_PLANALTINA)
    assert len(urls) == 1


async def test_max_registros_reduz_o_download_no_servidor(monkeypatch):
    tabular = (AREAS / "tabular.json").read_bytes()
    urls = instalar(monkeypatch, AREAS, tabular=primeiras(tabular, 5))
    frame, meta = await ibge.areas_urbanizadas(
        bbox=BBOX_PLANALTINA, max_registros=5, return_meta=True
    )
    assert consulta(urls[1])["count"] == "5"
    assert len(frame) == 5
    assert meta.source_details["coverage"]["expected"] == 25
    assert meta.source_details["coverage"]["downloaded"] == 5
    assert meta.source_details["coverage"]["truncated"] is True


async def test_waf_do_ibge_vira_source_unavailable(monkeypatch):
    instalar(monkeypatch, contagem=WAF)
    with levanta_exatamente(SourceUnavailableError, "HTML"):
        await ibge.malha_municipal(uf="DF")


async def test_areas_urbanizadas_de_planaltina(monkeypatch):
    urls = instalar(monkeypatch, AREAS)
    frame, meta = await ibge.areas_urbanizadas(bbox=BBOX_PLANALTINA, return_meta=True)
    assert len(frame) == 25
    assert frame["id"].is_unique
    assert frame.columns.tolist() == list(malhas.AREAS_URBANIZADAS.dtypes)
    assert frame.dtypes.equals(malhas.vazio(malhas.AREAS_URBANIZADAS).dtypes)
    assert frame.iloc[0].to_dict() == {
        "id": "11068",
        "densidade": "Densa",
        "tipo": "Área urbanizada",
        "comparacao": "Sem alteração",
        "data_imagem": pd.Timestamp("2022-09-01"),
        "area_ha": 464.65014945100546,
        "area_km2": 4.646501494510056,
    }
    assert (frame["area_ha"] > 0).all()
    assert ((frame["area_km2"] * 100 - frame["area_ha"]).abs() < 1e-6).all()
    assert consulta(urls[0])["CQL_FILTER"] == "BBOX(geom,-47.68,-15.64,-47.62,-15.58,'EPSG:4326')"
    assert consulta(urls[1])["sortBy"] == "fid"
    assert meta.selected_source == "ibge_areas_urbanizadas_wfs"
    assert meta.source_details["edition"] == 2022
    assert meta.source_details["layer"] == "CGEO:AU_2026_AreasUrbanizadas2022_Brasil"


@pytest.mark.parametrize(
    ("camada", "pasta", "alteracao", "motivo"),
    [
        (malhas.MALHA_MUNICIPAL, MALHA, {"cd_uf": "52"}, "UF incoerente"),
        (malhas.MALHA_MUNICIPAL, MALHA, {"sigla_uf": "XX"}, "UF incoerente"),
        (malhas.MALHA_MUNICIPAL, MALHA, {"cd_mun": "530010"}, "cd_mun"),
        (malhas.MALHA_MUNICIPAL, MALHA, {"nm_mun": ""}, "nm_mun"),
        (malhas.MALHA_MUNICIPAL, MALHA, {"area_km2": -1}, "area_km2"),
        (malhas.AREAS_URBANIZADAS, AREAS, {"DataImagem": "09/2022"}, "DataImagem"),
        (malhas.AREAS_URBANIZADAS, AREAS, {"DataImagem": 202209}, "AAAA/MM"),
        (malhas.AREAS_URBANIZADAS, AREAS, {"fid": ""}, "fid"),
        (malhas.AREAS_URBANIZADAS, AREAS, {"Area_ha": -2.0}, "Area_ha"),
    ],
    ids=lambda valor: str(valor) if isinstance(valor, dict) else "",
)
def test_deriva_da_camada_vira_parse_error(camada, pasta, alteracao, motivo):
    corpo = alterar((pasta / "tabular.json").read_bytes(), 0, **alteracao)
    with levanta_exatamente(ParseError, motivo):
        malhas.parse_geojson(corpo, camada, geo=False)


@pytest.mark.parametrize(
    ("corpo", "motivo"),
    [
        (b"{nao e json", "GeoJSON inválido"),
        (b'{"type": "Feature"}', "FeatureCollection esperada"),
        (b'{"type": "FeatureCollection", "features": [], "numberReturned": 3}', "numberReturned"),
        (
            b'{"type": "FeatureCollection", "features": [1], "numberReturned": 1}',
            "sem properties",
        ),
    ],
)
def test_resposta_fora_do_geojson_vira_parse_error(corpo, motivo):
    with levanta_exatamente(ParseError, motivo):
        malhas.parse_geojson(corpo, malhas.MALHA_MUNICIPAL, geo=False)


def test_vazio_e_espaco_em_texto_viram_nulo():
    corpo = alterar((AREAS / "tabular.json").read_bytes(), 0, Densidade=" ", DataImagem=None)
    frame = malhas.parse_geojson(corpo, malhas.AREAS_URBANIZADAS, geo=False)
    assert pd.isna(frame.loc[0, "densidade"])
    assert pd.isna(frame.loc[0, "data_imagem"])
    assert frame.dtypes.equals(malhas.vazio(malhas.AREAS_URBANIZADAS).dtypes)


async def test_as_polars_sem_polars(monkeypatch):
    urls = instalar(monkeypatch)
    monkeypatch.setitem(sys.modules, "polars", None)
    with levanta_exatamente(ImportError, "agrobr\\[polars\\]"):
        await ibge.malha_municipal(uf="DF", as_polars=True)
    assert urls == []


@pytest.mark.parametrize(
    ("pasta", "chamada"),
    [
        (MALHA, lambda **kw: ibge.malha_municipal(uf="DF", **kw)),
        (AREAS, lambda **kw: ibge.areas_urbanizadas(bbox=BBOX_PLANALTINA, **kw)),
    ],
    ids=["malha", "areas"],
)
async def test_as_polars_com_os_mesmos_tipos_no_vazio(monkeypatch, pasta, chamada):
    pl = pytest.importorskip("polars")
    instalar(monkeypatch, pasta)
    cheio = await chamada(as_polars=True)
    instalar(monkeypatch, pasta, contagem=hits(0))
    vazio = await chamada(as_polars=True)
    assert isinstance(cheio, pl.DataFrame)
    assert cheio.schema == vazio.schema
    assert cheio.schema[cheio.columns[0]] == pl.Utf8


async def test_malha_municipal_geo_do_df(monkeypatch):
    gpd = pytest.importorskip("geopandas")
    urls = instalar(monkeypatch)
    gdf, meta = await ibge.malha_municipal_geo(uf="DF", return_meta=True)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_epsg() == 4326
    assert gdf.columns.tolist() == [*malhas.MALHA_MUNICIPAL.dtypes, "geometry"]
    pd.testing.assert_frame_equal(
        pd.DataFrame(gdf.drop(columns="geometry")),
        malhas.parse_geojson(
            (MALHA / "tabular.json").read_bytes(), malhas.MALHA_MUNICIPAL, geo=False
        ),
    )
    minlon, minlat, maxlon, maxlat = gdf.total_bounds
    assert -48.3 < minlon < maxlon < -47.3
    assert -16.1 < minlat < maxlat < -15.5
    assert gdf.geometry.is_valid.all()
    assert consulta(urls[1])["srsName"] == "EPSG:4326"
    assert consulta(urls[1])["propertyName"].split(",")[0] == "geom"
    assert meta.selected_source == "ibge_malha_municipal_wfs_geo"
    assert meta.source_details["coverage"]["limit"] == 900


async def test_malha_municipal_geo_combina_uf_e_bbox_no_servidor(monkeypatch):
    pytest.importorskip("geopandas")
    urls = instalar(monkeypatch)
    await ibge.malha_municipal_geo(uf="DF", bbox=(-48.3, -16.1, -47.3, -15.5))
    assert consulta(urls[0])["CQL_FILTER"] == (
        "sigla_uf='DF' AND BBOX(geom,-48.3,-16.1,-47.3,-15.5,'EPSG:4326')"
    )


async def test_malha_municipal_geo_limite_antes_do_download(monkeypatch):
    pytest.importorskip("geopandas")
    urls = instalar(monkeypatch, contagem=hits(5573))
    with levanta_exatamente(
        ResourceLimitError, "5573 municípios excede o limite de 900 com geometria; refine com uf"
    ):
        await ibge.malha_municipal_geo()
    assert len(urls) == 1


async def test_malha_municipal_geo_max_registros_cabe_no_limite(monkeypatch):
    pytest.importorskip("geopandas")
    urls = instalar(monkeypatch, contagem=hits(5573))
    gdf, meta = await ibge.malha_municipal_geo(max_registros=1, return_meta=True)
    assert consulta(urls[1])["count"] == "1"
    assert len(gdf) == 1
    assert meta.source_details["coverage"]["truncated"] is True


async def test_geo_vazio_tem_os_dtypes_do_cheio(monkeypatch):
    gpd = pytest.importorskip("geopandas")
    instalar(monkeypatch, AREAS, contagem=hits(0))
    gdf = await ibge.areas_urbanizadas_geo(bbox=BBOX_PLANALTINA)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_epsg() == 4326
    assert gdf.columns.tolist() == [*malhas.AREAS_URBANIZADAS.dtypes, "geometry"]
    assert pd.DataFrame(gdf.drop(columns="geometry")).dtypes.equals(
        malhas.vazio(malhas.AREAS_URBANIZADAS).dtypes
    )


async def test_areas_urbanizadas_geo_de_planaltina(monkeypatch):
    gpd = pytest.importorskip("geopandas")
    instalar(monkeypatch, AREAS)
    gdf = await ibge.areas_urbanizadas_geo(bbox=BBOX_PLANALTINA)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 25
    assert gdf.geometry.is_valid.all()
    assert set(gdf.geom_type) == {"MultiPolygon"}
    minlon, minlat, maxlon, maxlat = gdf.total_bounds
    assert minlon < BBOX_PLANALTINA[2] and maxlon > BBOX_PLANALTINA[0]
    assert minlat < BBOX_PLANALTINA[3] and maxlat > BBOX_PLANALTINA[1]
    assert gdf["data_imagem"].dt.year.eq(2022).all()
    assert isinstance(gdf.loc[0, "data_imagem"], datetime)


@pytest.mark.parametrize(
    ("trocar", "motivo"),
    [
        ({"geometry": None}, "sem geometria"),
        ({"crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4674"}}}, "4674"),
        ({"crs": None}, "None"),
    ],
    ids=["sem_geometria", "crs_4674", "sem_crs"],
)
def test_geo_fora_do_contrato_vira_parse_error(trocar, motivo):
    pytest.importorskip("geopandas")
    colecao = json.loads((AREAS / "geo.json").read_bytes())
    if "geometry" in trocar:
        colecao["features"][3]["geometry"] = None
    else:
        colecao.update(trocar)
    with levanta_exatamente(ParseError, motivo):
        malhas.parse_geojson(json.dumps(colecao).encode(), malhas.AREAS_URBANIZADAS, geo=True)


@pytest.mark.parametrize(
    ("camada", "pasta"),
    [(malhas.MALHA_MUNICIPAL, MALHA), (malhas.AREAS_URBANIZADAS, AREAS)],
    ids=["malha", "areas"],
)
@pytest.mark.parametrize(
    ("geometria", "motivo"),
    [
        ({}, "sem geometria Polygon/MultiPolygon"),
        ({"type": "Point", "coordinates": [-47.65, -15.60]}, "sem geometria Polygon/MultiPolygon"),
        ({"type": "MultiPolygon", "coordinates": "malformada"}, "Geometria GeoJSON ilegível"),
    ],
    ids=["sem_tipo", "ponto_em_vez_de_poligono", "coordenadas_malformadas"],
)
def test_geometria_fora_do_layout_vira_parse_error(camada, pasta, geometria, motivo):
    pytest.importorskip("geopandas")
    colecao = json.loads((pasta / "geo.json").read_bytes())
    colecao["features"][0]["geometry"] = geometria
    with levanta_exatamente(ParseError, motivo):
        malhas.parse_geojson(json.dumps(colecao).encode(), camada, geo=True)
