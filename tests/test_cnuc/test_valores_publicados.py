from __future__ import annotations

from datetime import datetime
from typing import Any
from xml.etree import ElementTree

import pytest

from agrobr import cnuc
from tests import helpers
from tests.test_cnuc import golden

MS = "{http://mapserver.gis.umn.edu/mapserver}"
GML = "{http://www.opengis.net/gml/3.2}"
WFS = "{http://www.opengis.net/wfs/2.0}"


def _servidor(monkeypatch: pytest.MonkeyPatch, arquivo: str) -> dict[str, list[str]]:
    requests = []
    for nome in ("hits.xml", arquivo):
        path, params, skip = helpers.replay_signature(golden.MANIFESTO["files"][nome]["url"])
        requests.append(
            {
                "file": nome,
                "content_type": "text/xml",
                "match": {"path": path, "params": dict(params), "skip": skip},
            }
        )
    return helpers.install_replay_http(monkeypatch, {"requests": requests}, golden.PASTA)


def _feicoes(corpo: bytes) -> list[ElementTree.Element]:
    return ElementTree.fromstring(corpo).findall(f"{WFS}member/{MS}ucs_selected")


async def test_polars_preserva_area_ha_identidade_data_e_nulos_publicados(monkeypatch):
    pl = pytest.importorskip("polars")
    seen = _servidor(monkeypatch, "tabular.xml")
    with helpers.sem_excecao():
        frame = await cnuc.ucs(uf="SE", as_polars=True)
    helpers.assert_replay_served(seen)

    esperado = []
    for feicao in _feicoes(golden.TABULAR):
        area = feicao.findtext(f"{MS}ha_total")
        criacao = feicao.findtext(f"{MS}cria_ano")
        esperado.append(
            {
                "codigo": feicao.findtext(f"{MS}cd_cnuc"),
                "nome": feicao.findtext(f"{MS}nome_uc"),
                "area_ha": float(area) if area else None,
                "data_criacao": datetime.strptime(criacao, "%d-%m-%Y") if criacao else None,
                "wdpa_id": feicao.findtext(f"{MS}wdpa_pid") or None,
            }
        )
    assert len(esperado) == 19 and esperado[0]["area_ha"] == 8024.63
    assert any(linha["area_ha"] is None for linha in esperado)
    assert frame.schema["area_ha"] == pl.Float64
    assert frame.select(list(esperado[0])).to_dicts() == esperado


def _anel(posicoes: ElementTree.Element) -> list[tuple[float, float]]:
    assert posicoes.attrib["srsDimension"] == "2"
    assert posicoes.text is not None
    numeros = [float(numero) for numero in posicoes.text.split()]
    return [(lon, lat) for lat, lon in zip(numeros[::2], numeros[1::2], strict=True)]


def _geometria_publicada(feicao: ElementTree.Element, shapes: Any) -> Any:
    poligonos = []
    for poligono in feicao.findall(f".//{GML}Polygon"):
        exterior = poligono.find(f"{GML}exterior/{GML}LinearRing/{GML}posList")
        assert exterior is not None
        interiores = poligono.findall(f"{GML}interior/{GML}LinearRing/{GML}posList")
        poligonos.append(shapes.Polygon(_anel(exterior), [_anel(anel) for anel in interiores]))
    assert poligonos
    return shapes.MultiPolygon(poligonos) if len(poligonos) > 1 else poligonos[0]


async def test_geo_preserva_poligono_publicado_para_cada_codigo(monkeypatch):
    pytest.importorskip("geopandas")
    pytest.importorskip("pyogrio")
    shapes = pytest.importorskip("shapely.geometry")
    seen = _servidor(monkeypatch, "geo.xml")
    with helpers.sem_excecao():
        frame = await cnuc.ucs_geo(uf="SE")
    helpers.assert_replay_served(seen)

    esperado = {
        feicao.findtext(f"{MS}cd_cnuc"): _geometria_publicada(feicao, shapes)
        for feicao in _feicoes(golden.GEO)
    }
    assert len(esperado) == 19
    assert frame["codigo"].tolist() == list(esperado)
    assert frame.crs.to_epsg() == 4326
    for linha in frame.itertuples():
        assert linha.geometry.equals_exact(esperado[linha.codigo], tolerance=1e-14), linha.codigo
