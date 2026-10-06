from __future__ import annotations

import pytest
from lxml import etree

from agrobr.cnuc import parser
from agrobr.exceptions import ParseError
from tests import helpers
from tests.test_cnuc import golden


def test_grupo_publicado_desconhecido():
    root = etree.fromstring(golden.TABULAR)
    root.find(".//{http://mapserver.gis.umn.edu/mapserver}grupo").text = "Outro"
    with helpers.levanta_exatamente(ParseError, "grupo fora do domínio publicado"):
        parser.parse_ucs(etree.tostring(root))


def test_colecao_xml_truncado():
    with helpers.levanta_exatamente(ParseError, "GML inválido"):
        parser.parse_ucs(b"<FeatureCollection")


def test_colecao_camada_inesperada():
    body = golden.TABULAR.replace(b"ms:ucs_selected", b"ms:outra_camada")
    with helpers.levanta_exatamente(ParseError, "Feição 0 fora da camada"):
        parser.parse_ucs(body)


@pytest.mark.parametrize("defeito", ["leitura", "crs", "nula"])
def test_geometria_fronteira_gdal_invalida(monkeypatch, defeito):
    gpd = pytest.importorskip("geopandas")
    pyogrio = pytest.importorskip("pyogrio")
    shape = pytest.importorskip("shapely.geometry")
    frame = parser.parse_ucs(golden.TABULAR)
    geometries = [shape.Point(-37, -11)] * len(frame)
    if defeito == "nula":
        geometries[0] = None
    response = gpd.GeoDataFrame(
        {"cd_cnuc": frame["codigo"].tolist()},
        geometry=geometries,
        crs="EPSG:4674" if defeito == "crs" else "EPSG:4326",
    )

    def read_dataframe(*_args, **_kwargs):
        if defeito == "leitura":
            raise pyogrio.errors.DataSourceError("GML truncado")
        return response

    monkeypatch.setattr(pyogrio, "read_dataframe", read_dataframe)
    reason = {
        "leitura": "Geometria GML ilegível: GML truncado",
        "crs": "diferente de EPSG:4326",
        "nula": "UC sem geometria no GML",
    }[defeito]
    with helpers.levanta_exatamente(ParseError, reason):
        parser.parse_ucs_geo(golden.GEO)
