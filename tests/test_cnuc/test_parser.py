from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from agrobr.cnuc import parser
from agrobr.exceptions import ParseError
from tests.helpers import levanta_exatamente
from tests.test_cnuc.golden import GEO, HITS, MANIFESTO, PASTA, TABULAR


def test_golden_confere_com_o_manifesto():
    for nome, arquivo in MANIFESTO["files"].items():
        corpo = (PASTA / nome).read_bytes()
        assert len(corpo) == arquivo["bytes"]
        assert hashlib.sha256(corpo).hexdigest() == arquivo["sha256"]


def test_contagem():
    assert parser.parse_feature_count(HITS) == 19


def test_tabular_de_se():
    frame = parser.parse_ucs(TABULAR)
    assert frame.columns.tolist() == parser.COLUNAS_SAIDA
    assert frame.dtypes.equals(parser.empty_frame().dtypes)
    assert frame["codigo"].is_unique
    assert frame["codigo"].tolist() == sorted(frame["codigo"])
    assert frame["esfera"].value_counts().to_dict() == {
        "federal": 11,
        "estadual": 5,
        "municipal": 3,
    }
    assert (frame["categoria"] == "Reserva Particular do Patrimônio Natural").sum() == 7
    assert set(frame["grupo"]) == {"PI", "US"}
    assert frame.iloc[0].to_dict() == {
        "codigo": "0000.00.0147",
        "nome": "PARQUE NACIONAL SERRA DE ITABAIANA",
        "esfera": "federal",
        "categoria": "Parque",
        "grupo": "PI",
        "categoria_iucn": "Category II",
        "uf": "SE",
        "municipios": "AREIA BRANCA (SE), CAMPO DO BRITO (SE), ITABAIANA (SE), "
        "ITAPORANGA D'AJUDA (SE), LARANJEIRAS (SE), MALHADOR (SE)",
        "bioma": "Caatinga/Mata Atlântica",
        "area_ha": 8024.63,
        "data_criacao": pd.Timestamp("2005-06-15"),
        "ato_criacao": "Decreto S/N de 15-06-2005",
        "orgao_gestor": "INSTITUTO CHICO MENDES DE CONSERVAÇÃO DA BIODIVERSIDADE",
        "qualidade_poligono": "Polígono corresponde ao memorial descritivo",
        "wdpa_id": "351827",
    }
    multi = frame.set_index("codigo").loc["0000.00.1812"]
    assert multi["uf"] == "AL/BA/SE"
    assert pd.isna(frame.set_index("codigo").loc["0000.28.1603", "area_ha"])
    assert pd.isna(frame.set_index("codigo").loc["2007.28.5583", "wdpa_id"])


def test_vazio_tem_os_dtypes_do_cheio():
    vazio = parser.parse_ucs(
        b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        b'numberMatched="0" numberReturned="0"/>'
    )
    assert vazio.empty
    assert vazio.dtypes.equals(parser.parse_ucs(TABULAR).dtypes)


@pytest.mark.parametrize(
    ("trocar", "por", "motivo"),
    [
        (b'numberReturned="19"', b'numberReturned="18"', "numberReturned"),
        (b"<ms:uf>SERGIPE</ms:uf>", b"<ms:uf>ATLANTIDA</ms:uf>", "UF fora do cadastro"),
        (
            b"<ms:categoria>Parque</ms:categoria>",
            b"<ms:categoria>Bosque</ms:categoria>",
            "categoria",
        ),
        (b"<ms:esfera>Federal</ms:esfera>", b"<ms:esfera>Distrital</ms:esfera>", "esfera"),
        (b"<ms:limite>uc</ms:limite>", b"<ms:limite>za</ms:limite>", "limite"),
        (
            b"<ms:cria_ano>15-06-2005</ms:cria_ano>",
            b"<ms:cria_ano>2005-06-15</ms:cria_ano>",
            "cria_ano",
        ),
    ],
    ids=["contagem", "uf", "categoria", "esfera", "zona_amortecimento", "data"],
)
def test_deriva_da_camada_vira_parse_error(trocar, por, motivo):
    assert trocar in TABULAR
    with levanta_exatamente(ParseError, motivo):
        parser.parse_ucs(TABULAR.replace(trocar, por, 1))


def test_resposta_que_nao_e_feature_collection():
    with levanta_exatamente(ParseError, "FeatureCollection esperada"):
        parser.parse_ucs(b'<ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1"/>')


def test_doctype_recusado():
    with levanta_exatamente(ParseError, "DOCTYPE"):
        parser.parse_feature_count(
            b'<!DOCTYPE x [<!ENTITY a "1">]>'
            b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" numberMatched="1"/>'
        )


def test_contagem_invalida():
    with levanta_exatamente(ParseError, "Contagem WFS inválida"):
        parser.parse_feature_count(HITS.replace(b'numberMatched="19"', b'numberMatched="unknown"'))


def test_geo_de_se():
    gpd = pytest.importorskip("geopandas")
    gdf = parser.parse_ucs_geo(GEO)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_epsg() == 4326
    assert gdf.columns.tolist() == parser.COLUNAS_SAIDA_GEO
    pd.testing.assert_frame_equal(
        pd.DataFrame(gdf.drop(columns="geometry")), parser.parse_ucs(TABULAR)
    )
    minlon, minlat, maxlon, maxlat = gdf.total_bounds
    assert -38.3 < minlon < maxlon < -36.4
    assert -11.6 < minlat < maxlat < -9.3
    assert gdf.geometry.is_valid.all()
    assert set(gdf.geom_type) == {"Polygon", "MultiPolygon"}


def test_geo_com_geometria_fora_da_ordem_dos_atributos(monkeypatch):
    pytest.importorskip("geopandas")
    pyogrio = pytest.importorskip("pyogrio")

    class LeitorInvertido:
        errors = pyogrio.errors

        @staticmethod
        def read_dataframe(*args, **kwargs):
            return pyogrio.read_dataframe(*args, **kwargs).iloc[::-1].reset_index(drop=True)

    monkeypatch.setattr(parser, "check_pyogrio", lambda: LeitorInvertido)
    with levanta_exatamente(ParseError, "divergem"):
        parser.parse_ucs_geo(GEO)


def test_geo_le_sem_baixar_o_schema(monkeypatch):
    pytest.importorskip("geopandas")
    pyogrio = pytest.importorskip("pyogrio")
    chamadas = []

    class Leitor:
        errors = pyogrio.errors

        @staticmethod
        def read_dataframe(*args, **kwargs):
            chamadas.append(kwargs)
            return pyogrio.read_dataframe(*args, **kwargs)

    monkeypatch.setattr(parser, "check_pyogrio", lambda: Leitor)
    parser.parse_ucs_geo(GEO)
    assert chamadas == [{"columns": ["cd_cnuc"], "DOWNLOAD_SCHEMA": "NO"}]
