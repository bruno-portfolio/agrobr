from __future__ import annotations

from urllib import parse

from lxml import etree

from agrobr.cnuc import client, models
from tests.test_cnuc.golden import MANIFESTO

FES = "{http://www.opengis.net/fes/2.0}"
GML = "{http://www.opengis.net/gml/3.2}"


def _params(url: str) -> dict[str, str]:
    assert url.startswith(f"{models.MAPSERVER}?")
    return dict(parse.parse_qsl(parse.urlsplit(url).query, strict_parsing=True))


def _condicoes(filtro: client.FiltroServidor) -> list[tuple[str, str, str]]:
    raiz = etree.fromstring(client.build_filter(filtro).encode())
    conjunto = raiz.find(f"{FES}And")
    elementos = list(conjunto) if conjunto is not None else list(raiz)
    return [
        (
            etree.QName(elemento).localname,
            elemento.findtext(f"{FES}ValueReference"),
            elemento.findtext(f"{FES}Literal") or "",
        )
        for elemento in elementos
    ]


def test_filtro_sempre_exclui_zonas_de_amortecimento():
    assert _condicoes(client.FiltroServidor()) == [("PropertyIsEqualTo", "limite", "uc")]


def test_filtro_traduz_parametros_para_os_textos_publicados():
    filtro = client.FiltroServidor(uf="MT", esfera="estadual", categoria="Parque", grupo="PI")
    assert _condicoes(filtro) == [
        ("PropertyIsEqualTo", "limite", "uc"),
        ("PropertyIsLike", "uf", "%MATO GROSSO%"),
        ("PropertyIsEqualTo", "esfera", "Estadual"),
        ("PropertyIsEqualTo", "categoria", "Parque"),
        ("PropertyIsEqualTo", "grupo", "Proteção Integral"),
    ]


def test_filtro_bbox_usa_urn_com_latitude_primeiro():
    raiz = etree.fromstring(
        client.build_filter(client.FiltroServidor(bbox=(-37.2, -11.0, -37.0, -10.8))).encode()
    )
    envelope = raiz.find(f".//{FES}BBOX/{GML}Envelope")
    assert envelope.get("srsName") == "urn:ogc:def:crs:EPSG::4326"
    assert envelope.findtext(f"{GML}lowerCorner") == "-11.0 -37.2"
    assert envelope.findtext(f"{GML}upperCorner") == "-10.8 -37.0"


def test_url_de_contagem_pede_hits_sem_propriedades():
    params = _params(client.count_url(client.FiltroServidor()))
    assert params["RESULTTYPE"] == "hits"
    assert params["MAP"] == models.MAPFILE
    assert params["TYPENAMES"] == "ms:ucs_selected"
    assert "PROPERTYNAME" not in params
    assert "COUNT" not in params


def test_url_tabular_e_geo():
    tabular = _params(client.features_url(client.FiltroServidor(), geo=False, count=5))
    geo = _params(client.features_url(client.FiltroServidor(), geo=True))
    assert tabular["PROPERTYNAME"].split(",") == models.PROPERTY_NAMES
    assert tabular["SORTBY"] == "cd_cnuc"
    assert tabular["COUNT"] == "5"
    assert "SRSNAME" not in tabular
    assert geo["PROPERTYNAME"].split(",") == ["msGeometry", *models.PROPERTY_NAMES]
    assert geo["SRSNAME"] == "urn:ogc:def:crs:EPSG::4326"
    assert "COUNT" not in geo


def test_golden_foi_capturado_com_as_urls_do_client():
    filtro = client.FiltroServidor(uf="SE")
    arquivos = MANIFESTO["files"]
    assert arquivos["hits.xml"]["url"] == client.count_url(filtro)
    assert arquivos["tabular.xml"]["url"] == client.features_url(filtro, geo=False)
    assert arquivos["geo.xml"]["url"] == client.features_url(filtro, geo=True)
