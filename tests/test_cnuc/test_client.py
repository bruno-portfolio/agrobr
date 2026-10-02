from __future__ import annotations

import itertools
import re
from urllib import parse

from lxml import etree

from agrobr.cnuc import client, models
from agrobr.normalize.regions import UFS
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
        ("Or", None, ""),
        ("PropertyIsEqualTo", "esfera", "Estadual"),
        ("PropertyIsEqualTo", "categoria", "Parque"),
        ("PropertyIsEqualTo", "grupo", "Proteção Integral"),
    ]


def _casa(elemento: etree._Element, linha: dict[str, str]) -> bool:
    operador = etree.QName(elemento).localname
    filhos = [filho for filho in elemento if etree.QName(filho).namespace == FES[1:-1]]
    if operador in {"Filter", "And"}:
        return all(_casa(filho, linha) for filho in filhos)
    if operador == "Or":
        return any(_casa(filho, linha) for filho in filhos)
    if operador == "Not":
        return not _casa(filhos[0], linha)
    valor = linha[elemento.findtext(f"{FES}ValueReference")]
    literal = elemento.findtext(f"{FES}Literal")
    if operador == "PropertyIsEqualTo":
        return valor == literal
    assert operador == "PropertyIsLike"
    padrao = "".join(".*" if c == "%" else "." if c == "_" else re.escape(c) for c in literal)
    return re.fullmatch(padrao, valor) is not None


def test_filtro_de_uf_no_servidor_casa_so_a_uf_pedida():
    nomes = {str(info["nome"]).upper(): sigla for sigla, info in UFS.items()}
    linhas = [
        ", ".join(combinacao)
        for tamanho in (1, 2)
        for combinacao in itertools.permutations(sorted(nomes), tamanho)
    ] + ["GOIÁS, MATO GROSSO, MATO GROSSO DO SUL", "MATO GROSSO DO SUL, MATO GROSSO, GOIÁS"]
    divergentes = []
    for sigla in sorted(UFS):
        filtro = etree.fromstring(client.build_filter(client.FiltroServidor(uf=sigla)).encode())
        for linha in linhas:
            esperado = sigla in {nomes[nome] for nome in linha.split(", ")}
            if _casa(filtro, {"limite": "uc", "uf": linha}) != esperado:
                divergentes.append((sigla, linha))
    assert divergentes == []


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
