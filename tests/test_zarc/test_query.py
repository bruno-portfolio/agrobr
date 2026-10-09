from __future__ import annotations

import re

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.zarc.query import build_query
from tests.helpers import levanta_exatamente, sem_excecao


@pytest.mark.parametrize(
    ("kwargs", "motivo"),
    [
        ({"uf": 1}, "UF inválida: 1. Valores válidos: AC, AL"),
        ({"uf": "XX"}, "UF inválida: 'XX'. Valores válidos: AC, AL"),
        ({"produto": False}, "produto deve ser texto não vazio"),
        ({"produto": " "}, "produto deve ser texto não vazio"),
        ({"municipio": True}, "Município deve ser o nome ou o código IBGE de 7 dígitos"),
        ({"municipio": 1.5}, "Município deve ser o nome ou o código IBGE de 7 dígitos"),
        ({"municipio": 123}, "Código IBGE de município tem 7 dígitos"),
        ({"municipio": "１２３４５６７"}, "Município não encontrado"),
        ({"municipio": " "}, "Município deve ser o nome ou o código IBGE de 7 dígitos"),
        ({"municipio": "abc"}, "Município não encontrado: 'abc'"),
        ({"municipio": "Santa Rita", "uf": "MG"}, "Santa Rita de Caldas/MG (3159209)"),
        ({"municipio": "Bom Jesus"}, "informe a uf"),
        ({"municipio": "5107925", "uf": "GO"}, "não pertence à UF GO"),
        ({"safra": True}, "safra deve ser texto não vazio"),
        ({"safra": "2025/2027"}, "safra deve usar anos consecutivos YYYY/YYYY ou perene"),
        ({"safra": "２０２５/２０２６"}, "safra deve usar anos consecutivos YYYY/YYYY ou perene"),
        ({"safra": "olericola"}, "safra deve usar anos consecutivos YYYY/YYYY ou perene"),
        ({"solo": True}, "solo deve ser um destes códigos inteiros: [1, 2, 3, 11"),
        ({"solo": 1.0}, "solo deve ser um destes códigos inteiros: [1, 2, 3, 11"),
        ({"solo": 10}, "solo deve ser um destes códigos inteiros: [1, 2, 3, 11"),
        ({"ciclo": "20"}, "ciclo deve ser um destes códigos inteiros: [13, 19, 20"),
        ({"ciclo": False}, "ciclo deve ser um destes códigos inteiros: [13, 19, 20"),
        ({"ciclo": 23}, "ciclo deve ser um destes códigos inteiros: [13, 19, 20"),
    ],
)
def test_invalid_selector(kwargs, motivo):
    with levanta_exatamente(InvalidParameterError, match=re.escape(motivo)):
        build_query(**kwargs)


def test_normalized_query_keeps_original_values():
    with sem_excecao():
        query = build_query(
            produto=" Soja ", uf=" mt ", municipio=" sorriso ", safra=" PERENE ", solo=11, ciclo=13
        )
    assert query.cultura == "soja"
    assert query.uf == "MT"
    assert query.municipio == "5107925"
    assert query.safra == "perene"
    assert query.requested["uf"] == " mt "
    assert query.requested["produto"] == " Soja "
    assert query.requested["municipio"] == " sorriso "


@pytest.mark.parametrize("value", ["5103403", 5103403, " 5103403 ", "Cuiabá", "CUIABA"])
def test_municipio_por_codigo_ou_nome_inteiro_vira_o_geocodigo(value):
    with sem_excecao():
        selecionada = build_query(municipio=value)
    assert selecionada.municipio == "5103403"


@pytest.mark.parametrize("value", [" abobrinha ", "zzzzzzzz"])
def test_cultura_fora_catalogo_sem_sugestoes(value):
    with pytest.raises(InvalidParameterError) as error:
        build_query(produto=value)
    message = str(error.value)
    assert repr(value) in message
    assert "107 culturas" in message
    assert "não está no catálogo" in message
    assert "veja zarc.culturas()" in message
    assert "Semelhantes" not in message


def test_cultura_fora_catalogo_com_sugestao():
    with pytest.raises(InvalidParameterError, match="Semelhantes: soja"):
        build_query(produto="soj")


@pytest.mark.parametrize(
    "value,expected",
    [
        ("Milho 2ª Safra", "milho_2"),
        ("café arábica produção", "cafe_arabica"),
        ("cafe_arabica", "cafe_arabica"),
    ],
)
def test_cultura_catalogo_aceita_alias(value, expected):
    with sem_excecao():
        selecionada = build_query(produto=value)
    assert selecionada.cultura == expected


@pytest.mark.parametrize("value", ["2016/2017", "2026/2027", "perene"])
def test_supported_resource_selector(value):
    with sem_excecao():
        selecionada = build_query(safra=value)
    assert selecionada.safra == value
