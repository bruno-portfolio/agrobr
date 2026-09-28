from __future__ import annotations

import re

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.zarc.query import build_query
from tests.helpers import levanta_exatamente, sem_excecao


@pytest.mark.parametrize(
    ("kwargs", "motivo"),
    [
        ({"uf": 1}, "uf deve ser texto não vazio"),
        ({"uf": "XX"}, "UF invalida"),
        ({"cultura": False}, "cultura deve ser texto não vazio"),
        ({"cultura": " "}, "cultura deve ser texto não vazio"),
        ({"municipio": True}, "municipio deve ser código de sete dígitos ou nome"),
        ({"municipio": 1.5}, "municipio deve ser código de sete dígitos ou nome"),
        ({"municipio": 123}, "Código de municipio deve ter sete dígitos ASCII"),
        ({"municipio": "１２３４５６７"}, "Código de municipio deve ter sete dígitos ASCII"),
        ({"municipio": " "}, "municipio deve ser texto não vazio"),
        ({"safra": True}, "safra deve ser texto não vazio"),
        ({"safra": "2025/2027"}, "safra deve usar anos consecutivos YYYY/YYYY ou perene"),
        ({"safra": "２０２５/２０２６"}, "safra deve usar anos consecutivos YYYY/YYYY ou perene"),
        ({"safra": "olericola"}, "safra deve usar anos consecutivos YYYY/YYYY ou perene"),
        ({"solo": True}, "solo deve ser código inteiro homologado"),
        ({"solo": 1.0}, "solo deve ser código inteiro homologado"),
        ({"solo": 10}, "solo deve ser código inteiro homologado"),
        ({"ciclo": "20"}, "ciclo deve ser código inteiro homologado"),
        ({"ciclo": False}, "ciclo deve ser código inteiro homologado"),
        ({"ciclo": 23}, "ciclo deve ser código inteiro homologado"),
    ],
)
def test_invalid_selector(kwargs, motivo):
    with levanta_exatamente(InvalidParameterError, match=re.escape(motivo)):
        build_query(**kwargs)


def test_normalized_query_keeps_original_values():
    with sem_excecao():
        query = build_query(
            cultura=" Soja ", uf=" mt ", municipio=" Campo[1] ", safra=" PERENE ", solo=11, ciclo=13
        )
    assert query.cultura == "soja"
    assert query.uf == "MT"
    assert query.municipio == "Campo[1]"
    assert query.safra == "perene"
    assert query.requested["uf"] == " mt "
    assert query.requested["cultura"] == " Soja "


@pytest.mark.parametrize("value", ["0123456", 5103403])
def test_municipal_identity_is_not_coerced(value):
    with sem_excecao():
        selecionada = build_query(municipio=value)
    assert selecionada.municipio == value


@pytest.mark.parametrize("value", [" abobrinha ", "zzzzzzzz"])
def test_cultura_fora_catalogo_sem_sugestoes(value):
    with pytest.raises(InvalidParameterError) as error:
        build_query(cultura=value)
    message = str(error.value)
    assert repr(value) in message
    assert "107 culturas" in message
    assert "não está no catálogo" in message
    assert "veja zarc.culturas()" in message
    assert "Semelhantes" not in message


def test_cultura_fora_catalogo_com_sugestao():
    with pytest.raises(InvalidParameterError, match="Semelhantes: soja"):
        build_query(cultura="soj")


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
        selecionada = build_query(cultura=value)
    assert selecionada.cultura == expected


@pytest.mark.parametrize("value", ["2016/2017", "2026/2027", "perene"])
def test_supported_resource_selector(value):
    with sem_excecao():
        selecionada = build_query(safra=value)
    assert selecionada.safra == value
