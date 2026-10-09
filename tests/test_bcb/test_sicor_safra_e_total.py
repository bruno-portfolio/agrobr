from typing import Any

import pytest

from agrobr.bcb import client, parser
from agrobr.exceptions import ParseError
from tests.helpers import levanta_exatamente


def _emissao(ano: Any, mes: Any) -> dict[str, Any]:
    return {"AnoEmissao": ano, "MesEmissao": mes}


def _regiao_uf(**campos: Any) -> dict[str, Any]:
    return {
        "nomeUF": "MT",
        "AnoEmissao": "2024",
        "MesEmissao": "09",
        "cdPrograma": "0001",
        "QtdCusteio": 4,
        "VlCusteio": 1000.0,
        "QtdInvestimento": 0,
        "VlInvestimento": 0.0,
        "QtdComercializacao": 0,
        "VlComercializacao": 0.0,
        "QtdIndustrializacao": 0,
        "VlIndustrializacao": 0.0,
        **campos,
    }


@pytest.mark.parametrize(
    ("registro", "mensagem"),
    [
        (_emissao("2O24", "01"), "AnoEmissao não inteiro no registro 2: '2O24'"),
        (_emissao(2024.5, "01"), "AnoEmissao não inteiro no registro 2: 2024.5"),
        (_emissao("2024", "1.5"), "MesEmissao não inteiro no registro 2: '1.5'"),
        (_emissao(None, "01"), "AnoEmissao ausente no registro 2"),
        ({"AnoEmissao": "2024"}, "MesEmissao ausente no registro 2"),
    ],
)
def test_na_safra_recusa_ano_ou_mes_que_nao_identifica_a_safra(registro, mensagem):
    with levanta_exatamente(ParseError) as erro:
        client._na_safra([_emissao("2023", "08"), registro], 2023)

    assert mensagem in str(erro.value)


@pytest.mark.parametrize(
    ("campos", "mensagem"),
    [
        ({"AnoEmissao": "2O24"}, "AnoEmissao não inteiro no registro 2: '2O24'"),
        ({"MesEmissao": 9.5}, "MesEmissao não inteiro no registro 2: 9.5"),
        ({"QtdCusteio": 3.5}, "QtdCusteio não inteiro no registro 2: 3.5"),
    ],
)
def test_total_recusa_inteiro_que_nao_e_inteiro(campos, mensagem):
    with levanta_exatamente(ParseError) as erro:
        parser.parse_credito_rural_total([_regiao_uf(), _regiao_uf(**campos)], ("custeio",), "uf")

    assert mensagem in str(erro.value)


def test_total_aceita_inteiro_em_texto_e_float_inteiro():
    registros = [_regiao_uf(), _regiao_uf(AnoEmissao=2024, MesEmissao="10", QtdCusteio="6")]

    frame = parser.parse_credito_rural_total(registros, ("custeio",), "uf")

    assert frame[["safra", "uf", "qtd_contratos", "valor"]].values.tolist() == [
        ["2024/25", "MT", 10, 2000.0]
    ]
