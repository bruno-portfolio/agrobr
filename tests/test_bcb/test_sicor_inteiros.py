import pytest

from agrobr.bcb import parser
from agrobr.exceptions import ParseError
from tests.helpers import levanta_exatamente


def _registro(**campos) -> dict:
    return {
        "nomeProduto": '"SOJA"',
        "nomeUF": "MT",
        "AnoEmissao": "2024",
        "MesEmissao": "09",
        "QtdCusteio": 4,
        "VlCusteio": 1108967.95,
        **campos,
    }


@pytest.mark.parametrize(
    ("campos", "coluna", "publicado"),
    [
        ({"AnoEmissao": "2O24"}, "ano_emissao", "'2O24'"),
        ({"MesEmissao": ""}, "mes_emissao", "''"),
        ({"QtdCusteio": 3.5}, "qtd_contratos", "3.5"),
        ({"MesEmissao": "9.5"}, "mes_emissao", "'9.5'"),
    ],
)
def test_parse_credito_rural_recusa_inteiro_que_nao_e_inteiro(campos, coluna, publicado):
    registros = [_registro(), _registro(**campos)]

    with levanta_exatamente(ParseError) as erro:
        parser.parse_credito_rural(registros)

    assert f"{coluna} não inteiro no registro 2: {publicado}" in str(erro.value)


def test_parse_credito_rural_aceita_inteiro_em_texto_float_inteiro_e_nulo():
    registros = [
        _registro(),
        _registro(AnoEmissao=2025, MesEmissao="03", QtdCusteio=7.0),
        _registro(QtdCusteio=None),
    ]

    frame = parser.parse_credito_rural(registros)

    colunas = ["ano_emissao", "mes_emissao", "qtd_contratos"]
    publicados = frame[colunas].astype(object).where(frame[colunas].notna(), None)
    assert sorted(publicados.values.tolist(), key=str) == [
        [2024, 9, 4],
        [2024, 9, None],
        [2025, 3, 7],
    ]
    assert [str(frame[c].dtype) for c in ("ano_emissao", "mes_emissao", "qtd_contratos")] == [
        "Int64"
    ] * 3
