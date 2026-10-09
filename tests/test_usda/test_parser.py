from __future__ import annotations

import pandas as pd
import pytest

from agrobr.exceptions import ParseError
from agrobr.usda import parser
from tests.helpers import levanta_exatamente, sem_excecao

from .conftest import manifesto, registros

CORTES = sorted(
    arquivo
    for arquivo, entrada in manifesto().items()
    if entrada["origem"].startswith("Conferência de 26/09/2026")
    and not arquivo.startswith(("cat_", "oraculo_"))
)


def _ler(corpo: list[dict]) -> pd.DataFrame:
    with sem_excecao():
        return parser.parse_psd_response(corpo)


def _valor(df: pd.DataFrame, rotulo: str) -> float:
    return float(df.loc[df["attribute_br"] == rotulo, "value"].sum())


def test_identidade_de_oferta_e_distribuicao():
    assert len(CORTES) == 34
    for arquivo in CORTES:
        df = _ler(registros(arquivo))
        oferta = _valor(df, "oferta_total")
        assert _valor(df, "estoque_inicial") + _valor(df, "producao") + _valor(
            df, "importacao"
        ) == pytest.approx(oferta, abs=1e-6), arquivo
        distribuicao = (
            _valor(df, "exportacao")
            + _valor(df, "consumo_domestico")
            + _valor(df, "perdas")
            + _valor(df, "estoque_final")
        )
        assert distribuicao == pytest.approx(_valor(df, "distribuicao_total"), abs=1e-6), arquivo
        assert distribuicao == pytest.approx(oferta, abs=1e-6), arquivo
        assert (df["attribute_br"] == "consumo_domestico").sum() == 1, arquivo


def test_colunas_tipos_e_ordem():
    df = _ler(registros("soja_BR_2024.json"))
    assert df.columns.tolist() == parser.COLUNAS
    assert df.loc[0].to_dict() == {
        "commodity_code": "2222000",
        "commodity": "soja",
        "country_code": "BR",
        "country": "Brazil",
        "market_year": 2024,
        "attribute": "Area Harvested",
        "attribute_br": "area_colhida",
        "value": 47400.0,
        "unit": "(1000 HA)",
        "attribute_id": 4,
        "unit_id": 4,
        "last_update_year": 2026,
        "last_update_month": 4,
    }
    assert df["attribute"].is_monotonic_increasing
    assert str(df["last_update_month"].dtype) == "Int64"
    assert _ler([]).columns.tolist() == parser.COLUNAS


def test_serie_antiga_com_mes_00():
    df = _ler(registros("acucar_BR_1960.json"))
    assert set(df["last_update_year"]) == {1960}
    assert df["last_update_month"].isna().all()
    atual = _ler(registros("soja_BR_1980.json"))
    assert set(zip(atual["last_update_year"], atual["last_update_month"], strict=True)) == {
        (2011, 7)
    }


def test_mundo_e_todos_os_paises():
    mundo = _ler(registros("soja_world_2024.json"))
    assert set(zip(mundo["country_code"], mundo["country"], strict=True)) == {("00", "World")}
    todos = _ler(registros("soja_all_2024.json"))
    assert (len(todos), todos["country_code"].nunique()) == (871, 67)
    assert todos.loc[todos["country_code"] == "E4", "country"].unique().tolist() == [
        "European Union"
    ]


def test_layout_antigo_ou_corpo_que_nao_e_lista():
    registro = registros("soja_BR_2024.json")[0]
    pascal = {chave[0].upper() + chave[1:]: valor for chave, valor in registro.items()}
    with levanta_exatamente(ParseError, r"sem \['attributeId', 'calendarYear'"):
        parser.parse_psd_response([pascal])
    sem_mes = {chave: valor for chave, valor in registro.items() if chave != "month"}
    with levanta_exatamente(ParseError, r"sem \['month'\]"):
        parser.parse_psd_response([registro, sem_mes])
    for corpo in ({"message": "manutenção"}, [registro, "x"], None):
        with levanta_exatamente(ParseError, "não é uma lista de registros"):
            parser.parse_psd_response(corpo)


@pytest.mark.parametrize(
    ("campo", "valor", "motivo"),
    [
        ("attributeId", 999, r"attributeId fora do catálogo oficial local do agrobr: \[999\]"),
        ("unitId", 99, r"unitId fora do catálogo oficial local do agrobr: \[99\]"),
        ("countryCode", "AB", r"countryCode fora do catálogo oficial local do agrobr: \['AB'\]"),
        ("commodityCode", "9999999", r"commodityCode fora do catálogo .*\['9999999'\]"),
        ("marketYear", "2024/25", "campo do PSD com tipo inesperado"),
        ("month", "abr", "campo do PSD com tipo inesperado"),
        ("value", "n/d", "campo do PSD com tipo inesperado"),
    ],
)
def test_codigo_fora_do_catalogo_ou_tipo_inesperado(campo, valor, motivo):
    corpo = registros("soja_BR_2024.json")
    corpo[3] = {**corpo[3], campo: valor}
    with levanta_exatamente(ParseError, motivo):
        parser.parse_psd_response(corpo)


def test_registro_repetido():
    corpo = registros("soja_BR_2024.json")
    with levanta_exatamente(ParseError, r"registro repetido .*\['2222000', 'BR', 2024, 28\]"):
        parser.parse_psd_response([*corpo, {**corpo[2], "value": 1.0}])


def test_filtro_por_nome_oficial_ou_rotulo():
    df = _ler(registros("algodao_US_2024.json"))
    with sem_excecao():
        filtrado = parser.filter_attributes(df, [" loss", "PRODUCAO", "Stocks-to-Use"])
        vazio = parser.filter_attributes(df.iloc[:0], ["producao"])
        mesmo = parser.filter_attributes(df, None)
    assert filtrado["attribute_id"].tolist() == [150, 28, 195]
    assert vazio.empty
    assert mesmo is df


def test_pivot_por_rotulo_e_ambiguidade():
    df = _ler(registros("acucar_BR_2024.json"))
    with sem_excecao():
        largo = parser.pivot_attributes(df)
        vazio = parser.pivot_attributes(df.iloc[:0])
    assert len(largo) == 1
    assert largo.loc[0, "consumo_domestico"] == df.loc[df["attribute_id"] == 126, "value"].item()
    assert (
        largo.loc[0, "Human Dom. Consumption"] == df.loc[df["attribute_id"] == 139, "value"].item()
    )
    assert "Total Disappearance" not in largo.columns
    assert largo.columns.name is None
    assert vazio.empty and vazio.columns.tolist() == parser.COLUNAS
    corpo = registros("soja_BR_2024.json")
    corpo += [
        {**corpo[0], "attributeId": 126, "unitId": 8},
        {**corpo[0], "attributeId": 173, "unitId": 8},
    ]
    ambiguo = _ler(corpo)
    with levanta_exatamente(ParseError, r"pivot ambíguo.*\('Total Disappearance', 126\)"):
        parser.pivot_attributes(ambiguo)
