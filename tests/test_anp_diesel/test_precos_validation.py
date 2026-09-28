from __future__ import annotations

import io
from datetime import datetime

import pandas as pd
import pytest

from agrobr.alt.anp_diesel import parser
from agrobr.exceptions import ParseError


def _row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "DATA INICIAL": datetime(2026, 8, 30),
        "DATA FINAL": datetime(2026, 9, 5),
        "ESTADO": "Mato Grosso",
        "MUNICÍPIO": "Cuiabá",
        "PRODUTO": "ÓLEO DIESEL S10",
        "UNIDADE DE MEDIDA": "R$/l",
        "PREÇO MÉDIO REVENDA": 6.2,
        "PREÇO MÉDIO DISTRIBUIÇÃO": "-",
        "NÚMERO DE POSTOS PESQUISADOS": 10,
    }
    row.update(changes)
    return row


def _xlsx(rows: list[dict[str, object]]) -> bytes:
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_excel(buffer, index=False, engine="openpyxl")
    return buffer.getvalue()


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("UNIDADE DE MEDIDA", "R$/m3"),
        ("DATA INICIAL", "invalida"),
        ("DATA FINAL", "29/08/2026"),
        ("DATA FINAL", datetime(2026, 9, 5, 12)),
        ("DATA INICIAL", True),
        ("PREÇO MÉDIO REVENDA", True),
        ("PREÇO MÉDIO REVENDA", "inf"),
        ("PREÇO MÉDIO REVENDA", "NaN"),
        ("PREÇO MÉDIO REVENDA", "-"),
        ("PREÇO MÉDIO REVENDA", ""),
        ("PREÇO MÉDIO REVENDA", -1),
        ("PREÇO MÉDIO DISTRIBUIÇÃO", ""),
        ("PREÇO MÉDIO DISTRIBUIÇÃO", "invalido"),
        ("NÚMERO DE POSTOS PESQUISADOS", True),
        ("NÚMERO DE POSTOS PESQUISADOS", 1.5),
        ("NÚMERO DE POSTOS PESQUISADOS", -1),
        ("NÚMERO DE POSTOS PESQUISADOS", ""),
        ("ESTADO", "ZZ"),
        ("MUNICÍPIO", ""),
    ],
)
def test_precos_valores_invalidos(column: str, value: object):
    with pytest.raises(ParseError, match="Preco semanal invalido"):
        parser.parse_precos(_xlsx([_row(**{column: value})]))


@pytest.mark.parametrize(
    "column",
    ["DATA FINAL", "UNIDADE DE MEDIDA", "PREÇO MÉDIO REVENDA", "NÚMERO DE POSTOS PESQUISADOS"],
)
def test_precos_colunas_obrigatorias(column: str):
    row = _row()
    del row[column]
    with pytest.raises(ParseError, match="Colunas obrigatorias ausentes"):
        parser.parse_precos(_xlsx([row]))


def test_precos_nivel_divergente():
    with pytest.raises(ParseError, match="diverge"):
        parser.parse_precos(_xlsx([_row()]), nivel="brasil")


def test_precos_vazio_preserva_schema():
    weekly = parser.parse_precos(_xlsx([_row()]), municipio="INEXISTENTE")
    monthly = parser.agregar_mensal(weekly)
    pd.testing.assert_frame_equal(weekly, parser.empty_precos())
    pd.testing.assert_frame_equal(monthly, parser.empty_precos())


def test_mensal_nao_reagrega_media_mensal():
    monthly = parser.agregar_mensal(parser.parse_precos(_xlsx([_row()])))
    with pytest.raises(ParseError, match="observacoes semanais"):
        parser.agregar_mensal(monthly)


def test_semanal_preserva_repeticao_e_mensal_rejeita():
    weekly = parser.parse_precos(_xlsx([_row(), _row()]))
    assert len(weekly) == 2
    with pytest.raises(ParseError, match="Semanas duplicadas"):
        parser.agregar_mensal(weekly)
