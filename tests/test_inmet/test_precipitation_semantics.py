from __future__ import annotations

import pandas as pd
import pytest

from agrobr.inmet import parser


@pytest.mark.parametrize(
    "values,expected,parciais",
    [([None] * 31, None, 0), ([0.0] * 31, 0.0, 0), ([None] + [4.0] * 30, None, 1)],
)
def test_monthly_missing_zero_partial(values, expected, parciais):
    frame = pd.DataFrame(
        {
            "data": pd.date_range("2023-01-01", periods=31, freq="D"),
            "estacao": "A001",
            "uf": "DF",
            "precipitacao_mm": pd.Series(values, dtype=float),
        }
    )
    linha = parser.agregar_mensal_uf(frame).iloc[0]
    assert pd.isna(linha.precip_acum_mm) if expected is None else linha.precip_acum_mm == expected
    assert linha.estacoes_chuva_parciais == parciais


def test_cobertura_conta_o_dia_so_com_temperatura():
    frame = pd.DataFrame(
        {
            "data": pd.to_datetime(["2023-01-01", "2023-01-02"]),
            "estacao": "A001",
            "uf": "DF",
            "precipitacao_mm": [1.0, None],
            "temp_media": [None, 25.0],
        }
    )
    linha = parser.agregar_mensal_uf(frame).iloc[0]
    assert (linha.dias, linha.data_inicio, linha.data_fim) == (
        2,
        pd.Timestamp("2023-01-01"),
        pd.Timestamp("2023-01-02"),
    )
