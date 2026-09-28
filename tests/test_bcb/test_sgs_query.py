from __future__ import annotations

import re
from datetime import date, datetime

import pytest

from agrobr.bcb import sgs_query
from agrobr.exceptions import InvalidParameterError
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao

REFERENCE = date(2026, 9, 7)
RECUSAS = [
    *(
        ({"codigo": codigo}, "codigo deve ser inteiro positivo ou alias SGS")
        for codigo in [True, False, 1.0, 0, -1, None, []]
    ),
    *(({"codigo": codigo}, f"Serie '{codigo}' nao encontrada") for codigo in ["1", "", "unknown"]),
    ({"data_inicial": "31/02/2024"}, "data_inicial contém data inválida"),
    ({"data_final": "29/02/2023"}, "data_final contém data inválida"),
    *(
        ({"data_inicial": valor}, "data_inicial deve ter formato DD/MM/AAAA")
        for valor in ["", "2024-01-01", "1/01/2024", date(2024, 1, 1)]
    ),
    ({"data_final": True}, "data_final deve ter formato DD/MM/AAAA"),
    *(
        (argumentos, "Seleção SGS inválida ou intervalo invertido")
        for argumentos in [
            {"data_inicial": "02/01/2024", "data_final": "01/01/2024"},
            {"data_inicial": "01/01/2030"},
            {"codigo": 2**63},
        ]
    ),
    *(
        ({"ultimos": valor}, "ultimos deve ser inteiro positivo")
        for valor in [0, -1, True, 3.0, "3"]
    ),
    ({"reference_date": datetime(2026, 9, 7)}, "reference_date deve ser data civil"),
    ({"reference_date": date(5, 1, 1)}, "Referência não permite o intervalo padrão SGS"),
]


def build(codigo=1, **kwargs):
    return sgs_query.build_query(codigo, reference_date=REFERENCE, **kwargs)


def test_consulta_invalida_e_recusada_com_o_motivo():
    with collect_failures() as check:
        for argumentos, motivo in RECUSAS:
            with (
                check(argumentos),
                levanta_exatamente(InvalidParameterError, match=re.escape(motivo)),
            ):
                sgs_query.build_query(**{"codigo": 1, "reference_date": REFERENCE, **argumentos})


@pytest.mark.parametrize(
    "codigo,expected,name",
    [
        (1, 1, "dolar_ptax_venda"),
        ("ipca", 433, "ipca"),
        ("selic", 432, "selic"),
        (999999999, 999999999, None),
    ],
)
def test_query_preserves_alias_resolution_without_invented_catalog(codigo, expected, name):
    result = build(codigo)
    assert result.codigo == expected
    assert result.nome_serie == name


def test_modo_e_limites_seguem_os_argumentos_sem_janela_inventada():
    with sem_excecao():
        padrao = sgs_query.build_query(1, reference_date=date(2024, 2, 29))
        so_fim = build(data_final="31/12/2024")
        so_inicio = build(data_inicial="01/09/2026")
        ultimos = build(ultimos=21)
        cauda = build(data_inicial="01/01/2010", data_final="31/12/2024", ultimos=100)
    assert (padrao.mode, padrao.inicio, padrao.fim) == (
        "range",
        date(2014, 2, 28),
        date(2024, 2, 29),
    )
    assert (so_fim.mode, so_fim.inicio, so_fim.fim) == ("server_start", None, date(2024, 12, 31))
    assert len(sgs_query.plan_query(so_fim)) == 1
    assert (so_inicio.inicio, so_inicio.fim) == (date(2026, 9, 1), REFERENCE)
    assert (ultimos.mode, ultimos.ultimos, ultimos.inicio, ultimos.fim) == (
        "latest",
        21,
        None,
        None,
    )
    assert (cauda.mode, cauda.ultimos) == ("range", 100)
    assert len(sgs_query.plan_query(cauda)) == 2


@pytest.mark.parametrize(
    "start,end,expected",
    [
        ("01/01/2010", "31/12/2024", [("2010-01-01", "2019-12-31"), ("2020-01-01", "2024-12-31")]),
        ("29/02/2012", "28/02/2022", [("2012-02-29", "2021-12-31"), ("2022-01-01", "2022-02-28")]),
        (
            "15/06/2000",
            "15/06/2021",
            [
                ("2000-06-15", "2009-12-31"),
                ("2010-01-01", "2019-12-31"),
                ("2020-01-01", "2021-06-15"),
            ],
        ),
        ("30/12/9999", "31/12/9999", [("9999-12-30", "9999-12-31")]),
    ],
)
def test_plan_uses_disjoint_calendar_blocks_without_fixed_day_approximation(start, end, expected):
    blocks = sgs_query.plan_query(build(data_inicial=start, data_final=end))
    assert [(block.inicio.isoformat(), block.fim.isoformat()) for block in blocks] == expected
    assert len({block.id for block in blocks}) == len(blocks)
