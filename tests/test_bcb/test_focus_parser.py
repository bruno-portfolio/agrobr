from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import pytest

from agrobr.bcb import focus_parser
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures
from tests.test_bcb.focus_replay import encode

RAIZ = Path(__file__).parents[1] / "golden_data/bcb/focus_selecao_20260907"
MANIFESTO = json.loads((RAIZ / "manifest.json").read_text(encoding="utf-8"))
CORPOS = {item["case"]: (RAIZ / item["file"]).read_bytes() for item in MANIFESTO["artifacts"]}
ANUAL = json.loads(CORPOS["annual_api_ge"])["value"][0]
MENSAL = json.loads(CORPOS["monthly_api_ge"])["value"][0]
ENVELOPE = "Envelope Focus inválido: exige objeto com value lista e anotações tipadas"
OBSERVACAO = "Observação Focus inválida na linha 1"
BRUTO = encode([dict(ANUAL, Media="RAW")])
RECUSAS = [
    *(
        (corpo, "anual", ENVELOPE)
        for corpo in [
            b"{}",
            b"[]",
            b"null",
            b"bad",
            b'{"value":null}',
            b'{"value":{}}',
            b'{"value":[null]}',
            b'{"value":[1]}',
        ]
    ),
    (b'{"value":[],"value":[]}', "anual", "Objeto JSON Focus contém campo repetido"),
    *(
        (encode([], **{anotacao: valor}), "anual", ENVELOPE)
        for anotacao, valor in [
            ("@odata.count", "1"),
            ("@odata.count", -1),
            ("@odata.count", True),
            ("@odata.count", None),
            ("@odata.nextLink", ""),
            ("@odata.nextLink", " "),
            ("@odata.nextLink", 1),
            ("@odata.nextLink", None),
        ]
    ),
    *(
        (
            encode([{chave: valor for chave, valor in ANUAL.items() if chave != campo}]),
            "anual",
            OBSERVACAO,
        )
        for campo in ANUAL
    ),
    *(
        (encode([dict(ANUAL, **{campo: valor})]), "anual", OBSERVACAO)
        for campo, valor in [
            ("Media", "1.2"),
            ("Media", True),
            ("numeroRespondentes", 1.0),
            ("numeroRespondentes", True),
            ("numeroRespondentes", -1),
            ("baseCalculo", "0"),
            ("baseCalculo", 2**31),
            ("Indicador", " "),
            ("Indicador", 1),
            ("Data", "2026-02-30"),
            ("Data", "2026-8-28"),
            ("Data", "2026-08-28T00:00:00Z"),
            ("DataReferencia", "08/2028"),
            ("IndicadorDetalhe", 1),
        ]
    ),
    (
        encode([dict(ANUAL, Media=float("inf"))]),
        "anual",
        "Constante JSON Focus não finita: Infinity",
    ),
    (
        encode([dict(ANUAL, Mediana=float("nan"))]),
        "anual",
        "Constante JSON Focus não finita: NaN",
    ),
    (
        BRUTO.replace(b'"RAW"', b"1e400"),
        "anual",
        "Número JSON Focus fora do domínio finito float64",
    ),
    *(
        (
            BRUTO.replace(b'"RAW"', numero),
            "anual",
            "Número JSON Focus perde magnitude na conversão para float64",
        )
        for numero in [b"1e-400", b"1e-999999999999999999999999999999"]
    ),
    *(
        (encode([ANUAL, segunda]), "anual", "Chave Focus duplicada na página, linha 2")
        for segunda in [dict(ANUAL), dict(ANUAL, Media=999.0)]
    ),
    ((RAIZ.parent / "focus_sample.json").read_bytes(), "anual", OBSERVACAO),
    *(
        (encode([dict(MENSAL, DataReferencia=referencia)]), "mensal", OBSERVACAO)
        for referencia in ["2028", "13/2028", "00/2028", "1/2028", "01/0000", " 01/2028"]
    ),
    *(
        (encode([dict(MENSAL, IndicadorDetalhe=detalhe)]), "mensal", OBSERVACAO)
        for detalhe in ["Exportações", ""]
    ),
    (encode([ANUAL]), "semanal", "Periodicidade Focus não suportada"),
]
COLUNAS = {
    "Indicador": "indicador",
    "DataReferencia": "data_referencia",
    "Media": "media",
    "Mediana": "mediana",
    "DesvioPadrao": "desvio_padrao",
    "Minimo": "minimo",
    "Maximo": "maximo",
    "numeroRespondentes": "numero_respondentes",
    "baseCalculo": "base_calculo",
}


def test_pagina_invalida_vira_parse_error_com_o_motivo():
    with collect_failures() as check:
        for corpo, periodicidade, motivo in RECUSAS:
            with (
                check((periodicidade, corpo)),
                pytest.raises(ParseError, match=re.escape(motivo)),
            ):
                focus_parser.parse_page(corpo, periodicidade)
        for data in ["0001-01-01", "9999-12-31"]:
            pagina = focus_parser.parse_page(encode([dict(ANUAL, Data=data)]), "anual")
            with (
                check(data),
                pytest.raises(
                    ParseError, match=re.escape("Data Focus fora do domínio datetime64[ns]")
                ),
            ):
                focus_parser.build_frame(pagina.records)


@pytest.mark.parametrize("periodicidade,prefix", [("anual", "annual"), ("mensal", "monthly")])
def test_official_six_rows_equal_two_pages_all_fields_and_order(periodicidade, prefix):
    one = focus_parser.parse_page(CORPOS[f"{prefix}_six"], periodicidade)
    first = focus_parser.parse_page(CORPOS[f"{prefix}_page1"], periodicidade)
    second = focus_parser.parse_page(CORPOS[f"{prefix}_page2"], periodicidade)
    assert len(one.records) == 6
    assert one.records == first.records + second.records
    assert one.warnings == first.warnings == second.warnings == []
    raw = json.loads(CORPOS[f"{prefix}_six"])["value"]
    frame = focus_parser.build_frame(one.records)
    for external, column in COLUNAS.items():
        assert frame[column].tolist() == [row[external] for row in raw]
    assert frame["data"].dt.strftime("%Y-%m-%d").tolist() == [row["Data"] for row in raw]
    assert frame["periodicidade"].eq(periodicidade).all()
    if periodicidade == "anual":
        assert frame["indicador_detalhe"].tolist() == [row["IndicadorDetalhe"] for row in raw]
        assert set(frame["indicador_detalhe"]) == {"Exportações", "Importações", "Saldo"}
    else:
        assert frame["indicador_detalhe"].isna().all()


def test_valores_publicados_sao_preservados_com_diagnostico_exato():
    for campo in ["Media", "Mediana", "DesvioPadrao", "Minimo", "Maximo"] + [
        "numeroRespondentes",
        "baseCalculo",
    ]:
        page = focus_parser.parse_page(encode([dict(ANUAL, **{campo: None})]), "anual")
        frame = focus_parser.build_frame(page.records)
        assert frame.isna().sum().sum() == 1 and pd.isna(frame.iloc[0][COLUNAS[campo]])
        assert (page.source_rows, page.warnings) == (1, [])
    inconsistente = dict(ANUAL, Media=-8.0, Mediana=9.0, DesvioPadrao=-1.0, Minimo=3.0, Maximo=2.0)
    page = focus_parser.parse_page(encode([inconsistente]), "anual")
    frame = focus_parser.build_frame(page.records)
    assert frame[["media", "mediana", "desvio_padrao", "minimo", "maximo"]].iloc[0].tolist() == [
        -8.0,
        9.0,
        -1.0,
        3.0,
        2.0,
    ]
    assert page.warnings == [
        "Focus linha 1: desvio_padrao negativo; minimo maior que maximo; media fora dos extremos "
        "disponíveis; mediana fora dos extremos disponíveis; valores preservados."
    ]
    base = focus_parser.parse_page(encode([dict(ANUAL, baseCalculo=2)]), "anual")
    assert base.records[0].base_calculo == 2
    assert base.warnings == [
        "Focus linha 1: base_calculo não documentada neste conjunto de capturas; valores preservados."
    ]
    detalhes = focus_parser.parse_page(
        encode([dict(ANUAL, IndicadorDetalhe=""), dict(ANUAL, IndicadorDetalhe=None)]), "anual"
    )
    frame = focus_parser.build_frame(detalhes.records)
    assert frame.iloc[0]["indicador_detalhe"] == "" and pd.isna(frame.iloc[1]["indicador_detalhe"])
    mensal = focus_parser.parse_page(encode([dict(MENSAL, IndicadorDetalhe=None)]), "mensal")
    assert mensal.records[0].indicador_detalhe is None
    assert mensal.warnings == [
        "Focus mensal incluiu IndicadorDetalhe null; variante de layout preservada."
    ]
    plain = focus_parser.parse_page(encode([ANUAL]), "anual")
    changed = focus_parser.parse_page(encode([dict(ANUAL, Media=ANUAL["Media"] + 1)]), "anual")
    extra = focus_parser.parse_page(encode([dict(ANUAL, extra="synthetic")], extraenv=1), "anual")
    assert plain.layout_fingerprint == changed.layout_fingerprint != extra.layout_fingerprint
    assert extra.records == plain.records
    assert extra.warnings == [
        "Campos adicionais no envelope Focus: ['extraenv']",
        "Campos adicionais nas observações Focus: ['extra']",
    ]
    vazio = focus_parser.build_frame(focus_parser.parse_page(b'{"value":[]}', "mensal").records)
    assert vazio.empty and len(vazio.columns) == 12
    assert str(vazio["data"].dtype) == "datetime64[ns]"
    assert [
        str(vazio[col].dtype) for col in ["media", "mediana", "desvio_padrao", "minimo", "maximo"]
    ] == ["float64"] * 5
    assert str(vazio["numero_respondentes"].dtype) == str(vazio["base_calculo"].dtype) == "Int64"
