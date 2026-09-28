from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import pytest

from agrobr.bcb import ptax_parser
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures, sem_excecao
from tests.test_bcb.ptax_replay import encode

RAIZ = Path(__file__).parents[1] / "golden_data/bcb/ptax_selecao_20260907"
MANIFESTO = json.loads((RAIZ / "manifest.json").read_text(encoding="utf-8-sig"))
CORPOS = {item["case"]: (RAIZ / item["file"]).read_bytes() for item in MANIFESTO["artifacts"]}
COTACAO = json.loads(CORPOS["usd_day"])["value"][0]
MOEDA = json.loads(CORPOS["currencies"])["value"][0]
ENVELOPE = "Envelope PTAX inválido: exige objeto com value lista e anotações tipadas"
COTACAO_INVALIDA = "Cotação PTAX inválida na linha 1"
BRUTO = encode([dict(COTACAO, cotacaoCompra="RAW")])
RECUSAS_ENVELOPE = [
    *(
        (corpo, ENVELOPE)
        for corpo in [b"{}", b"[]", b"null", b"HTML", b'{"value":null}', b'{"value":[null]}']
    ),
    (b'{"value":[],"value":[]}', "Objeto JSON PTAX contém campo repetido"),
    *(
        (encode([], **{anotacao: valor}), ENVELOPE)
        for anotacao, valor in [
            ("@odata.count", None),
            ("@odata.count", True),
            ("@odata.count", -1),
            ("@odata.count", 1.0),
            ("@odata.count", "1"),
            ("@odata.nextLink", None),
            ("@odata.nextLink", ""),
            ("@odata.nextLink", " "),
            ("@odata.nextLink", 1),
        ]
    ),
]
RECUSAS_COTACAO = [
    *(
        (encode([dict(COTACAO, dataHoraCotacao=valor)]), COTACAO_INVALIDA)
        for valor in [
            "2026-09-04T13:03:59",
            "2026-09-04 13:03:59Z",
            "2026-09-04 13:03:59+00:00",
            "2026-09-04 13:03:59.1234567890",
            "2026-02-30 13:03:59",
            "2026-09-04 24:00:00",
            "NaT",
            "0001-01-01 00:00:00",
            "9999-12-31 23:59:59",
            "1677-09-21 00:12:43.145224193",
            None,
            123,
        ]
    ),
    *(
        (
            encode([{chave: valor for chave, valor in COTACAO.items() if chave != campo}]),
            COTACAO_INVALIDA,
        )
        for campo in COTACAO
    ),
    *(
        (encode([dict(COTACAO, **{campo: valor})]), COTACAO_INVALIDA)
        for campo, valor in [("cotacaoCompra", "1.2"), ("cotacaoVenda", True), ("tipoBoletim", 1)]
    ),
    (
        encode([dict(COTACAO, paridadeCompra=float("inf"))]),
        "Constante JSON PTAX não finita: Infinity",
    ),
    (encode([dict(COTACAO, paridadeVenda=float("nan"))]), "Constante JSON PTAX não finita: NaN"),
    (BRUTO.replace(b'"RAW"', b"1e400"), "Número JSON PTAX fora do domínio finito float64"),
    *(
        (
            BRUTO.replace(b'"RAW"', numero),
            "Número JSON PTAX perde magnitude na conversão para float64",
        )
        for numero in [b"1e-400", b"1e-999999999999999999999999999"]
    ),
    *(
        (encode([COTACAO, segunda]), "Chave PTAX duplicada na página, linha 2")
        for segunda in [dict(COTACAO), dict(COTACAO, cotacaoCompra=999.0)]
    ),
    ((RAIZ.parent / "ptax_sample.json").read_bytes(), COTACAO_INVALIDA),
]
RECUSAS_MOEDA = [
    *(
        (encode([dict(MOEDA, **{campo: valor})]), "Moeda PTAX inválida na linha 1")
        for campo, valor in [
            ("simbolo", "usd"),
            ("simbolo", "US"),
            ("simbolo", "US1"),
            ("simbolo", "ÉUR"),
            ("nomeFormatado", " "),
            ("tipoMoeda", ""),
            ("nomeFormatado", None),
        ]
    ),
    *(
        (encode([MOEDA, segunda]), "Moeda PTAX duplicada na página, linha 2")
        for segunda in [dict(MOEDA), dict(MOEDA, nomeFormatado="different")]
    ),
]
COLUNAS = {
    "cotacaoCompra": "cotacao_compra",
    "cotacaoVenda": "cotacao_venda",
    "paridadeCompra": "paridade_compra",
    "paridadeVenda": "paridade_venda",
    "tipoBoletim": "tipo_boletim",
}


def test_pagina_invalida_vira_parse_error_com_o_motivo():
    with collect_failures() as check:
        for corpo, motivo in RECUSAS_COTACAO + RECUSAS_ENVELOPE:
            with check(("cotacoes", corpo)), pytest.raises(ParseError, match=re.escape(motivo)):
                ptax_parser.parse_quotes_page(corpo, "USD")
        with check("moeda da seleção"), pytest.raises(ParseError, match=COTACAO_INVALIDA):
            ptax_parser.parse_quotes_page(encode([COTACAO]), "usd")
        for corpo, motivo in RECUSAS_MOEDA + RECUSAS_ENVELOPE:
            with check(("moedas", corpo)), pytest.raises(ParseError, match=re.escape(motivo)):
                ptax_parser.parse_currencies_page(corpo)


@pytest.mark.parametrize(
    "name,moeda", [("usd_day", "USD"), ("jpy_day", "JPY"), ("eur_period", "EUR")]
)
def test_original_observations_preserve_all_six_published_fields(name, moeda):
    raw = json.loads(CORPOS[name])["value"]
    page = ptax_parser.parse_quotes_page(CORPOS[name], moeda)
    frame = ptax_parser.build_quotes_frame(page.records)
    for external, column in COLUNAS.items():
        assert frame[column].tolist() == [row[external] for row in raw]
    assert frame["data_hora"].tolist() == [pd.Timestamp(row["dataHoraCotacao"]) for row in raw]
    assert frame["data"].equals(frame["data_hora"].dt.normalize())
    assert frame["moeda"].eq(moeda).all()
    assert (page.source_rows, page.parser_version, page.warnings) == (len(raw), 2, [])


def test_original_microseconds_and_synthetic_nanoseconds_do_not_collapse():
    raw = json.loads(CORPOS["usd_day"])["value"]
    assert {row["dataHoraCotacao"] for row in raw} >= {
        "2026-09-04 13:03:59.452044",
        "2026-09-04 13:03:59.556874",
    }
    rows = [
        dict(COTACAO, dataHoraCotacao="2026-09-04 13:03:59.452044001"),
        dict(COTACAO, dataHoraCotacao="2026-09-04 13:03:59.452044002"),
    ]
    with sem_excecao():
        frame = ptax_parser.build_quotes_frame(
            ptax_parser.parse_quotes_page(encode(rows), "USD").records
        )
    assert str(frame["data_hora"].dtype) == "datetime64[ns]"
    assert frame["data_hora"].iloc[1].value - frame["data_hora"].iloc[0].value == 1


def test_valores_publicados_sao_preservados_com_diagnostico_exato():
    for campo in COLUNAS:
        page = ptax_parser.parse_quotes_page(encode([dict(COTACAO, **{campo: None})]), "USD")
        frame = ptax_parser.build_quotes_frame(page.records)
        assert frame.isna().sum().sum() == 1 and pd.isna(frame.iloc[0][COLUNAS[campo]])
        assert page.warnings == (
            ["PTAX linha 1: tipo_boletim ausente ou não reconhecido; valores preservados."]
            if campo == "tipoBoletim"
            else []
        )
    invertida = dict(
        COTACAO, cotacaoCompra=0.0, cotacaoVenda=-1.0, paridadeCompra=5.0, paridadeVenda=4.0
    )
    page = ptax_parser.parse_quotes_page(encode([invertida]), "USD")
    frame = ptax_parser.build_quotes_frame(page.records)
    assert frame[["cotacao_compra", "cotacao_venda", "paridade_compra", "paridade_venda"]].iloc[
        0
    ].tolist() == [0.0, -1.0, 5.0, 4.0]
    assert page.warnings == [
        "PTAX linha 1: cotacao não positiva; cotacao_compra maior que venda; "
        "paridade_compra maior que venda; valores preservados."
    ]
    extras = ptax_parser.parse_quotes_page(encode([dict(COTACAO, extra=1)], extraenv=2), "USD")
    assert extras.warnings == [
        "Campos adicionais no envelope PTAX: ['extraenv']",
        "Campos adicionais nos registros PTAX: ['extra']",
    ]
    nova = ptax_parser.parse_currencies_page(encode([dict(MOEDA, tipoMoeda="C")]))
    assert nova.records[0].tipo_moeda == "C"
    assert nova.warnings == ["PTAX moeda linha 1: tipo_moeda novo; texto preservado."]
    quotes = ptax_parser.build_quotes_frame(
        ptax_parser.parse_quotes_page(encode([]), "USD").records
    )
    catalog = ptax_parser.build_currencies_frame(
        ptax_parser.parse_currencies_page(encode([])).records
    )
    assert quotes.empty and len(quotes.columns) == 8
    assert catalog.empty and len(catalog.columns) == 3
    assert str(quotes["data"].dtype) == str(quotes["data_hora"].dtype) == "datetime64[ns]"
    assert str(quotes["cotacao_compra"].dtype) == str(quotes["paridade_compra"].dtype) == "float64"
