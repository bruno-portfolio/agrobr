from __future__ import annotations

from typing import Any

import pytest

from agrobr.anec import parser
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures, sem_excecao


def _word(text: str, center: float, top: float) -> dict[str, Any]:
    return {"text": text, "x0": center - 5, "x1": center + 5, "top": top, "bottom": top + 10}


def _weekly_header() -> list[dict[str, Any]]:
    words = [_word("PORT", 30, 0)]
    for offset in (0, 400):
        words += [
            _word("Soybean", 100 + offset, 0),
            _word("Soybean", 150 + offset, 0),
            _word("meal", 165 + offset, 0),
            _word("Maize", 200 + offset, 0),
            _word("Wheat", 250 + offset, 0),
            _word("DDGS", 300 + offset, 0),
            _word("Sorghum", 350 + offset, 0),
        ]
    return words


def test_concat_fragmented_numbers():
    with collect_failures() as check:
        for caso, textos, bordas, esperado in [
            ("fragmento com ponto", ("3", ".250.783"), (100, 105, 105.5, 130), ["3.250.783"]),
            ("1º dígito colado", ("5", "17.423"), (461.6, 464.2, 464.2, 478.4), ["517.423"]),
            ("1º dígito sobreposto", ("2", "1.921"), (157.2, 159.8, 159.7, 171.4), ["21.921"]),
            ("colunas vizinhas", ("5", "17.423"), (430.0, 435.5, 464.2, 478.4), ["5", "17.423"]),
            ("junção sem milhar", ("1.234", "567"), (100, 110, 110.5, 120), ["1.234", "567"]),
        ]:
            with check(caso):
                row = [
                    {"text": textos[0], "x0": bordas[0], "x1": bordas[1], "top": 0, "bottom": 10},
                    {"text": textos[1], "x0": bordas[2], "x1": bordas[3], "top": 0, "bottom": 10},
                ]
                assert [w["text"] for w in parser._concat_fragmented_numbers(row)] == esperado


def test_valor_da_coluna_usa_a_palavra_mais_proxima_dentro_do_intervalo():
    row = [_word("222", 91, 0), _word("111", 115, 0)]
    assert parser._value_for_column(row, 100.0, (80.0, 125.0)) == 222.0
    assert parser._value_for_column(row, 100.0, (95.0, 125.0)) == 111.0


def test_semanal_sem_porto_reconhecido_recusado():
    words = [*_weekly_header(), _word("PORTO", 30, 20), _word("X", 50, 20), _word("100", 100, 20)]
    with pytest.raises(ParseError, match="Nenhuma linha de porto"):
        parser._parse_weekly_shipments(words)


def test_linha_sem_porto_e_linha_total_sem_numeros_nao_viram_registro_nem_aviso():
    edicao = [
        _word("Week", 30, -40),
        _word("01/2026", 60, -40),
        _word("(04th", 100, -20),
        _word("to", 120, -20),
        _word("10th", 140, -20),
        _word("Jan)", 160, -20),
        _word("(11th", 500, -20),
        _word("to", 520, -20),
        _word("17th", 540, -20),
        _word("Jan)", 560, -20),
    ]
    linhas = [_word("SANTOS", 30, 20), _word("100", 100, 20), _word("200", 100, 40)]
    words = [*edicao, *_weekly_header(), *linhas, _word("TOTAL", 30, 60)]
    with sem_excecao():
        frame, avisos = parser._parse_weekly_shipments(words)
    preenchidos = frame[frame["valor_ton"].notna()]
    assert preenchidos[["porto", "produto", "periodo", "valor_ton"]].values.tolist() == [
        ["SANTOS", "soybean", "last_week", 100.0]
    ]
    assert avisos == []


def test_semanal_sem_cabecalho_port_recusado():
    with pytest.raises(ParseError, match="Header row"):
        parser._parse_weekly_shipments([])


def test_pdf_ilegivel_vira_parse_error():
    with pytest.raises(ParseError, match="Erro abrindo"):
        parser.parse_anec_pdf(b"not a pdf at all")
