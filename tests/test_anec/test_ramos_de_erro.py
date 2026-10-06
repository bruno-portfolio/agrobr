from __future__ import annotations

import sys
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from agrobr.anec import client, parser
from agrobr.exceptions import ParseError
from tests import helpers
from tests.test_anec.test_parser import _weekly_header, _word


def _pdf(monkeypatch, paginas):
    pdfplumber = pytest.importorskip("pdfplumber")
    documento = SimpleNamespace(
        pages=[
            SimpleNamespace(extract_words=lambda words=words, **_kwargs: words) for words in paginas
        ]
    )
    monkeypatch.setattr(pdfplumber, "open", lambda _: nullcontext(documento))


def _semanal():
    return [
        _word("Weekly shipments", 200, -80),
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
        *_weekly_header(),
        _word("SANTOS", 30, 20),
        _word("100", 100, 20),
    ]


def _mensal():
    return [
        _word("Monthly shipments 2026", 200, 0),
        _word("Soybean", 200, 10),
        _word("Soybean", 245, 10),
        _word("Meal", 255, 10),
        _word("Maize", 300, 10),
        _word("Wheat", 400, 10),
        _word("DDGS", 450, 10),
        _word("Sorghum", 475, 10),
        _word("Total", 490, 10),
        _word("Products", 510, 10),
    ]


def test_pdf_dependencia_ausente_orienta_instalacao(monkeypatch):
    monkeypatch.setitem(sys.modules, "pdfplumber", None)
    with helpers.levanta_exatamente(ImportError, match="Instale com: pip install agrobr"):
        parser.parse_anec_pdf(b"")


def test_pdf_sem_paginas_recusado(monkeypatch):
    _pdf(monkeypatch, [])
    with helpers.levanta_exatamente(ParseError, match="PDF sem páginas extraíveis"):
        parser.parse_anec_pdf(b"pdf externo")


def test_pdf_sem_cabecalho_semanal_recusado(monkeypatch):
    _pdf(monkeypatch, [_mensal()])
    with helpers.levanta_exatamente(ParseError, match="Header 'Weekly shipments' não encontrado"):
        parser.parse_anec_pdf(b"pdf externo")


def test_pdf_sem_cabecalho_mensal_recusado(monkeypatch):
    _pdf(monkeypatch, [_semanal()])
    with helpers.levanta_exatamente(ParseError, match="Header 'Monthly shipments' não encontrado"):
        parser.parse_anec_pdf(b"pdf externo")


def test_mensal_sem_ano_da_secao_recusado(monkeypatch):
    _pdf(monkeypatch, [_semanal(), [_word("Monthly shipments", 200, 0)]])
    with helpers.levanta_exatamente(ParseError, match="Nenhuma seção 'Monthly shipments YYYY'"):
        parser.parse_anec_pdf(b"pdf externo")


def test_mensal_sem_produtos_recusado(monkeypatch):
    mensal = [_word("Monthly shipments 2026", 200, 0), _word("January", 50, 20)]
    _pdf(monkeypatch, [_semanal(), mensal])
    with helpers.levanta_exatamente(ParseError, match="Cabeçalhos mensais ausentes para 2026"):
        parser.parse_anec_pdf(b"pdf externo")


def test_mensal_sem_meses_avisa(monkeypatch):
    _pdf(monkeypatch, [_semanal(), _mensal()])
    with helpers.capturar_logs() as logs:
        report = parser.parse_anec_pdf(b"pdf externo")
    assert report.monthly_shipments.empty
    assert any(item["event"] == "anec_monthly_empty" for item in logs)


def test_mensal_faixa_invertida_recusada(monkeypatch):
    mensal = [*_mensal(), _word("January", 50, 20), _word("200-100", 200, 20)]
    _pdf(monkeypatch, [_semanal(), mensal])
    with helpers.levanta_exatamente(ParseError, match="Faixa mensal inválida: 200-100"):
        parser.parse_anec_pdf(b"pdf externo")


def test_artigo_invalido_avisa(category_2026_p1_payload):
    artigos = category_2026_p1_payload["props"]["pageProps"]["paginatedArticles"]["articles"]
    artigos[0]["id"] = "não inteiro"
    with helpers.capturar_logs() as logs:
        client._parse_articles(category_2026_p1_payload)
    assert any(item["event"] == "anec_article_invalid" for item in logs)


@pytest.mark.parametrize(
    ("payload", "mensagem"),
    [
        ({}, "Catálogo de categorias ANEC inválido"),
        (
            {"props": {"pageProps": {"homeLayoutData": {"navbarCategories": []}}}},
            "Categorias anuais ausentes",
        ),
    ],
)
def test_catalogo_sem_categorias_recusado(payload, mensagem):
    with helpers.levanta_exatamente(ParseError, match=mensagem):
        client._parse_categories(payload)
