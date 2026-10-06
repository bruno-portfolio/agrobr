from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from agrobr.exceptions import ParseError
from agrobr.rio_verde import parser
from tests import helpers


def test_pdf_dependencia_ausente_orienta_instalacao(monkeypatch):
    monkeypatch.setitem(sys.modules, "pdfplumber", None)
    with helpers.levanta_exatamente(ImportError, match="Instale com: pip install agrobr"):
        parser.parse_ensaio_soja(b"", "2023/24")


def test_resumo_produtividade_media_ausente_recusada(monkeypatch):
    pdfplumber = pytest.importorskip("pdfplumber")
    tabela = [
        ["Empresa Cultivar G.M. Ciclo Produtividade"],
        [],
        [],
        ["Empresa", "Cultivar", "7.0", "110", "50", "50", "50", "50", "-"],
    ]
    documento = SimpleNamespace(
        pages=[
            SimpleNamespace(
                extract_text=lambda: "Empresa Cultivar Ciclo Media",
                extract_tables=lambda: [tabela],
            )
        ],
        close=lambda: None,
    )
    monkeypatch.setattr(pdfplumber, "open", lambda _: documento)
    with helpers.levanta_exatamente(ParseError, match="produtividade média ausente"):
        parser.parse_ensaio_soja(b"pdf externo", "2023/24")
