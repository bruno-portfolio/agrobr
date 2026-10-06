from __future__ import annotations

import contextlib
import io
import sys
import types

import openpyxl
import pytest

from agrobr import unica
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.unica import parser
from tests import helpers


def _instalar_paginas(monkeypatch, textos):
    paginas = [types.SimpleNamespace(extract_text=lambda texto=texto: texto) for texto in textos]
    documento = types.SimpleNamespace(pages=paginas)
    modulo = types.SimpleNamespace(open=lambda _: contextlib.nullcontext(documento))
    monkeypatch.setitem(sys.modules, "pdfplumber", modulo)


def _paginas(*, resumo=True, series=True):
    linhas = [
        "Cana-de-açúcar 10 11 10% 10 11 10% 10 11 10%",
        "Açúcar 1 2 100% 1 2 100% 1 2 100%",
        "Etanol total 1 2 100% 1 2 100% 1 2 100%",
        "Açúcar 50% 50% 50% 50% 50% 50%",
        "Etanol 50% 50% 50% 50% 50% 50%",
    ]
    tabela_resumo = "Tabela 1. posição MENSAL referente a junho de 2026\n"
    if resumo:
        tabela_resumo += "\n".join(linhas) + "\n"
    tabela_resumo += "Tabela 2. posição MENSAL referente a junho de 2026"
    tabelas_series = [
        f"Tabela {numero}. Histórico\n"
        + ("16/04 1 2 100% 1 2 100% 1 2 100%" if series else "16/04")
        for numero in range(3, 8)
    ]
    return ["Safra 2026/2027\nPosição até 01/07/2026", tabela_resumo, *tabelas_series]


def _planilha(linhas):
    livro = openpyxl.Workbook()
    for linha in linhas:
        livro.active.append(linha)
    destino = io.BytesIO()
    livro.save(destino)
    livro.close()
    return destino.getvalue()


@pytest.mark.parametrize("funcao", [unica.moagem_quinzenal, unica.producao_historica])
async def test_produto_nao_textual_recusado_antes_da_rede(funcao):
    with helpers.levanta_exatamente(InvalidParameterError, match="produto deve ser uma string"):
        await funcao(produto=7)


def test_pdf_sem_extra_informa_como_instalar(monkeypatch):
    monkeypatch.setitem(sys.modules, "pdfplumber", None)

    with helpers.levanta_exatamente(ImportError, match=r"pip install agrobr\[pdf\]"):
        parser.parse_quinzenal_pdf(b"%PDF-1.4")


def test_pdf_sem_paginas_recusa_relatorio(monkeypatch):
    _instalar_paginas(monkeypatch, [])

    with helpers.levanta_exatamente(ParseError, match="PDF sem páginas legíveis"):
        parser.parse_quinzenal_pdf(b"%PDF-1.4")


def test_pdf_sem_linhas_de_resumo_recusa_relatorio(monkeypatch):
    _instalar_paginas(monkeypatch, _paginas(resumo=False))

    with helpers.levanta_exatamente(ParseError, match="Nenhuma linha reconhecida nas Tabelas 1-2"):
        parser.parse_quinzenal_pdf(b"%PDF-1.4")


def test_pdf_sem_valores_quinzenais_recusa_relatorio(monkeypatch):
    _instalar_paginas(monkeypatch, _paginas(series=False))

    with helpers.levanta_exatamente(ParseError, match="Nenhuma quinzena com dados nas Tabelas 3-7"):
        parser.parse_quinzenal_pdf(b"%PDF-1.4")


def test_historico_sem_linhas_de_dados_recusa_planilha():
    conteudo = _planilha([["Unidade: Mil toneladas"], ["Estado/Safra", "2020/2021"]])

    with helpers.levanta_exatamente(ParseError, match="Nenhuma linha de dados no XLSX histórico"):
        parser.parse_historico_xlsx(conteudo, "cana")


def test_historico_sem_colunas_de_safra_recusa_planilha():
    conteudo = _planilha(
        [["Unidade: Mil toneladas"], ["Estado/Safra", "Observação"], ["São Paulo", "Disponível"]]
    )

    with helpers.levanta_exatamente(ParseError, match="Header do XLSX sem colunas de safra"):
        parser.parse_historico_xlsx(conteudo, "cana")
