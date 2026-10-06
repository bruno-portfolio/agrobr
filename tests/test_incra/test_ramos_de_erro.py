from __future__ import annotations

import io
import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.incra import bruto, parser
from agrobr.incra.andamento import parser as andamento
from tests import helpers


@pytest.mark.parametrize(
    "defeito,motivo",
    [
        ("bool", "include_geometry deve ser bool"),
        ("ausente", "Membro geometry ausente"),
        ("crs", "Declaração CRS vazia"),
    ],
    ids=["bool", "ausente", "crs"],
)
def test_pagina_layout_invalido(defeito, motivo):
    features = helpers.incra_features()[:1]
    page = {
        "type": "FeatureCollection",
        "features": features,
        "numberMatched": len(features),
        "numberReturned": len(features),
    }
    if defeito == "ausente":
        del features[0]["geometry"]
    elif defeito == "crs":
        page["crs"] = {"type": "name", "properties": {"name": " "}}
    with helpers.levanta_exatamente(ParseError, motivo):
        parser.parse_page(
            json.dumps(page).encode(), include_geometry=0 if defeito == "bool" else False
        )


@pytest.mark.parametrize(
    "corpo,motivo",
    [
        (b"<FeatureCollection", "contagem WFS ilegível"),
        (
            b'<!DOCTYPE FeatureCollection><FeatureCollection numberMatched="1"/>',
            "contagem WFS com DOCTYPE",
        ),
        (b'<OutraCamada numberMatched="1"/>', "contagem WFS sem numberMatched"),
        (b'<FeatureCollection numberMatched="-1"/>', "numberMatched inválido"),
    ],
    ids=["xml", "doctype", "camada", "negativa"],
)
def test_contagem_bruta_layout_invalido(corpo, motivo):
    with helpers.levanta_exatamente(ParseError, motivo):
        bruto.ler_contagem(corpo)


def _pdf(stream):
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] "
        b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
    ]
    content = b"%PDF-1.4\n"
    offsets = [0]
    for index, value in enumerate(objects, 1):
        offsets.append(len(content))
        content += str(index).encode() + b" 0 obj\n" + value + b"\nendobj\n"
    start = len(content)
    content += b"xref\n0 6\n0000000000 65535 f \n"
    content += b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets[1:])
    return (
        content
        + b"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n"
        + str(start).encode()
        + b"\n%%EOF"
    )


def _table():
    values = [
        ["ANDAMENTO DOS PROCESSOS", *[None] * 14],
        list(constants.INCRA_ANDAMENTO_HEADERS),
        [None, "1", *[None] * 13],
        ["TOTAL", "1 processos com algum tipo de andamento no INCRA", *[None] * 13],
    ]
    boxes = [(float(i * 10), 80.0, float((i + 1) * 10), 90.0) for i in range(15)]
    rows = [
        SimpleNamespace(cells=boxes.copy()),
        SimpleNamespace(cells=boxes.copy()),
        SimpleNamespace(cells=[None, (10.0, 100.0, 20.0, 110.0), *[None] * 13]),
        SimpleNamespace(cells=boxes.copy()),
    ]
    return SimpleNamespace(extract=lambda: values, rows=rows, bbox=(0, 70, 150, 130))


@pytest.mark.parametrize(
    "defeito,motivo",
    [
        ("titulo", "Título administrativo não reconhecido"),
        ("caixas", "Cabeçalho administrativo sem 15 caixas"),
        ("bordas", "Colunas administrativas sem ordem geométrica"),
        ("largura", "Largura da tabela administrativa divergente"),
        ("coluna", "Caixa ordinal fora da coluna declarada"),
        ("altura", "Caixa ordinal sem altura positiva"),
        ("total", "Total declarado administrativo inválido"),
        ("repetido", "Cabeçalho repetido divergente"),
        ("linha", "Linha administrativa sem ordinal ou total reconhecido"),
        ("ordinal", "Ordinal textual diverge da grade"),
        ("regional", "Rótulo regional não reconhecido"),
        ("edicao", "Edição e proveniência interna do PDF não reconhecidas"),
        ("apos", "Página com registros após encerramento declarado"),
        ("sem_tabela", "Página de dados sem tabela reconhecida"),
        ("sem_ordinais", "Página de dados sem ordinais"),
        ("continuacao", "Continuação regional com divisória de abertura"),
        ("sem_fechamento", "Grupo regional final sem fechamento"),
    ],
    ids=[
        "titulo",
        "caixas",
        "bordas",
        "largura",
        "coluna",
        "altura",
        "total",
        "repetido",
        "linha",
        "ordinal",
        "regional",
        "edicao",
        "apos",
        "sem_tabela",
        "sem_ordinais",
        "continuacao",
        "sem_fechamento",
    ],
)
def test_publicacao_pdf_layout_invalido(monkeypatch, defeito, motivo):
    pdfplumber = pytest.importorskip("pdfplumber")
    table = _table()
    values = table.extract()
    edition = "01/10/2026\nFonte: INCRA\nAutorizada a reprodução"
    stream = b"BT /F1 5 Tf 12 93 Td (1) Tj ET\nBT /F1 1 Tf 1 95 Td (SR\\(01\\)XX) Tj ET\n"
    if defeito == "titulo":
        values[0][0] = "OUTRO RELATORIO"
    elif defeito == "caixas":
        table.rows[1].cells[0] = None
    elif defeito == "bordas":
        table.rows[1].cells[1] = table.rows[1].cells[0]
    elif defeito == "largura":
        values[2].pop()
    elif defeito == "coluna":
        table.rows[2].cells[1] = (30.0, 100.0, 40.0, 110.0)
    elif defeito == "altura":
        table.rows[2].cells[1] = (10.0, 110.0, 20.0, 100.0)
    elif defeito == "total":
        values[3][1] = "total desconhecido"
    elif defeito == "repetido":
        values[2] = ["SR", *[None] * 14]
    elif defeito == "linha":
        values[2][1] = "ordinal ilegível"
    elif defeito == "ordinal":
        stream = stream.replace(b"(1)", b"(2)")
    elif defeito == "regional":
        stream = stream.replace(b"SR\\(01\\)XX", b"SR\\(X\\)")
    elif defeito == "edicao":
        edition = "data ausente\nFonte: INCRA\nAutorizada a reprodução"
    elif defeito == "sem_ordinais":
        del values[2:]
        del table.rows[2:]
    if defeito == "continuacao":
        values.pop()
        table.rows.pop()
    elif defeito != "sem_fechamento":
        stream += b"0 G 0 90 m 10 90 l S\n"
    content = _pdf(stream)
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        page = pdf.pages[0]
        monkeypatch.setattr(page, "find_tables", lambda: [] if defeito == "sem_tabela" else [table])
        monkeypatch.setattr(page, "extract_text", lambda: edition)
        monkeypatch.setattr(page, "close", lambda: None)
        pages = [page]
        if defeito in {"apos", "continuacao"}:
            if defeito == "continuacao":
                second_content = _pdf(stream + b"0 G 0 100 m 10 100 l S\n")
                second = pdfplumber.open(io.BytesIO(second_content))
                second_page = second.pages[0]
                monkeypatch.setattr(second_page, "find_tables", lambda: [table])
                monkeypatch.setattr(second_page, "extract_text", lambda: edition)
                pages.append(second_page)
            else:
                pages.append(page)
        monkeypatch.setattr(
            pdfplumber, "open", lambda *_a, **_k: nullcontext(SimpleNamespace(pages=pages))
        )
        with helpers.levanta_exatamente(ParseError, motivo):
            andamento.parse_publication(content)
        if defeito == "continuacao":
            second.close()


def test_publicacao_pdf_dependencia_ausente(monkeypatch):
    original = andamento.importlib.import_module

    def import_module(name, *args, **kwargs):
        if name == "pdfplumber":
            raise ImportError("pdfplumber indisponível")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(andamento.importlib, "import_module", import_module)
    with helpers.levanta_exatamente(ImportError, "pdfplumber é necessário"):
        andamento.parse_publication(b"%PDF-1.4")
