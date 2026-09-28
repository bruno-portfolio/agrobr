from __future__ import annotations

import csv
import io
import re
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.lista_suja import _parsing, _pdf, models, parser
from tests import helpers


@pytest.fixture
def csv_row():
    return [
        "1",
        "2023",
        "MT",
        "Empregador de teste",
        "001.234.567-89",
        "Fazenda teste",
        "5",
        "0111-3/01",
        "01/06/2024",
        "07/10/2024",
    ]


def _csv_bytes(
    rows: list[list[str]], header: list[str] | None = None, encoding: str = "cp1252"
) -> bytes:
    text = io.StringIO(newline="")
    writer = csv.writer(text, delimiter=";")
    writer.writerow(header if header is not None else models.SOURCE_COLUMNS)
    writer.writerows(rows)
    return text.getvalue().encode(encoding)


def _pdf_from_tables(tables, publication_text):
    pdf = MagicMock()
    pdf.__enter__.return_value = pdf
    pages = []
    for table in tables:
        page = MagicMock()
        page.extract_table.return_value = table
        page.extract_tables.return_value = [table] if table else []
        page.extract_text.return_value = publication_text
        pages.append(page)
    pdf.pages = pages
    return pdf


def _parse_mock_pdf(pdf):
    with patch.object(_pdf, "pdfplumber_module") as check:
        check.return_value.open.return_value = pdf
        return parser.parse_empregadores(b"%PDF-mock")


@pytest.mark.parametrize("encoding", ["cp1252", "utf-8", "utf-8-sig"])
def test_csv_preserves_textual_identity_and_quoted_delimiters(csv_row, encoding):
    csv_row[3] = "Empregador Á; referência"
    csv_row[5] = "Endereço; com separador"
    frame, details = parser.parse_empregadores_bundle(
        _csv_bytes([csv_row], encoding=encoding), formato="csv", companion=None
    )
    assert frame.iloc[0]["cpf_cnpj"] == "001.234.567-89"
    assert frame.iloc[0]["cnae"] == "0111-3/01"
    assert frame.iloc[0]["empregador"] == csv_row[3]
    assert frame.iloc[0]["estabelecimento"] == csv_row[5]
    assert frame.iloc[0]["id_registro"] == "1"
    assert frame.iloc[0]["data_decisao"] == pd.Timestamp("2024-06-01")
    assert frame.iloc[0]["data_inclusao"] == pd.Timestamp("2024-10-07")
    assert frame.iloc[0]["data_inclusao_texto"] == "07/10/2024"
    assert str(frame["trabalhadores_resgatados"].dtype) == "Int64"
    assert str(frame["ano_acao_fiscal"].dtype) == "Int64"
    assert frame["data_atualizacao"].isna().all()
    assert details["warnings"]


@pytest.mark.parametrize(
    "index,value",
    [
        (0, "x"),
        (1, "0"),
        (1, "24"),
        (3, " "),
        (4, ""),
        (8, "1/6/2024"),
        (1, "2023.5"),
        (2, "XX"),
        (6, "-1"),
        (6, "1.5"),
        (6, "NaN"),
        (8, "31/02/2025"),
        (9, "31/02/2025"),
        (9, "01/01/2020 a invalid, 02/02/2026"),
    ],
)
def test_csv_invalid_present_values_do_not_coerce_to_missing(csv_row, index, value):
    csv_row[index] = value
    with pytest.raises(ParseError):
        parser.parse_empregadores_bundle(_csv_bytes([csv_row]), formato="csv", companion=None)


@pytest.mark.parametrize(
    "damage,message",
    [
        ("missing_header", "Cabeçalho ausente"),
        ("missing_column", "Cabeçalho ausente"),
        ("duplicate_column", "Cabeçalho ausente"),
        ("swapped_columns", "Cabeçalho ausente"),
        ("short_record", "Linha 2: quantidade de campos incompatível"),
        ("extra_field", "Linha 2: quantidade de campos incompatível"),
        ("duplicate_id", "ID 1: registro duplicado"),
        ("html", "Cabeçalho ausente"),
        ("empty", "CSV vazio"),
    ],
)
def test_csv_invalid_layout_never_returns_successful_empty(csv_row, damage, message):
    if damage == "missing_header":
        raw = b"a;b\n1;2\n"
    elif damage == "missing_column":
        raw = _csv_bytes([csv_row[:-1]], models.SOURCE_COLUMNS[:-1])
    elif damage == "duplicate_column":
        raw = _csv_bytes([csv_row + [csv_row[0]]], models.SOURCE_COLUMNS + ["ID"])
    elif damage == "swapped_columns":
        swapped = models.SOURCE_COLUMNS.copy()
        swapped[2], swapped[3] = swapped[3], swapped[2]
        raw = _csv_bytes([csv_row], swapped)
    elif damage == "short_record":
        raw = _csv_bytes([csv_row[:-1]])
    elif damage == "extra_field":
        raw = _csv_bytes([csv_row + ["extra"]])
    elif damage == "duplicate_id":
        raw = _csv_bytes([csv_row, csv_row])
    elif damage == "html":
        raw = b"<!doctype html><html>Service unavailable</html>"
    else:
        raw = b""
    with helpers.levanta_exatamente(ParseError, match=message):
        parser.parse_empregadores_bundle(raw, formato="csv", companion=None)


def test_csv_zero_workers_is_not_missing(csv_row):
    csv_row[6] = "0"
    frame, _ = parser.parse_empregadores_bundle(
        _csv_bytes([csv_row]), formato="csv", companion=None
    )
    assert frame["trabalhadores_resgatados"].tolist() == [0]


@pytest.mark.parametrize(
    "damage,message",
    [
        ("no_pages", "PDF sem páginas"),
        ("no_table", "PDF página 1: nenhuma tabela encontrada"),
        ("no_header", "PDF página 1, linha 1: estrutura incompatível"),
        ("only_decoration", "PDF página 1: tabela sem cabeçalho"),
        ("wrong_width", "PDF página 1, linha 3: estrutura incompatível"),
        ("wrong_repeated_header", "Cabeçalho ausente, ambíguo ou incompatível com a Lista Suja"),
        ("no_records", "PDF sem registros"),
    ],
)
def test_pdf_layout_errors_are_not_silent_empty_or_skipped_records(
    damage, message, csv_row, publication_files
):
    text = "\n".join(publication_files["txt"].decode("cp1252").splitlines()[:4])
    if damage == "no_pages":
        tables = []
    elif damage == "no_table":
        tables = [None]
    elif damage == "no_header":
        tables = [[["foo", "bar"], ["1", "2"]]]
    elif damage == "only_decoration":
        tables = [[["xrLabelNumeracaoPagina", *[""] * 9]]]
    elif damage == "wrong_width":
        tables = [[models.SOURCE_COLUMNS, csv_row, ["2", "2023"]]]
    elif damage == "no_records":
        tables = [[models.SOURCE_COLUMNS]]
    else:
        wrong = models.SOURCE_COLUMNS.copy()
        wrong[2], wrong[3] = wrong[3], wrong[2]
        tables = [[models.SOURCE_COLUMNS, csv_row], [wrong, csv_row]]
    with helpers.levanta_exatamente(ParseError, match=re.escape(message)):
        _parse_mock_pdf(_pdf_from_tables(tables, text))


def test_pdf_valido_publica_registro_edicao_e_atualizacao(csv_row, publication_files):
    text = "\n".join(publication_files["txt"].decode("cp1252").splitlines()[:4])
    decoration = ["Atualização periódica de 6 de abril de 2026.", *[""] * 9]
    pdf = _pdf_from_tables([[decoration, [None] * 10, models.SOURCE_COLUMNS, csv_row]], text)
    with patch.object(_pdf, "pdfplumber_module") as check:
        check.return_value.open.return_value = pdf
        with helpers.sem_excecao():
            frame, details = parser.parse_empregadores_bundle(b"%PDF-mock", formato="pdf")
    assert frame["id_registro"].tolist() == ["1"]
    assert frame["data_atualizacao"].tolist() == [pd.Timestamp("2026-09-04")]
    assert details["pages"] == 1
    assert details["publication"]["periodic_update"] == "2026-04-06"
    assert details["publication"]["registry_updated_at"] == "2026-09-04"
    assert details["formato"] == "pdf" and not details["companion_validated"]


@pytest.mark.parametrize(
    "content,message",
    [(b"%PDF-invalid document", "PDF inválido ou ilegível"), (b"<html>", "Conteúdo não é PDF")],
)
def test_pdf_corrupt_bytes_raise_parse_error(content, message):
    pytest.importorskip("pdfplumber")
    with helpers.levanta_exatamente(ParseError, match=message):
        parser.parse_empregadores(content)


@pytest.mark.parametrize("column", range(10))
def test_companion_must_match_each_source_column_before_attaching_edition(
    column, publication_files
):
    lines = publication_files["txt"].decode("cp1252").splitlines(keepends=True)
    for position, line in enumerate(lines):
        cells = line.rstrip("\r\n").split("\t")
        if len(cells) == 10 and cells[0].strip() == "1":
            cells[column] = cells[column].strip() + " changed"
            lines[position] = "\t".join(cells) + "\n"
            break
    else:
        pytest.fail("Official companion row 1 not found")
    with pytest.raises(ParseError):
        parser.parse_empregadores_bundle(
            publication_files["csv"], formato="csv", companion="".join(lines).encode("cp1252")
        )


TITULO = constants.LISTA_SUJA_PUBLICATION_TITLE
EDICAO = "Atualização periódica de 6 de abril de 2026."
CABECALHO_TXT = "\t".join(models.SOURCE_COLUMNS)
REGISTRO_TXT = "\t".join(
    [
        "1",
        "2023",
        "MT",
        "Empregador",
        "001.234.567-89",
        "Fazenda",
        "5",
        "0111-3/01",
        "",
        "07/10/2024",
    ]
)


@pytest.mark.parametrize(
    "damage,message",
    [
        ("sem_registros", "CSV sem registros"),
        ("aspas", "CSV inválido ou encoding incompatível"),
        ("data_1500", "Data fora do intervalo suportado em data_decisao"),
        ("formato", "Formato deve ser csv ou pdf"),
    ],
)
def test_csv_invalido_ou_formato_desconhecido_vira_parse_error(damage, message, csv_row):
    formato = "xlsx" if damage == "formato" else "csv"
    if damage == "sem_registros":
        raw = _csv_bytes([])
    elif damage == "aspas":
        raw = _csv_bytes([csv_row]).replace(b"\r\n1;", b'\r\n"1"x;')
    else:
        csv_row[8] = "01/01/1500"
        raw = _csv_bytes([csv_row])
    with helpers.levanta_exatamente(ParseError, match=message):
        parser.parse_empregadores_bundle(raw, formato=formato, companion=None)


@pytest.mark.parametrize(
    "texto,message",
    [
        ('"x"y\tz', "TXT companheiro inválido ou encoding incompatível"),
        (f"{TITULO}\n{EDICAO}", "TXT companheiro sem tabela válida"),
        (
            f"{TITULO}\n{EDICAO}\n{CABECALHO_TXT}\n{REGISTRO_TXT}\nlinha estranha\tcom dois campos",
            "TXT linha 5: conteúdo não reconhecido",
        ),
    ],
)
def test_txt_companheiro_mal_formado_ou_sem_tabela_vira_parse_error(texto, message):
    with helpers.levanta_exatamente(ParseError, match=message):
        _parsing.read_companion(texto.encode("cp1252"))


@pytest.mark.parametrize(
    "linhas,message",
    [
        ([TITULO, "Cadastro de Empregadores em Ajustamento de Conduta", EDICAO], "Publicação CEAC"),
        ([TITULO, EDICAO, "Atualização periódica de 7 de abril de 2026."], "conflitante"),
        ([TITULO, "Atualização periódica de 31 de fevereiro de 2026."], "periódica inválida"),
        ([TITULO, f"{EDICAO} Cadastro atualizado em 4/9/2026."], "atualização inválida na"),
        ([EDICAO], "não identifica o cadastro principal"),
    ],
    ids=["ceac", "edicao_conflitante", "dia_inexistente", "atualizacao_sem_data", "sem_titulo"],
)
def test_publicacao_recusa_ceac_edicao_conflitante_e_datas_invalidas(linhas, message):
    with helpers.levanta_exatamente(ParseError, match=message):
        _parsing.publication("\n".join(linhas))


def test_publicacao_junta_nota_de_varias_linhas_e_nota_final():
    texto = "\n".join([TITULO, EDICAO, "(*1) Nota que continua", "na linha", "(*2) Última nota"])
    assert _parsing.publication(texto)["notes"] == [
        "(*1) Nota que continua na linha",
        "(*2) Última nota",
    ]


def test_pdf_sem_pdfplumber_explica_o_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "pdfplumber", None)
    with helpers.levanta_exatamente(ImportError, match=re.escape("pip install agrobr[pdf]")):
        _pdf.pdfplumber_module()
