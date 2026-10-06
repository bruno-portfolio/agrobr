from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from agrobr.exceptions import ParseError
from agrobr.ibge import bruto, ftp_client, legacy_parser, lspa_parser, pam_parser
from tests import helpers

GOLDEN = Path(__file__).parents[1] / "golden_data" / "ibge" / "censo_legado_oficial"


@pytest.mark.parametrize(
    "content,message",
    [
        (b"<FeatureCollection", "contagem WFS ilegível"),
        (b'<!DOCTYPE FeatureCollection><FeatureCollection numberMatched="1"/>', "DOCTYPE"),
        (b"<FeatureCollection/>", "sem numberMatched"),
        (b'<FeatureCollection numberMatched="-1"/>', "numberMatched inválido"),
    ],
    ids=["1709", "1710", "1711", "1712"],
)
def test_contagem_wfs_recusa_envelope_invalido(content, message):
    with helpers.levanta_exatamente(ParseError, message):
        bruto.ler_contagem(content)


@pytest.mark.parametrize(
    "old,new,message",
    [
        (b"BRASIL", b"MARTE!", "Geografia não reconhecida"),
        (b"TitTab", b"IgnTab", "Cabeçalhos XLS ausentes"),
        (b"Box", b"Ign", "Colunas de medidas não reconhecidas"),
    ],
    ids=["1750", "1752", "1753"],
)
def test_censo_xls_recusa_cabecalho_corrompido(old, new, message):
    name, content = ftp_client.extract_tables_from_zip((GOLDEN / "Brasil_Tab_7.zip").read_bytes())[
        0
    ]
    assert old in content
    with helpers.levanta_exatamente(ParseError, message):
        legacy_parser.parse_legacy_xls(content.replace(old, new), "maquinas", name)


@pytest.mark.parametrize(
    "content,message",
    [
        (
            '<table><thead><tr><th colspan="2">Tabela 7. Tratores</th></tr>'
            "<tr><th>Local</th><th>Total</th></tr></thead>"
            '<tbody><tr><td align="left">Goiânia</td></tr></tbody></table>',
            "Linha HTML com quantidade inesperada de colunas",
        ),
        (
            '<table><thead><tr><th colspan="2">Tabela 7. Tratores</th></tr>'
            "<tr><th>Local</th><th>Total</th></tr></thead>"
            "<tbody><tr><td>Goiânia</td><td>1</td></tr></tbody></table>",
            "Geografias HTML não reconhecidas",
        ),
    ],
    ids=["1755", "1756"],
)
def test_censo_html_recusa_layout_incompleto(content, message):
    with helpers.levanta_exatamente(ParseError, message):
        legacy_parser.parse_legacy_html(content.encode(), "maquinas", "GO")


def test_censo_html_recusa_falha_do_leitor(monkeypatch):
    failure = ValueError("No tables found")
    reader = mock.Mock(side_effect=failure)
    monkeypatch.setattr(pd, "read_html", reader)
    with helpers.levanta_exatamente(ParseError, "Tabela HTML não reconhecida") as caught:
        legacy_parser.parse_legacy_html(b"<html><p>Sem tabela</p></html>", "maquinas", "GO")
    reader.assert_called_once()
    assert caught.value.__cause__ is failure


def test_censo_html_sem_tabela_preserva_erro_de_layout():
    with helpers.levanta_exatamente(ParseError, "Tabela HTML não reconhecida"):
        legacy_parser.parse_legacy_html(b"<html><p>Sem tabela</p></html>", "maquinas", "GO")


def test_lspa_recusa_dimensao_invalida():
    frame = pd.DataFrame(
        [
            {
                "D1C": "codigo-invalido",
                "D1N": "Brasil",
                "D2C": "202609",
                "D4C": "109",
                "D4N": "Produção",
                "MN": "Toneladas",
                "V": "1",
            }
        ]
    )
    with helpers.levanta_exatamente(ParseError, "Dimensões da tabela 6588 inválidas"):
        lspa_parser.parse_lspa(frame, "soja")


def test_pam_recusa_variavel_sem_mapeamento():
    frame = pd.DataFrame(
        [
            {
                "localidade": "Brasil",
                "ano": "2024",
                "variavel": "medida desconhecida",
                "valor": 1,
            }
        ]
    )
    with helpers.levanta_exatamente(ParseError, "Variáveis PAM sem mapeamento"):
        pam_parser.pivot_observations(frame)
