from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from agrobr.cepea.parsers import v1
from agrobr.exceptions import ParseError

GOLDEN = Path(__file__).parents[1] / "golden_data/cepea/trigo_duas_pracas_20260925"
HTML = (GOLDEN / "trigo_cepea.html").read_text(encoding="utf-8")
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("titulo", "faltando"),
    [("PARANÁ", "Paraná"), ("RIO GRANDE DO SUL", "Rio Grande do Sul")],
)
def test_praca_declarada_sem_tabela_e_erro_de_layout(titulo, faltando):
    original = f"CEPEA/ESALQ - {titulo}</div>"
    assert HTML.count(original) == 1
    with pytest.raises(ParseError, match=f"tabela de \\['{faltando}'\\] não encontrada"):
        v1.CepeaParserV1().parse(HTML.replace(original, "CEPEA/ESALQ - XX</div>"), "trigo")


@pytest.mark.parametrize(
    ("id_tabela", "titulo", "faltando"),
    [
        ("imagenet-indicador1", "PARANÁ", "Paraná"),
        ("imagenet-indicador2", "RIO GRANDE DO SUL", "Rio Grande do Sul"),
    ],
)
def test_titulo_preservado_sem_a_tabela_da_praca_e_erro_de_layout(id_tabela, titulo, faltando):
    inicio = HTML.index(f'<table id="{id_tabela}"')
    fim = HTML.index("</table>", inicio) + len("</table>")
    html = HTML[:inicio] + HTML[fim:]
    assert html.count("<table") == HTML.count("<table") - 1
    assert html.count(f"CEPEA/ESALQ - {titulo}</div>") == 1
    with pytest.raises(ParseError, match=f"tabela de \\['{faltando}'\\] não encontrada"):
        v1.CepeaParserV1().parse(html, "trigo")


def test_titulo_com_as_duas_pracas_nao_divide_a_tabela():
    original = "CEPEA/ESALQ - PARANÁ</div>"
    html = HTML.replace(original, "CEPEA/ESALQ - PARANÁ E RIO GRANDE DO SUL</div>")
    with pytest.raises(ParseError, match="mesma tabela"):
        v1.CepeaParserV1().parse(html, "trigo")


def test_as_duas_pracas_seguem_saindo():
    saida = v1.CepeaParserV1().parse(HTML, "trigo")
    assert sorted((i.data.isoformat(), i.praca, i.valor) for i in saida) == sorted(
        (linha["data"], linha["praca"], Decimal(linha["valor"])) for linha in MANIFESTO["rows"]
    )
