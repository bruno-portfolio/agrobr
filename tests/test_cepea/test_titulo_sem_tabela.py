from __future__ import annotations

import re
from pathlib import Path

import pytest

from agrobr.cepea.parsers import v1
from agrobr.exceptions import ParseError

CEPEA = Path(__file__).parents[1] / "golden_data/cepea"
BEZERRO = (CEPEA / "pages_20260905/bezerro.html").read_text(encoding="utf-8")
TITULO_DO_PESO = '<div class="imagenet-col-8 imagenet-sm-12 imagenet-table-titulo">Peso Médio do Bezerro - MS</div>'
TITULO_DO_BEZERRO = (
    '<div class="imagenet-col-8 imagenet-sm-12 imagenet-table-titulo">INDICADOR DO BEZERRO'
)


def sem_a_tabela_do_titulo(html: str, titulo: str) -> str:
    inicio = html.index("<table", html.index(f">{titulo}</div>"))
    fim = html.index("</table>", inicio) + len("</table>")
    return html[:inicio] + html[fim:]


@pytest.mark.parametrize(
    ("pagina", "produto", "titulo"),
    [
        ("pages_20260905/soja.html", "soja", "INDICADOR DA SOJA CEPEA/ESALQ - PARANAGUÁ"),
        ("pages_20260905/cafe.html", "cafe_arabica", "INDICADOR DO CAFÉ ARÁBICA CEPEA/ESALQ"),
        (
            "sanity_20260906/frango.html",
            "frango_congelado",
            "PREÇOS DO FRANGO CONGELADO CEPEA/ESALQ - ESTADO SP",
        ),
    ],
)
def test_titulo_sem_tabela_na_secao_e_erro_de_layout(pagina, produto, titulo):
    html = (CEPEA / pagina).read_text(encoding="utf-8")
    alterado = sem_a_tabela_do_titulo(html, titulo)
    assert alterado.count("<table") == html.count("<table") - 1
    assert alterado.count(f">{titulo}</div>") == 1
    with pytest.raises(ParseError, match=f"tabela de \\['{re.escape(titulo)}'\\] não encontrada"):
        v1.CepeaParserV1().parse(alterado, produto)


def test_titulo_do_peso_sem_tabela_nao_le_a_tabela_de_outra_secao():
    sem_peso = sem_a_tabela_do_titulo(BEZERRO, "Peso Médio do Bezerro - MS").replace(
        TITULO_DO_PESO, ""
    )
    assert BEZERRO.count(TITULO_DO_BEZERRO) == 1
    html = sem_peso.replace(TITULO_DO_BEZERRO, TITULO_DO_PESO + TITULO_DO_BEZERRO)
    indicadores = v1.CepeaParserV1().parse(html, "bezerro")
    assert indicadores
    assert [i.meta.get("peso_medio_kg") for i in indicadores] == [None] * len(indicadores)


def test_peso_da_propria_secao_segue_saindo():
    indicadores = v1.CepeaParserV1().parse(BEZERRO, "bezerro")
    assert {i.data.isoformat(): i.meta["peso_medio_kg"] for i in indicadores}[
        "2026-09-04"
    ] == 211.69
