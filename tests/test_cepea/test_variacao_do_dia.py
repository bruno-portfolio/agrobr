from __future__ import annotations

import json
from pathlib import Path

from agrobr.cepea.parsers.v1 import CepeaParserV1
from tests.helpers import collect_failures, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data" / "cepea"
SANITY = GOLDEN / "sanity_20260906"
TRIGO = GOLDEN / "trigo_duas_pracas_20260925"
TRIGO_LINHAS = json.loads((TRIGO / "manifest.json").read_text(encoding="utf-8"))["rows"]


def _trigo(praca: str) -> tuple[str, dict[str, str]]:
    linha = next(r for r in TRIGO_LINHAS if r["praca"] == praca and r["linha"] == 0)
    return linha["data"], {"variacao": linha["celulas"][2], "variacao_mes": linha["celulas"][3]}


def test_variacao_do_dia_do_mes_e_da_semana_em_chaves_proprias():
    casos = [
        (
            "soja",
            SANITY / "soja.html",
            None,
            "2026-09-04",
            {"variacao": "0,46%", "variacao_mes": "0,90%"},
        ),
        (
            "etanol_hidratado",
            SANITY / "etanol.html",
            None,
            "2026-09-04",
            {"variacao_semana": "3,79%"},
        ),
        ("trigo", TRIGO / "trigo_cepea.html", "Paraná", *_trigo("Paraná")),
        ("trigo", TRIGO / "trigo_cepea.html", "Rio Grande do Sul", *_trigo("Rio Grande do Sul")),
    ]
    with collect_failures() as check:
        for produto, pagina, praca, dia, esperado in casos:
            with check((produto, praca)):
                with sem_excecao():
                    indicadores = CepeaParserV1().parse(
                        pagina.read_bytes().decode("utf-8"), produto
                    )
                linha = next(
                    ind
                    for ind in indicadores
                    if ind.data.isoformat() == dia and praca in {None, ind.praca}
                )
                assert {
                    chave: texto
                    for chave, texto in linha.meta.items()
                    if chave.startswith("variacao")
                } == esperado
