from __future__ import annotations

import json
import re

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.zarc import api, models
from tests.helpers import levanta_exatamente
from tests.test_zarc import test_oraculo_edicoes as edicoes

ORACULO = json.loads(
    (
        edicoes.reconciliacao.ROOT / "tests/golden_data/zarc/renomeacoes_20260925/manifest.json"
    ).read_bytes()
)
CHAVES = {
    **edicoes.reconciliacao.MANIFEST["alias_decisions"],
    **edicoes.MANIFEST["alias_decisions"],
}
PARES = {CHAVES[par["rotulo_ate_2023_2024"]]: par for par in ORACULO["pares"]}
NOVAS = {CHAVES[par["rotulo_desde_2024_2025"]] for par in ORACULO["pares"]}


def _equivalentes(mensagem: str) -> set[str]:
    trecho = re.search(r"a mesma cultura sai como (.+) \(o ZARC renomeou", mensagem)
    assert trecho, mensagem
    return set(re.findall(r"'([a-z_0-9]+)'(?: com [a-z_]+ '[^']+')?", trecho.group(1)))


def test_tabela_de_renomeacoes_segue_as_tabuas_oficiais():
    assert set(models.RENOMEACOES_2024_2025) == set(PARES)
    for antiga, par in PARES.items():
        nova, coluna, valor = models.RENOMEACOES_2024_2025[antiga]
        assert nova == CHAVES[par["rotulo_desde_2024_2025"]]
        assert (
            par["linhas_identicas"] == par["linhas_ate_2023_2024"] == par["linhas_desde_2024_2025"]
        )
        grupos = ORACULO["grupos_desde_2024_2025"][par["rotulo_desde_2024_2025"]]
        campo = {"manejo": "manejo", "cultura_codigo": "codigo"}[coluna]
        escolhidos = [grupo for grupo in grupos if grupo[campo] == valor]
        assert escolhidos == [
            {
                "codigo": par["codigo_desde_2024_2025"],
                "manejo": par["manejo_desde_2024_2025"],
                "linhas": par["linhas_desde_2024_2025"],
            }
        ]


@pytest.mark.parametrize("antiga", sorted(PARES))
async def test_chave_de_ate_2023_2024_aponta_a_de_2024_2025(monkeypatch, antiga):
    visto = edicoes.servir(monkeypatch, edicoes.RESOURCES["2024_2025"])
    par = PARES[antiga]
    with levanta_exatamente(InvalidParameterError, f"Cultura '{antiga}' não encontrada") as erro:
        await api.zoneamento(produto=antiga, safra="2024/2025", use_cache=False)
    mensagem = str(erro.value)
    assert _equivalentes(mensagem) == {CHAVES[par["rotulo_desde_2024_2025"]]}
    grupos = ORACULO["grupos_desde_2024_2025"][par["rotulo_desde_2024_2025"]]
    mesmo_codigo = [g for g in grupos if g["codigo"] == par["codigo_desde_2024_2025"]]
    filtro = (
        f"cultura_codigo '{par['codigo_desde_2024_2025']}'"
        if len(mesmo_codigo) == 1
        else f"manejo '{par['manejo_desde_2024_2025']}'"
    )
    assert filtro in mensagem
    assert visto["served"]


@pytest.mark.parametrize("nova", sorted(NOVAS - {"mamona"}))
async def test_chave_de_2024_2025_aponta_as_de_ate_2023_2024(monkeypatch, nova):
    visto = edicoes.servir(monkeypatch, edicoes.RESOURCES["2023_2024"])
    with levanta_exatamente(InvalidParameterError, f"Cultura '{nova}' não encontrada") as erro:
        await api.zoneamento(produto=nova, safra="2023/2024", use_cache=False)
    esperadas = {
        antiga for antiga, par in PARES.items() if CHAVES[par["rotulo_desde_2024_2025"]] == nova
    }
    assert _equivalentes(str(erro.value)) == esperadas
    assert visto["served"]
