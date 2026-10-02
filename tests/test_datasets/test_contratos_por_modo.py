from __future__ import annotations

import pytest

from agrobr import contracts, datasets
from agrobr.datasets import registry

MODOS = {
    "desmatamento": {
        "tipo='prodes'": "desmatamento_prodes",
        "tipo='deter'": "desmatamento_deter",
    },
    "futuros_agricolas": {
        "tipo='ajustes' ou 'historico'": "ajuste_diario",
        "tipo='posicoes' ou 'oi_historico'": "posicoes_abertas",
    },
    "seguro_rural": {
        "tipo='apolices'": "mapa_psr_apolices",
        "tipo='sinistros'": "mapa_psr_sinistros",
    },
    "uso_do_solo": {
        "tipo='cobertura'": "mapbiomas_cobertura",
        "tipo='transicao'": "mapbiomas_transicao",
        "nivel='municipio'": "mapbiomas_cobertura_municipal",
    },
    "clima": {
        "sem estacao": "clima",
        "estacao": "clima_estacao",
        "estacao, agregacao='horario'": "clima_estacao_horaria",
    },
    "credito_rural": {
        "agregacao='uf' ou 'programa'": "credito_rural",
        "agregacao='registro'": "bcb_credito_rural_registro",
    },
}


@pytest.mark.parametrize("nome", sorted(MODOS))
def test_describe_lista_o_contrato_de_cada_modo(nome):
    texto = datasets.describe(nome)
    info = datasets.info(nome)
    padrao = next(iter(MODOS[nome].values()))
    assert contracts.get_contract(padrao).version == info["contract_version"]
    assert f"  Contract: v{info['contract_version']} (modo padrão)" in texto.splitlines()
    for rotulo, contrato in MODOS[nome].items():
        linha = f"    {rotulo}: {contrato} v{contracts.get_contract(contrato).version}"
        assert linha in texto.splitlines()


@pytest.mark.parametrize("nome", sorted(MODOS))
def test_o_modo_listado_e_o_contrato_que_o_fetch_valida(nome):
    dataset = registry.get_dataset(nome)
    assert {
        rotulo: dataset._contract_name(**argumentos)
        for rotulo, argumentos in dataset._modos_de_contrato.items()
    } == MODOS[nome]


def test_dataset_de_um_contrato_so_segue_com_uma_linha():
    linhas = [
        linha for linha in datasets.describe("preco_diario").splitlines() if "Contract" in linha
    ]
    assert linhas == [f"  Contract: v{datasets.info('preco_diario')['contract_version']}"]
