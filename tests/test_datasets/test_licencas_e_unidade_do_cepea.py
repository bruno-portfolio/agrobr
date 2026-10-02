from __future__ import annotations

import pytest

from agrobr import datasets
from agrobr.contracts import cepea as cepea_contracts
from agrobr.datasets import registry


@pytest.mark.parametrize("nome", datasets.list_datasets())
def test_to_dict_traz_as_mesmas_licencas_do_info(nome):
    assert registry.get_dataset(nome).info.to_dict()["licenses"] == datasets.info(nome)["licenses"]


def test_to_dict_do_preco_diario_traz_a_fonte_interna_do_cepea():
    licencas = registry.get_dataset("preco_diario").info.to_dict()["licenses"]
    assert list(licencas)[:2] == ["cepea", "noticias_agricolas"]


def test_contrato_do_indicador_diz_a_unidade_do_algodao():
    valor = next(c for c in cepea_contracts.CEPEA_INDICADOR_V1.columns if c.name == "valor")
    assert valor.unit == datasets.info("preco_diario")["unit"]
    assert "cBRL/lb" in valor.unit
