from __future__ import annotations

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente

from .conftest import make_source

RECORTE_TROCADO = [
    ("comercio_internacional", "arroz em casca"),
    ("comercio_internacional", "Arroz Casca"),
    ("comercio_internacional", "etanol hidratado"),
    ("oferta_demanda_global", "Arroz em Casca"),
    ("oferta_demanda_global", "arroz_casca"),
    ("abate_trimestral", "Frango Congelado"),
]

MESMO_RECORTE = [
    ("comercio_internacional", "ethanol", "etanol"),
    ("futuros_agricolas", "etanol hidratado", "etanol"),
    ("producao_anual", "Arroz em casca", "arroz"),
]


@pytest.mark.parametrize(("nome", "produto"), RECORTE_TROCADO)
async def test_sinonimo_que_muda_o_recorte_e_recusado_antes_da_fonte(nome, produto, monkeypatch):
    dataset = datasets.get_dataset(nome)
    fonte = make_source(pd.DataFrame())
    for source in type(dataset).info.sources:
        monkeypatch.setattr(source, "fetch_fn", fonte)

    with levanta_exatamente(InvalidParameterError, produto) as recusa:
        await getattr(datasets, nome)(produto)

    assert all(f"'{valido}'" in str(recusa.value) for valido in dataset.info.products)
    fonte.assert_not_awaited()


@pytest.mark.parametrize(("nome", "produto", "nome_no_dataset"), MESMO_RECORTE)
def test_sinonimo_do_mesmo_recorte_segue_aceito(nome, produto, nome_no_dataset):
    assert datasets.get_dataset(nome)._produto_do_dataset(produto) == nome_no_dataset
