import pytest

from agrobr import conab, datasets
from agrobr.exceptions import InvalidParameterError
from tests.helpers import assert_replay_served
from tests.test_conab_ceasa import test_nome_fora_do_publicado, test_precos_oraculo


@pytest.mark.parametrize("dataset", [False, True])
async def test_produto_fora_do_publicado_falha_depois_da_rede(monkeypatch, dataset):
    servido = test_nome_fora_do_publicado._servir(monkeypatch)
    consultar = datasets.preco_atacado if dataset else conab.ceasa_precos
    with pytest.raises(InvalidParameterError, match="Produto.*kiwi.*Válidos.*'ABACATE'"):
        await consultar(produto="kiwi")
    assert_replay_served(servido)


@pytest.mark.parametrize("dataset", [False, True])
async def test_produto_acentuado_preserva_celulas_publicadas(monkeypatch, dataset):
    servido = test_nome_fora_do_publicado._servir(monkeypatch)
    consultar = datasets.preco_atacado if dataset else conab.ceasa_precos
    quadro = await consultar(produto=" LIMÃO TAHITI ")
    esperado = sorted(
        linha for linha in test_precos_oraculo._oficial() if linha[1] == "LIMAO TAHITI"
    )
    assert esperado
    assert (
        sorted(
            (
                linha.data.date().isoformat(),
                linha.produto,
                linha.unidade,
                linha.ceasa,
                linha.ceasa_uf,
                linha.preco,
            )
            for linha in quadro.itertuples()
        )
        == esperado
    )
    assert_replay_served(servido)
