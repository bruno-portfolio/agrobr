from unittest.mock import Mock

import pytest

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.noticias_agricolas import client


@pytest.mark.parametrize("produto", ["inexistente", "", None, 1, []])
async def test_produto_invalido_lista_validos_antes_do_http(produto, monkeypatch):
    http = Mock(side_effect=AssertionError("HTTP não deve ser chamado"))
    monkeypatch.setattr(client.httpx, "AsyncClient", http)
    with pytest.raises(InvalidParameterError) as erro:
        await client.fetch_indicador_page(produto)
    assert all(nome in str(erro.value) for nome in constants.NOTICIAS_AGRICOLAS_PRODUTOS)
    http.assert_not_called()


def test_produto_normaliza_caixa_e_espacos():
    assert client._get_produto_url(" SOJA ") == client._get_produto_url("soja")


@pytest.mark.parametrize("produto,canonico", [("Açúcar", "acucar"), ("Boi Gordo", "boi_gordo")])
def test_produto_normaliza_acento_e_espaco_como_o_parser(produto, canonico):
    assert client._get_produto_url(produto) == client._get_produto_url(canonico)
