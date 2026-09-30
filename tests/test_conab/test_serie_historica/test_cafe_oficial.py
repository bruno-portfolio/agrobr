import pytest

from agrobr.conab._serie_historica import client
from agrobr.exceptions import InvalidParameterError


def test_cana_industria_nao_anunciada_e_erro_explicito():
    products = [item["produto"] for item in client.list_produtos()]
    assert "cana_industria" not in products
    assert "cana" in products
    with pytest.raises(InvalidParameterError, match="industriais"):
        client.get_xls_url("cana_industria")
