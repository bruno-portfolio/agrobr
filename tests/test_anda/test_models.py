"""Testes para os modelos ANDA."""

from agrobr.anda.models import (
    normalize_fertilizante,
    resolve_produto,
)
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


class TestNormalizeFertilizante:
    def test_unknown_passthrough(self):
        assert normalize_fertilizante("fosfato natural") == "fosfato natural"


def test_produto_que_nao_e_texto_recusado():
    with levanta_exatamente(InvalidParameterError, "string"):
        resolve_produto(1)
