from __future__ import annotations

import json

import pytest

from agrobr.exceptions import ParseError
from agrobr.mapbiomas_alerta import parser
from tests import helpers
from tests.test_mapbiomas_alerta.oficial import GOLDEN


@pytest.mark.parametrize("codigo", [1.5, "não numérico"])
def test_alerta_codigo_nao_inteiro_recusado(codigo):
    document = json.loads((GOLDEN / "referencia_antes.json").read_bytes())
    records = [document["data"]["alerts"]["collection"][0]]
    records[0]["alertCode"] = codigo
    with helpers.levanta_exatamente(ParseError, match="alertCode publicado sem código inteiro"):
        parser.parse_alertas(records)
