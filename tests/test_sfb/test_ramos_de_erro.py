from __future__ import annotations

import json

import pytest

from agrobr.exceptions import ParseError
from agrobr.sfb import parser
from tests import helpers
from tests.test_sfb import oficial


def test_cnfp_identificador_fracionario_recusado():
    registro = oficial.respostas("cnfp_df")[1]
    payload = json.loads((oficial.GOLDEN / registro["file"]).read_bytes())
    payload["features"][0]["attributes"]["fid"] = 1.5
    with helpers.levanta_exatamente(ParseError, match="cannot safely cast") as exc:
        parser.parse_layer_tabular([json.dumps(payload).encode()], layer_key="cnfp")
    assert exc.value.source == "sfb"


@pytest.mark.parametrize(
    ("payload", "mensagem"),
    [({}, "esperado objeto com lista de features"), ({"features": [None]}, "feição sem atributos")],
)
def test_ifn_envelope_malformado_recusado(payload, mensagem):
    with helpers.levanta_exatamente(ParseError, match=mensagem):
        parser.parse_ifn([json.dumps(payload).encode()], [])
