from __future__ import annotations

import json

from agrobr.ana import parser
from agrobr.exceptions import ParseError
from tests import helpers
from tests.test_ana.test_massas_dagua_layout import BARRAGEM


def test_hidrografia_identificador_fracionario_recusado():
    pagina = json.dumps(
        {
            "features": [
                {
                    "attributes": {
                        "OBJECTID": 1.5,
                        "COCURSODAG": "123",
                        "COBACIA": "456",
                        "NORIOCOMP": "Rio",
                        "DEDOMINIAL": "Federal",
                    }
                }
            ]
        }
    ).encode()
    with helpers.levanta_exatamente(ParseError, match="cannot safely cast") as exc:
        parser.parse_layer_tabular([pagina], layer_key="hidrografia")
    assert exc.value.source == "ana"


def test_massas_medida_nao_numerica_recusada():
    payload = json.loads((BARRAGEM / "faixa_0.json").read_bytes())
    payload["features"][0]["attributes"]["nuareaha"] = "sem medição"
    with helpers.levanta_exatamente(ParseError, match="Unable to parse string") as exc:
        parser.parse_massas_dagua([json.dumps(payload).encode()], geo=False)
    assert exc.value.source == "ana"
