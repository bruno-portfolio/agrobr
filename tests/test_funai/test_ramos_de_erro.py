from __future__ import annotations

import json

import pytest

from agrobr.exceptions import ParseError
from agrobr.funai import parser
from tests import helpers


@pytest.mark.parametrize(
    "defeito,motivo",
    [
        ("coluna", "Nome geométrico não corresponde à camada"),
        ("links", "Links de continuação contraditórios"),
        ("bool", "include_geometry deve ser bool"),
        ("ausente", "Membro geometry ausente"),
    ],
    ids=["coluna", "links", "bool", "ausente"],
)
def test_pagina_layout_invalido(defeito, motivo):
    features = helpers.funai_features()[:1]
    page = {"type": "FeatureCollection", "features": features}
    if defeito == "coluna":
        features[0]["geometry_name"] = "outra_geometria"
    elif defeito == "links":
        page.update(next="pagina-a", links=[{"rel": "next", "href": "pagina-b"}])
    elif defeito == "ausente":
        del features[0]["geometry"]
    with helpers.levanta_exatamente(ParseError, motivo):
        parser.parse_page(
            json.dumps(page).encode(), include_geometry=0 if defeito == "bool" else False
        )
