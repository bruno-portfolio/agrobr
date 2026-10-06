from __future__ import annotations

import pytest

from agrobr.exceptions import ParseError
from tests import helpers
from tests.test_desmatamento.test_json_parser import parse_payload, source_payload


def test_pagina_total_menor_que_feicoes_recusado():
    payload = source_payload()
    payload.pop("numberReturned", None)
    payload["numberMatched"] = 0
    payload["totalFeatures"] = 0
    with helpers.levanta_exatamente(
        ParseError, match="Declared total smaller than returned features"
    ):
        parse_payload(payload)


def test_pagina_bbox_dimensao_invalida_recusado():
    payload = source_payload()
    payload["bbox"] = [0, 1, 2]
    with helpers.levanta_exatamente(ParseError, match="Invalid bbox dimension"):
        parse_payload(payload)


def test_pampa_geometria_nula_recusada():
    payload = source_payload("prodes_pampa")
    payload["crs"] = {"type": "name", "properties": {"name": "EPSG:4326"}}
    payload["features"][0]["geometry"] = None
    with helpers.levanta_exatamente(ParseError, match="Pampa geometry is not nullable"):
        parse_payload(payload, biome="Pampa", geo=True)


@pytest.mark.parametrize(
    ("anel", "mensagem"),
    [
        ([[0, 0], [1, 0], [1, 1], [0, 1]], "Invalid linear ring"),
        ([[0], [1], [2], [0]], "Invalid coordinate dimension"),
    ],
)
def test_geometria_anel_invalido_recusado(anel, mensagem):
    payload = source_payload("prodes_pampa")
    payload["crs"] = {"type": "name", "properties": {"name": "EPSG:4326"}}
    payload["features"][0]["geometry"] = {"type": "MultiPolygon", "coordinates": [[anel]]}
    with helpers.levanta_exatamente(ParseError, match=mensagem):
        parse_payload(payload, biome="Pampa", geo=True)
