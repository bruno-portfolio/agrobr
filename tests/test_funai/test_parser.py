from __future__ import annotations

import json

import pytest

from agrobr.exceptions import ParseError
from agrobr.funai import parser
from tests.helpers import funai_features


def page(features, **extra):
    return json.dumps(
        {
            "type": "FeatureCollection",
            "features": features,
            "numberMatched": len(features),
            "numberReturned": len(features),
            **extra,
        },
        ensure_ascii=False,
    ).encode()


def test_unrequested_geometry_is_validated_then_discarded():
    features = funai_features(include_geometry=True)
    parsed = parser.parse_page(page(features), include_geometry=False)
    assert parsed.geometries is None and parsed.records[0].geometry is None
    features[0]["geometry"]["coordinates"][0][0][0][0] = True
    with pytest.raises(ParseError):
        parser.parse_page(page(features), include_geometry=False)


@pytest.mark.parametrize(
    "extra", [{"numberReturned": 7}, {"numberMatched": 0}, {"totalFeatures": 8}, {"unknown": True}]
)
def test_envelope_disagreement_rejected(extra):
    with pytest.raises(ParseError):
        parser.parse_page(page(funai_features(), **extra), include_geometry=False)


def test_diagnostics_bounded_but_totals_complete():
    feature = funai_features()[0]
    feature["properties"].update(uf_sigla="XX", superficie_perimetro_ha=-1, cr="  ")
    parsed = parser.parse_page(page([feature] * 13), include_geometry=False)
    for name in ("unknown_uf_token", "negative_area"):
        assert parsed.diagnostics[name]["count"] == 13
        assert (
            len(parsed.diagnostics[name]["examples"]) == 10
            and parsed.diagnostics[name]["examples_omitted"] == 3
        )
    assert parsed.statistics["cr"]["whitespace_count"] == 13
