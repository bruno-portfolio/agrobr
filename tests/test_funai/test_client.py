from __future__ import annotations

import copy

import pytest

from agrobr.exceptions import ParseError
from agrobr.funai import client, query
from tests.helpers import funai_features, install_funai_wfs


@pytest.mark.parametrize(
    "defect", ["property", "signed_zero", "descending", "empty", "only_overlap", "excess", "total"]
)
async def test_second_page_invalid_before_local_filter(defect, monkeypatch):
    features = funai_features()
    features[1]["properties"]["superficie_perimetro_ha"] = -0.0

    def transform(request, envelope):
        if request.url.params.get("startIndex") != "1":
            return
        if defect == "property":
            envelope["features"][0]["properties"]["terrai_nome"] = "changed"
        elif defect == "signed_zero":
            envelope["features"][0]["properties"]["superficie_perimetro_ha"] = 0.0
        elif defect == "descending":
            envelope["features"][1]["properties"]["terrai_codigo"] = 1
        elif defect == "total":
            envelope.update(numberMatched=999, totalFeatures=999)
        else:
            envelope["features"] = {
                "empty": [],
                "only_overlap": [features[1]],
                "excess": features[1:],
            }[defect]
            envelope["numberReturned"] = len(envelope["features"])

    install_funai_wfs(monkeypatch, features, transform)
    with pytest.raises(ParseError):
        await client.fetch_acquisition(
            query.build_query(include_geometry=False, max_registros=None, tamanho_pagina=2, uf="SP")
        )


async def test_literal_duplicates_preserved_with_bounded_diagnostics(monkeypatch):
    features = [copy.deepcopy(funai_features()[0]) for _ in range(25)]
    for feature in features:
        feature["properties"]["gid"] = None
    install_funai_wfs(monkeypatch, features)
    result = await client.fetch_acquisition(
        query.build_query(include_geometry=False, max_registros=None, tamanho_pagina=1)
    )
    assert len(result.frame) == 25
    assert result.diagnostics["nullable_sort_key"]["count"] == 49
    assert result.details["accepted_diagnostics"]["nullable_sort_key"]["count"] == 25
    assert len(result.diagnostics["nullable_sort_key"]["examples"]) == 10
    assert result.details["statistics"]["gid"]["null_count"] == 49
    assert result.details["accepted_statistics"]["gid"]["null_count"] == 25
    assert result.coverage.ambiguity_count > 0


async def test_bbox_null_geometry_fails(monkeypatch):
    install_funai_wfs(monkeypatch, funai_features()[:1])
    with pytest.raises(ParseError):
        await client.fetch_acquisition(
            query.build_query(include_geometry=False, max_registros=None, bbox=(0, 0, 1, 1))
        )
