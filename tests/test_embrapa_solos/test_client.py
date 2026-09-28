from __future__ import annotations

import pytest

from agrobr.embrapa_solos import client, query
from agrobr.exceptions import ParseError
from tests.helpers import (
    embrapa_solos_features,
    install_embrapa_solos_wfs,
)


@pytest.mark.parametrize(
    "defect",
    ["overlap_changed", "descending", "empty", "overlap_only", "excess", "count", "missing_count"],
)
async def test_second_page_integrity_aborts(defect, monkeypatch):
    features = embrapa_solos_features()

    def transform(request, envelope):
        if request.url.params.get("startIndex") != "1":
            return
        if defect == "overlap_changed":
            envelope["features"][0]["properties"]["titulo"] = "changed"
        elif defect == "descending":
            envelope["features"][1]["properties"]["fid"] = 1
        elif defect == "count":
            envelope.update(numberMatched=100, totalFeatures=100)
        elif defect == "missing_count":
            del envelope["numberMatched"]
        else:
            envelope["features"] = {
                "empty": [],
                "overlap_only": [features[1]],
                "excess": features[1:],
            }[defect]
            envelope["numberReturned"] = len(envelope["features"])

    install_embrapa_solos_wfs(monkeypatch, features, transform)
    with pytest.raises(ParseError):
        await client.fetch_acquisition(
            query.build_query(
                product="perfis",
                include_geometry=False,
                max_registros=None,
                tamanho_pagina=2,
                uf="SP",
            )
        )


@pytest.mark.parametrize("geo", [False, True])
async def test_bbox_null_geometry_fails_even_if_uf_excludes(geo, monkeypatch):
    features = embrapa_solos_features(include_geometry=True)[:1]
    features[0]["geometry"] = None
    install_embrapa_solos_wfs(monkeypatch, features)
    with pytest.raises(ParseError, match="nula"):
        await client.fetch_acquisition(
            query.build_query(
                product="perfis", include_geometry=geo, max_registros=1, uf="SP", bbox=(0, 0, 1, 1)
            )
        )
