from __future__ import annotations

import httpx
import pytest

from agrobr import constants
from agrobr.incra import client, query


@pytest.mark.parametrize("geometry", [False, True])
def test_query_bbox_without_cql(geometry):
    selection = query.build_query(
        include_geometry=geometry, max_registros=1500, bbox=(-60.0, -15.0, -50.0, -10.0)
    )
    params = httpx.URL(client.acquisition_url(selection, count=10)).params
    assert "BBOX" in params and "CQL_FILTER" not in params
    assert params["srsName"] == "EPSG:4326"
    assert params["propertyName"].split(",") == [*constants.INCRA_PROPERTIES, "geom"]
