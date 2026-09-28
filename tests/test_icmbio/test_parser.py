from __future__ import annotations

import json

import pytest

gpd = pytest.importorskip("geopandas")


class TestParseUcsGeojson:
    def test_empty_features_returns_empty_geodataframe(self):
        from agrobr.icmbio.parser import parse_ucs_geojson

        data = json.dumps({"type": "FeatureCollection", "features": []}).encode()
        gdf = parse_ucs_geojson(data)
        assert len(gdf) == 0
