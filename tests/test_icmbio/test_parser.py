from __future__ import annotations

import json
from pathlib import Path

import pytest

gpd = pytest.importorskip("geopandas")

GEO = Path(__file__).parents[1] / "golden_data/icmbio/ucs_geo_sample/response.geojson"


class TestParseUcsGeojson:
    def test_empty_features_returns_empty_geodataframe(self):
        from agrobr.icmbio.parser import parse_ucs_geojson

        data = json.dumps({"type": "FeatureCollection", "features": []}).encode()
        gdf = parse_ucs_geojson(data)
        assert len(gdf) == 0

    def test_vazio_tem_os_dtypes_do_cheio(self):
        from agrobr.icmbio.parser import parse_ucs_geojson

        cheio = parse_ucs_geojson(GEO.read_bytes())
        vazio = parse_ucs_geojson(
            json.dumps({"type": "FeatureCollection", "features": []}).encode()
        )
        assert not cheio.empty and vazio.empty
        assert list(vazio.columns) == list(cheio.columns)
        assert vazio.dtypes.astype(str).to_dict() == cheio.dtypes.astype(str).to_dict()
