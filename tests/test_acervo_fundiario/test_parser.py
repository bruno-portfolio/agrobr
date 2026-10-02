from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd
import pytest

from agrobr.acervo_fundiario import parser
from agrobr.exceptions import ParseError


class TestSchemaDriftDetection:
    def test_missing_required_raises(self, tmp_path: Path):
        pytest.importorskip("geopandas")
        import geopandas as gpd
        import pyogrio
        from shapely.geometry import Polygon

        df = pd.DataFrame({"some_col": ["x"]})
        gdf = gpd.GeoDataFrame(df, geometry=[Polygon([(0, 0), (1, 0), (1, 1)])], crs="EPSG:4674")
        shp_dir = tmp_path / "broken_layer"
        shp_dir.mkdir()
        pyogrio.write_dataframe(gdf, shp_dir / "broken.shp", encoding="latin1")
        zip_path = tmp_path / "broken.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            for f in shp_dir.iterdir():
                zf.write(f, f.name)

        with pytest.raises(ParseError, match="Colunas obrigatorias"):
            parser.parse_sigef(zip_path, natureza="privado")
