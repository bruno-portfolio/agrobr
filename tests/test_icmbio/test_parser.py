from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from agrobr.exceptions import ParseError
from agrobr.icmbio.models import COLUNAS_SAIDA, COLUNAS_SAIDA_GEO, MAX_FEATURES_GEO
from agrobr.icmbio.parser import PARSER_VERSION, parse_ucs_csv

UCS_DIR = Path(__file__).parent.parent / "golden_data" / "icmbio" / "ucs_sample"
UCS_GEO_DIR = Path(__file__).parent.parent / "golden_data" / "icmbio" / "ucs_geo_sample"


class TestParserVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1


class TestParseUcsCsv:
    def test_valid_csv(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        df = parse_ucs_csv(csv_bytes)

        assert len(df) == 10
        assert "codigo" in df.columns
        assert "nome" in df.columns
        assert "area_ha" in df.columns
        assert "grupo" in df.columns

    def test_golden_data_columns(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        expected = json.loads(UCS_DIR.joinpath("expected.json").read_text(encoding="utf-8"))
        df = parse_ucs_csv(csv_bytes)

        for col in expected["columns"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_golden_data_count(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        expected = json.loads(UCS_DIR.joinpath("expected.json").read_text(encoding="utf-8"))
        df = parse_ucs_csv(csv_bytes)

        assert len(df) == expected["count"]

    def test_golden_data_non_null_columns(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        expected = json.loads(UCS_DIR.joinpath("expected.json").read_text(encoding="utf-8"))
        df = parse_ucs_csv(csv_bytes)

        for col in expected["non_null_columns"]:
            assert df[col].notna().all(), f"Column {col!r} has nulls"

    def test_area_ha_is_float(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        df = parse_ucs_csv(csv_bytes)

        assert pd.api.types.is_float_dtype(df["area_ha"])

    def test_ano_criacao_is_int64(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        df = parse_ucs_csv(csv_bytes)

        assert df["ano_criacao"].dtype == pd.Int64Dtype()

    def test_grupo_is_uppercase(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        df = parse_ucs_csv(csv_bytes)

        for val in df["grupo"].dropna():
            assert val == val.upper(), f"grupo {val!r} not uppercase"

    def test_uf_preserved_multi(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        df = parse_ucs_csv(csv_bytes)

        multi_uf = df[df["uf"].str.contains("/", na=False, regex=False)]
        assert len(multi_uf) >= 1

    def test_area_ha_non_negative(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        df = parse_ucs_csv(csv_bytes)

        assert (df["area_ha"].dropna() >= 0).all()

    def test_output_columns_match(self):
        csv_bytes = UCS_DIR.joinpath("response.csv").read_bytes()
        df = parse_ucs_csv(csv_bytes)

        for col in COLUNAS_SAIDA:
            assert col in df.columns, f"Missing output column: {col}"

    def test_empty_csv_returns_empty_dataframe(self):
        csv = b"cnuc,nomeuc,sigla_cate,grupouc,areahaalb,uf,biomas,criacaoano,criacaoato\n"
        df = parse_ucs_csv(csv)
        assert len(df) == 0
        assert list(df.columns) == COLUNAS_SAIDA

    def test_invalid_csv_raises(self):
        csv = b"id,nome,valor\n1,teste,100\n2,teste2,200\n"
        with pytest.raises(ParseError, match="Colunas obrigatorias ausentes"):
            parse_ucs_csv(csv)

    def test_missing_columns_raises(self):
        csv = b"id,nome,valor\n1,teste,100\n"
        with pytest.raises(ParseError, match="Colunas obrigatorias ausentes"):
            parse_ucs_csv(csv)


gpd = pytest.importorskip("geopandas")


class TestParseUcsGeojson:
    def _parse(self):
        from agrobr.icmbio.parser import parse_ucs_geojson

        data = UCS_GEO_DIR.joinpath("response.geojson").read_bytes()
        return parse_ucs_geojson(data)

    def test_valid_geojson(self):
        gdf = self._parse()
        assert len(gdf) >= 10
        assert "codigo" in gdf.columns
        assert "nome" in gdf.columns
        assert "area_ha" in gdf.columns
        assert "grupo" in gdf.columns

    def test_output_columns_match_schema(self):
        gdf = self._parse()
        for col in COLUNAS_SAIDA_GEO:
            assert col in gdf.columns, f"Missing column: {col}"

    def test_geometry_column_exists(self):
        gdf = self._parse()
        assert "geometry" in gdf.columns

    def test_geometry_is_valid(self):
        gdf = self._parse()
        assert gdf.geometry.is_valid.all()

    def test_crs_is_4326(self):
        gdf = self._parse()
        assert gdf.crs.to_epsg() == 4326

    def test_area_ha_non_negative(self):
        gdf = self._parse()
        assert (gdf["area_ha"].dropna() >= 0).all()

    def test_ano_criacao_is_int64(self):
        gdf = self._parse()
        assert gdf["ano_criacao"].dtype == pd.Int64Dtype()

    def test_grupo_is_uppercase(self):
        gdf = self._parse()
        for val in gdf["grupo"].dropna():
            assert val == val.upper()

    def test_empty_features_returns_empty_geodataframe(self):
        from agrobr.icmbio.parser import parse_ucs_geojson

        data = json.dumps({"type": "FeatureCollection", "features": []}).encode()
        gdf = parse_ucs_geojson(data)
        assert len(gdf) == 0

    def test_invalid_json_raises(self):
        from agrobr.icmbio.parser import parse_ucs_geojson

        with pytest.raises(ParseError):
            parse_ucs_geojson(b"not json at all {{{")

    def test_missing_columns_raises(self):
        from agrobr.icmbio.parser import parse_ucs_geojson

        data = json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [-54.0, -3.0]},
                        "properties": {"id": 1, "nome": "test"},
                    }
                ],
            }
        ).encode()
        with pytest.raises(ParseError, match="Colunas obrigatorias ausentes"):
            parse_ucs_geojson(data)

    def test_geopandas_not_installed(self):
        from agrobr.icmbio.parser import parse_ucs_geojson

        with (
            patch(
                "agrobr.icmbio.parser.check_geopandas",
                side_effect=ImportError(
                    "geopandas is required for geo functions. Install with: pip install agrobr[geo]"
                ),
            ),
            pytest.raises(ImportError, match="agrobr\\[geo\\]"),
        ):
            parse_ucs_geojson(b"{}")

    def test_truncation_warning(self):
        from agrobr.icmbio.parser import parse_ucs_geojson

        data = UCS_GEO_DIR.joinpath("response.geojson").read_bytes()
        geojson = json.loads(data)
        features = geojson["features"]
        geojson["features"] = features * (MAX_FEATURES_GEO // len(features) + 1)
        assert len(geojson["features"]) >= MAX_FEATURES_GEO
        big_data = json.dumps(geojson).encode()

        with patch("agrobr.utils.geo.logger") as mock_logger:
            gdf = parse_ucs_geojson(big_data)

        assert len(gdf) >= MAX_FEATURES_GEO
        mock_logger.warning.assert_called_once()
        call_kwargs = mock_logger.warning.call_args
        assert call_kwargs[0][0] == "icmbio_ucs_geo_truncated"
