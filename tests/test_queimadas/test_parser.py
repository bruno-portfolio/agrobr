from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr.exceptions import ParseError
from agrobr.queimadas.parser import PARSER_VERSION, parse_focos_csv

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "queimadas" / "focos_sample"


class TestParseVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1


class TestParseFocosCsv:
    def test_valid_csv(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        df = parse_focos_csv(csv_bytes)

        assert len(df) >= 8
        assert "data" in df.columns
        assert "lat" in df.columns
        assert "lon" in df.columns
        assert "satelite" in df.columns
        assert "uf" in df.columns

    def test_golden_data_columns(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        expected = json.loads(GOLDEN_DIR.joinpath("expected.json").read_text(encoding="latin-1"))
        df = parse_focos_csv(csv_bytes)

        for col in expected["columns"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_golden_data_biomas(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        df = parse_focos_csv(csv_bytes)

        biomas = sorted(df["bioma"].dropna().unique().tolist())
        assert len(biomas) >= 1, "Expected at least one bioma"
        assert "Cerrado" in biomas, "Missing bioma: Cerrado"

    def test_golden_data_satelites(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        df = parse_focos_csv(csv_bytes)

        satelites = sorted(df["satelite"].dropna().unique().tolist())
        assert len(satelites) >= 1, "Expected at least one satelite"
        assert "GOES-16" in satelites, "Missing satelite: GOES-16"

    def test_golden_data_ufs(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        df = parse_focos_csv(csv_bytes)

        ufs = sorted(df["uf"].dropna().unique().tolist())
        assert len(ufs) >= 1, "Expected at least one UF"
        assert "MT" in ufs, "Missing uf: MT"

    def test_lat_lon_ranges(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        df = parse_focos_csv(csv_bytes)

        assert df["lat"].min() >= -35.0
        assert df["lat"].max() <= 6.0
        assert df["lon"].min() >= -74.0
        assert df["lon"].max() <= -30.0

    def test_frp_non_negative(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        df = parse_focos_csv(csv_bytes)

        frp_vals = df["frp"].dropna()
        assert (frp_vals >= 0).all()

    def test_data_column_is_date(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        df = parse_focos_csv(csv_bytes)

        for val in df["data"].dropna():
            assert hasattr(val, "year")

    def test_hora_gmt_format(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        df = parse_focos_csv(csv_bytes)

        for val in df["hora_gmt"].dropna():
            assert ":" in str(val)

    def test_empty_csv_raises_parse_error(self):
        with pytest.raises(ParseError):
            parse_focos_csv(b"id,lat,lon,data_hora_gmt,satelite\n")

    def test_invalid_csv_raises_parse_error(self):
        with pytest.raises(ParseError):
            parse_focos_csv(b"invalid data without headers")

    def test_missing_columns_raises_parse_error(self):
        csv = b"id,nome,valor\n1,teste,100\n"
        with pytest.raises(ParseError, match="Colunas obrigatorias ausentes"):
            parse_focos_csv(csv)

    def test_numeric_columns_coerced(self):
        csv_bytes = GOLDEN_DIR.joinpath("response.csv").read_bytes()
        df = parse_focos_csv(csv_bytes)

        assert pd.api.types.is_numeric_dtype(df["lat"])
        assert pd.api.types.is_numeric_dtype(df["lon"])
        assert pd.api.types.is_numeric_dtype(df["frp"])
