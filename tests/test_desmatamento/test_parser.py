from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr.desmatamento.parser import PARSER_VERSION, parse_deter_csv, parse_prodes_csv
from agrobr.exceptions import ParseError

PRODES_DIR = Path(__file__).parent.parent / "golden_data" / "desmatamento" / "prodes_sample"
DETER_DIR = Path(__file__).parent.parent / "golden_data" / "desmatamento" / "deter_sample"


class TestParserVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1


class TestParseProdesCsv:
    def test_valid_csv(self):
        csv_bytes = PRODES_DIR.joinpath("response.csv").read_bytes()
        df = parse_prodes_csv(csv_bytes, "Cerrado")

        assert len(df) >= 5
        assert "ano" in df.columns
        assert "area_km2" in df.columns
        assert "uf" in df.columns
        assert "bioma" in df.columns

    def test_golden_data_columns(self):
        csv_bytes = PRODES_DIR.joinpath("response.csv").read_bytes()
        expected = json.loads(PRODES_DIR.joinpath("expected.json").read_text(encoding="utf-8"))
        df = parse_prodes_csv(csv_bytes, "Cerrado")

        for col in expected["columns"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_golden_data_ufs(self):
        csv_bytes = PRODES_DIR.joinpath("response.csv").read_bytes()
        expected = json.loads(PRODES_DIR.joinpath("expected.json").read_text(encoding="utf-8"))
        df = parse_prodes_csv(csv_bytes, "Cerrado")

        ufs = sorted(df["uf"].dropna().unique().tolist())
        for u in expected["ufs_expected"]:
            assert u in ufs, f"Missing uf: {u}"

    def test_area_non_negative(self):
        csv_bytes = PRODES_DIR.joinpath("response.csv").read_bytes()
        df = parse_prodes_csv(csv_bytes, "Cerrado")

        assert (df["area_km2"] >= 0).all()

    def test_bioma_column(self):
        csv_bytes = PRODES_DIR.joinpath("response.csv").read_bytes()
        df = parse_prodes_csv(csv_bytes, "Cerrado")

        assert (df["bioma"] == "Cerrado").all()

    def test_ano_is_numeric(self):
        csv_bytes = PRODES_DIR.joinpath("response.csv").read_bytes()
        df = parse_prodes_csv(csv_bytes, "Cerrado")

        assert pd.api.types.is_integer_dtype(df["ano"])

    def test_empty_csv_raises(self):
        with pytest.raises(ParseError):
            parse_prodes_csv(b"year,area_km,state\n", "Cerrado")

    def test_invalid_csv_raises(self):
        with pytest.raises(ParseError):
            parse_prodes_csv(b"invalid data", "Cerrado")

    def test_missing_columns_raises(self):
        csv = b"id,nome,valor\n1,teste,100\n"
        with pytest.raises(ParseError, match="Colunas obrigatorias ausentes"):
            parse_prodes_csv(csv, "Cerrado")


class TestParseDeterCsv:
    def test_valid_csv(self):
        csv_bytes = DETER_DIR.joinpath("response.csv").read_bytes()
        df = parse_deter_csv(csv_bytes, "Amazônia")

        assert len(df) >= 5
        assert "data" in df.columns
        assert "area_km2" in df.columns
        assert "uf" in df.columns
        assert "classe" in df.columns
        assert "bioma" in df.columns

    def test_golden_data_columns(self):
        csv_bytes = DETER_DIR.joinpath("response.csv").read_bytes()
        expected = json.loads(DETER_DIR.joinpath("expected.json").read_text(encoding="utf-8"))
        df = parse_deter_csv(csv_bytes, "Amazônia")

        for col in expected["columns"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_golden_data_ufs(self):
        csv_bytes = DETER_DIR.joinpath("response.csv").read_bytes()
        expected = json.loads(DETER_DIR.joinpath("expected.json").read_text(encoding="utf-8"))
        df = parse_deter_csv(csv_bytes, "Amazônia")

        ufs = sorted(df["uf"].dropna().unique().tolist())
        for u in expected["ufs_expected"]:
            assert u in ufs, f"Missing uf: {u}"

    def test_golden_data_classes(self):
        csv_bytes = DETER_DIR.joinpath("response.csv").read_bytes()
        expected = json.loads(DETER_DIR.joinpath("expected.json").read_text(encoding="utf-8"))
        df = parse_deter_csv(csv_bytes, "Amazônia")

        classes = sorted(df["classe"].dropna().unique().tolist())
        for c in expected["classes_expected"]:
            assert c in classes, f"Missing class: {c}"

    def test_area_non_negative(self):
        csv_bytes = DETER_DIR.joinpath("response.csv").read_bytes()
        df = parse_deter_csv(csv_bytes, "Amazônia")

        assert (df["area_km2"] >= 0).all()

    def test_bioma_column(self):
        csv_bytes = DETER_DIR.joinpath("response.csv").read_bytes()
        df = parse_deter_csv(csv_bytes, "Amazônia")

        assert (df["bioma"] == "Amazônia").all()

    def test_data_column_is_date(self):
        csv_bytes = DETER_DIR.joinpath("response.csv").read_bytes()
        df = parse_deter_csv(csv_bytes, "Amazônia")

        for val in df["data"].dropna():
            assert hasattr(val, "year")

    def test_municipio_id_is_ibge(self):
        csv_bytes = DETER_DIR.joinpath("response.csv").read_bytes()
        df = parse_deter_csv(csv_bytes, "Amazônia")

        valid_ids = df["municipio_id"].dropna()
        assert len(valid_ids) > 0
        for mid in valid_ids:
            assert mid > 1000000

    def test_empty_csv_raises(self):
        with pytest.raises(ParseError):
            parse_deter_csv(b"view_date,areamunkm,uf\n", "Amazônia")

    def test_invalid_csv_raises(self):
        with pytest.raises(ParseError):
            parse_deter_csv(b"invalid data", "Amazônia")

    def test_missing_columns_raises(self):
        csv = b"id,nome,valor\n1,teste,100\n"
        with pytest.raises(ParseError, match="Colunas obrigatorias ausentes"):
            parse_deter_csv(csv, "Amazônia")
