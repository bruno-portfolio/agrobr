from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr.defensivos.parser import (
    PARSER_VERSION,
    _split_composite_ia,
    _strip_all_str_cols,
    parse_formulados_csv,
    parse_tecnicos_csv,
)
from agrobr.exceptions import ParseError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "defensivos"


class TestParseFormuladosCsv:
    def _load_golden(self) -> bytes:
        return (GOLDEN_DIR / "formulados_sample" / "response.csv").read_bytes()

    def _load_expected(self) -> dict:
        return json.loads((GOLDEN_DIR / "formulados_sample" / "expected.json").read_text())

    def test_returns_tuple_of_two_dataframes(self):
        result = parse_formulados_csv(self._load_golden())
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], pd.DataFrame)

    def test_dedup_count(self):
        expected = self._load_expected()
        form_df, auth_df = parse_formulados_csv(self._load_golden())
        assert len(form_df) == expected["formulados_count"]
        assert len(auth_df) == expected["autorizacoes_count"]

    def test_formulados_columns(self):
        expected = self._load_expected()
        form_df, _ = parse_formulados_csv(self._load_golden())
        assert form_df.columns.tolist() == expected["formulados_columns"]

    def test_autorizacoes_columns(self):
        expected = self._load_expected()
        _, auth_df = parse_formulados_csv(self._load_golden())
        assert auth_df.columns.tolist() == expected["autorizacoes_columns"]

    def test_drops_empresa_pais_tipo_and_situacao(self):
        form_df, auth_df = parse_formulados_csv(self._load_golden())
        for df in (form_df, auth_df):
            assert "EMPRESA_PAIS_TIPO" not in df.columns
            assert "SITUACAO" not in df.columns

    def test_strip_whitespace(self):
        form_df, _ = parse_formulados_csv(self._load_golden())
        roundup = form_df[form_df["nr_registro"] == "001598"]
        assert len(roundup) == 1
        assert roundup.iloc[0]["marca_comercial"] == "ROUNDUP ORIGINAL"

    def test_en_dash_preserved(self):
        form_df, _ = parse_formulados_csv(self._load_golden())
        special = form_df[form_df["nr_registro"] == "005000"]
        assert len(special) == 1
        assert "\u2013" in special.iloc[0]["marca_comercial"]

    def test_organicos_kept_as_string(self):
        form_df, _ = parse_formulados_csv(self._load_golden())
        organicos_values = set(form_df["organicos"].dropna().unique())
        assert "NAO" in organicos_values
        assert "SIM" in organicos_values
        assert "OUTROS" in organicos_values

    def test_first_formulado(self):
        expected = self._load_expected()
        form_df, _ = parse_formulados_csv(self._load_golden())
        first = form_df.iloc[0]
        for k, v in expected["first_formulado"].items():
            assert first[k] == v, f"{k}: expected {v}, got {first[k]}"

    def test_empty_csv_raises_parse_error(self):
        with pytest.raises(ParseError, match="vazio"):
            parse_formulados_csv(b"")

    def test_missing_columns_raises_parse_error(self):
        csv = b"COL_A;COL_B\nval1;val2\n"
        with pytest.raises(ParseError, match="Colunas faltando"):
            parse_formulados_csv(csv)

    def test_latin1_encoding(self):
        csv = (
            "MARCA_COMERCIAL;INGREDIENTE_ATIVO;CULTURA;NR_REGISTRO\nPRODUTO Á;2,4-D;CAFÉ;000001\n"
        ).encode("latin-1")
        form_df, auth_df = parse_formulados_csv(csv)
        assert form_df.iloc[0]["marca_comercial"] == "PRODUTO Á"
        assert auth_df.iloc[0]["cultura"] == "CAFÉ"


class TestParseTecnicosCsv:
    def _load_golden(self) -> bytes:
        return (GOLDEN_DIR / "tecnicos_sample" / "response.csv").read_bytes()

    def _load_expected(self) -> dict:
        return json.loads((GOLDEN_DIR / "tecnicos_sample" / "expected.json").read_text())

    def test_valid_output(self):
        expected = self._load_expected()
        df = parse_tecnicos_csv(self._load_golden())
        assert isinstance(df, pd.DataFrame)
        assert len(df) == expected["total_rows"]
        assert df.columns.tolist() == expected["columns"]

    def test_drops_empresa_pais_tipo(self):
        df = parse_tecnicos_csv(self._load_golden())
        assert "EMPRESA_PAIS_TIPO" not in df.columns

    def test_strip_whitespace(self):
        df = parse_tecnicos_csv(self._load_golden())
        bt = df[df["nr_registro"] == "T00006"]
        assert len(bt) == 1
        assert bt.iloc[0]["marca_comercial"] == "BACILLUS THURINGIENSIS TECNICO"
        t2 = df[df["nr_registro"] == "T00002"]
        assert t2.iloc[0]["ingrediente_ativo"] == "2,4-D"

    def test_first_row(self):
        expected = self._load_expected()
        df = parse_tecnicos_csv(self._load_golden())
        first = df.iloc[0]
        for k, v in expected["first_row"].items():
            assert first[k] == v

    def test_empty_csv_raises(self):
        with pytest.raises(ParseError, match="vazio"):
            parse_tecnicos_csv(b"")

    def test_missing_columns_raises(self):
        csv = b"COL_A;COL_B\nval1;val2\n"
        with pytest.raises(ParseError, match="Colunas faltando"):
            parse_tecnicos_csv(csv)

    def test_c1_en_dash_normalized(self):
        csv = ("CLASSE;NR_REGISTRO;INGREDIENTE_ATIVO\nHerbicida;T00001;A \x96 B\n").encode()
        df = parse_tecnicos_csv(csv)
        assert df.iloc[0]["ingrediente_ativo"] == "A – B"


class TestHelpers:
    def test_parser_version(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1

    def test_strip_all_str_cols(self):
        df = pd.DataFrame({"a": ["  hello  ", " world "], "b": [1, 2]})
        result = _strip_all_str_cols(df)
        assert result["a"].tolist() == ["hello", "world"]
        assert result["b"].tolist() == [1, 2]


class TestSplitCompositeIA:
    def test_composite_format_splits(self):
        ia, grupo = _split_composite_ia("Glifosato (Fosfonometilglicina) (480)")
        assert ia == "Glifosato"
        assert grupo == "Fosfonometilglicina"

    def test_plain_value_returns_empty_grupo(self):
        ia, grupo = _split_composite_ia("Glifosato")
        assert ia == "Glifosato"
        assert grupo == ""

    def test_no_match_strips_value(self):
        ia, grupo = _split_composite_ia("  Glifosato  ")
        assert ia == "Glifosato"
        assert grupo == ""


class TestParseTecnicosCompositeIA:
    def test_composite_ia_column_split(self):
        csv = (
            b"INGREDIENTE_ATIVO(GRUPO_QUIMICI)(CONCENTRACAO);CLASSE;NR_REGISTRO;"
            b"MARCA_COMERCIAL;TITULAR_DE_REGISTRO\n"
            b"Glifosato (Fosfonometilglicina) (480);Herbicida;T00001;PRODUTO X;EMPRESA Y\n"
        )
        df = parse_tecnicos_csv(csv)
        assert "ingrediente_ativo" in df.columns
        assert "grupo_quimico" in df.columns
        assert df.iloc[0]["ingrediente_ativo"] == "Glifosato"
        assert df.iloc[0]["grupo_quimico"] == "Fosfonometilglicina"

    def test_drop_cols_applied(self):
        csv = (
            b"CLASSE;NR_REGISTRO;MARCA_COMERCIAL;TITULAR_DE_REGISTRO;"
            b"EMPRESA_PAIS_TIPO;INGREDIENTE_ATIVO\n"
            b"Herbicida;T00001;PRODUTO X;EMPRESA Y;Brasil/Fabricante;2,4-D\n"
        )
        df = parse_tecnicos_csv(csv)
        assert "EMPRESA_PAIS_TIPO" not in df.columns


class TestFormulados_NoDrop:
    def test_no_drop_cols_present(self):
        csv = (
            b"MARCA_COMERCIAL;INGREDIENTE_ATIVO;CULTURA;NR_REGISTRO\n"
            b"PRODUTO X;Glifosato;Soja;001598\n"
        )
        form_df, auth_df = parse_formulados_csv(csv)
        assert len(form_df) == 1
        assert form_df.iloc[0]["marca_comercial"] == "PRODUTO X"
