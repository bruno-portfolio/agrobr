from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr.conab.ceasa.models import COLUNAS_SAIDA
from agrobr.conab.ceasa.parser import (
    PARSER_VERSION,
    parse_lista_ceasas,
    parse_lista_produtos,
    parse_precos,
)
from agrobr.exceptions import ParseError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "conab_ceasa" / "precos_sample"


def _precos_json() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("precos_response.json").read_text(encoding="utf-8"))


def _ceasas_json() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("ceasas_response.json").read_text(encoding="utf-8"))


def _expected() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("expected.json").read_text(encoding="utf-8"))


class TestParsePrecos:
    def test_returns_dataframe(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_columns(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        for col in COLUNAS_SAIDA:
            assert col in df.columns

    def test_non_null_prices_min(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        expected = _expected()
        assert len(df) >= expected["non_null_prices_min"]

    def test_no_null_precos(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        assert df["preco"].notna().all()

    def test_all_precos_positive(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        assert (df["preco"] > 0).all()

    def test_48_produtos(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        assert df["produto"].nunique() == _expected()["total_produtos"]

    def test_43_ceasas(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        assert df["ceasa"].nunique() == _expected()["total_ceasas"]

    def test_categorias_present(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        cats = set(df["categoria"].unique())
        assert "FRUTAS" in cats
        assert "HORTALICAS" in cats

    def test_unidades_present(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        unidades = set(df["unidade"].unique())
        for u in _expected()["unidades"]:
            assert u in unidades

    def test_produtos_un(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        expected = _expected()
        for prod in expected["produtos_un"]:
            rows = df[df["produto"] == prod]
            assert (rows["unidade"] == "UN").all(), f"{prod} should be UN"

    def test_produtos_dz(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        expected = _expected()
        for prod in expected["produtos_dz"]:
            rows = df[df["produto"] == prod]
            assert (rows["unidade"] == "DZ").all(), f"{prod} should be DZ"

    def test_sample_tomate_ceagesp(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        expected = _expected()["sample_tomate_ceagesp_sp"]
        row = df[(df["produto"] == expected["produto"]) & (df["ceasa"] == expected["ceasa"])]
        assert len(row) == 1
        assert row.iloc[0]["preco"] == pytest.approx(expected["preco"])
        assert row.iloc[0]["unidade"] == expected["unidade"]
        assert row.iloc[0]["ceasa_uf"] == expected["ceasa_uf"]
        assert row.iloc[0]["categoria"] == expected["categoria"]

    def test_sample_abacaxi_fortaleza(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        expected = _expected()["sample_abacaxi_fortaleza"]
        row = df[(df["produto"] == expected["produto"]) & (df["ceasa"] == expected["ceasa"])]
        assert len(row) == 1
        assert row.iloc[0]["preco"] == pytest.approx(expected["preco"])
        assert row.iloc[0]["unidade"] == expected["unidade"]

    def test_data_column_not_null(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        assert df["data"].notna().all()

    def test_ceasa_uf_not_empty(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        assert (df["ceasa_uf"] != "").all()

    def test_no_duplicate_pk(self):
        df = parse_precos(_precos_json(), _ceasas_json())
        pk = df[["data", "produto", "ceasa"]]
        assert not pk.duplicated().any()

    def test_parser_version(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1


class TestParseEdgeCases:
    def test_empty_resultset(self):
        df = parse_precos({"resultset": [], "metadata": []}, _ceasas_json())
        assert len(df) == 0
        for col in COLUNAS_SAIDA:
            assert col in df.columns

    def test_empty_ceasas_raises(self):
        with pytest.raises(ParseError, match="CEASAs"):
            parse_precos(_precos_json(), {"resultset": []})

    def test_all_null_row(self):
        precos = {
            "metadata": _precos_json()["metadata"],
            "resultset": [["TESTE (KG)"] + [None] * 43],
        }
        df = parse_precos(precos, _ceasas_json())
        assert len(df) == 0


class TestParseListaProdutos:
    def test_returns_sorted_list(self):
        result = parse_lista_produtos(_precos_json())
        assert isinstance(result, list)
        assert result == sorted(result)
        assert len(result) == 48

    def test_tomate_present(self):
        result = parse_lista_produtos(_precos_json())
        assert "TOMATE" in result


class TestParseListaCeasas:
    def test_returns_list_of_dicts(self):
        result = parse_lista_ceasas(_ceasas_json())
        assert isinstance(result, list)
        assert len(result) == 43
        for item in result:
            assert "nome" in item
            assert "uf" in item

    def test_sorted_by_name(self):
        result = parse_lista_ceasas(_ceasas_json())
        names = [r["nome"] for r in result]
        assert names == sorted(names)

    def test_uf_present(self):
        result = parse_lista_ceasas(_ceasas_json())
        for item in result:
            assert len(item["uf"]) == 2
