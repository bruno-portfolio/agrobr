"""Tests for stability contracts."""

from __future__ import annotations

import pandas as pd

from agrobr.contracts import BreakingChangePolicy, Column, ColumnType, Contract
from agrobr.contracts.cepea import CEPEA_INDICADOR_V1
from agrobr.contracts.conab import CONAB_BALANCO_V1, CONAB_SAFRA_V1
from agrobr.contracts.ibge import IBGE_LSPA_V1, IBGE_PAM_V1


class TestColumn:
    def test_column_creation(self):
        col = Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=False,
            unit="BRL",
            description="Preco",
        )
        assert col.name == "valor"
        assert col.type == ColumnType.FLOAT
        assert col.nullable is False
        assert col.unit == "BRL"

    def test_column_validate_nullable(self):
        col = Column(name="test", type=ColumnType.STRING, nullable=False)
        series = pd.Series(["a", None, "b"])
        errors = col.validate(series)
        assert len(errors) == 1
        assert "null values" in errors[0]

    def test_column_validate_nullable_ok(self):
        col = Column(name="test", type=ColumnType.STRING, nullable=True)
        series = pd.Series(["a", None, "b"])
        errors = col.validate(series)
        assert len(errors) == 0

    def test_column_validate_numeric(self):
        col = Column(name="valor", type=ColumnType.FLOAT, nullable=True)
        series = pd.Series([1.5, 2.5, 3.5])
        errors = col.validate(series)
        assert len(errors) == 0

    def test_column_validate_date(self):
        col = Column(name="data", type=ColumnType.DATE, nullable=False)
        series = pd.Series(pd.date_range("2024-01-01", periods=3))
        errors = col.validate(series)
        assert len(errors) == 0


class TestContract:
    def test_contract_creation(self):
        contract = Contract(
            name="test.contract",
            version="1.0",
            columns=[
                Column(name="id", type=ColumnType.INTEGER, nullable=False),
                Column(name="name", type=ColumnType.STRING, nullable=False),
            ],
        )
        assert contract.name == "test.contract"
        assert contract.version == "1.0"
        assert len(contract.columns) == 2

    def test_contract_validate_valid(self):
        contract = Contract(
            name="test",
            version="1.0",
            columns=[
                Column(name="id", type=ColumnType.INTEGER, nullable=False),
                Column(name="name", type=ColumnType.STRING, nullable=False),
            ],
        )
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        valid, errors = contract.validate(df)
        assert valid is True
        assert len(errors) == 0

    def test_contract_validate_missing_column(self):
        contract = Contract(
            name="test",
            version="1.0",
            columns=[
                Column(name="id", type=ColumnType.INTEGER, nullable=False),
                Column(name="name", type=ColumnType.STRING, nullable=False),
            ],
        )
        df = pd.DataFrame({"id": [1, 2, 3]})
        valid, errors = contract.validate(df)
        assert valid is False
        assert any("Missing required columns" in e for e in errors)

    def test_contract_get_column(self):
        contract = Contract(
            name="test",
            version="1.0",
            columns=[
                Column(name="id", type=ColumnType.INTEGER),
                Column(name="name", type=ColumnType.STRING),
            ],
        )
        col = contract.get_column("id")
        assert col is not None
        assert col.name == "id"

        col_missing = contract.get_column("missing")
        assert col_missing is None

    def test_contract_list_columns(self):
        contract = Contract(
            name="test",
            version="1.0",
            columns=[
                Column(name="id", type=ColumnType.INTEGER, stable=True),
                Column(name="temp", type=ColumnType.STRING, stable=False),
            ],
        )
        all_cols = contract.list_columns()
        assert len(all_cols) == 2

        stable_cols = contract.list_columns(stable_only=True)
        assert len(stable_cols) == 1
        assert "id" in stable_cols

    def test_contract_to_markdown(self):
        contract = Contract(
            name="test.contract",
            version="1.0",
            effective_from="0.3.0",
            columns=[
                Column(name="id", type=ColumnType.INTEGER, description="ID"),
            ],
            guarantees=["IDs are unique"],
        )
        md = contract.to_markdown()
        assert "# Contract: test.contract" in md
        assert "**Version:** 1.0" in md
        assert "IDs are unique" in md

    def test_contract_to_dict(self):
        contract = Contract(
            name="test",
            version="1.0",
            columns=[Column(name="id", type=ColumnType.INTEGER)],
        )
        d = contract.to_dict()
        assert d["name"] == "test"
        assert d["version"] == "1.0"
        assert len(d["columns"]) == 1


class TestCEPEAContract:
    def test_cepea_indicador_contract_exists(self):
        assert CEPEA_INDICADOR_V1 is not None
        assert CEPEA_INDICADOR_V1.name == "cepea.indicador"
        assert CEPEA_INDICADOR_V1.version == "1.0"

    def test_cepea_indicador_required_columns(self):
        required = ["data", "produto", "valor", "unidade", "fonte"]
        for col_name in required:
            col = CEPEA_INDICADOR_V1.get_column(col_name)
            assert col is not None, f"Column {col_name} not found"
            assert col.stable is True

    def test_cepea_indicador_validate_valid_df(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range("2024-01-01", periods=5),
                "produto": ["soja"] * 5,
                "praca": ["paranagua"] * 5,
                "valor": [150.0, 151.0, 152.0, 153.0, 154.0],
                "unidade": ["BRL/sc60kg"] * 5,
                "fonte": ["cepea"] * 5,
                "metodologia": [None] * 5,
                "anomalies": [None] * 5,
            }
        )
        valid, errors = CEPEA_INDICADOR_V1.validate(df)
        assert valid is True


class TestCONABContracts:
    def test_conab_safra_contract_exists(self):
        assert CONAB_SAFRA_V1 is not None
        assert CONAB_SAFRA_V1.name == "conab.safras"

    def test_conab_balanco_contract_exists(self):
        assert CONAB_BALANCO_V1 is not None
        assert CONAB_BALANCO_V1.name == "conab.balanco"


class TestIBGEContracts:
    def test_ibge_pam_contract_exists(self):
        assert IBGE_PAM_V1 is not None
        assert IBGE_PAM_V1.name == "ibge.pam"

    def test_ibge_lspa_contract_exists(self):
        assert IBGE_LSPA_V1 is not None
        assert IBGE_LSPA_V1.name == "ibge.lspa"


class TestBreakingChangePolicy:
    def test_policy_values(self):
        assert BreakingChangePolicy.MAJOR_VERSION == "major"
        assert BreakingChangePolicy.NEVER == "never"
        assert BreakingChangePolicy.DEPRECATE_FIRST == "deprecate"
