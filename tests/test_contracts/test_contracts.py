from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr.contracts import (
    Column,
    ColumnType,
    Contract,
    generate_json_schemas,
    get_contract,
    has_contract,
    list_contracts,
    validate_dataset,
)
from agrobr.exceptions import ContractViolationError
from tests import helpers


class TestColumn:
    def test_column_validate_nullable(self):
        col = Column(name="test", type=ColumnType.STRING, nullable=False)
        series = pd.Series(["a", None, "b"])
        errors = col.validate(series)
        assert len(errors) == 1
        assert "null values" in errors[0]
        assert errors == ["Column 'test' has 1 null values but nullable=False"]

    def test_column_validate_min_max_range(self):
        col = Column(name="lev", type=ColumnType.FLOAT, min_value=1, max_value=12)
        series = pd.Series([0, 6, 13])
        errors = col.validate(series)
        assert len(errors) == 2


class TestContract:
    def test_contract_validate_missing_nullable_stable_column(self):
        contract = Contract(
            name="test",
            version="1.0",
            columns=[
                Column(name="id", type=ColumnType.INTEGER),
                Column(name="optional_value", type=ColumnType.FLOAT, nullable=True),
            ],
        )
        df = pd.DataFrame({"id": [1, 2, 3]})

        valid, errors = contract.validate(df)

        assert valid is False
        assert any("optional_value" in error for error in errors)

    def test_contract_validate_primary_key_column_missing(self):
        contract = Contract(
            name="test",
            version="1.0",
            primary_key=["id", "uf"],
            columns=[
                Column(name="id", type=ColumnType.INTEGER),
                Column(name="uf", type=ColumnType.STRING),
            ],
        )
        df = pd.DataFrame({"id": [1, 2, 3]})

        valid, errors = contract.validate(df)

        assert valid is False
        assert "Primary key columns missing: ['uf']" in errors

    def test_contract_validate_empty_df(self):
        contract = Contract(
            name="test",
            version="1.0",
            primary_key=["id"],
            columns=[
                Column(name="id", type=ColumnType.INTEGER, nullable=False),
            ],
        )
        df = pd.DataFrame({"id": pd.Series([], dtype=int)})
        valid, errors = contract.validate(df)
        assert valid is True

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
            primary_key=["id"],
            columns=[
                Column(name="id", type=ColumnType.INTEGER),
            ],
            guarantees=["IDs are unique"],
        )
        md = contract.to_markdown()
        assert "# Contract: test.contract" in md
        assert "**Version:** 1.0" in md
        assert "IDs are unique" in md
        assert "Primary key" in md


class TestContractRegistry:
    def test_has_contract(self):
        assert has_contract("preco_diario") is True
        assert has_contract("nonexistent") is False

    def test_get_contract_not_found(self):
        with pytest.raises(KeyError, match="nonexistent"):
            get_contract("nonexistent")


class TestValidateDataset:
    @pytest.mark.parametrize(
        "scenario,parameters",
        [
            *[
                pytest.param(
                    "test_validate_raises_on_missing_column",
                    parameters,
                    id="validate_raises_on_missing_column" + "-" + str(index),
                )
                for index, parameters in enumerate([{}])
            ],
            *[
                pytest.param(
                    "test_validate_raises_on_dtype_wrong",
                    parameters,
                    id="validate_raises_on_dtype_wrong" + "-" + str(index),
                )
                for index, parameters in enumerate([{}])
            ],
            *[
                pytest.param(
                    "test_validate_raises_on_duplicates",
                    parameters,
                    id="validate_raises_on_duplicates" + "-" + str(index),
                )
                for index, parameters in enumerate([{}])
            ],
            *[
                pytest.param(
                    "test_validate_raises_on_constraint_violation",
                    parameters,
                    id="validate_raises_on_constraint_violation" + "-" + str(index),
                )
                for index, parameters in enumerate([{}])
            ],
        ],
    )
    def test_validate_dataset_violation_messages(self, scenario: str, parameters: dict[str, Any]):
        with helpers.collect_failures() as check, check((scenario, parameters)):
            if scenario == "test_validate_raises_on_missing_column":
                df = pd.DataFrame({"data": pd.date_range("2024-01-01", periods=3)})
                with pytest.raises(ContractViolationError, match="Missing required columns"):
                    validate_dataset(df, "preco_diario")
            elif scenario == "test_validate_raises_on_dtype_wrong":
                df = pd.DataFrame(
                    {
                        "data": pd.date_range("2024-01-01", periods=3),
                        "produto": ["soja"] * 3,
                        "valor": ["abc", "def", "ghi"],
                        "unidade": ["BRL/sc60kg"] * 3,
                        "fonte": ["cepea"] * 3,
                    }
                )
                with pytest.raises(ContractViolationError, match="not numeric"):
                    validate_dataset(df, "preco_diario")
            elif scenario == "test_validate_raises_on_duplicates":
                df = pd.DataFrame(
                    {
                        "data": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                        "produto": ["soja", "soja"],
                        "valor": [150.0, 151.0],
                        "unidade": ["BRL/sc60kg"] * 2,
                        "fonte": ["cepea"] * 2,
                    }
                )
                with pytest.raises(ContractViolationError, match="duplicate"):
                    validate_dataset(df, "preco_diario")
            elif scenario == "test_validate_raises_on_constraint_violation":
                df = pd.DataFrame(
                    {
                        "data": pd.date_range("2024-01-01", periods=2),
                        "produto": ["soja", "milho"],
                        "valor": [150.0, -10.0],
                        "unidade": ["BRL/sc60kg"] * 2,
                        "fonte": ["cepea"] * 2,
                    }
                )
                with pytest.raises(ContractViolationError, match="below minimum"):
                    validate_dataset(df, "preco_diario")

    def test_validate_with_contract_object_raises(self):
        contract = Contract(
            name="inline",
            version="1.0",
            columns=[Column(name="x", type=ColumnType.FLOAT, nullable=False)],
        )
        df = pd.DataFrame({"y": [1.0, 2.0]})
        with pytest.raises(ContractViolationError):
            validate_dataset(df, contract)


class TestGenerateJsonSchemas:
    def test_generate_all_schemas(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            files = generate_json_schemas(tmpdir)
            assert len(files) == len(list_contracts())

            for filepath in files:
                path = Path(filepath)
                assert path.exists()
                data = json.loads(path.read_text(encoding="utf-8"))
                assert "name" in data
                assert "schema_version" in data
                assert "columns" in data
                assert "constraints" in data
                assert "required_columns" in data
                assert "dtypes" in data

    def test_committed_schemas_match_registry(self):
        schema_dir = Path(__file__).parents[2] / "agrobr" / "schemas"
        schema_paths = {path.stem: path for path in schema_dir.glob("*.json")}
        contract_names = set(list_contracts())
        regenerate = (
            'python -c "from agrobr.contracts import generate_json_schemas; '
            "generate_json_schemas('agrobr/schemas')\""
        )

        assert set(schema_paths) == contract_names, (
            f"Schemas em disco não correspondem ao registry. Regenere com: {regenerate}"
        )
        for name in sorted(contract_names):
            schema = json.loads(schema_paths[name].read_text(encoding="utf-8"))
            assert schema == get_contract(name).to_dict(), (
                f"Schema {name}.json diverge do contrato. Regenere com: {regenerate}"
            )
