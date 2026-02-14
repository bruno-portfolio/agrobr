from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from agrobr.contracts import (
    BreakingChangePolicy,
    Column,
    ColumnType,
    Contract,
    generate_json_schemas,
    get_contract,
    has_contract,
    list_contracts,
    validate_dataset,
)
from agrobr.contracts.cepea import CEPEA_INDICADOR_V1
from agrobr.contracts.conab import CONAB_BALANCO_V1, CONAB_CUSTO_PRODUCAO_V1, CONAB_SAFRA_V1
from agrobr.contracts.datasets import CREDITO_RURAL_V1, EXPORTACAO_V1, FERTILIZANTE_V1
from agrobr.contracts.ibge import IBGE_LSPA_V1, IBGE_PAM_V1
from agrobr.exceptions import ContractViolationError


class TestColumn:
    def test_column_creation(self):
        col = Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=False,
            unit="BRL",
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

    def test_column_validate_min_value(self):
        col = Column(name="preco", type=ColumnType.FLOAT, min_value=0)
        series = pd.Series([10.0, -5.0, 20.0])
        errors = col.validate(series)
        assert len(errors) == 1
        assert "below minimum" in errors[0]

    def test_column_validate_min_value_ok(self):
        col = Column(name="preco", type=ColumnType.FLOAT, min_value=0)
        series = pd.Series([10.0, 5.0, 20.0])
        errors = col.validate(series)
        assert len(errors) == 0

    def test_column_validate_max_value(self):
        col = Column(name="pct", type=ColumnType.FLOAT, max_value=100)
        series = pd.Series([50.0, 150.0, 80.0])
        errors = col.validate(series)
        assert len(errors) == 1
        assert "above maximum" in errors[0]

    def test_column_validate_max_value_ok(self):
        col = Column(name="pct", type=ColumnType.FLOAT, max_value=100)
        series = pd.Series([50.0, 99.0, 80.0])
        errors = col.validate(series)
        assert len(errors) == 0

    def test_column_validate_min_max_range(self):
        col = Column(name="lev", type=ColumnType.FLOAT, min_value=1, max_value=12)
        series = pd.Series([0, 6, 13])
        errors = col.validate(series)
        assert len(errors) == 2

    def test_column_validate_min_with_nulls(self):
        col = Column(name="val", type=ColumnType.FLOAT, nullable=True, min_value=0)
        series = pd.Series([10.0, None, 20.0])
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

    def test_contract_validate_dtype_wrong(self):
        contract = Contract(
            name="test",
            version="1.0",
            columns=[
                Column(name="valor", type=ColumnType.FLOAT, nullable=False),
            ],
        )
        df = pd.DataFrame({"valor": ["abc", "def", "ghi"]})
        valid, errors = contract.validate(df)
        assert valid is False
        assert any("not numeric" in e for e in errors)

    def test_contract_validate_primary_key_duplicates(self):
        contract = Contract(
            name="test",
            version="1.0",
            primary_key=["id"],
            columns=[
                Column(name="id", type=ColumnType.INTEGER, nullable=False),
                Column(name="name", type=ColumnType.STRING, nullable=False),
            ],
        )
        df = pd.DataFrame({"id": [1, 1, 2], "name": ["a", "b", "c"]})
        valid, errors = contract.validate(df)
        assert valid is False
        assert any("duplicate" in e for e in errors)

    def test_contract_validate_primary_key_no_duplicates(self):
        contract = Contract(
            name="test",
            version="1.0",
            primary_key=["id"],
            columns=[
                Column(name="id", type=ColumnType.INTEGER, nullable=False),
                Column(name="name", type=ColumnType.STRING, nullable=False),
            ],
        )
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        valid, errors = contract.validate(df)
        assert valid is True

    def test_contract_validate_composite_primary_key(self):
        contract = Contract(
            name="test",
            version="1.0",
            primary_key=["ano", "produto"],
            columns=[
                Column(name="ano", type=ColumnType.INTEGER, nullable=False),
                Column(name="produto", type=ColumnType.STRING, nullable=False),
            ],
        )
        df = pd.DataFrame(
            {
                "ano": [2024, 2024, 2024],
                "produto": ["soja", "milho", "soja"],
            }
        )
        valid, errors = contract.validate(df)
        assert valid is False

    def test_contract_validate_constraint_violated(self):
        contract = Contract(
            name="test",
            version="1.0",
            columns=[
                Column(name="preco", type=ColumnType.FLOAT, min_value=0),
            ],
        )
        df = pd.DataFrame({"preco": [10.0, -1.0]})
        valid, errors = contract.validate(df)
        assert valid is False
        assert any("below minimum" in e for e in errors)

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

    def test_contract_to_dict(self):
        contract = Contract(
            name="test",
            version="1.0",
            primary_key=["id"],
            columns=[Column(name="id", type=ColumnType.INTEGER, min_value=0)],
        )
        d = contract.to_dict()
        assert d["name"] == "test"
        assert d["schema_version"] == "1.0"
        assert d["primary_key"] == ["id"]
        assert d["required_columns"] == ["id"]
        assert d["dtypes"]["id"] == "int"
        assert d["constraints"]["no_duplicates"] is True
        assert d["constraints"]["id_min"] == 0
        assert len(d["columns"]) == 1

    def test_contract_to_json(self):
        contract = Contract(
            name="test",
            version="1.0",
            columns=[Column(name="id", type=ColumnType.INTEGER)],
        )
        json_str = contract.to_json()
        data = json.loads(json_str)
        assert data["name"] == "test"
        assert data["schema_version"] == "1.0"

    def test_contract_from_json_roundtrip(self):
        original = Contract(
            name="roundtrip",
            version="2.0",
            primary_key=["data", "produto"],
            columns=[
                Column(name="data", type=ColumnType.DATE, nullable=False),
                Column(name="produto", type=ColumnType.STRING, nullable=False),
                Column(name="valor", type=ColumnType.FLOAT, min_value=0, unit="BRL"),
            ],
            guarantees=["Test guarantee"],
            breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
            effective_from="0.10.0",
        )
        json_str = original.to_json()
        restored = Contract.from_json(json_str)

        assert restored.name == original.name
        assert restored.version == original.version
        assert restored.primary_key == original.primary_key
        assert len(restored.columns) == len(original.columns)
        assert restored.columns[2].min_value == 0
        assert restored.columns[2].unit == "BRL"
        assert restored.guarantees == original.guarantees
        assert restored.breaking_policy == original.breaking_policy
        assert restored.effective_from == original.effective_from

    def test_contract_from_dict(self):
        data = {
            "name": "test",
            "schema_version": "1.0",
            "primary_key": ["id"],
            "columns": [
                {"name": "id", "type": "int", "nullable": False, "stable": True},
            ],
            "guarantees": ["IDs unique"],
        }
        contract = Contract.from_dict(data)
        assert contract.name == "test"
        assert contract.version == "1.0"
        assert contract.primary_key == ["id"]
        assert len(contract.columns) == 1

    def test_contract_from_json_file(self):
        original = Contract(
            name="file_test",
            version="1.0",
            columns=[Column(name="x", type=ColumnType.FLOAT)],
        )
        json_str = original.to_json()
        restored = Contract.from_json(json_str)
        assert restored.name == "file_test"


class TestContractRegistry:
    def test_all_datasets_registered(self):
        expected = [
            "balanco",
            "credito_rural",
            "custo_producao",
            "estimativa_safra",
            "exportacao",
            "fertilizante",
            "preco_diario",
            "producao_anual",
        ]
        registered = list_contracts()
        for name in expected:
            assert name in registered, f"Dataset '{name}' not registered"

    def test_has_contract(self):
        assert has_contract("preco_diario") is True
        assert has_contract("nonexistent") is False

    def test_get_contract(self):
        contract = get_contract("preco_diario")
        assert contract is CEPEA_INDICADOR_V1

    def test_get_contract_not_found(self):
        with pytest.raises(KeyError, match="nonexistent"):
            get_contract("nonexistent")


class TestValidateDataset:
    def test_validate_valid_df(self):
        df = pd.DataFrame(
            {
                "data": pd.date_range("2024-01-01", periods=3),
                "produto": ["soja"] * 3,
                "praca": ["paranagua"] * 3,
                "valor": [150.0, 151.0, 152.0],
                "unidade": ["BRL/sc60kg"] * 3,
                "fonte": ["cepea"] * 3,
            }
        )
        validate_dataset(df, "preco_diario")

    def test_validate_raises_on_missing_column(self):
        df = pd.DataFrame({"data": pd.date_range("2024-01-01", periods=3)})
        with pytest.raises(ContractViolationError, match="Missing required columns"):
            validate_dataset(df, "preco_diario")

    def test_validate_raises_on_dtype_wrong(self):
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

    def test_validate_raises_on_duplicates(self):
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

    def test_validate_raises_on_constraint_violation(self):
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

    def test_validate_with_contract_object(self):
        contract = Contract(
            name="inline",
            version="1.0",
            columns=[Column(name="x", type=ColumnType.FLOAT, nullable=False)],
        )
        df = pd.DataFrame({"x": [1.0, 2.0]})
        validate_dataset(df, contract)

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
            assert len(files) == 9

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

    def test_generated_schema_loadable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_json_schemas(tmpdir)
            schema_path = Path(tmpdir) / "preco_diario.json"
            data = json.loads(schema_path.read_text(encoding="utf-8"))
            contract = Contract.from_dict(data)
            assert contract.name == "cepea.indicador"
            assert contract.version == "1.0"
            assert contract.primary_key == ["data", "produto"]


class TestCEPEAContract:
    def test_cepea_indicador_contract_exists(self):
        assert CEPEA_INDICADOR_V1 is not None
        assert CEPEA_INDICADOR_V1.name == "cepea.indicador"
        assert CEPEA_INDICADOR_V1.version == "1.0"
        assert CEPEA_INDICADOR_V1.primary_key == ["data", "produto"]

    def test_cepea_indicador_required_columns(self):
        required = ["data", "produto", "valor", "unidade", "fonte"]
        for col_name in required:
            col = CEPEA_INDICADOR_V1.get_column(col_name)
            assert col is not None, f"Column {col_name} not found"
            assert col.stable is True

    def test_cepea_indicador_valor_min(self):
        col = CEPEA_INDICADOR_V1.get_column("valor")
        assert col is not None
        assert col.min_value == 0

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
        assert CONAB_SAFRA_V1.primary_key == ["safra", "produto", "uf", "levantamento"]

    def test_conab_safra_levantamento_range(self):
        col = CONAB_SAFRA_V1.get_column("levantamento")
        assert col is not None
        assert col.min_value == 1
        assert col.max_value == 12

    def test_conab_balanco_contract_exists(self):
        assert CONAB_BALANCO_V1 is not None
        assert CONAB_BALANCO_V1.name == "conab.balanco"
        assert CONAB_BALANCO_V1.primary_key == ["safra", "produto"]

    def test_conab_custo_producao_contract_exists(self):
        assert CONAB_CUSTO_PRODUCAO_V1 is not None
        assert CONAB_CUSTO_PRODUCAO_V1.name == "conab.custo_producao"
        assert CONAB_CUSTO_PRODUCAO_V1.primary_key == [
            "cultura",
            "uf",
            "safra",
            "categoria",
            "item",
        ]

    def test_conab_custo_producao_participacao_range(self):
        col = CONAB_CUSTO_PRODUCAO_V1.get_column("participacao_pct")
        assert col is not None
        assert col.min_value == 0
        assert col.max_value == 100


class TestIBGEContracts:
    def test_ibge_pam_contract_exists(self):
        assert IBGE_PAM_V1 is not None
        assert IBGE_PAM_V1.name == "ibge.pam"
        assert IBGE_PAM_V1.primary_key == ["ano", "produto", "localidade"]

    def test_ibge_pam_ano_min(self):
        col = IBGE_PAM_V1.get_column("ano")
        assert col is not None
        assert col.min_value == 1974

    def test_ibge_lspa_contract_exists(self):
        assert IBGE_LSPA_V1 is not None
        assert IBGE_LSPA_V1.name == "ibge.lspa"
        assert IBGE_LSPA_V1.primary_key == ["ano", "mes", "produto"]

    def test_ibge_lspa_mes_range(self):
        col = IBGE_LSPA_V1.get_column("mes")
        assert col is not None
        assert col.min_value == 1
        assert col.max_value == 12


class TestNewContracts:
    def test_credito_rural_contract(self):
        assert CREDITO_RURAL_V1.name == "bcb.credito_rural"
        assert CREDITO_RURAL_V1.primary_key == ["safra", "produto", "uf", "finalidade"]

    def test_exportacao_contract(self):
        assert EXPORTACAO_V1.name == "comexstat.exportacao"
        assert EXPORTACAO_V1.primary_key == ["ano", "mes", "produto", "uf"]

    def test_exportacao_ano_min(self):
        col = EXPORTACAO_V1.get_column("ano")
        assert col is not None
        assert col.min_value == 1997

    def test_fertilizante_contract(self):
        assert FERTILIZANTE_V1.name == "anda.fertilizante"
        assert FERTILIZANTE_V1.primary_key == ["ano", "mes", "uf", "produto_fertilizante"]

    def test_fertilizante_mes_range(self):
        col = FERTILIZANTE_V1.get_column("mes")
        assert col is not None
        assert col.min_value == 1
        assert col.max_value == 12


class TestBreakingChangePolicy:
    def test_policy_values(self):
        assert BreakingChangePolicy.MAJOR_VERSION == "major"
        assert BreakingChangePolicy.NEVER == "never"
        assert BreakingChangePolicy.DEPRECATE_FIRST == "deprecate"
