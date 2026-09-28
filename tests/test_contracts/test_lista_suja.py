from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts.lista_suja import LISTA_SUJA_EMPREGADORES_V2
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente


@pytest.fixture
def contract_frame():
    return pd.DataFrame(
        {
            "empregador": ["Exemplo", "Exemplo"],
            "cpf_cnpj": ["001.234.567-89"] * 2,
            "estabelecimento": [None, "Fazenda"],
            "uf": [None, "MT"],
            "cnae": [None, "0111-3/01"],
            "data_inclusao": pd.Series([pd.NaT, "2025-10-06"], dtype="datetime64[ns]"),
            "trabalhadores_resgatados": pd.Series([pd.NA, 0], dtype="Int64"),
            "ano_acao_fiscal": pd.Series([pd.NA, 2024], dtype="Int64"),
            "id_registro": ["001", "2"],
            "data_decisao": pd.Series([pd.NaT, "2025-05-15"], dtype="datetime64[ns]"),
            "data_atualizacao": pd.Series(["2026-09-04"] * 2, dtype="datetime64[ns]"),
            "data_inclusao_texto": ["01/01/2024 a 02/02/2025, 03/03/2026", "06/10/2025"],
        }
    )


def test_contract_empty_frame_has_stable_nullable_types():
    frame = LISTA_SUJA_EMPREGADORES_V2.empty_frame()
    assert frame.empty
    assert len(frame.columns) == 12
    assert LISTA_SUJA_EMPREGADORES_V2.validate(frame) == (True, [])
    assert str(frame.ano_acao_fiscal.dtype) == str(frame.trabalhadores_resgatados.dtype) == "Int64"
    for column in ("data_inclusao", "data_decisao", "data_atualizacao"):
        assert str(frame[column].dtype) == "datetime64[ns]"


@pytest.mark.parametrize(
    "column,value",
    [
        ("id_registro", ""),
        ("id_registro", " "),
        ("id_registro", "1A"),
        ("id_registro", 1),
        ("id_registro", None),
        ("empregador", ""),
        ("cpf_cnpj", " "),
        ("uf", "XX"),
        ("uf", ""),
        ("cnae", 111301),
        ("estabelecimento", ""),
        ("data_inclusao_texto", ""),
        ("ano_acao_fiscal", 0),
        ("ano_acao_fiscal", 10000),
        ("trabalhadores_resgatados", -1),
    ],
)
def test_contract_rejects_invalid_values(column, value, contract_frame):
    if isinstance(value, int) and column in {"id_registro", "cnae"}:
        contract_frame[column] = contract_frame[column].astype(object)
    contract_frame.loc[0, column] = value
    valid, errors = LISTA_SUJA_EMPREGADORES_V2.validate(contract_frame)
    assert not valid
    assert errors


def test_contract_rejects_arbitrary_scalar_for_compound_inclusion(contract_frame):
    contract_frame.loc[0, "data_inclusao"] = pd.Timestamp("2024-01-01")
    valid, errors = LISTA_SUJA_EMPREGADORES_V2.validate(contract_frame)
    assert not valid
    assert any("Compound" in error for error in errors)


@pytest.mark.parametrize("column", ["ano_acao_fiscal", "trabalhadores_resgatados"])
@pytest.mark.parametrize("dtype", ["float64", "object", "int64"])
def test_contract_rejects_non_nullable_integer_dtype(column, dtype, contract_frame):
    contract_frame[column] = pd.Series([2024, 2025], dtype=dtype)
    valid, errors = LISTA_SUJA_EMPREGADORES_V2.validate(contract_frame)
    assert not valid
    assert any("Int64" in error for error in errors)


@pytest.mark.parametrize("column", ["data_inclusao", "data_decisao", "data_atualizacao"])
@pytest.mark.parametrize("dtype", ["datetime64[ns, UTC]", "datetime64[us]", "object"])
def test_contract_rejects_non_civil_ns_dates_even_when_all_missing(column, dtype, contract_frame):
    contract_frame[column] = pd.Series([pd.NaT, pd.NaT], dtype=dtype)
    valid, errors = LISTA_SUJA_EMPREGADORES_V2.validate(contract_frame)
    assert not valid
    assert any("datetime64[ns]" in error for error in errors)


def test_contract_duplicate_columns_are_reported_without_attribute_error(contract_frame):
    malformed = pd.concat([contract_frame, contract_frame[["id_registro"]]], axis=1)
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(malformed, LISTA_SUJA_EMPREGADORES_V2)
    assert erro.value.violation == "Duplicate column labels: ['id_registro']"
