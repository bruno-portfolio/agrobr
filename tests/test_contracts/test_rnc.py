from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts import rnc
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente, sem_excecao


@pytest.fixture(params=["registradas", "protegidas"])
def cultivar_contract(request):
    return contracts.get_contract(f"rnc_{request.param}")


@pytest.fixture
def valid_frame(cultivar_contract):
    data = {}
    for column in cultivar_contract.columns:
        if column.type == contracts.ColumnType.DATE:
            data[column.name] = pd.Series(["2026-09-04", None], dtype="datetime64[ns]")
        else:
            data[column.name] = pd.Series(["texto", ""], dtype=object)
    key = cultivar_contract.primary_key[0]
    data[key] = ["00001", "00002"]
    if key == "nr_processo":
        data[key] = ["21806.000202/2014", "21806.000122/2019"]
        data["nr_certificado"] = ["20170031", "20170031"]
        data["termino_protecao_texto"] = ["04/09/2026", ""]
    return pd.DataFrame(data).astype(
        {
            column.name: "object"
            for column in cultivar_contract.columns
            if column.type == contracts.ColumnType.STRING
        }
    )


def test_valid_contract_preserves_empty_strings_and_distinct_rows(cultivar_contract, valid_frame):
    assert cultivar_contract.validate(valid_frame) == (True, [])
    assert valid_frame.loc[1, "cultivar"] == ""


@pytest.mark.parametrize("invalid", [None, "", "  ", 1, True, float("inf")])
def test_primary_key_rejects_missing_blank_or_nontext(invalid, cultivar_contract, valid_frame):
    valid_frame[cultivar_contract.primary_key[0]] = pd.Series([invalid, "other"], dtype=object)
    assert not cultivar_contract.validate(valid_frame)[0]


@pytest.mark.parametrize("all_missing", [False, True])
@pytest.mark.parametrize("dtype", ["object", "datetime64[us]", "datetime64[ns, UTC]"])
def test_date_requires_civil_ns_even_when_all_missing(
    all_missing, dtype, cultivar_contract, valid_frame
):
    column = next(c.name for c in cultivar_contract.columns if c.type == contracts.ColumnType.DATE)
    values = [None, None] if all_missing else ["2026-09-04", None]
    valid_frame[column] = pd.Series(values, dtype=dtype)
    assert not cultivar_contract.validate(valid_frame)[0]


def test_date_rejects_time_of_day(cultivar_contract, valid_frame):
    column = next(c.name for c in cultivar_contract.columns if c.type == contracts.ColumnType.DATE)
    valid_frame.loc[0, column] = pd.Timestamp("2026-09-04T01:00:00")
    assert not cultivar_contract.validate(valid_frame)[0]


@pytest.mark.parametrize(
    "text,scalar",
    [
        ("31/02/2026", None),
        ("04/09/2026", None),
        ("04/09/2026", "2026-09-05"),
        ("4/9/2026", "2026-09-04"),
        (" 04/09/2026 ", "2026-09-04"),
        ("04/09/2026 00:00", "2026-09-04"),
        ("até a emissão do certificado definitivo ", None),
        ("indeterminado", None),
        (None, None),
    ],
)
def test_protection_end_rejects_inconsistent_or_unknown_text(text, scalar):
    contract = rnc.RNC_PROTEGIDAS_V1
    row = {column.name: "" for column in contract.columns}
    row.update(nr_processo="21806.000132/2019", termino_protecao_texto=text)
    frame = pd.DataFrame([row])
    frame["inicio_protecao"] = pd.Series([pd.NaT], dtype="datetime64[ns]")
    frame["termino_protecao"] = pd.Series(pd.to_datetime([scalar]), dtype="datetime64[ns]")
    with sem_excecao():
        valid, errors = contract.validate(frame)
    assert not valid
    assert "Protection end date must agree with its published text" in errors


def test_duplicate_column_fails_without_attribute_error(cultivar_contract, valid_frame):
    date_column = next(
        column.name
        for column in cultivar_contract.columns
        if column.type == contracts.ColumnType.DATE
    )
    duplicated = pd.concat([valid_frame, valid_frame[[date_column]]], axis=1)
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(duplicated, cultivar_contract)
    assert erro.value.violation == f"Duplicate column labels: [{date_column!r}]"
