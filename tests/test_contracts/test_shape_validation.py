from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.exceptions import ContractViolationError


@pytest.mark.parametrize("column", ["valor", "extra"])
def test_duplicate_column_labels_report_contract_violation(column):
    contract = contracts.Contract(
        "teste", "1.0", [contracts.Column("valor", contracts.ColumnType.FLOAT)]
    )
    frame = pd.DataFrame([[1.0, 2.0, 3.0]], columns=["valor", column, column])

    valid, errors = contract.validate(frame)

    assert not valid
    assert any("Duplicate column" in error and column in error for error in errors)
    with pytest.raises(ContractViolationError, match="Duplicate column"):
        contracts.validate_dataset(frame, contract)


@pytest.mark.parametrize("kind", [contracts.ColumnType.DATE, contracts.ColumnType.DATETIME])
@pytest.mark.parametrize("value", ["", "NaT"])
@pytest.mark.parametrize("nullable", [False, True])
def test_date_text_that_becomes_nat_is_invalid(kind, value, nullable):
    column = contracts.Column("data", kind, nullable=nullable)

    assert column.validate(pd.Series([value]))
