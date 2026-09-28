from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts import zarc
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente, zarc_frame


@pytest.fixture
def contract():
    return zarc.ZONEAMENTO_AGRICOLA_V2


@pytest.mark.parametrize("value", [None, 0, 20, 30, 40, 50])
def test_contract_accepts_published_risk_and_absence(contract, value):
    frame = zarc_frame(dec10=value)
    assert contract.validate(frame) == (True, [])
    assert str(frame["dec10"].dtype) == "Int64"


@pytest.mark.parametrize("value", [-1, 1, 5, 19, 25, 60, 101, 999])
def test_contract_rejects_unvalidated_risk_domain(contract, value):
    valid, errors = contract.validate(zarc_frame(dec10=value))
    assert not valid
    assert any("dec10" in error for error in errors)


@pytest.mark.parametrize(
    "value,dtype", [(20, "int64"), (20.5, "float64"), (True, "bool"), ("20", "object")]
)
def test_contract_rejects_inconsistent_risk_type(contract, value, dtype):
    frame = zarc_frame()
    frame["dec10"] = pd.Series([value], dtype=dtype)
    valid, errors = contract.validate(frame)
    assert not valid
    assert any("Int64" in error for error in errors)


@pytest.mark.parametrize(
    "initial,final,season",
    [
        ("", "2027", "perene"),
        ("2026", "", "perene"),
        ("2026", "2028", "2026/2028"),
        ("２０２６", "２０２７", "２０２６/２０２７"),
        ("", "SEM SAFRA", "perene"),
        ("", "OLERÍCOLA", "perene"),
        ("", "UNKNOWN", "perene"),
    ],
)
def test_contract_rejects_wrong_season_derivation(contract, initial, final, season):
    assert not contract.validate(zarc_frame(safra_inicio=initial, safra_fim=final, safra=season))[0]


@pytest.mark.parametrize("value", ["123456", "12345678", "5208707.0", "５２０８７０７", None])
def test_contract_rejects_invalid_geocode(contract, value):
    assert not contract.validate(zarc_frame(geocodigo=value))[0]


@pytest.mark.parametrize("value", ["go", "XX", "", None])
def test_contract_rejects_invalid_state(contract, value):
    assert not contract.validate(zarc_frame(uf=value))[0]


@pytest.mark.parametrize("value", ["1.0", "-1", "１２", " 001", None])
def test_contract_rejects_nonliteral_text_code(contract, value):
    assert not contract.validate(zarc_frame(nm_codigo=value))[0]


@pytest.mark.parametrize("value", ["", " ", None])
def test_contract_rejects_missing_culture_code(contract, value):
    assert not contract.validate(zarc_frame(cultura_codigo=value))[0]


@pytest.mark.parametrize("positions", [(0, 2), (2, 2), (4, 2)])
def test_contract_rejects_ambiguous_source_position(contract, positions):
    frame = pd.concat([zarc_frame(registro_origem=value) for value in positions], ignore_index=True)
    assert not contract.validate(frame)[0]


def test_contract_rejects_duplicate_column(contract):
    frame = zarc_frame()
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(pd.concat([frame, frame[["dec1"]]], axis=1), contract)
    assert erro.value.violation == "Duplicate column labels: ['dec1']"
