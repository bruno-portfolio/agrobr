from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts import agrofit
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente


@pytest.fixture
def component():
    frame = agrofit.AGROFIT_COMPOSICAO_V1.empty_frame().reindex([0, 1])
    frame["tipo"] = pd.Series(["tecnicos", "tecnicos"], dtype=object)
    frame["nr_registro"] = pd.Series(["00301", "00301"], dtype=object)
    frame["ordem_componente"] = pd.Series([1, 2], dtype="Int64")
    frame["componente_texto"] = pd.Series(["A (X) (7 g/L)", "B (Y) (6.8 g/L)"], dtype=object)
    frame["concentracao_valor"] = pd.Series([7.0, 6.8], dtype="Float64")
    return frame


@pytest.mark.parametrize("name", ["formulados", "tecnicos", "autorizacoes", "composicao"])
@pytest.mark.parametrize("value", ["", " ", "ABC DEF", 123, True, None])
def test_registration_domain_rejects_invalid_identity(name, value, component):
    contract = contracts.get_contract(f"agrofit_{name}")
    frame = (
        component.iloc[:1].copy() if name == "composicao" else contract.empty_frame().reindex([0])
    )
    frame["nr_registro"] = pd.Series([value], dtype=object)
    valid, errors = contract.validate(frame)
    assert not valid
    assert any("nr_registro" in error for error in errors)


@pytest.mark.parametrize("value", ["000189", "TC02523", "14017/Pré-Mistura"])
def test_registration_preserves_zeros_letters_and_official_unicode(value):
    contract = agrofit.AGROFIT_TECNICOS_V1_1
    frame = contract.empty_frame().reindex([0])
    frame["nr_registro"] = pd.Series([value], dtype=object)
    assert contract.validate(frame) == (True, [])
    assert frame.iloc[0]["nr_registro"] == value


@pytest.mark.parametrize("value", ["alien", "", None])
def test_composition_family_domain(value, component):
    component["tipo"] = pd.Series([value, "tecnicos"], dtype=object)
    valid, errors = agrofit.AGROFIT_COMPOSICAO_V1.validate(component)
    assert not valid
    assert any("tipo" in error for error in errors)


@pytest.mark.parametrize("value", ["", " ", None, 3])
def test_composition_requires_published_component_text(value, component):
    component["componente_texto"] = pd.Series([value, "B"], dtype=object)
    valid, errors = agrofit.AGROFIT_COMPOSICAO_V1.validate(component)
    assert not valid
    assert any("componente_texto" in error for error in errors)


def test_duplicate_column_labels_return_validation_errors(component):
    duplicated = pd.concat([component, component[["tipo"]]], axis=1)
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(duplicated, agrofit.AGROFIT_COMPOSICAO_V1)
    assert erro.value.violation == "Duplicate column labels: ['tipo']"
