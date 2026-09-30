from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import constants, contracts
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente
from tests.test_incra import replay

TEMPORAIS = {f"perimetro_{name}": dtype for name, dtype in constants.INCRA_DTYPES_TEMPORAIS.items()}

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/incra/vinculos_20260908/expected.json"
INTEGER_COLUMNS = {
    "perimetro_posicao",
    "perimetro_referencia_posicao",
    "administrativo_referencia_posicao",
    "ocorrencias_perimetro_referencia",
    "ocorrencias_administrativo_referencia",
    "perimetro_codigo",
    "perimetro_familias",
    "administrativo_numero_publicado",
}
NUP = "54330.000697/2006-18"
NUP2 = "54160.001672/2013-51"


def _frame(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            name: pd.Series(
                [
                    replay.publicado(name.removeprefix("perimetro_"), row[name])
                    if name in TEMPORAIS
                    else row[name]
                    for row in rows
                ],
                dtype="Int64"
                if name in INTEGER_COLUMNS
                else "float64"
                if name == "perimetro_area_ha"
                else "boolean"
                if name == "referencia_repetida"
                else TEMPORAIS.get(name, pd.Series([""]).dtype),
            )
            for name in columns
        }
    )


def _pair_rows(
    seed: dict[str, Any], *, left: int = 2, right: int = 2, repeated_tokens: bool = False
) -> list[dict[str, Any]]:
    rows = []
    for lp in range(1, left + 1):
        for rp in range(1, right + 1):
            row = copy.deepcopy(seed)
            row.update(
                estado_vinculo="vinculo_exato",
                referencia_tipo="nup_literal",
                referencia_literal=NUP,
                perimetro_posicao=1 if repeated_tokens else lp,
                perimetro_referencia_posicao=lp if repeated_tokens else 1,
                administrativo_numero_publicado=1 if repeated_tokens else rp,
                administrativo_referencia_posicao=rp if repeated_tokens else 1,
                ocorrencias_perimetro_referencia=left,
                ocorrencias_administrativo_referencia=right,
                referencia_repetida=left > 1 or right > 1,
                perimetro_processo=(NUP + "\n" + NUP) if repeated_tokens else NUP,
                administrativo_processo=(NUP + "\n" + NUP) if repeated_tokens else NUP,
            )
            rows.append(row)
    return rows


def _unrecognized(seed: dict[str, Any], value: str | None) -> dict[str, Any]:
    row = copy.deepcopy(seed)
    for key in row:
        if key.startswith("administrativo_"):
            row[key] = None
    absent = value is None or not value.strip()
    row.update(
        perimetro_posicao=1,
        perimetro_referencia_posicao=None,
        perimetro_processo=value,
        referencia_literal=value,
        referencia_tipo="ausente" if absent else "texto_nao_reconhecido",
        estado_vinculo="referencia_ausente" if absent else "referencia_nao_reconhecida",
        ocorrencias_perimetro_referencia=None,
        ocorrencias_administrativo_referencia=None,
        referencia_repetida=None,
    )
    return row


@pytest.fixture
def contract() -> contracts.Contract:
    return contracts.get_contract("incra_vinculos_quilombolas")


@pytest.fixture
def rows() -> list[dict[str, Any]]:
    content = GOLDEN.read_bytes()
    assert hashlib.sha256(content).hexdigest() == (
        "00cc60e23ca8b224126664529bacb0b986ff5b7bfc401086a5664860a0531ed1"
    )
    return json.loads(content)


@pytest.fixture
def national(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return _frame(rows, list(rows[0]))


@pytest.fixture
def four_pairs(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return _frame(_pair_rows(rows[0]), list(rows[0]))


def test_incra_vinculos_typed_empty_without_external_population_claim(contract, national):
    empty = contract.empty_frame()
    assert empty.empty and empty.dtypes.to_dict() == national.dtypes.to_dict()
    assert contract.validate(empty) == (True, [])


@pytest.mark.parametrize(
    "state", ["unknown_state", "sem_referencia_geografica", "referencia_ausente"]
)
def test_incra_vinculos_inconsistent_state(contract, national, state):
    national.loc[0, "estado_vinculo"] = state
    assert not contract.validate(national)[0]


def test_incra_vinculos_source_attribute_without_side(contract, national):
    index = national.index[national["perimetro_posicao"].isna()][0]
    national.loc[index, "perimetro_nome"] = "inventado"
    assert not contract.validate(national)[0]


@pytest.mark.parametrize(
    "column,value",
    [
        ("perimetro_referencia_posicao", 2),
        ("perimetro_referencia_posicao", None),
        ("administrativo_referencia_posicao", 0),
    ],
)
def test_incra_vinculos_invalid_reference_ordinal(contract, national, column, value):
    national.loc[0, column] = value
    assert not contract.validate(national)[0]


def test_incra_vinculos_cartesian_all_pairs(contract, four_pairs):
    assert len(four_pairs) == 4
    assert contract.validate(four_pairs) == (True, [])


def test_incra_vinculos_missing_pair_with_all_parents_represented(contract, four_pairs):
    missing = four_pairs.drop(index=1).reset_index(drop=True)
    assert missing["perimetro_posicao"].nunique() == 2
    assert missing["administrativo_numero_publicado"].nunique() == 2
    assert not contract.validate(missing)[0]


def test_incra_vinculos_duplicate_pair_same_attributes(contract, four_pairs):
    repeated = pd.concat([four_pairs.iloc[:1], four_pairs], ignore_index=True)
    assert not contract.validate(repeated)[0]


@pytest.mark.parametrize(
    "column,value",
    [
        ("ocorrencias_perimetro_referencia", 0),
        ("ocorrencias_administrativo_referencia", 2),
        ("referencia_repetida", True),
    ],
)
def test_incra_vinculos_inconsistent_counters(contract, national, column, value):
    national.loc[0, column] = value
    assert not contract.validate(national)[0]


@pytest.mark.parametrize("value", [None, "", " \n ", "NULL", "atípico"])
def test_incra_vinculos_null_empty_conflation_rejected(contract, rows, value):
    one = _frame([_unrecognized(rows[0], value)], list(rows[0]))
    one.loc[0, "referencia_literal"] = "" if value is None else None
    assert not contract.validate(one)[0]


@pytest.mark.parametrize("column,index", [("perimetro_nome", 1), ("administrativo_comunidade", 2)])
def test_incra_vinculos_conflicting_parent_text(contract, four_pairs, column, index):
    four_pairs.loc[index, column] = "alterado"
    assert not contract.validate(four_pairs)[0]


def test_incra_vinculos_reversed_row_order(contract, national):
    assert not contract.validate(national.iloc[::-1].reset_index(drop=True))[0]


def test_incra_vinculos_reversed_column_order(contract, national):
    assert not contract.validate(national[list(reversed(national.columns))])[0]


def test_incra_vinculos_parent_datetime_without_utc_rejected(contract, national):
    national["perimetro_data_cadastro"] = national["perimetro_data_cadastro"].dt.tz_localize(None)
    assert not contract.validate(national)[0]


@pytest.mark.parametrize("omit_second", [False, True])
def test_incra_vinculos_two_distinct_tokens_same_parents(contract, rows, omit_second):
    distinct = _pair_rows(rows[0], left=1, right=1)
    distinct.append(copy.deepcopy(distinct[0]))
    for index, row in enumerate(distinct):
        row.update(
            perimetro_processo=NUP + "\n" + NUP2,
            administrativo_processo=NUP + "\n" + NUP2,
            perimetro_referencia_posicao=index + 1,
            administrativo_referencia_posicao=index + 1,
            referencia_literal=(NUP, NUP2)[index],
        )
    candidate = _frame(distinct[:1] if omit_second else distinct, list(rows[0]))
    assert contract.validate(candidate)[0] is not omit_second


@pytest.mark.parametrize(
    "column,dtype",
    [
        ("perimetro_data_titulo", "object"),
        ("referencia_repetida", "object"),
        ("perimetro_codigo", "float64"),
        ("perimetro_area_ha", "Float64"),
    ],
)
def test_incra_vinculos_alternative_dtype_rejected(contract, national, column, dtype):
    national[column] = national[column].astype(dtype)
    assert not contract.validate(national)[0]


def test_incra_vinculos_duplicate_column_names(contract, national):
    duplicated = national.columns[0]
    national.columns = [duplicated, *national.columns[:-1]]
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(national, contract)
    assert erro.value.violation == f"Duplicate column labels: [{duplicated!r}]"
