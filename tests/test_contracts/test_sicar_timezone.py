from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts import sicar
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente

CAPTURE_PATH = (
    Path(__file__).parent.parent
    / "golden_data"
    / "sicar"
    / "selecao_20260906"
    / "df_same_record.json"
)


@pytest.fixture
def captured_frame() -> pd.DataFrame:
    payload = json.loads(CAPTURE_PATH.read_bytes())
    frame = pd.DataFrame([feature["properties"] for feature in payload["features"]]).rename(
        columns={
            "status_imovel": "status",
            "dat_criacao": "data_criacao",
            "area": "area_ha",
            "m_fiscal": "modulos_fiscais",
            "tipo_imovel": "tipo",
        }
    )
    for column in ("data_criacao", "data_atualizacao"):
        frame[column] = pd.to_datetime(frame[column], utc=True)
    return frame


def test_sicar_v2_empty_frame_is_valid_and_utc():
    contract = sicar.SICAR_IMOVEIS_V2
    frame = contract.empty_frame()
    assert frame.empty
    for column in ("data_criacao", "data_atualizacao"):
        assert str(frame[column].dtype) == "datetime64[ns, UTC]"
    assert contract.validate(frame) == (True, [])


@pytest.mark.parametrize("column", ["data_criacao", "data_atualizacao"])
@pytest.mark.parametrize("timezone", [None, "Etc/GMT+3"])
@pytest.mark.parametrize("contents", ["observed", "all_na", "empty"])
def test_sicar_v2_rejects_non_utc_datetime_dtype(column, timezone, contents, captured_frame):
    frame = captured_frame.copy()
    if contents == "empty":
        frame = frame.iloc[:0].copy()
    if contents == "all_na":
        frame[column] = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    frame[column] = (
        frame[column].dt.tz_localize(None)
        if timezone is None
        else frame[column].dt.tz_convert(timezone)
    )

    valid, errors = sicar.SICAR_IMOVEIS_V2.validate(frame)

    assert not valid
    assert any(column in error and "UTC" in error for error in errors)


def test_sicar_v2_accepts_utc_null_update(captured_frame):
    captured_frame["data_atualizacao"] = pd.Series(
        pd.NaT, index=captured_frame.index, dtype="datetime64[ns, UTC]"
    )
    assert sicar.SICAR_IMOVEIS_V2.validate(captured_frame) == (True, [])


@pytest.mark.parametrize(
    "column,value",
    [
        ("status", "ativo"),
        ("status", "at"),
        ("status", None),
        ("tipo", "rural"),
        ("tipo", ""),
        ("tipo", False),
        ("uf", "XX"),
        ("uf", "df"),
        ("uf", None),
    ],
)
def test_sicar_v2_rejects_invalid_domain_values(column, value, captured_frame):
    captured_frame[column] = pd.Series([value], dtype=object)

    valid, errors = sicar.SICAR_IMOVEIS_V2.validate(captured_frame)

    assert not valid
    assert any(column in error and "invalid SICAR value" in error for error in errors)


@pytest.mark.parametrize("column", ["cod_imovel", "municipio"])
@pytest.mark.parametrize("value", ["", " \t ", None, 123])
def test_sicar_v2_rejects_empty_or_nontext_identifiers(column, value, captured_frame):
    captured_frame[column] = pd.Series([value], dtype=object)

    valid, errors = sicar.SICAR_IMOVEIS_V2.validate(captured_frame)

    assert not valid
    assert any(column in error and "non-empty strings" in error for error in errors)


def test_sicar_v2_duplicate_column_labels_are_reported(captured_frame):
    duplicated = pd.concat([captured_frame, captured_frame[["data_atualizacao"]]], axis=1)
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(duplicated, sicar.SICAR_IMOVEIS_V2)
    assert erro.value.violation == "Duplicate column labels: ['data_atualizacao']"
