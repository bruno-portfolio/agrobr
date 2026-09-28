from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts import clima
from agrobr.exceptions import ContractViolationError
from agrobr.models import MetaInfo
from tests.helpers import levanta_exatamente


@pytest.fixture
def hourly_frame() -> pd.DataFrame:
    contract = clima.CLIMA_ESTACAO_HORARIA_V1
    frame = contract.empty_frame().reindex([0, 1])
    frame["data"] = pd.to_datetime(["2001-01-01", "2001-01-01"])
    frame["hora_utc"] = ["0000", "0100"]
    frame["estacao"] = ["A001", "A001"]
    frame["uf"] = ["DF", "DF"]
    return frame


def test_hourly_contract_keeps_two_hours_and_nullable_measurements(hourly_frame):
    contract = contracts.get_contract("clima_estacao_horaria")
    assert contract is clima.CLIMA_ESTACAO_HORARIA_V1
    assert contract.primary_key == ["data", "hora_utc", "estacao"]
    assert contract.version == "1.0"
    assert contract.validate(hourly_frame) == (True, [])
    assert hourly_frame["precipitacao_mm"].isna().all()


@pytest.mark.parametrize("hour", ["2400", "0030", "00:00", "000", "9999", 0, None])
def test_hourly_contract_rejects_invalid_utc_hour(hour, hourly_frame):
    hourly_frame["hora_utc"] = pd.Series([hour, "0100"], dtype=object)
    valid, errors = clima.CLIMA_ESTACAO_HORARIA_V1.validate(hourly_frame)
    assert not valid
    assert any("hora_utc" in error for error in errors)


def test_source_details_default_does_not_change_old_serialization():
    meta = MetaInfo(
        source="inmet",
        source_url="https://example.test/2001.zip",
        source_method="httpx+zip+csv",
        fetched_at=datetime(2026, 9, 6, tzinfo=UTC),
    )
    assert meta.source_details == {}
    assert "source_details" not in meta.to_dict()
    assert MetaInfo.from_dict(meta.to_dict()).source_details == {}


def test_source_details_roundtrip_keeps_nested_context_and_copies_export():
    details = {
        "time_basis": "UTC",
        "resources": [{"url": "https://example.test/2001.zip", "members": ["A001.CSV"]}],
        "spatial_aggregation": {"precip_acum_mm": "mean_station_monthly_totals"},
    }
    meta = MetaInfo(
        source="inmet",
        source_url="https://example.test/2001.zip",
        source_method="httpx+zip+csv",
        fetched_at=datetime(2026, 9, 6, tzinfo=UTC),
        source_details=details,
    )
    exported = meta.to_dict()
    restored = MetaInfo.from_dict(exported)
    assert restored.source_details == details
    exported["source_details"]["resources"][0]["members"].append("changed.CSV")
    assert meta.source_details["resources"][0]["members"] == ["A001.CSV"]


def test_hourly_contract_reports_duplicate_columns(hourly_frame):
    frame = pd.concat([hourly_frame, hourly_frame[["hora_utc"]]], axis=1)
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(frame, clima.CLIMA_ESTACAO_HORARIA_V1)
    assert erro.value.violation == "Duplicate column labels: ['hora_utc']"
