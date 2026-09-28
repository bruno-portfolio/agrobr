from __future__ import annotations

import time
from dataclasses import replace
from datetime import UTC, datetime

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.comexstat import models, result, transport_models
from agrobr.exceptions import ContractViolationError, ResourceLimitError


@pytest.fixture
def inputs():
    frame = pd.DataFrame(
        {
            "ano": pd.Series([2026], dtype="Int64"),
            "mes": pd.Series([1], dtype="Int64"),
            "ncm": pd.Series(["12019000"], dtype="string[python]"),
            "uf": pd.Series(["MT"], dtype="string[python]"),
            "kg_liquido": pd.Series([1000], dtype="Int64"),
            "valor_fob_usd": pd.Series([100.0], dtype="float64"),
            "volume_ton": pd.Series([1.0], dtype="float64"),
        }
    )
    parsed = models.ParsedResource(
        frame,
        {
            "frame_resident_bytes": 4096,
            "eof_reached": True,
            "source_rows": 1,
            "validated_rows": 1,
            "selected_rows": 1,
            "parser_version": 2,
            "statistics": {"source": {"sum": "100.00"}},
        },
    )
    now = datetime.now(UTC)
    acquired = transport_models.DownloadedResource(
        transport_models.ResourceSpec(
            kind="annual",
            url="https://balanca.mdic.gov.br/balanca/bd/comexstat-bd/ncm/EXP_2026.csv",
            fluxo="exportacao",
            ano=2026,
        ),
        fetched_at=now,
        finished_at=now,
        complete=True,
        client_closed=True,
        spool_closed=True,
        size_bytes=123,
        sha256="a" * 64,
        tls={"hostname_verified": True},
    )
    return parsed, acquired, {"pedido": {"produto": "soja"}}


def finish(inputs, **kwargs):
    return result.finish(
        *inputs,
        "comexstat_exportacao_mensal",
        as_polars=kwargs.pop("as_polars", False),
        return_meta=kwargs.pop("return_meta", True),
        max_memoria_bytes=kwargs.pop("max_memoria_bytes", 16 * 1024**2),
        fetch_ms=5,
        parse_started=time.monotonic(),
        **kwargs,
    )


def test_conversion_failure_preserves_original_exception_and_cause(inputs, monkeypatch):
    cause = OSError("original import failure")
    primary = ImportError("optional unavailable")

    def fail(*_args):
        raise primary from cause

    monkeypatch.setattr(result, "_polars", fail)
    with pytest.raises(ImportError) as error:
        finish(inputs, as_polars=True)
    assert error.value is primary and error.value.__cause__ is cause
    assert error.value.comexstat_acquisition["complete"]


def test_full_parsing_metadata_budget_precedes_serialization(inputs, monkeypatch):
    inputs[0].details["diagnostic"] = "x" * (1024**2)

    def forbidden(*_args):
        pytest.fail("manifest serialization preceded the full metadata guard")

    monkeypatch.setattr(result, "_manifest_hash", forbidden)
    with pytest.raises(ResourceLimitError, match="metadados completos"):
        finish(inputs, max_memoria_bytes=4 * 1024**2)


def test_metadata_graph_exact_budget_and_one_byte_below():
    details = {"parsing": {"literal": "x" * 10000, "records": [1, 2, 3]}}
    graph, peak = result._metadata_estimate(details, 1000, 16 * 1024**2)
    assert graph > 10000
    assert result._metadata_estimate(details, 1000, 1000 + peak) == (graph, peak)
    with pytest.raises(ResourceLimitError):
        result._metadata_estimate(details, 1000, 999 + peak)


def test_secondary_provenance_error_cannot_replace_budget_error(inputs, monkeypatch):
    def fail():
        raise OSError("secondary provenance failure")

    monkeypatch.setattr(inputs[1], "details", fail)
    with pytest.raises(ResourceLimitError) as error:
        finish(inputs, max_memoria_bytes=1)
    assert "OSError" in error.value.__notes__[0]


@pytest.mark.parametrize("field", ["eof_reached", "validated_rows"])
def test_incomplete_validation_fails_without_metadata_request(inputs, field):
    inputs[0].details[field] = False if field == "eof_reached" else 0
    with pytest.raises(ContractViolationError, match="incompleta") as error:
        finish(inputs, return_meta=False)
    assert error.value.comexstat_query == inputs[2]


def test_polars_budget_counts_each_occurrence_of_shared_long_text():
    contract = contracts.get_contract("comexstat_dicionario_unidades")
    literal = "\u00e9" * 32768
    frame = pd.DataFrame(
        {
            name: pd.Series([literal] * 128, dtype="string[python]")
            for name in contract.list_columns()
        }
    )
    resident = 128 * len(frame.columns) * 8 + 100000
    estimate = result._polars_estimate(frame, contract, resident, 256 * 1024**2)
    utf8_payload = 128 * len(frame.columns) * len(literal.encode("utf-8"))
    assert estimate["polars_payload_estimated_bytes"] >= utf8_payload
    assert estimate["polars_bridge_peak_estimated_bytes"] > resident * 4
    assert (
        result._polars_estimate(
            frame, contract, resident, estimate["polars_bridge_peak_estimated_bytes"]
        )
        == estimate
    )
    with pytest.raises(ResourceLimitError):
        result._polars_estimate(
            frame, contract, resident, estimate["polars_bridge_peak_estimated_bytes"] - 1
        )
    with pytest.raises(ResourceLimitError, match="Polars"):
        result._polars_estimate(frame, contract, resident, resident * 4)


def test_polars_empty_and_null_output_dtypes(inputs):
    polars = pytest.importorskip("polars")
    inputs[0].frame.loc[0, ["kg_liquido", "valor_fob_usd", "volume_ton"]] = pd.NA
    output, meta = finish(inputs, as_polars=True)
    assert output["kg_liquido"].to_list() == [None]
    assert output["volume_ton"].to_list() == [None]
    assert output.schema["ncm"] == polars.Utf8
    assert output.schema["kg_liquido"] == polars.Int64
    assert meta.source_details["output_dtypes"] == {
        name: str(dtype) for name, dtype in output.schema.items()
    }
    empty_parsed = replace(
        inputs[0],
        frame=inputs[0].frame.iloc[:0].copy(),
        details={**inputs[0].details, "selected_rows": 0},
    )
    empty, empty_meta = finish((empty_parsed, *inputs[1:]), as_polars=True)
    assert empty.schema == output.schema
    assert empty.height == 0
    assert empty_meta.source_details["coverage"]["selected_rows"] == 0
    assert empty_meta.source_details["coverage"]["validated_rows"] == 1
    assert empty_meta.source_details["output_dtypes"] == meta.source_details["output_dtypes"]
