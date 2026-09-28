from __future__ import annotations

import io
import re
import sys
import warnings
from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pandas as pd
import pytest

from agrobr import constants, contracts
from agrobr.alt.antt_pedagio import _fluxo, _memory, parser, query
from agrobr.exceptions import ContractViolationError, ResourceLimitError
from tests.helpers import install_anttpedagio_source

HEADER = "concessionaria;mes_ano;sentido;praca;tipo_cobranca;categoria_eixo;tipo_de_veiculo;volume_total\n"
PLAZAS = (
    "concessionaria;praca_de_pedagio;rodovia;uf;municipio\nEmpresa;Praça;BR-101;sc;Município\n"
).encode("cp1252")


def selected(**overrides: Any) -> query.FluxoQuery:
    parameters = {
        "ano": 2026,
        "ano_inicio": None,
        "ano_fim": None,
        "frequencia": "mensal",
        "concessionaria": None,
        "rodovia": None,
        "uf": None,
        "praca": None,
        "tipo_veiculo": None,
        "tipo_cobranca": None,
        "data_inicio": None,
        "data_fim": None,
        "apenas_pesados": False,
        "enriquecer": False,
        "max_linhas": 1000,
        "max_memoria_bytes": 16 * 1024**2,
    }
    parameters.update(overrides)
    return query.build_query(**parameters)


def csv_body(*rows: str) -> bytes:
    return (HEADER + "\n".join(rows) + ("\n" if rows else "")).encode("cp1252")


def install(monkeypatch: Any, body: bytes, **kwargs: Any) -> Any:
    return install_anttpedagio_source(monkeypatch, {"trafego_2026_mensal.csv": body}, **kwargs)


@pytest.mark.asyncio
async def test_geographic_filter_ignores_non_candidate_but_validates_it(monkeypatch: Any):
    body = csv_body(
        "Empresa;01/2026;N;Praça;Manual;2;Passeio;1,00",
        "Outra;01/2026;N;SemCadastro;Manual;2;Passeio;2,00",
    )
    install(monkeypatch, body, plazas=PLAZAS)
    frame, meta = await _fluxo.fetch(
        selected(enriquecer=True, uf="SC", concessionaria="empresa"),
        as_polars=False,
        return_meta=True,
    )
    assert len(frame) == 1
    assert meta.source_details["coverage"]["source_validated_rows"] == 2


@pytest.mark.asyncio
async def test_heavy_ambiguous_candidate_is_excluded_with_warning(monkeypatch: Any):
    install(monkeypatch, csv_body("Empresa;01/2026;N;Praça;Manual;19;Comercial;1,00"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame, meta = await _fluxo.fetch(
            selected(apenas_pesados=True), as_polars=False, return_meta=True
        )
    assert any(re.search("pesado indeterminada", str(item.message)) for item in caught), caught
    assert frame.empty
    assert meta.source_details["heavy_filter"]["unknown_rows_excluded"] == 1
    assert meta.source_details["heavy_filter"]["unknown_volume_excluded"] == 1
    assert any("apenas_pesados" in message for message in meta.validation_warnings)


@pytest.mark.asyncio
async def test_accumulated_row_limit_includes_multiple_years(monkeypatch: Any):
    files = {
        "trafego_2025_mensal.csv": csv_body("Empresa;01/2025;N;Praça;Manual;2;Passeio;1,00"),
        "trafego_2026_mensal.csv": csv_body("Empresa;01/2026;N;Praça;Manual;2;Passeio;2,00"),
    }
    install_anttpedagio_source(monkeypatch, files)
    with pytest.raises(ResourceLimitError, match="grupos de saída"):
        await _fluxo.fetch(
            selected(ano=None, ano_inicio=2025, ano_fim=2026, max_linhas=1),
            as_polars=False,
            return_meta=False,
        )


def test_concat_memory_guard_runs_before_allocation(monkeypatch: Any):
    frame = pd.DataFrame({"value": ["x" * 1000]})
    state = _fluxo.Pipeline(selected(max_memoria_bytes=1024))
    monkeypatch.setattr(
        pd, "concat", lambda *_a, **_k: pytest.fail("concat allocated before budget")
    )
    with pytest.raises(ResourceLimitError, match="concat_sort_preallocation"):
        _fluxo._assemble([frame, frame], state)


@pytest.mark.asyncio
async def test_small_memory_budget_fails_without_returning_partial(monkeypatch: Any):
    install(monkeypatch, csv_body("Empresa;01/2026;N;Praça;Manual;2;Passeio;1,00"))
    with pytest.raises(ResourceLimitError, match="retenção estimada") as caught:
        await _fluxo.fetch(selected(max_memoria_bytes=1), as_polars=False, return_meta=False)
    completed = getattr(caught.value, "antt_completed_sources", None)
    assert completed is not None and len(completed) == 1
    assert completed[0]["role"] == "trafego"
    assert completed[0]["acquisition"]["complete"]
    assert all(attempt["closed"] for attempt in completed[0]["acquisition"]["attempts"])


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
async def test_polars_exact_int64_null_schema_without_pyarrow(monkeypatch: Any, empty: bool):
    pl = pytest.importorskip("polars")
    body = csv_body("Empresa;01/2026;N;Praça;Manual;19;Comercial;9007199254740993,00")
    install(monkeypatch, body)
    for name in ("from_pandas", "from_arrow"):
        monkeypatch.setattr(pl, name, Mock(side_effect=RuntimeError(f"{name} is not permitted")))
    try:
        frame, meta = await _fluxo.fetch(
            selected(concessionaria="ausente" if empty else None), as_polars=True, return_meta=True
        )
    except Exception as exc:
        frame, meta = exc, None
    assert not isinstance(frame, Exception), frame
    assert frame.columns == list(constants.ANTT_FLUXO_COLUMNS)
    assert frame.schema["volume"] == pl.Int64
    assert frame.schema["n_eixos"] == pl.Int64
    assert frame.schema["data"] == pl.Datetime("ns")
    assert frame.schema["rodovia"] == pl.Utf8
    assert meta.source_details["output_dtypes"] == {
        name: str(dtype) for name, dtype in frame.schema.items()
    }
    assert frame["volume"].to_list() == ([] if empty else [9007199254740993])
    assert frame["n_eixos"].to_list() == ([] if empty else [None])
    assert frame["data"].cast(pl.Int64).to_list() == ([] if empty else [1767225600000000000])


def test_shared_unicode_retention_and_accounting_budget_boundary():
    text = "á文字" * 200
    frame = pd.DataFrame({"value": pd.Series([text] * 1000, dtype="string[python]")})
    usage = _memory.frame_usage([frame], max_bytes=10**8)
    assert usage.resident_bytes < int(frame.memory_usage(deep=True).sum()) // 10
    assert usage.accounting_peak_bytes > usage.resident_bytes
    assert _memory.frame_usage([frame], max_bytes=usage.accounting_peak_bytes) == usage
    with pytest.raises(ResourceLimitError, match="contabilidade"):
        _memory.frame_usage([frame], max_bytes=usage.accounting_peak_bytes - 1)


@pytest.mark.parametrize("dtype", ["string[python]", "object"])
def test_equal_unpooled_strings_are_counted_as_separate_objects(dtype: str):
    text = "literal ç" * 100
    values = [text.encode().decode() for _index in range(100)]
    frame = pd.DataFrame({"value": pd.Series(values, dtype=dtype)})
    assert len({id(value) for value in frame["value"]}) == 100
    usage = _memory.frame_usage([frame], max_bytes=10**8)
    assert usage.resident_bytes >= sum(sys.getsizeof(value) for value in values)
    with pytest.raises(ResourceLimitError):
        _memory.frame_usage([frame], max_bytes=usage.resident_bytes - 1)


def test_single_frame_assembly_avoids_concat_and_preserves_sort(monkeypatch: Any):
    frame = parser.parse_trafego_file(
        io.BytesIO(
            csv_body(
                "Empresa;02/2026;N;Praça;Manual;19;Comercial;2,00",
                "Empresa;01/2026;N;Praça;Manual;19;Comercial;1,00",
            )
        ),
        ano=2026,
        frequencia="mensal",
    ).frame
    monkeypatch.setattr(pd, "concat", Mock(side_effect=RuntimeError("Redundant concat")))
    frames = [frame]
    try:
        result = _fluxo._assemble(frames, _fluxo.Pipeline(selected()))
    except RuntimeError as exc:
        result = exc
    assert not isinstance(result, RuntimeError), result
    assert not frames
    assert result["volume"].tolist() == [1, 2]


def test_concat_scratch_budget_precedes_allocation(monkeypatch: Any):
    rows = (f"Empresa;01/2026;N;P{index};Manual;2;Passeio;1,00" for index in range(200))
    frame = parser.parse_trafego_file(
        io.BytesIO(csv_body(*rows)), ano=2026, frequencia="mensal"
    ).frame
    peak = _memory.frame_usage([frame, frame], max_bytes=10**9).accounting_peak_bytes
    state = _fluxo.Pipeline(selected(max_memoria_bytes=peak + 4096))
    monkeypatch.setattr(pd, "concat", Mock(side_effect=RuntimeError("concat allocated")))
    try:
        _fluxo._assemble([frame, frame], state)
    except Exception as exc:
        caught = exc
    else:
        caught = None
    assert isinstance(caught, ResourceLimitError), caught
    assert "concat_sort_preallocation" in str(caught)


@pytest.mark.asyncio
async def test_contract_violation_is_raised(monkeypatch: Any):
    install(monkeypatch, csv_body("Empresa;01/2026;N;Praça;Manual;2;Passeio;1,00"))
    contract = contracts.get_contract("antt_pedagio_fluxo")
    monkeypatch.setattr(type(contract), "validate", lambda _self, _frame: (False, ["sintética"]))
    try:
        await _fluxo.fetch(selected(), as_polars=False, return_meta=False)
    except Exception as exc:
        caught = exc
    else:
        caught = None
    assert isinstance(caught, ContractViolationError) and "sintética" in str(caught), caught


def test_polars_memory_guard_precedes_python_bridge():
    pytest.importorskip("polars")
    frame = pd.DataFrame({"value": ["x" * 1000]})
    state = _fluxo.Pipeline(selected())
    state.validated = replace(state.validated, max_memoria_bytes=1)
    with pytest.raises(ResourceLimitError, match="polars_bridge_preallocation"):
        _fluxo._polars(frame, state)


def test_polars_bridge_releases_previous_python_list(monkeypatch: Any):
    pl = pytest.importorskip("polars")
    frame = parser.parse_trafego_file(
        io.BytesIO(csv_body("Empresa;01/2026;N;Praça;Manual;19;Comercial;9007199254740993,00")),
        ano=2026,
        frequencia="mensal",
    ).frame
    previous = None
    calls = []
    original = pl.Series

    def series(name: str, values: Any, **kwargs: Any) -> Any:
        nonlocal previous
        if previous is not None:
            assert sys.getrefcount(previous) == 2
        previous = values
        calls.append(name)
        return original(name, values, **kwargs)

    proxy = SimpleNamespace(
        Series=series, DataFrame=pl.DataFrame, Int64=pl.Int64, Utf8=pl.Utf8, Datetime=pl.Datetime
    )
    original_import = _fluxo.importlib.import_module
    monkeypatch.setattr(
        _fluxo.importlib,
        "import_module",
        lambda name: proxy if name == "polars" else original_import(name),
    )
    state = _fluxo.Pipeline(selected())
    result = _fluxo._polars(frame, state)
    assert calls == list(constants.ANTT_FLUXO_COLUMNS)
    assert result["volume"].to_list() == [9007199254740993]
    assert result["data"].cast(pl.Int64).to_list() == [1767225600000000000]
    assert all(
        item["estimated_bytes"] <= state.validated.max_memoria_bytes for item in state.memory_checks
    )


def test_polars_column_budget_stops_before_list_allocation(monkeypatch: Any):
    pytest.importorskip("polars")
    frame = parser.parse_trafego_file(
        io.BytesIO(csv_body("Empresa;01/2026;N;Praça;Manual;19;Comercial;1,00")),
        ano=2026,
        frequencia="mensal",
    ).frame
    limit = _memory.frame_usage([frame], max_bytes=10**8).accounting_peak_bytes + 1
    state = _fluxo.Pipeline(selected(max_memoria_bytes=limit))
    monkeypatch.setattr(
        _fluxo, "_polars_column", lambda *_a: pytest.fail("List allocated before guard")
    )
    with pytest.raises(ResourceLimitError, match="polars_column_preallocation"):
        _fluxo._polars(frame, state)


def test_polars_datetime_column_preserves_single_ns_preepoch_and_null():
    pl = pytest.importorskip("polars")
    values = pd.Series(
        [pd.Timestamp(-1, unit="ns"), pd.Timestamp(1, unit="ns"), pd.NaT], dtype="datetime64[ns]"
    )
    result = _fluxo._polars_column(pl, values, "data", pl.Datetime("ns"))
    assert result.dtype == pl.Datetime("ns")
    assert result.cast(pl.Int64).to_list() == [-1, 1, None]


@pytest.mark.asyncio
async def test_heavy_filter_distinguishes_proven_range_and_ambiguous_category(monkeypatch: Any):
    body = csv_body(
        "Empresa;01/2026;N;Praça;Manual;Veículo Comercial Acima de 10 Eixos;Comercial;1,00",
        "Empresa;01/2026;N;Praça;Manual;2;Passeio;2,00",
        "Empresa;01/2026;N;Praça;Manual;3 eixos;Comercial;3,00",
    )
    install(monkeypatch, body)
    frame, _ = await _fluxo.fetch(selected(apenas_pesados=True), as_polars=False, return_meta=True)
    assert sorted(frame["volume"].tolist()) == [1, 3]
    assert frame["n_eixos"].isna().sum() == 1
