from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from agrobr import contracts, datasets
from agrobr.datasets import base
from agrobr.models import MetaInfo
from tests import helpers

FIELDS = [
    "raw_content_hash",
    "raw_content_size",
    "cache_key",
    "cache_expires_at",
    "fetch_duration_ms",
    "parse_duration_ms",
]
CONSUMERS = ["preco_diario", "clima", "comercio_internacional", "defensivos_formulados"]


@pytest.fixture
def source_meta():
    acquired = datetime(2026, 9, 6, 12, 30, tzinfo=UTC)
    return MetaInfo(
        source="selected",
        source_method="fixture",
        source_url="https://example.org/published.csv",
        fetched_at=acquired,
        fetch_timestamp=acquired + timedelta(seconds=2),
        parser_version=3,
        raw_content_hash="a" * 64,
        raw_content_size=12345,
        cache_key="origin/key",
        cache_expires_at=acquired + timedelta(hours=24),
        fetch_duration_ms=321,
        parse_duration_ms=87,
        validation_warnings=["published diagnostic"],
        source_details={
            "resources": [{"url": "https://example.org/published.csv", "sha256": "a" * 64}],
            "query": {"selection": ["one"]},
        },
    )


@pytest.mark.parametrize(
    "scenario,parameters",
    [
        (
            "test_consumers_preserve_all_six_selected_source_fields",
            {"name": "preco_diario", "from_cache": False},
        ),
        (
            "test_consumers_preserve_all_six_selected_source_fields",
            {"name": "preco_diario", "from_cache": True},
        ),
        (
            "test_consumers_preserve_all_six_selected_source_fields",
            {"name": "clima", "from_cache": False},
        ),
        (
            "test_consumers_preserve_all_six_selected_source_fields",
            {"name": "clima", "from_cache": True},
        ),
        (
            "test_consumers_preserve_all_six_selected_source_fields",
            {"name": "comercio_internacional", "from_cache": False},
        ),
        (
            "test_consumers_preserve_all_six_selected_source_fields",
            {"name": "comercio_internacional", "from_cache": True},
        ),
        (
            "test_consumers_preserve_all_six_selected_source_fields",
            {"name": "defensivos_formulados", "from_cache": False},
        ),
        (
            "test_consumers_preserve_all_six_selected_source_fields",
            {"name": "defensivos_formulados", "from_cache": True},
        ),
        ("test_source_details_and_warnings_are_deeply_independent", {"name": "preco_diario"}),
        ("test_source_details_and_warnings_are_deeply_independent", {"name": "clima"}),
        (
            "test_source_details_and_warnings_are_deeply_independent",
            {"name": "comercio_internacional"},
        ),
        (
            "test_source_details_and_warnings_are_deeply_independent",
            {"name": "defensivos_formulados"},
        ),
    ],
    ids=[
        "consumers_preserve_all_six_selected_source_fields-0",
        "consumers_preserve_all_six_selected_source_fields-1",
        "consumers_preserve_all_six_selected_source_fields-2",
        "consumers_preserve_all_six_selected_source_fields-3",
        "consumers_preserve_all_six_selected_source_fields-4",
        "consumers_preserve_all_six_selected_source_fields-5",
        "consumers_preserve_all_six_selected_source_fields-6",
        "consumers_preserve_all_six_selected_source_fields-7",
        "source_details_and_warnings_are_deeply_independent-0",
        "source_details_and_warnings_are_deeply_independent-1",
        "source_details_and_warnings_are_deeply_independent-2",
        "source_details_and_warnings_are_deeply_independent-3",
    ],
)
def test_metadados_da_fonte(scenario: str, parameters: dict[str, Any], source_meta: Any):
    with (
        helpers.collect_failures() as check,
        check((scenario, parameters)),
        helpers.isolated_dataset_case((scenario, parameters)),
    ):
        if scenario == "test_consumers_preserve_all_six_selected_source_fields":
            name = parameters["name"]
            from_cache = parameters["from_cache"]
            source_meta.from_cache = from_cache
            frame = pd.DataFrame({"value": [1]})
            instance = datasets.get_dataset(name)
            meta = instance._build_meta(frame, "selected", source_meta, ["selected"], None)
            assert {field: getattr(meta, field) for field in FIELDS} == {
                field: getattr(source_meta, field) for field in FIELDS
            }
            assert meta.from_cache is from_cache
            assert meta.fetched_at == source_meta.fetched_at
            assert meta.selected_source == "selected" and meta.attempted_sources == ["selected"]
            assert meta.dataset == name and meta.records_count == 1 and (meta.columns == ["value"])
            assert meta.validation_warnings == source_meta.validation_warnings
            assert meta.parser_version == 3
            assert meta.source_url == source_meta.source_url
        elif scenario == "test_source_details_and_warnings_are_deeply_independent":
            name = parameters["name"]
            instance = datasets.get_dataset(name)
            one = instance._build_meta(pd.DataFrame(), "selected", source_meta, ["selected"], None)
            two = instance._build_meta(pd.DataFrame(), "selected", source_meta, ["selected"], None)
            one.source_details["resources"][0]["sha256"] = "outside"
            one.source_details["query"]["selection"].append("outside")
            one.validation_warnings.append("outside")
            assert two.source_details == source_meta.source_details
            assert source_meta.source_details["resources"][0]["sha256"] == "a" * 64
            assert source_meta.source_details["query"]["selection"] == ["one"]
            assert (
                two.validation_warnings
                == source_meta.validation_warnings
                == ["published diagnostic"]
            )


def test_nested_cache_provenance_preserves_origin_fields_and_fetch_time(source_meta):
    source_meta.attempted_sources = ["remote", "cache"]
    source_meta.selected_source = "cache"
    source_meta.from_cache = True
    meta = datasets.get_dataset("preco_diario")._build_meta(
        pd.DataFrame(), "cepea", source_meta, ["cepea"], None
    )
    assert meta.attempted_sources == ["remote", "cache"] and meta.selected_source == "cache"
    assert meta.from_cache and meta.fetched_at == source_meta.fetched_at
    assert meta.fetch_timestamp == source_meta.fetch_timestamp
    assert all(getattr(meta, field) == getattr(source_meta, field) for field in FIELDS)


async def test_disabled_source_is_skipped_without_entering_provenance(source_meta: MetaInfo):
    instance = type(datasets.get_dataset("defensivos_tecnicos"))()
    frame = pd.DataFrame({"value": [1]})
    disabled = AsyncMock(return_value=(pd.DataFrame({"value": [2]}), source_meta))
    enabled = AsyncMock(return_value=(frame, source_meta))
    instance.info = replace(
        instance.info,
        sources=[
            base.DatasetSource("disabled", 1, disabled, enabled=False),
            base.DatasetSource("selected", 2, enabled),
        ],
    )
    result, source, acquired, attempted = await instance._try_sources("")
    assert result is frame and source == "selected" and acquired is source_meta
    assert attempted == ["selected"]
    assert disabled.await_count == 0 and enabled.await_count == 1


@pytest.mark.parametrize("return_meta", [False, True])
async def test_output_format_forwards_frame_metadata_and_flags(
    return_meta: bool, source_meta: MetaInfo, monkeypatch: pytest.MonkeyPatch
):
    frame = pd.DataFrame({"value": [1]})
    original = (frame, source_meta) if return_meta else frame
    fetch = AsyncMock(return_value=original)
    converted = object()
    finalize = Mock(return_value=converted)
    monkeypatch.setattr(base.result_utils, "finalize_result", finalize)
    wrapped = base._with_output_format(fetch)
    instance = datasets.get_dataset("preco_diario")
    assert await wrapped(instance, "soja") is original
    assert finalize.call_count == 0
    actual = await wrapped(instance, "soja", as_polars=True)
    assert actual is converted
    assert finalize.call_count == 1
    assert finalize.call_args.args[0] is frame
    assert finalize.call_args.args[1] is (source_meta if return_meta else None)
    assert finalize.call_args.kwargs == {"as_polars": True, "return_meta": return_meta}


async def test_as_polars_data_toda_nula_sai_datetime_pelo_contrato():
    pl = pytest.importorskip("polars")
    frame = pd.DataFrame({"data_inicio": pd.Series([pd.NA], dtype=object)})
    wrapped = base._with_output_format(AsyncMock(return_value=frame))
    resultado = await wrapped(datasets.get_dataset("embarques_anec"), as_polars=True)
    assert resultado["data_inicio"].null_count() == 1
    assert resultado.schema["data_inicio"] == pl.Datetime("ns")


async def test_as_polars_meta_columns_segue_a_ordem_do_contrato_na_saida(source_meta):
    pl = pytest.importorskip("polars")
    contrato = contracts.get_contract("embarques_anec").list_columns()
    invertidas = [*reversed(contrato[:3]), "extra"]
    frame = pl.DataFrame({nome: pl.Series([], dtype=pl.Utf8) for nome in invertidas})
    meta = replace(source_meta, columns=list(frame.columns))

    async def nativo(_dataset, *, as_polars=False, return_meta=False):
        assert as_polars and return_meta
        return frame, meta

    wrapped = base._with_output_format(nativo)

    resultado, saida = await wrapped(
        datasets.get_dataset("embarques_anec"), as_polars=True, return_meta=True
    )

    assert resultado.columns == [*contrato[:3], "extra"]
    assert saida.columns == resultado.columns
