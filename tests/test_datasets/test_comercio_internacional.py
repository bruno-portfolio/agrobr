from __future__ import annotations

import inspect

from agrobr import contracts, datasets
from agrobr.datasets.deterministic import deterministic
from tests.helpers import collect_failures, fixture_instance, isolated_dataset_case
from tests.test_datasets.conftest import replay_http

_case_fixture_replay_http = inspect.unwrap(replay_http)


async def test_comercio_internacional_casos_1(captures):
    with collect_failures() as check:
        case = "test_public_dataset_replay_preserves_source_context_and_contract"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(
                _case_fixture_replay_http, captures=captures, monkeypatch=monkeypatch
            ) as replay_http,
        ):
            replay_http()
            frame, meta = await datasets.comercio_internacional(
                "1201",
                declarante="BR",
                parceiro="CN",
                periodo=2023,
                exigir_completo=True,
                return_meta=True,
            )
            assert len(frame) == 1 and len(frame.columns) == 27
            assert frame.iloc[0]["peso_liquido_kg"] == 74471954170.0
            assert meta.dataset == "comercio_internacional"
            assert meta.schema_version == meta.contract_version == "3.0"
            assert meta.selected_source == "comtrade_guest"
            assert meta.attempted_sources == ["comtrade_guest"]
            assert meta.source_details["coverage"]["state"] == "complete"
            assert meta.raw_content_hash
            assert meta.raw_content_size > 0
            assert meta.fetched_at.tzinfo is not None
            contracts.validate_dataset(frame, "comercio_internacional")
        case = "test_dataset_empty_retains_all_27_columns_and_types"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(
                _case_fixture_replay_http, captures=captures, monkeypatch=monkeypatch
            ) as replay_http,
        ):
            replay_http()
            frame, meta = await datasets.comercio_internacional(
                "1201", parceiro="999", periodo=2023, return_meta=True
            )
            assert frame.empty and len(frame.columns) == 27
            assert str(frame["mes"].dtype) == "Int64"
            assert str(frame["classificacao_original"].dtype) == "boolean"
            assert str(frame["valor_fob_usd"].dtype) == "float64"
            assert meta.records_count == 0
            assert meta.source_details["coverage"]["state"] == "complete"
            contracts.validate_dataset(frame, "comercio_internacional")


async def test_comercio_internacional_casos_2(captures):
    with collect_failures() as check:
        case = "test_snapshot_selects_year_without_freezing_or_truncating_publication"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(
                _case_fixture_replay_http, captures=captures, monkeypatch=monkeypatch
            ) as replay_http,
        ):
            requests, _ = replay_http()
            async with deterministic("2023-06-15"):
                try:
                    frame, meta = await datasets.comercio_internacional(
                        "1201", parceiro="CN", return_meta=True
                    )
                except Exception as erro:
                    raise AssertionError(f"snapshot não virou o período: {erro!r}") from erro
            assert frame["periodo"].tolist() == ["2023"]
            assert meta.snapshot == "2023-06-15"
            assert all(request.url.params["period"] == "2023" for request in requests)
            assert meta.source_details["query"]["periods"] == ["2023"]
        case = "test_explicit_period_takes_precedence_over_snapshot_year"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(
                _case_fixture_replay_http, captures=captures, monkeypatch=monkeypatch
            ) as replay_http,
        ):
            replay_http()
            async with deterministic("2022-01-01"):
                frame, meta = await datasets.comercio_internacional(
                    "1201", parceiro="CN", periodo=2023, return_meta=True
                )
            assert frame["periodo"].tolist() == ["2023"]
            assert meta.source_details["query"]["periods"] == ["2023"]
