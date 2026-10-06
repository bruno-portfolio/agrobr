from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from agrobr import datasets, nasa_power
from agrobr.inmet import client as inmet_client
from tests.helpers import (
    assert_replay_samples,
    assert_replay_served,
    assert_replay_structure,
    collect_failures,
    fixture_instance,
    install_replay_http,
    isolated_dataset_case,
)

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/reconciliacao_clima_20260918"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
CASES = {case["id"]: case for case in MANIFEST["cases"]}


@pytest.fixture(autouse=True)
def _fresh_inmet_cache():
    for ano in (2000, 2001, 2026):
        inmet_client.invalidate_historico(ano)
    yield
    for ano in (2000, 2001, 2026):
        inmet_client.invalidate_historico(ano)


_case_fixture__fresh_inmet_cache = inspect.unwrap(_fresh_inmet_cache)


async def test_clima_reconciliacao_r10_casos_1():
    with collect_failures() as check:
        for case_id in ["inmet_hist_df_2001_mensal", "inmet_hist_go_2001_mensal"]:
            case = f"test_clima_uf_mensal_from_official_zip[{(case_id,)!r}]"
            with (
                check(case),
                isolated_dataset_case(case) as monkeypatch,
                fixture_instance(_case_fixture__fresh_inmet_cache) as _fresh_inmet_cache,
            ):
                case = CASES[case_id]
                seen = install_replay_http(monkeypatch, case, GOLDEN)
                frame, meta = await datasets.clima(**case["selection"], return_meta=True)
                assert_replay_served(seen)
                assert len(frame) == case["period"]["rows"]
                assert sorted(frame["mes"].dt.strftime("%Y-%m").unique()) == case["period"]["meses"]
                assert_replay_structure(frame, case)
                assert_replay_samples(frame, case)
                assert meta.selected_source == "inmet_historico"
                assert meta.source_details["spatial_scope"] == "stations"
                assert meta.source_details["station_selection"] == "archive_members"
                assert "points" not in meta.source_details
                assert "deterministic" not in meta.source_details
        for case_id in ["inmet_hist_a001_diario_virada", "inmet_hist_a001_2026_diario"]:
            case = f"test_clima_estacao_diario_from_official_zip[{(case_id,)!r}]"
            with (
                check(case),
                isolated_dataset_case(case) as monkeypatch,
                fixture_instance(_case_fixture__fresh_inmet_cache) as _fresh_inmet_cache,
            ):
                case = CASES[case_id]
                seen = install_replay_http(monkeypatch, case, GOLDEN)
                frame, meta = await datasets.clima(**case["selection"], return_meta=True)
                assert_replay_served(seen)
                assert len(frame) == case["period"]["rows"]
                assert frame["data"].is_monotonic_increasing
                assert_replay_structure(frame, case)
                assert_replay_samples(frame, case)
                assert meta.selected_source == "inmet_historico"
                assert meta.source_details["spatial_scope"] == "station"
                assert "station_selection" not in meta.source_details
        for case_id in ["inmet_hist_a001_horario_na", "inmet_hist_a001_horario_20001231"]:
            case = f"test_clima_estacao_horario_from_official_zip[{(case_id,)!r}]"
            with (
                check(case),
                isolated_dataset_case(case) as monkeypatch,
                fixture_instance(_case_fixture__fresh_inmet_cache) as _fresh_inmet_cache,
            ):
                case = CASES[case_id]
                seen = install_replay_http(monkeypatch, case, GOLDEN)
                frame, meta = await datasets.clima(**case["selection"], return_meta=True)
                assert_replay_served(seen)
                assert len(frame) == case["period"]["rows"]
                assert_replay_structure(frame, case)
                assert_replay_samples(frame, case)
                assert meta.selected_source == "inmet_historico"
                if case_id.endswith("_na"):
                    measures = [
                        c for c in frame.columns if c not in ("data", "hora_utc", "estacao", "uf")
                    ]
                    assert frame[measures].isna().all().all()


async def test_clima_reconciliacao_r10_casos_2():
    with collect_failures() as check:
        case = "test_clima_estacao_falls_back_without_token"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(_case_fixture__fresh_inmet_cache) as _fresh_inmet_cache,
        ):
            case = CASES["inmet_fallback_sem_token"]
            monkeypatch.delenv("AGROBR_INMET_TOKEN", raising=False)
            seen = install_replay_http(monkeypatch, case, GOLDEN)
            frame, meta = await datasets.clima(**case["selection"], return_meta=True)
            assert_replay_served(seen)
            assert meta.attempted_sources == ["inmet", "inmet_historico"]
            assert meta.selected_source == "inmet_historico"
            assert len(frame) == case["period"]["rows"]
            assert_replay_samples(frame, case)
        case = "test_clima_uf_mensal_from_official_nasa_body"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(_case_fixture__fresh_inmet_cache) as _fresh_inmet_cache,
        ):
            case = CASES["nasa_mt_2025_mensal"]
            seen = install_replay_http(monkeypatch, case, GOLDEN)
            frame, meta = await datasets.clima(**case["selection"], return_meta=True)
            assert_replay_served(seen)
            assert len(frame) == case["period"]["rows"]
            assert_replay_structure(frame, case)
            assert_replay_samples(frame, case)
            assert meta.selected_source == "nasa_power"
            assert meta.source_details["time_basis"] == "LST"
            assert meta.source_details["spatial_scope"] == "point"
            assert "station_selection" not in meta.source_details


async def test_nasa_daily_from_official_body(monkeypatch: pytest.MonkeyPatch):
    case = CASES["nasa_mt_2025_diario"]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    selection = dict(case["selection"])
    uf = selection.pop("uf")
    ano = selection.pop("ano")
    frame = await nasa_power.clima_uf(uf, ano, **selection)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    assert_replay_structure(frame, case)
    assert_replay_samples(frame, case)
