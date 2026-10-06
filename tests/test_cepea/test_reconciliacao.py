from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import constants, datasets
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import levanta_exatamente

GOLDEN = Path(__file__).parents[1] / "golden_data" / "reconciliacao_precos_diarios_20260918"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
CASES = {case["id"]: case for case in MANIFEST["cases"]}
CEPEA_IDS = sorted(cid for cid in CASES if cid.startswith("cepea_"))
NA_IDS = sorted(cid for cid in CASES if cid.startswith("noticias_agricolas_"))
CEPEA_HOSTS = {httpx.URL(endpoint).host for endpoint in constants._CEPEA_ENDPOINTS}
NA_HOST = httpx.URL(constants.URLS[constants.Fonte.NOTICIAS_AGRICOLAS]["cotacoes"]).host
_REAL_ASYNC_CLIENT = httpx.AsyncClient


def _bytes(case: dict[str, Any]) -> bytes:
    return (GOLDEN / case["file"]).resolve().read_bytes()


def _matches(got: Any, expected: Any) -> bool:
    if expected is None:
        return got is None or (isinstance(got, float) and math.isnan(got))
    if isinstance(expected, float):
        return isinstance(got, (int, float)) and abs(float(got) - expected) <= 1e-9 * max(
            1.0, abs(expected)
        )
    return got == expected


def _assert_samples(frame: pd.DataFrame, case: dict[str, Any]) -> None:
    indexed = frame.assign(_dia=frame["data"].dt.strftime("%Y-%m-%d")).set_index("_dia")
    for sample in case["samples"]:
        dia = sample["key"]["data"]
        assert dia in indexed.index, f"{case['id']}: {dia} ausente na saída"
        got = indexed.loc[dia, sample["column"]]
        assert _matches(got, sample["value"]), (
            f"{case['id']} {dia} {sample['column']}: saída={got!r} esperado={sample['value']!r}"
        )


def _freeze_today(monkeypatch: pytest.MonkeyPatch, iso: str) -> None:
    from agrobr.cepea import api as cepea_api

    frozen = date.fromisoformat(iso)
    monkeypatch.setattr(cepea_api, "_today", lambda: frozen)


def _mock_transport(
    monkeypatch: pytest.MonkeyPatch, *, cepea: bytes | None, na: bytes | None
) -> list[str]:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        headers = {"content-type": "text/html; charset=utf-8"}
        if request.url.host in CEPEA_HOSTS:
            if cepea is None:
                return httpx.Response(503, content=b"unavailable", headers=headers)
            return httpx.Response(200, content=cepea, headers=headers)
        if request.url.host == NA_HOST:
            if na is None:
                return httpx.Response(503, content=b"unavailable", headers=headers)
            return httpx.Response(200, content=na, headers=headers)
        return httpx.Response(404, content=b"", headers=headers)

    class MockedClient(_REAL_ASYNC_CLIENT):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(handler)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", MockedClient)
    return seen


@pytest.mark.parametrize("case_id", CEPEA_IDS)
async def test_public_dataset_from_cepea_bytes(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    _freeze_today(monkeypatch, "2026-09-06")
    seen = _mock_transport(monkeypatch, cepea=_bytes(case), na=None)
    frame, meta = await datasets.preco_diario(
        case["product"],
        inicio=case["period"]["oldest"],
        fim=case["period"]["newest"],
        return_meta=True,
    )
    assert seen and all(httpx.URL(url).host in CEPEA_HOSTS for url in seen)
    assert len(frame) == case["period"]["rows"]
    assert frame["data"].is_monotonic_decreasing
    assert frame["data"].dt.strftime("%Y-%m-%d").iloc[0] == case["period"]["newest"]
    assert frame["data"].dt.strftime("%Y-%m-%d").iloc[-1] == case["period"]["oldest"]
    for column in ("valor", "valor_usd", "peso_medio_kg"):
        assert str(frame[column].dtype) == "float64", column
    _assert_samples(frame, case)
    assert meta.selected_source == "cepea"
    assert meta.parser_version == constants.CEPEA_PARSER_VERSION
    assert meta.records_count == len(frame)


@pytest.mark.parametrize("case_id", NA_IDS)
async def test_public_dataset_falls_back_to_noticias_agricolas(
    case_id: str, monkeypatch: pytest.MonkeyPatch
):
    case = CASES[case_id]
    _freeze_today(monkeypatch, "2026-09-18")
    seen = _mock_transport(monkeypatch, cepea=None, na=_bytes(case))
    frame, meta = await datasets.preco_diario(
        case["product"],
        inicio=case["period"]["oldest"],
        fim=case["period"]["newest"],
        return_meta=True,
    )
    assert any(httpx.URL(url).host in CEPEA_HOSTS for url in seen)
    assert any(httpx.URL(url).host == NA_HOST for url in seen)
    assert len(frame) == case["period"]["rows_canonical"]
    assert set(frame["fonte"]) == {"noticias_agricolas"}
    assert frame["valor_usd"].isna().all()
    assert frame["peso_medio_kg"].isna().all()
    _assert_samples(frame, case)
    assert meta.selected_source == "noticias_agricolas"
    assert meta.attempted_sources == ["cepea", "noticias_agricolas"]


async def test_public_dataset_replays_cache_when_sources_fail(monkeypatch: pytest.MonkeyPatch):
    case = CASES["cepea_bezerro"]
    _freeze_today(monkeypatch, "2026-09-06")
    _mock_transport(monkeypatch, cepea=_bytes(case), na=None)
    first = await datasets.preco_diario(
        case["product"], inicio=case["period"]["oldest"], fim=case["period"]["newest"]
    )
    seen = _mock_transport(monkeypatch, cepea=None, na=None)
    with pytest.warns(UserWarning, match="stale|cache"):
        second, meta = await datasets.preco_diario(
            case["product"],
            inicio=case["period"]["oldest"],
            fim=case["period"]["newest"],
            force_refresh=True,
            return_meta=True,
        )
    assert seen
    assert first.columns.tolist() == second.columns.tolist(), (
        first.columns.tolist(),
        second.columns.tolist(),
    )
    pd.testing.assert_frame_equal(
        first.reset_index(drop=True),
        second.reset_index(drop=True),
    )
    assert meta.from_cache is True
    _assert_samples(second, case)


async def test_window_inside_published_pregoes_is_fetched(monkeypatch: pytest.MonkeyPatch):
    case = CASES["cepea_soja"]
    _freeze_today(monkeypatch, "2026-09-18")
    seen = _mock_transport(monkeypatch, cepea=_bytes(case), na=None)
    frame = await datasets.preco_diario("soja", inicio="2026-08-17", fim="2026-09-04")
    assert seen
    assert len(frame) == case["period"]["rows"]


@pytest.mark.usefixtures("serie_historica")
async def test_window_older_than_source_sem_serie_levanta_em_vez_de_vazio(
    monkeypatch: pytest.MonkeyPatch,
):
    case = CASES["cepea_soja"]
    _freeze_today(monkeypatch, "2026-10-15")
    seen = _mock_transport(monkeypatch, cepea=_bytes(case), na=None)
    with levanta_exatamente(SourceUnavailableError, "série histórica indisponível"):
        await datasets.preco_diario("soja", inicio="2026-08-17", fim="2026-09-04")
    assert seen
    assert [url for url in seen if "/series/soja.aspx?id=92" not in url] == []


@pytest.mark.usefixtures("serie_historica")
async def test_inicio_anterior_a_janela_avisa_o_que_o_agrobr_entrega(
    monkeypatch: pytest.MonkeyPatch,
):
    case = CASES["cepea_soja"]
    _freeze_today(monkeypatch, "2026-09-18")
    _mock_transport(monkeypatch, cepea=_bytes(case), na=None)
    with pytest.warns(UserWarning) as avisos:
        frame = await datasets.preco_diario("soja", inicio="2025-09-22", fim="2026-09-04")
    assert [
        str(aviso.message)
        for aviso in avisos
        if str(aviso.message).startswith("cepea: série histórica de 'soja' indisponível")
    ]
    assert len(frame) == case["period"]["rows"]
    assert frame["data"].min() == pd.Timestamp(case["period"]["oldest"])
