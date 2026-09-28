from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest

from agrobr import datasets, nasa_power
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import InvalidParameterError, SourceFallbackWarning, SourceUnavailableError
from agrobr.inmet import client
from tests.helpers import levanta_exatamente

GOLDEN = Path(__file__).parent.parent / "golden_data" / "inmet" / "selecao_20260906"
NASA_GOLDEN = (
    Path(__file__).parent.parent / "golden_data" / "nasa_power" / "daily_sample" / "response.json"
)


@pytest.fixture
def install_climate_transport(monkeypatch) -> Callable[..., list[httpx.Request]]:
    original_client = httpx.AsyncClient
    monkeypatch.delenv("AGROBR_INMET_TOKEN", raising=False)
    monkeypatch.setattr(client, "_historico_zip_cache", None)

    def install(
        *,
        zip_available: bool = True,
        archive_started: asyncio.Event | None = None,
        release_archive: asyncio.Event | None = None,
    ) -> list[httpx.Request]:
        requests: list[httpx.Request] = []

        async def respond(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            assert request.method == "GET"
            if request.url.host == "portal.inmet.gov.br":
                if archive_started is not None and release_archive is not None:
                    archive_started.set()
                    await release_archive.wait()
                year = Path(request.url.path).stem
                archive = GOLDEN / f"{year}.zip"
                if zip_available and archive.is_file():
                    return httpx.Response(
                        200,
                        content=archive.read_bytes(),
                        headers={"content-type": "application/zip"},
                    )
                return httpx.Response(404)
            if request.url.host == "apitempo.inmet.gov.br":
                return httpx.Response(204)
            if request.url.host == "power.larc.nasa.gov":
                return httpx.Response(
                    200,
                    content=NASA_GOLDEN.read_bytes(),
                    headers={"content-type": "application/json"},
                )
            raise AssertionError(f"Unexpected climate transport host: {request.url.host}")

        def captured_client(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
            return original_client(*args, transport=httpx.MockTransport(respond), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", captured_client)
        return requests

    return install


async def test_explicit_sources_remain_isolated_during_concurrent_requests(
    install_climate_transport,
):
    archive_started = asyncio.Event()
    release_archive = asyncio.Event()
    install_climate_transport(archive_started=archive_started, release_archive=release_archive)
    singleton = datasets.get_dataset("clima")
    original_sources = tuple(source.name for source in singleton.info.sources)
    historical_task = asyncio.create_task(
        datasets.clima("GO", 2001, fonte="inmet_historico", return_meta=True)
    )
    try:
        await asyncio.wait_for(archive_started.wait(), timeout=10)
        assert tuple(source.name for source in singleton.info.sources) == original_sources
        nasa_frame, nasa_meta = await datasets.clima(
            "MT", 2025, fonte="nasa_power", return_meta=True
        )
        assert nasa_meta.attempted_sources == ["nasa_power"]
        assert nasa_frame["fonte"].eq("nasa_power").all()
    finally:
        release_archive.set()
    historical_frame, historical_meta = await asyncio.wait_for(historical_task, timeout=30)
    assert historical_meta.attempted_sources == ["inmet_historico"]
    assert historical_frame["fonte"].eq("inmet").all()
    assert tuple(source.name for source in singleton.info.sources) == original_sources


async def test_snapshot_selects_year_without_freezing_archive_or_truncating_observations(
    install_climate_transport,
):
    requests = install_climate_transport()
    async with deterministic("2001-06-15"):
        frame, meta = await datasets.clima("GO", fonte="inmet_historico", return_meta=True)
        _, cached = await datasets.clima("GO", fonte="inmet_historico", return_meta=True)
    assert not meta.from_cache and cached.from_cache
    assert meta.fetch_timestamp == cached.fetch_timestamp == meta.fetched_at
    assert frame["mes"].dt.year.eq(2001).all()
    assert frame["mes"].max() == pd.Timestamp("2001-12-01")
    assert meta.snapshot == "2001-06-15"
    assert meta.source_details["deterministic"] == {
        "snapshot": "2001-06-15",
        "year_selection_only": True,
        "freezes_source_revision": False,
        "truncates_observations_at_snapshot": False,
    }
    assert len(requests) == 1
    resource = meta.source_details["resources"][0]
    assert resource["sha256"] == hashlib.sha256((GOLDEN / "2001.zip").read_bytes()).hexdigest()


@pytest.mark.parametrize("fonte", [None, "inmet_historico"])
async def test_empty_uf_in_real_archive_falls_back_only_in_auto_mode(
    fonte,
    install_climate_transport,
    monkeypatch,
):
    requests = install_climate_transport()
    nasa_frame = pd.DataFrame(
        {
            "mes": pd.to_datetime(["2000-01-01"]),
            "uf": ["MG"],
            "precip_acum_mm": [123.0],
            "temp_media": [22.0],
            "temp_max_media": [27.0],
            "temp_min_media": [17.0],
            "lat": [-18.5],
            "lon": [-44.6],
        }
    )
    nasa_fetch = AsyncMock(return_value=nasa_frame)
    monkeypatch.setattr(nasa_power, "clima_uf", nasa_fetch)
    if fonte is None:
        with pytest.warns(SourceFallbackWarning):
            frame, meta = await datasets.clima("MG", 2000, return_meta=True)
        assert meta.attempted_sources == ["inmet", "inmet_historico", "nasa_power"]
        assert meta.selected_source == "nasa_power"
        assert frame["precip_acum_mm"].tolist() == [123.0]
        nasa_fetch.assert_awaited_once_with("MG", 2000, agregacao="mensal", return_meta=True)
    else:
        with pytest.raises(SourceUnavailableError):
            await datasets.clima("MG", 2000, fonte=fonte)
        nasa_fetch.assert_not_awaited()
    assert any(Path(request.url.path).name == "2000.zip" for request in requests)


@pytest.mark.parametrize("fonte", ["inmet", "inmet_historico"])
async def test_explicit_source_failure_does_not_fall_back(fonte, install_climate_transport):
    requests = install_climate_transport(zip_available=False)
    with pytest.raises(SourceUnavailableError) as caught:
        await datasets.clima("MT", 2025, fonte=fonte)
    if fonte == "inmet_historico":
        assert caught.value.errors[0][1] == "unavailable"
    assert not any(request.url.host == "power.larc.nasa.gov" for request in requests)
    if fonte == "inmet":
        assert not any(request.url.host == "portal.inmet.gov.br" for request in requests)


@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"uf": "XX"},
        {"uf": "DF", "ano": True},
        {"uf": "DF", "ano": 2001.5},
        {"uf": "DF", "agregacao": "horario"},
        {"uf": "DF", "fonte": "unknown"},
        {"uf": "DF", "inicio": "2001-01-01"},
        {"uf": "DF", "estacao": "A001", "inicio": "2001-01-01", "fim": "2001-01-02"},
        {"estacao": "A001", "inicio": "2001-01-01"},
        {"estacao": "A001", "inicio": "2001-01-02", "fim": "2001-01-01"},
        {"estacao": "A001", "inicio": "2001-02-29", "fim": "2001-03-01"},
        {"estacao": "A001", "inicio": "2001-01-01", "fim": "2001-01-02", "agregacao": "mensal"},
        {"estacao": "A001", "inicio": "2001-01-01", "fim": "2001-01-02", "fonte": "nasa_power"},
        {"uf": "DF", "fonte": "inmet_historico", "ano": 1999},
    ],
)
async def test_invalid_climate_selectors_fail_before_transport(
    arguments, install_climate_transport
):
    requests = install_climate_transport()
    with levanta_exatamente(InvalidParameterError):
        await datasets.clima(**arguments)
    assert requests == []


@pytest.mark.parametrize(
    ("arguments", "motivo"),
    [
        ({"ano": 2024}, "uf é obrigatório"),
        ({"uf": 35, "ano": 2024}, "string de duas letras"),
        ({"estacao": "A001"}, "inicio e fim são obrigatórios"),
    ],
)
async def test_seletor_incompleto_explica_o_que_falta(arguments, motivo, install_climate_transport):
    requests = install_climate_transport()
    with levanta_exatamente(InvalidParameterError, match=motivo):
        await datasets.clima(**arguments)
    assert requests == []
