from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import contracts, datasets
from agrobr.alt.sicar import client
from agrobr.datasets.deterministic import deterministic, get_snapshot
from agrobr.exceptions import InvalidParameterError, ParseError
from tests.helpers import (
    collect_failures,
    fixture_instance,
    isolated_dataset_case,
    levanta_exatamente,
)

CAPTURE_DIR = Path(__file__).parent.parent / "golden_data" / "sicar" / "selecao_20260906"
CAPTURE_PATH = CAPTURE_DIR / "mt_tabular.json"


@pytest.fixture
def install_wfs_transport(monkeypatch) -> Callable[..., list[httpx.Request]]:
    original_client = httpx.AsyncClient

    def install(
        *,
        total: int = 10,
        body: bytes | None = None,
        uf: str = "MT",
        pages: dict[int, bytes] | None = None,
        boundary: bool = False,
    ) -> list[httpx.Request]:
        requests: list[httpx.Request] = []
        response_body = CAPTURE_PATH.read_bytes() if body is None else body

        def respond(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            assert request.url.host == "geoserver.car.gov.br"
            assert request.url.params["request"] == "GetFeature"
            assert request.url.params["typeNames"] == f"sicar:sicar_imoveis_{uf.lower()}"
            assert "geo_area_imovel" not in request.url.params["propertyName"]
            requests.append(request)
            if request.url.params.get("resultType") == "hits":
                if boundary and "data_atualizacao>" in request.url.params.get("CQL_FILTER", ""):
                    literal = (
                        request.url.params["CQL_FILTER"]
                        .split("data_atualizacao>'")[1]
                        .split("'")[0]
                    )
                    assert literal == "2026-09-01T16:44:58.004Z"
                    return httpx.Response(
                        200,
                        content=(CAPTURE_DIR / "data_atualizacao_utc_with_z.xml").read_bytes(),
                        headers={"content-type": "application/xml"},
                        request=request,
                    )
                content = (
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
                    f'numberMatched="{total}" numberReturned="0" '
                    'timeStamp="2026-09-06T12:00:00Z"/>'
                ).encode()
                content_type = "application/xml"
            else:
                assert request.url.params["outputFormat"] == "application/json"
                start = int(request.url.params["startIndex"])
                content = pages[start] if pages else response_body
                content_type = "application/json; charset=utf-8"
            return httpx.Response(
                200, content=content, headers={"content-type": content_type}, request=request
            )

        def make_client(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
            return original_client(*args, transport=httpx.MockTransport(respond), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", make_client)
        return requests

    return install


_case_fixture_install_wfs_transport = inspect.unwrap(install_wfs_transport)


async def test_cadastro_filtros_casos_3():
    with collect_failures() as check:
        for cutoff in [
            "from_result",
            "2026-09-01T16:44:58.004Z",
            "2026-09-01T13:44:58.004-03:00",
            "2026-09-01T16:44:58.004",
            "2026-09-01T16:44:58.004000+00:00",
            "2026-09-01T16:44:58.004000000Z",
        ]:
            case = f"test_update_cutoff_roundtrip_and_equivalent_offsets_exclude_same_instant[{(cutoff,)!r}]"
            with (
                check(case),
                isolated_dataset_case(case) as monkeypatch,
                fixture_instance(
                    _case_fixture_install_wfs_transport, monkeypatch=monkeypatch
                ) as install_wfs_transport,
            ):
                requests = install_wfs_transport(
                    uf="DF",
                    total=1,
                    body=(CAPTURE_DIR / "df_same_record.json").read_bytes(),
                    boundary=True,
                )
                frame = await datasets.cadastro_rural(
                    "DF", municipio=5300108, area_min=12.5846, area_max=12.5846
                )
                timestamp = frame.iloc[0]["data_atualizacao"]
                assert timestamp == pd.Timestamp("2026-09-01T16:44:58.004Z")
                assert frame.iloc[0]["data_criacao"] == pd.Timestamp("2014-11-12T04:23:04.299Z")
                effective_cutoff = timestamp.isoformat() if cutoff == "from_result" else cutoff

                after = await datasets.cadastro_rural(
                    "DF",
                    municipio=5300108,
                    area_min=12.5846,
                    area_max=12.5846,
                    atualizado_apos=effective_cutoff,
                )

                assert after.empty
                assert len(requests) == 3
                assert "data_atualizacao>=" not in requests[-1].url.params["CQL_FILTER"]
                for column in ("data_criacao", "data_atualizacao"):
                    assert str(after[column].dtype) == "datetime64[ns, UTC]"
        for cutoff in [
            "2026-09-03T14:27:12.212000+00:00",
            "2026-09-03T14:27:12.212000000Z",
            "2026-09-03T11:27:12.212000000-03:00",
        ]:
            case = (
                f"test_update_cutoff_serializes_exact_official_millisecond_literal[{(cutoff,)!r}]"
            )
            with (
                check(case),
                isolated_dataset_case(case) as monkeypatch,
                fixture_instance(
                    _case_fixture_install_wfs_transport, monkeypatch=monkeypatch
                ) as install_wfs_transport,
            ):
                capture = json.loads((CAPTURE_DIR / "df_tabular.json").read_bytes())
                record = next(
                    feature["properties"]
                    for feature in capture["features"]
                    if feature["properties"]["cod_imovel"]
                    == "DF-5300108-D17B050DE12E436198BD0DDCEBC64606"
                )
                assert record["data_atualizacao"] == "2026-09-03T14:27:12.212Z"
                requests = install_wfs_transport(uf="DF", total=0)

                frame = await datasets.cadastro_rural(
                    "DF", municipio=5300108, atualizado_apos=cutoff
                )

                assert frame.empty
                assert len(requests) == 1
                assert requests[0].url.params["CQL_FILTER"] == (
                    "cod_municipio_ibge=5300108 AND data_atualizacao>'2026-09-03T14:27:12.212Z'"
                )


async def test_cadastro_filtros_casos_1():
    with collect_failures() as check:
        case = "test_zero_fraction_update_cutoff_omits_decimal_part"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(
                _case_fixture_install_wfs_transport, monkeypatch=monkeypatch
            ) as install_wfs_transport,
        ):
            requests = install_wfs_transport(uf="DF", total=0)

            await datasets.cadastro_rural(
                "DF",
                municipio=5300108,
                atualizado_apos="2026-09-03T14:27:12.000000000+00:00",
            )

            assert len(requests) == 1
            assert requests[0].url.params["CQL_FILTER"] == (
                "cod_municipio_ibge=5300108 AND data_atualizacao>'2026-09-03T14:27:12Z'"
            )
        case = "test_named_filters_combine_in_wfs_query"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(
                _case_fixture_install_wfs_transport, monkeypatch=monkeypatch
            ) as install_wfs_transport,
        ):
            requests = install_wfs_transport()

            frame = await datasets.cadastro_rural(
                "MT",
                municipio=5103403,
                status="at",
                tipo="iru",
                area_min=0.0,
                area_max=1000.0,
                criado_apos="2014-01-01",
            )

            assert len(frame) == 10
            for request in requests:
                assert set(request.url.params["CQL_FILTER"].split(" AND ")) == {
                    "cod_municipio_ibge=5103403",
                    "status_imovel='AT'",
                    "tipo_imovel='IRU'",
                    "area>=0.0",
                    "area<=1000.0",
                    "dat_criacao>='2014-01-01'",
                }
        case = "test_seven_positional_filters_keep_meaning"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(
                _case_fixture_install_wfs_transport, monkeypatch=monkeypatch
            ) as install_wfs_transport,
        ):
            requests = install_wfs_transport()

            frame, meta = await datasets.cadastro_rural(
                "MT", "Cuiabá", "AT", "IRU", 0.0, 1000.0, "2014-01-01", return_meta=True
            )
            with pytest.raises(TypeError, match="positional"):
                await datasets.cadastro_rural(
                    "MT", "Cuiabá", "AT", "IRU", 0.0, 1000.0, "2014-01-01", True
                )

            assert len(frame) == 10
            assert meta.contract_version == "2.1"
            for request in requests:
                assert set(request.url.params["CQL_FILTER"].split(" AND ")) == {
                    "cod_municipio_ibge=5103403",
                    "status_imovel='AT'",
                    "tipo_imovel='IRU'",
                    "area>=0.0",
                    "area<=1000.0",
                    "dat_criacao>='2014-01-01'",
                }


async def test_official_json_pages_accumulate_all_twenty_records(
    monkeypatch, install_wfs_transport
):
    monkeypatch.setattr(client, "PAGE_SIZE", 7)
    pages = {
        start: (CAPTURE_DIR / f"df_page_start_{start}.json").read_bytes() for start in (0, 7, 14)
    }
    requests = install_wfs_transport(uf="DF", total=20, pages=pages)

    frame = await datasets.cadastro_rural("DF", municipio=5300108, atualizado_apos="2026-09-01")

    expected = json.loads((CAPTURE_DIR / "df_tabular.json").read_bytes())
    assert set(frame["cod_imovel"]) == {f["properties"]["cod_imovel"] for f in expected["features"]}
    assert len(frame) == 20
    page_requests = [r for r in requests if r.url.params.get("resultType") != "hits"]
    assert [int(r.url.params["startIndex"]) for r in page_requests] == [0, 7, 14]
    assert all(r.url.params["count"] == "7" for r in page_requests)
    contracts.validate_dataset(frame, "cadastro_rural")


async def test_repeated_wfs_property_rejects_inconsistent_dataset_collection(install_wfs_transport):
    payload = json.loads(CAPTURE_PATH.read_bytes())
    payload["features"].append(payload["features"][0])
    payload["numberReturned"] = payload["numberMatched"] = payload["totalFeatures"] = 11
    requests = install_wfs_transport(total=11, body=json.dumps(payload).encode())

    with pytest.raises(ParseError, match="repetido") as error:
        await datasets.cadastro_rural("MT", municipio=5103403)

    assert len(requests) == 2
    assert error.value.source == "cadastro_rural/MT"
    assert error.value.errors[0][:2] == ("sicar", "parse")


@pytest.mark.parametrize("snapshot", ["2024-01-01", date.today().isoformat(), "2099-01-01"])
@pytest.mark.parametrize(
    "filters", [{}, {"criado_apos": "2023-01-01"}, {"atualizado_apos": "2023-01-01"}]
)
async def test_deterministic_rejected_before_any_wfs_request(
    snapshot, filters, install_wfs_transport
):
    requests = install_wfs_transport()

    async with deterministic(snapshot):
        with pytest.raises(InvalidParameterError, match="deterministic|snapshot"):
            await datasets.cadastro_rural("MT", municipio=5103403, **filters)

    assert requests == []
    assert get_snapshot() is None


async def test_deterministic_rejection_isolated_from_concurrent_current_query(
    install_wfs_transport,
):
    requests = install_wfs_transport()
    context_active = asyncio.Event()
    current_query_done = asyncio.Event()

    async def historical_query():
        async with deterministic("2024-01-01"):
            context_active.set()
            await current_query_done.wait()
            with pytest.raises(InvalidParameterError, match="deterministic|snapshot"):
                await datasets.cadastro_rural("MT", municipio=5103403)
            assert get_snapshot() == "2024-01-01"
        assert get_snapshot() is None

    async def current_query():
        await context_active.wait()
        try:
            frame, meta = await datasets.cadastro_rural("MT", municipio=5103403, return_meta=True)
            assert len(frame) == 10
            assert meta.snapshot is None
            assert get_snapshot() is None
        finally:
            current_query_done.set()

    await asyncio.wait_for(asyncio.gather(historical_query(), current_query()), timeout=5)
    assert len(requests) == 2
    assert get_snapshot() is None


@pytest.mark.parametrize(
    "filters",
    [
        {"uf": "XX"},
        {"uf": None},
        {"municipio": "   "},
        {"municipio": "Cuiab"},
        {"municipio": True},
        {"municipio": 3550308},
        {"municipio": 510792},
        {"status": 1},
        {"tipo": False},
        {"area_min": -1},
        {"area_max": float("inf")},
        {"area_min": float("nan")},
        {"area_min": True},
        {"area_min": 100, "area_max": 99},
        {"criado_apos": "2025-02-29"},
        {"atualizado_apos": "2024-01-01T25:00:00"},
    ],
)
async def test_invalid_filter_rejected_before_transport(filters, install_wfs_transport):
    requests = install_wfs_transport()
    arguments = {"uf": "MT", **filters}

    with levanta_exatamente(InvalidParameterError):
        await datasets.cadastro_rural(**arguments)

    assert requests == []


@pytest.mark.parametrize(
    "unknown_filter",
    [{"bbox": (-56.0, -13.0, -55.0, -12.0)}, {"atualizado_aposs": "2024-01-01"}],
)
async def test_unknown_filter_rejected_before_transport(unknown_filter, install_wfs_transport):
    requests = install_wfs_transport()

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        await datasets.cadastro_rural("MT", municipio=5103403, **unknown_filter)

    assert requests == []
