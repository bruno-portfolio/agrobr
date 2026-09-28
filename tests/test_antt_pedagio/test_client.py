from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable

import httpx
import pytest

from agrobr import constants
from agrobr.alt.antt_pedagio import client
from agrobr.exceptions import ParseError, ResourceLimitError, SourceUnavailableError
from tests.helpers import install_anttpedagio_http


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> Callable:
    def install(
        *,
        years: tuple[int, ...] = (2026,),
        body: bytes = b"header;value\nrow;1\n",
        transform: Callable | None = None,
        announced: int | None = None,
        frequency: str = "mensal",
    ) -> list[httpx.Request]:
        resources = [
            {
                "id": str(year),
                "name": f"{year} {frequency}",
                "format": "CSV",
                "url": f"https://dados.antt.gov.br/{year}_{frequency}.csv",
                "size": len(body) if announced is None else announced,
                "last_modified": "2026-08-28T11:35:14.750047",
                "extra_source": {"literal": "untouched"},
            }
            for year in years
        ]

        def respond(request: httpx.Request) -> httpx.Response:
            if "package_show" in request.url.path:
                slug = request.url.params["id"]
                payload = {
                    "success": True,
                    "result": {
                        "id": "package",
                        "name": slug,
                        "license_id": "cc-by",
                        "resources": resources,
                    },
                }
                response = httpx.Response(200, json=payload)
            else:
                response = httpx.Response(200, content=body)
            return transform(request, response) if transform is not None else response

        return install_anttpedagio_http(monkeypatch, respond)

    return install


@pytest.mark.asyncio
async def test_spooled_acquisition_complete_and_closed(server: Callable):
    calls = server(years=(2025, 2026))
    async with client.open_trafego_anos([2025, 2026]) as bundle:
        assert bundle.complete and len(bundle.files) == 2
        assert bundle.requested_years == [2025, 2026]
        handles = [item.file for item in bundle.files]
        assert all(handle.seekable() and handle.tell() == 0 for handle in handles)
        assert all(handle.fileno() >= 0 for handle in handles)
        assert [item.ano for item in bundle.files] == [2025, 2026]
        assert all(
            item.sha256 == hashlib.sha256(item.file.read()).hexdigest() for item in bundle.files
        )
        assert [receipt.role for receipt in bundle.attempts] == ["catalog", "trafego", "trafego"]
        assert all(receipt.complete_body and receipt.closed for receipt in bundle.attempts)
        assert bundle.transfer_bytes == sum(item.size_bytes for item in bundle.attempts)
        assert bundle.spool_bytes == sum(item.size_bytes for item in bundle.files)
        assert bundle.files[0].resource.last_modified == "2026-08-28T11:35:14.750047"
        assert len(calls) == 3 and all(call.headers.get("range") is None for call in calls)
        assert all(call.headers["accept-encoding"] == "identity" for call in calls)
    assert all(handle.closed for handle in handles)


@pytest.mark.parametrize(
    "years,frequency",
    [
        ([], "mensal"),
        ([True], "mensal"),
        ([2026.0], "mensal"),
        (["2026"], "mensal"),
        ([2009], "mensal"),
        ([10000], "mensal"),
        ([2026, 2026], "mensal"),
        ((2026,), "mensal"),
        ([2026], "diario"),
        ([2026], None),
    ],
)
@pytest.mark.asyncio
async def test_invalid_query_zero_http(server: Callable, years: object, frequency: object):
    calls = server()
    try:
        async with client.open_trafego_anos(years, frequencia=frequency):
            caught = None
    except Exception as exc:
        caught = exc
    assert type(caught) is ValueError, caught
    assert calls == []


@pytest.mark.parametrize("status", [206, 302, 403, 404])
@pytest.mark.asyncio
async def test_unsuccessful_http_preserves_receipt_no_follow(server: Callable, status: int):
    def transform(request: httpx.Request, response: httpx.Response) -> httpx.Response:
        return (
            response
            if "package_show" in request.url.path
            else httpx.Response(
                status,
                content=b"failure",
                headers={"location": "https://dados.antt.gov.br/redirect.csv"},
            )
        )

    calls = server(transform=transform)
    try:
        async with client.open_trafego_anos([2026]):
            caught = None
    except Exception as exc:
        caught = exc
    assert isinstance(caught, SourceUnavailableError), caught
    last = caught.antt_acquisition["attempts"][-1]
    assert last["status"] == status and last["closed"] and last["sha256"]
    assert last["error_type"] is not None
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_exhausted_status_retries_stop_at_three(server: Callable):
    calls = server(
        transform=lambda request, response: (
            response
            if "package_show" in request.url.path
            else httpx.Response(503, content=b"retry")
        )
    )
    try:
        async with client.open_trafego_anos([2026]):
            caught = None
    except Exception as exc:
        caught = exc
    assert isinstance(caught, SourceUnavailableError), caught
    assert len(calls) == 4 and len(caught.antt_acquisition["attempts"]) == 4


@pytest.mark.parametrize(
    "headers,message",
    [
        ({"content-length": "999"}, "Content-Length divergente"),
        ({"content-length": "-1"}, "Content-Length inválido"),
        ({"content-range": "bytes 0-2/99"}, "Content-Range"),
        ({"content-range": "bytes 0-2/19"}, "Content-Range"),
        ({"content-range": "bytes */99"}, "Content-Range"),
        ({"content-length": "9" * 5000}, "Content-Length divergente"),
        ({"content-range": "bytes 0-" + "9" * 5000 + "/99"}, "Content-Range"),
        ({"content-range": "bytes 0-18/" + "9" * 5000}, "Content-Range"),
    ],
)
@pytest.mark.asyncio
async def test_size_and_range_contradiction(
    server: Callable, headers: dict[str, str], message: str
):
    calls = server(
        transform=lambda request, response: (
            response
            if "package_show" in request.url.path
            else httpx.Response(200, content=b"header;value\nrow;1\n", headers=headers)
        )
    )
    try:
        async with client.open_trafego_anos([2026]):
            caught = None
    except Exception as exc:
        caught = exc
    assert isinstance(caught, SourceUnavailableError) and message in str(caught), caught
    assert caught.antt_acquisition["attempts"][-1]["complete_body"]
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_ckan_announced_size_mismatch(server: Callable):
    calls = server(announced=20)
    try:
        async with client.open_trafego_anos([2026]):
            caught = None
    except Exception as exc:
        caught = exc
    assert isinstance(caught, SourceUnavailableError) and re.search(
        "CKAN divergente", str(caught)
    ), caught
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_announced_budget_before_csv(server: Callable, monkeypatch: pytest.MonkeyPatch):
    calls = server()
    monkeypatch.setattr(constants, "ANTT_MAX_CSV_BYTES", len(b"header;value\nrow;1\n") - 1)
    try:
        async with client.open_trafego_anos([2026]):
            caught = None
    except Exception as exc:
        caught = exc
    assert isinstance(caught, ResourceLimitError) and re.search(
        "antes do GET de CSV", str(caught)
    ), caught
    assert len(calls) == 1


@pytest.mark.parametrize(
    "body,error",
    [(b"", ParseError), (b"<html>failure</html>", SourceUnavailableError)],
)
@pytest.mark.asyncio
async def test_empty_or_html_body_rejected(server: Callable, body: bytes, error: type[Exception]):
    server(body=body)
    try:
        async with client.open_trafego_anos([2026]):
            caught = None
    except Exception as exc:
        caught = exc
    assert isinstance(caught, error), caught
    assert caught.antt_acquisition["attempts"][-1]["error_type"] == error.__name__


@pytest.mark.parametrize("prefix", [b"", b" \n", b"\xef\xbb\xbf"])
async def test_catalog_waf_is_source_unavailable(monkeypatch: pytest.MonkeyPatch, prefix: bytes):
    calls = install_anttpedagio_http(
        monkeypatch,
        lambda _request: httpx.Response(
            200, content=prefix + b"<!DOCTYPE html><html>Request Rejected</html>"
        ),
    )
    try:
        async with client.open_trafego_anos([2025], frequencia="diaria"):
            caught = None
    except Exception as exc:
        caught = exc
    assert isinstance(caught, SourceUnavailableError) and re.search("WAF", str(caught)), caught
    assert len(calls) == 1
    receipt = caught.antt_acquisition["attempts"][-1]
    assert receipt["error_type"] == "SourceUnavailableError"
    assert receipt["closed"] and receipt["complete_body"]


@pytest.mark.asyncio
async def test_plazas_budget_is_its_own(server: Callable, monkeypatch: pytest.MonkeyPatch):
    def transform(request: httpx.Request, response: httpx.Response) -> httpx.Response:
        if "package_show" not in request.url.path:
            return response
        payload = json.loads(response.content)
        payload["result"]["resources"][0]["size"] = None
        return httpx.Response(200, json=payload)

    calls = server(transform=transform)
    monkeypatch.setattr(constants, "ANTT_MAX_PRACAS_BYTES", 5)
    try:
        await client.fetch_pracas()
    except Exception as exc:
        caught = exc
    else:
        caught = None
    assert isinstance(caught, ResourceLimitError) and re.search("após chunk", str(caught)), caught
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_all_resource_origins_checked_before_first_download(server: Callable):
    def transform(request: httpx.Request, response: httpx.Response) -> httpx.Response:
        if "package_show" not in request.url.path:
            return response
        payload = json.loads(response.content)
        payload["result"]["resources"][1]["url"] = "https://outro.gov.br/2026_mensal.csv"
        return httpx.Response(200, json=payload)

    calls = server(years=(2025, 2026), transform=transform)
    try:
        async with client.open_trafego_anos([2025, 2026]):
            caught = None
    except Exception as exc:
        caught = exc
    assert isinstance(caught, SourceUnavailableError) and re.search("origem", str(caught)), caught
    assert len(calls) == 1
