from __future__ import annotations

import asyncio
import io
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from agrobr.alt.antt_pedagio import acquisition, catalog, client
from tests.helpers import install_anttpedagio_http


def test_details_independent_json_and_source_time_literal():
    resource = catalog.CatalogResource(
        id="r",
        name="2026 mensal",
        url="https://dados.antt.gov.br/a.csv",
        format="CSV",
        last_modified="2026-08-28T11:35:14.750047",
        extra_source={"value": [1]},
    )
    file = io.BytesIO(b"csv")
    bundle = acquisition.TrafegoAcquisition([2026], "mensal")
    bundle.selected_resources = [{"ano": 2026, "resource": resource.model_dump(mode="json")}]
    bundle.files = [acquisition.DownloadedCSV(2026, "mensal", resource, file, 3, "digest", 1)]
    bundle.attempts = [
        acquisition.AttemptReceipt(
            role="trafego",
            logical_index=1,
            attempt=1,
            url=resource.url,
            started_at=datetime.now(UTC),
        )
    ]
    details = bundle.details()
    json.dumps(details)
    details["selected_resources"][0]["resource"]["extra_source"]["value"].append(2)
    details["files"][0]["resource"]["extra_source"]["value"].append(3)
    assert bundle.details()["selected_resources"][0]["resource"]["extra_source"]["value"] == [1]
    assert resource.model_extra["extra_source"]["value"] == [1]
    assert details["files"][0]["resource"]["last_modified"] == "2026-08-28T11:35:14.750047"
    assert "+00:00" in details["started_at"] and not details["source_revision_snapshot"]
    assert "file" not in details["files"][0]
    bundle.close()
    assert file.closed


@pytest.mark.parametrize("primary_kind", ["none", "value", "cancel"])
@pytest.mark.asyncio
async def test_all_spools_close_without_masking_primary(
    monkeypatch: pytest.MonkeyPatch, primary_kind: str
):
    resources = [
        {
            "id": str(year),
            "name": f"{year} mensal",
            "format": "CSV",
            "url": f"https://dados.antt.gov.br/{year}_mensal.csv",
            "size": 4,
        }
        for year in (2025, 2026)
    ]

    def respond(request: httpx.Request) -> httpx.Response:
        if "package_show" in request.url.path:
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "result": {
                        "id": "package",
                        "name": "volume-trafego-praca-pedagio",
                        "resources": resources,
                    },
                },
            )
        return httpx.Response(200, content=b"a;b\n")

    calls = install_anttpedagio_http(monkeypatch, respond)
    primary = (
        ValueError("consumer primary")
        if primary_kind == "value"
        else asyncio.CancelledError("cancel primary")
    )
    closing = OSError("first spool close failure")
    handles = []
    caught = None
    try:
        async with client.open_trafego_anos([2025, 2026]) as bundle:
            handles = [item.file for item in bundle.files]

            def bad_close() -> None:
                handles[0].close()
                raise closing

            bundle.files[0].file = SimpleNamespace(close=bad_close)
            if primary_kind != "none":
                raise primary
    except BaseException as exc:
        caught = exc
    assert caught is (closing if primary_kind == "none" else primary)
    assert len(calls) == 3 and all(handle.closed for handle in handles)
    errors = caught.antt_acquisition["spool_close_errors"]
    assert len(errors) == 1 and errors[0]["file_index"] == 0 and errors[0]["resource_id"] == "2025"
    assert errors[0]["error_type"] == "OSError" and errors[0]["at"].endswith("+00:00")
