from __future__ import annotations

import hashlib
import json
import zipfile
from io import BytesIO
from pathlib import Path

import httpx
import pytest

from agrobr import constants


@pytest.fixture(scope="session")
def municipal_capture():
    directory = Path(__file__).parents[1] / "golden_data/mapbiomas/municipal11_official"
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8-sig"))
    for item in manifest["files"]:
        content = (directory / item["file"]).read_bytes()
        assert hashlib.sha256(content).hexdigest() == item["sha256"]
        assert len(content) == item["size_bytes"]
    content = (directory / "municipal11_selected.xlsx").read_bytes()
    directory10 = directory.parent / "municipal10_official"
    manifest10 = json.loads((directory10 / "manifest.json").read_text(encoding="utf-8-sig"))
    for item in manifest10["files"]:
        assert (
            hashlib.sha256((directory10 / item["file"]).read_bytes()).hexdigest() == item["sha256"]
        )
    stream = BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE_MUNICIPALITY.xlsx", content)
    return {
        "xlsx": content,
        "zip": stream.getvalue(),
        "oracle": json.loads((directory / "raw_oracle.json").read_text(encoding="utf-8")),
        "municipal10": (directory10 / "municipal10_duplicates_minimal.xlsx").read_bytes(),
        "oracle10": json.loads(
            (directory10 / "municipal10_duplicates_minimal_oracle.json").read_text(encoding="utf-8")
        ),
        "confirmation": (
            Path(__file__).parent / "fixtures/drive_collection11_confirmation.html"
        ).read_bytes(),
        "state11": (Path(__file__).parent / "fixtures/collection11_state_sample.xlsx").read_bytes(),
        "state10": (directory.parent / "biome_state_sample/response.xlsx").read_bytes(),
    }


@pytest.fixture
def replay_mapbiomas(monkeypatch, municipal_capture):
    original = httpx.AsyncClient

    def install(overrides=None, *, allow_small_municipal10=False):
        requests = []
        if allow_small_municipal10:
            monkeypatch.setattr(constants, "MIN_XLSX_SIZE", len(municipal_capture["municipal10"]))

        async def respond(request):
            requests.append(request)
            assert request.method == "GET"
            host = request.url.host
            if host == "drive.google.com":
                key, mime = "confirmation", "text/html"
            elif host == "drive.usercontent.google.com":
                key, mime = "zip", "application/zip"
            elif host == "brasil.mapbiomas.org":
                key, mime = (
                    "state11",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            elif host == "data.mapbiomas.org":
                key, mime = (
                    "municipal10" if request.url.path.endswith("/254") else "state10",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            else:
                raise AssertionError(f"Unexpected MapBiomas host: {host}")
            selected = (overrides or {}).get(key, municipal_capture[key])
            if isinstance(selected, Exception):
                raise selected
            status, body = selected if isinstance(selected, tuple) else (200, selected)
            return httpx.Response(status, content=body, headers={"content-type": mime})

        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            lambda **kwargs: original(transport=httpx.MockTransport(respond), **kwargs),
        )
        return requests

    return install
