from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import httpx
import pytest


@pytest.fixture(scope="session")
def focus_captures():
    root = Path(__file__).parents[1] / "golden_data/bcb/focus_selecao_20260907"
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    bodies = {item["case"]: (root / item["file"]).read_bytes() for item in manifest["artifacts"]}
    for item in manifest["artifacts"]:
        body = bodies[item["case"]]
        assert (len(body), hashlib.sha256(body).hexdigest()) == (item["size_bytes"], item["sha256"])
        assert "set-cookie" not in item["headers"]
    oracles = (root / "oracles.json").read_bytes()
    assert hashlib.sha256(oracles).hexdigest() == manifest["oracles_sha256"]
    return {"root": root, "manifest": manifest, "bodies": bodies, "oracles": json.loads(oracles)}


@pytest.fixture
def annual_row(focus_captures):
    return json.loads(focus_captures["bodies"]["annual_api_ge"])["value"][0]


@pytest.fixture
def monthly_row(focus_captures):
    return json.loads(focus_captures["bodies"]["monthly_api_ge"])["value"][0]


def signature(url: httpx.URL) -> tuple[Any, ...]:
    return url.path, tuple(sorted(url.params.multi_items()))


@pytest.fixture
def focus_http(focus_captures, monkeypatch):
    original = httpx.AsyncClient
    exact = {
        signature(httpx.URL(item["url"])): item for item in focus_captures["manifest"]["artifacts"]
    }

    def install(override=None):
        requests = []

        def respond(request):
            requests.append(request)
            assert request.method == "GET"
            assert request.url.host == "olinda.bcb.gov.br"
            if override is not None:
                result = override(request, len(requests))
                if isinstance(result, Exception):
                    raise result
                if result is not None:
                    return result
            key = signature(request.url)
            assert key in exact, f"No captured Focus query or explicit simulation: {request.url}"
            item = exact[key]
            return httpx.Response(
                item["status"],
                content=focus_captures["bodies"][item["case"]],
                headers=item["headers"],
            )

        def client(*args, **kwargs):
            return original(*args, transport=httpx.MockTransport(respond), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", client)
        return requests

    return install


def encode(rows, **annotations):
    return json.dumps({"value": rows, **annotations}, ensure_ascii=False).encode()
