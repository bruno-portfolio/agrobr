from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import httpx
import pytest


@pytest.fixture(scope="session")
def sgs_captures():
    root = Path(__file__).parents[1] / "golden_data/bcb/sgs_selecao_20260907"
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    bodies = {item["case"]: (root / item["file"]).read_bytes() for item in manifest["artifacts"]}
    for item in manifest["artifacts"]:
        body = bodies[item["case"]]
        assert (len(body), hashlib.sha256(body).hexdigest()) == (item["size_bytes"], item["sha256"])
        assert "set-cookie" not in item["headers"]
    oracles = (root / "oracles.json").read_bytes()
    assert hashlib.sha256(oracles).hexdigest() == manifest["oracles_sha256"]
    return {"root": root, "manifest": manifest, "bodies": bodies, "oracles": json.loads(oracles)}


def signature(url: httpx.URL) -> tuple[Any, ...]:
    return url.path, tuple(sorted(url.params.multi_items()))


@pytest.fixture
def sgs_http(sgs_captures, monkeypatch):
    original = httpx.AsyncClient
    exact = {
        signature(httpx.URL(item["url"])): item for item in sgs_captures["manifest"]["artifacts"]
    }

    def install(override=None):
        requests = []

        def respond(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            assert request.method == "GET"
            assert request.url.host == "api.bcb.gov.br"
            assert request.url.params["formato"] == "json"
            if override is not None:
                response = override(request, len(requests))
                if isinstance(response, Exception):
                    raise response
                if response is not None:
                    return response
            key = signature(request.url)
            assert key in exact, f"No captured SGS query or explicit simulation: {request.url}"
            item = exact[key]
            return httpx.Response(
                item["status"],
                content=sgs_captures["bodies"][item["case"]],
                headers=item["headers"],
            )

        def client(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
            return original(*args, transport=httpx.MockTransport(respond), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", client)
        return requests

    return install
