from __future__ import annotations

import hashlib
import json
from pathlib import Path

import httpx
import pytest


@pytest.fixture(scope="session")
def ptax_captures():
    root = Path(__file__).parents[1] / "golden_data/bcb/ptax_selecao_20260907"
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8-sig"))
    bodies = {item["case"]: (root / item["file"]).read_bytes() for item in manifest["artifacts"]}
    for item in manifest["artifacts"]:
        body = bodies[item["case"]]
        assert (len(body), hashlib.sha256(body).hexdigest()) == (item["size_bytes"], item["sha256"])
        assert "set-cookie" not in item["headers"]
    oracles = (root / "oracles.json").read_bytes()
    assert hashlib.sha256(oracles).hexdigest() == manifest["oracles_sha256"]
    return {"root": root, "manifest": manifest, "bodies": bodies, "oracles": json.loads(oracles)}


@pytest.fixture
def quote_row(ptax_captures):
    return json.loads(ptax_captures["bodies"]["usd_day"])["value"][0]


@pytest.fixture
def currency_row(ptax_captures):
    return json.loads(ptax_captures["bodies"]["currencies"])["value"][0]


def encode(rows, **annotations):
    return json.dumps({"value": rows, **annotations}, ensure_ascii=False).encode()


@pytest.fixture
def ptax_http(ptax_captures, monkeypatch):
    original = httpx.AsyncClient

    def install(quotes=None, *, catalog=None):
        requests = []
        quote_requests = []
        catalog_requests = []

        def respond(request):
            requests.append(request)
            assert request.method == "GET" and request.url.host == "olinda.bcb.gov.br"
            if request.url.path.endswith("/Moedas"):
                catalog_requests.append(request)
                if catalog is not None:
                    response = catalog(request, len(catalog_requests))
                else:
                    body = (
                        ptax_captures["bodies"]["currencies"]
                        if int(request.url.params.get("$skip", "0")) == 0
                        else encode([])
                    )
                    response = httpx.Response(200, content=body)
            else:
                quote_requests.append(request)
                assert quotes is not None, f"No explicit PTAX quote replay: {request.url}"
                response = quotes(request, len(quote_requests))
            if isinstance(response, Exception):
                raise response
            return response

        def client(*args, **kwargs):
            return original(*args, transport=httpx.MockTransport(respond), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", client)
        return {"all": requests, "quotes": quote_requests, "catalog": catalog_requests}

    return install
