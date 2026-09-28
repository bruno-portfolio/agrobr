from __future__ import annotations

import gzip
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest


@pytest.fixture(scope="session")
def publication_files():
    root = Path(__file__).parents[1] / "golden_data/lista_suja/selecao_20260906"
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    return {
        "root": root,
        "manifest": manifest,
        "csv": (root / "cadastro_de_empregadores.csv").read_bytes(),
        "txt": (root / "cadastro_de_empregadores.txt").read_bytes(),
        "pdf": gzip.decompress((root / manifest["existing_identical_pdf"]).read_bytes()),
        "html": (root / "portal.html").read_bytes(),
        "url": next(item["url"] for item in manifest["artifacts"] if item["file"] == "portal.html"),
    }


@pytest.fixture
def replay_http(publication_files, monkeypatch) -> Callable[..., list[httpx.Request]]:
    original_client = httpx.AsyncClient

    def install(overrides: dict[str, Any] | None = None) -> list[httpx.Request]:
        requests: list[httpx.Request] = []
        responses = {
            "portal": (200, publication_files["html"], "text/html;charset=utf-8"),
            "csv": (200, publication_files["csv"], "text/csv"),
            "txt": (200, publication_files["txt"], "text/plain"),
            "pdf": (200, publication_files["pdf"], "application/pdf"),
        }
        responses.update(overrides or {})

        async def respond(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            assert request.method == "GET"
            assert request.url.host == "www.gov.br"
            path = request.url.path
            if path == httpx.URL(publication_files["url"]).path:
                key = "portal"
            elif "/cadastro_de_empregadores." in path:
                key = path.rsplit(".", 1)[-1]
            else:
                raise AssertionError(f"Unannounced or separate dataset requested: {path}")
            response = responses[key]
            if isinstance(response, Exception):
                raise response
            status, content, content_type = response
            return httpx.Response(status, content=content, headers={"content-type": content_type})

        def captured_client(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
            return original_client(*args, transport=httpx.MockTransport(respond), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", captured_client)
        return requests

    return install
