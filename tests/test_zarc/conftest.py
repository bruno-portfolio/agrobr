from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from agrobr.zarc import cache


@pytest.fixture
def zarc_replay(monkeypatch):
    root = Path(__file__).parents[1] / "golden_data/zarc/selecao_20260907"
    resources = [
        {
            "id": name,
            "name": f"Safra {safra}",
            "url": f"https://dados.agricultura.gov.br/{name}.csv",
            "format": "CSV",
            "last_modified": "2026-09-07T00:00:00",
        }
        for name, safra in (
            ("2016_2017", "2016/2017"),
            ("2026_2027", "2026/2027"),
            ("perene", "perene"),
        )
    ]
    state = {
        "catalog": {"success": True, "result": {"resources": resources}},
        "bodies": {
            name: (root / f"{name}.csv").read_bytes()
            for name in ("2016_2017", "2026_2027", "perene")
        },
        "requests": [],
        "status": 200,
    }

    async def handle(request):
        state["requests"].append(request)
        if "package_show" in request.url.path:
            return httpx.Response(200, json=state["catalog"])
        if state.get("csv_started") is not None:
            state["csv_started"].set()
            await state["csv_release"].wait()
        name = Path(request.url.path).stem
        return httpx.Response(
            state["status"],
            content=state["bodies"][name],
            headers={"content-type": "text/csv", "etag": "replayed"},
        )

    original = httpx.AsyncClient

    def session(**kwargs):
        return original(transport=httpx.MockTransport(handle), **kwargs)

    cache.clear()
    monkeypatch.setattr(httpx, "AsyncClient", session)
    yield state
    cache.clear()
