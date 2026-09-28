from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import httpx
import pytest


@pytest.fixture(scope="session")
def captures():
    root = Path(__file__).parents[1] / "golden_data/comtrade/selecao_20260906"
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    bodies = {
        item["name"]: (root / item["body_file"]).read_bytes() for item in manifest["artifacts"]
    }
    return {
        "root": root,
        "manifest": manifest,
        "bodies": bodies,
        "oracles": json.loads((root / "oracles.json").read_text(encoding="utf-8")),
    }


def record(
    captures: dict[str, Any], name: str = "soy_br_cn_2023", **changes: Any
) -> dict[str, Any]:
    result = copy.deepcopy(json.loads(captures["bodies"][name])["data"][0])
    result.update(changes)
    return result


def signature(params: Any, freq: str = "A") -> tuple[Any, ...]:
    return (
        int(params["reporterCode"]),
        params.get("partnerCode"),
        params["flowCode"],
        freq,
        tuple(sorted(params["period"].split(","))),
        tuple(sorted(params["cmdCode"].split(","))),
        params.get("countOnly", "false").lower() == "true",
    )


@pytest.fixture
def replay_http(captures, monkeypatch):
    monkeypatch.setenv("AGROBR_COMTRADE_API_KEY", "")
    original = httpx.AsyncClient
    exact = {}
    for artifact in captures["manifest"]["artifacts"]:
        params = artifact["params"]
        if "reporterCode" in params and httpx.URL(artifact["requested_url"]).path.endswith("/HS"):
            key = signature(params)
            if key not in exact or params.get("maxRecords") == "500":
                exact[key] = artifact
    pool = []
    for name in [
        "soy_br_omitted_2023",
        "coffee_2023",
        "corn_2023",
        "sugar_2023",
        "meal_2023",
        "agro_2022",
        "soy_br_cn_2021",
        "soy_cn_br_2023_mirror",
    ]:
        pool.extend(json.loads(captures["bodies"][name])["data"])
    placeholder = json.loads(captures["bodies"]["agro5_2023_count"])["data"]

    def install(override=None):
        requests = []
        synthetic = []

        def respond(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            assert request.method == "GET"
            assert request.url.host == "comtradeapi.un.org"
            params = request.url.params
            expected_limit = "100000" if "/data/v1/get/" in request.url.path else "500"
            assert params["maxRecords"] == expected_limit
            assert params["partner2Code"] == "0"
            assert params["motCode"] == "0"
            assert params["customsCode"] == "C00"
            freq = request.url.path.split("/")[-2]
            key = signature(params, freq)
            if override is not None:
                changed = override(request, len(requests))
                if isinstance(changed, Exception):
                    raise changed
                if changed is not None:
                    return changed
            artifact = exact.get(key)
            if artifact is not None:
                return httpx.Response(
                    artifact["status"],
                    content=captures["bodies"][artifact["name"]],
                    headers={"content-type": "application/json"},
                )
            reporter, partner, flow, frequency, periods, codes, count_only = key
            assert frequency == "A"
            assert set(periods) <= {"2021", "2022", "2023"}
            assert set(codes) <= {"1201", "1005", "0901", "1701", "2304"}
            assert reporter in {76, 156}
            rows = [
                row
                for row in pool
                if row["reporterCode"] == reporter
                and (partner is None or row["partnerCode"] == int(partner))
                and row["flowCode"] == flow
                and row["period"] in periods
                and row["cmdCode"] in codes
            ]
            synthetic.append(
                {
                    "query": key,
                    "reason": "Synthetic envelope over independently captured disjoint rows",
                }
            )
            if count_only:
                body = {
                    "elapsedTime": "synthetic",
                    "count": len(rows),
                    "data": copy.deepcopy(placeholder),
                    "error": "",
                }
            else:
                body = {
                    "elapsedTime": "synthetic",
                    "count": min(len(rows), 500),
                    "data": rows[:500],
                    "error": "",
                }
            return httpx.Response(200, json=body)

        def client(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
            return original(*args, transport=httpx.MockTransport(respond), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", client)
        return requests, synthetic

    return install
