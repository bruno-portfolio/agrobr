from __future__ import annotations

import hashlib
import json
import traceback

import httpx
import pytest

from agrobr.comtrade import client, query
from agrobr.exceptions import ParseError, SourceUnavailableError


def selection(**changes):
    return query.build_query(
        **{
            "reporter": 76,
            "partner": 156,
            "hs_codes": ["1201"],
            "flow": "X",
            "period": "2023",
            "freq": "A",
            **changes,
        }
    )


async def test_replay_bilateral_count_and_data_preserve_resources(replay_http, captures):
    requests, synthetic = replay_http()
    result = await client.fetch_trade_acquisition(selection(), require_complete=True)
    assert len(result.records) == 1
    assert result.records[0].model_dump(by_alias=True)["netWgt"] == 74471954170.0
    assert result.coverage.state == "complete"
    assert result.selected_source == "comtrade_guest"
    assert result.attempted_sources == ["comtrade_guest"]
    assert len(requests) == len(result.resources) == 2
    data = next(item for item in result.resources if item.role == "data")
    assert data.sha256 == hashlib.sha256(captures["bodies"]["soy_br_cn_2023"]).hexdigest()
    assert data.size_bytes == len(captures["bodies"]["soy_br_cn_2023"])
    assert data.fetched_at.utcoffset().total_seconds() == 0
    assert all("Ocp-Apim-Subscription-Key" not in request.headers for request in requests)
    assert len(synthetic) == 1


async def test_replay_500_preview_refines_to_all_516_without_counting_parent(replay_http, captures):
    requests, synthetic = replay_http()
    result = await client.fetch_trade_acquisition(
        selection(
            partner=None,
            hs_codes=["1201", "1005", "0901", "1701", "2304"],
        ),
        require_complete=True,
    )
    assert len(result.records) == 516
    assert result.coverage.state == "complete"
    assert result.coverage.expected_count == 516
    assert len(requests) == 7
    assert synthetic == []
    records = [row.model_dump(by_alias=True) for row in result.records]
    assert len({(row["period"], row["cmdCode"], row["partnerCode"]) for row in records}) == 516
    parent = [
        resource
        for resource in result.resources
        if resource.role == "data" and resource.received_count == 500
    ]
    assert len(parent) == 1
    assert not parent[0].accepted
    assert (
        sum(
            resource.received_count
            for resource in result.resources
            if resource.role == "data" and resource.accepted
        )
        == 516
    )
    expected = []
    for name in ["soy_br_omitted_2023", "coffee_2023", "corn_2023", "sugar_2023", "meal_2023"]:
        expected.extend(json.loads(captures["bodies"][name])["data"])

    def by_key(row):
        return row["period"], row["cmdCode"], row["partnerCode"]

    assert {by_key(row): row["netWgt"] for row in records} == {
        by_key(row): row["netWgt"] for row in expected
    }


@pytest.mark.parametrize("status", [404, 429, 500, 599])
async def test_second_period_http_failure_never_returns_first_period_as_complete(
    status, replay_http
):
    def override(request, _index):
        if request.url.params["period"] == "2023":
            return httpx.Response(status, json={"error": "unavailable"})
        return None

    requests, _ = replay_http(override)
    with pytest.raises((httpx.HTTPStatusError, SourceUnavailableError)):
        await client.fetch_trade_acquisition(selection(period="2022-2023"))
    assert any(request.url.params["period"] == "2022" for request in requests)
    assert any(request.url.params["period"] == "2023" for request in requests)


@pytest.mark.parametrize("require_complete", [False, True])
async def test_minimal_leaf_missing_records_is_explicit_partial_or_strict_error(
    require_complete, replay_http, captures
):
    def override(request, _index):
        if request.url.params.get("countOnly", "").lower() == "true":
            body = json.loads(captures["bodies"]["agro5_2023_count"])
            body["count"] = 599
            return httpx.Response(200, json=body)
        return None

    replay_http(override)
    if require_complete:
        with pytest.raises(SourceUnavailableError):
            await client.fetch_trade_acquisition(selection(), require_complete=True)
    else:
        result = await client.fetch_trade_acquisition(selection())
        assert len(result.records) == 1
        assert result.coverage.state == "partial"
        assert result.coverage.expected_count == 599
        assert result.warnings


@pytest.mark.parametrize("damage", ["count_below_rows", "record_outside_query", "duplicate_record"])
async def test_inconsistent_data_is_rejected_without_global_dedup(damage, replay_http, captures):
    def override(request, _index):
        if request.url.params.get("countOnly", "").lower() == "true":
            return None
        body = json.loads(captures["bodies"]["soy_br_cn_2023"])
        if damage == "count_below_rows":
            body["count"] = 0
        elif damage == "record_outside_query":
            body["data"][0]["reporterCode"] = 842
        else:
            body["data"].append(body["data"][0].copy())
            body["count"] = 2
        return httpx.Response(200, json=body)

    replay_http(override)
    with pytest.raises(ParseError):
        await client.fetch_trade_acquisition(selection())


@pytest.mark.parametrize("status", [401, 403])
async def test_simulated_auth_failure_restarts_guest_without_secret_in_provenance(
    status, replay_http
):
    secret = "synthetic-comtrade-secret-09"

    def override(request, _index):
        if "/data/v1/get/" in request.url.path:
            assert request.headers["Ocp-Apim-Subscription-Key"] == secret
            return httpx.Response(status, json={"error": secret})
        assert "Ocp-Apim-Subscription-Key" not in request.headers
        return None

    requests, _ = replay_http(override)
    result = await client.fetch_trade_acquisition(selection(), api_key=secret)
    assert result.attempted_sources == ["comtrade_authenticated", "comtrade_guest"]
    assert result.selected_source == "comtrade_guest"
    assert len(result.records) == 1
    assert secret not in result.model_dump_json(exclude={"records"})
    assert all(secret not in str(request.url) for request in requests)
    assert result.fallback
    assert result.warnings[0] == (
        f"Chave do Comtrade recusada (HTTP {status}): confira AGROBR_COMTRADE_API_KEY ou o "
        "argumento api_key=; plano reiniciado integralmente no preview público."
    )


@pytest.mark.parametrize(
    "received,expected,state",
    [
        (499, 499, "complete"),
        (500, 500, "complete"),
        (500, 501, "partial"),
        (500, 599, "partial"),
        (500, 499, "invalid"),
    ],
)
async def test_limit_uses_independent_count_not_size_alone(
    received, expected, state, replay_http, captures
):
    original = json.loads(captures["bodies"]["soy_br_cn_2023"])["data"][0]
    rows = [{**original, "partnerCode": code} for code in range(received)]
    placeholder = json.loads(captures["bodies"]["agro5_2023_count"])["data"]

    def override(request, _index):
        counting = request.url.params.get("countOnly", "").lower() == "true"
        return httpx.Response(
            200,
            json={
                "count": expected if counting else received,
                "data": placeholder if counting else rows,
                "error": "",
            },
        )

    requests, _ = replay_http(override)
    if state == "invalid":
        with pytest.raises(ParseError):
            await client.fetch_trade_acquisition(selection(partner=None))
    else:
        result = await client.fetch_trade_acquisition(selection(partner=None))
        assert result.coverage.state == state
        assert len(result.records) == received
        assert result.coverage.expected_count == expected
    assert len(requests) == 2


async def test_simulated_auth_partial_plan_is_discarded_and_all_periods_restart_guest(
    replay_http, captures
):
    raw = json.loads(captures["bodies"]["soy_br_cn_2023"])["data"][0]
    placeholder = json.loads(captures["bodies"]["agro5_2023_count"])["data"]

    def override(request, _index):
        params = request.url.params
        auth = "/data/v1/get/" in request.url.path
        years = params["period"].split(",")
        if auth and years == ["2023"]:
            return httpx.Response(401, json={"error": "synthetic rejected second block"})
        assert len(years) <= (12 if auth else 1)
        rows = []
        if "2022" in years:
            rows = [{**raw, "period": "2022", "refYear": 2022, "refPeriodId": 20220101}]
        elif "2023" in years:
            rows = [raw]
        if auth:
            rows[0]["netWgt"] = 123.0
        counting = params.get("countOnly", "").lower() == "true"
        return httpx.Response(
            200, json={"count": len(rows), "data": placeholder if counting else rows, "error": ""}
        )

    requests, _ = replay_http(override)
    result = await client.fetch_trade_acquisition(
        selection(period="2011-2023"), api_key="synthetic-auth-key", require_complete=True
    )
    assert len(result.records) == 2
    assert all(row.model_dump(by_alias=True)["netWgt"] == raw["netWgt"] for row in result.records)
    assert result.selected_source == "comtrade_guest"
    assert all(
        not resource.accepted for resource in result.resources if resource.access == "authenticated"
    )
    assert {
        request.url.params["period"] for request in requests if "/public/" in request.url.path
    } == {str(year) for year in range(2011, 2024)}


@pytest.mark.parametrize("failure", ["transport", "http", "json"])
async def test_secret_in_untrusted_failure_does_not_escape_in_logs_or_formatted_exception(
    failure, replay_http, capsys
):
    secret = "sentinel-secret-do-not-publish-09"

    def override(request, _index):
        if failure == "transport":
            return httpx.ReadTimeout(secret, request=request)
        if failure == "http":
            return httpx.Response(400, json={"error": secret})
        return httpx.Response(200, content=secret.encode())

    replay_http(override)
    with pytest.raises((SourceUnavailableError, ParseError)) as exc:
        await client.fetch_trade_acquisition(selection(), api_key=secret)
    captured = capsys.readouterr()
    assert secret not in captured.out + captured.err
    assert secret not in "".join(traceback.format_exception(exc.value))


@pytest.mark.parametrize("damage", ["lost_parent_identity", "changed_parent_value"])
async def test_refinement_cannot_claim_complete_by_replacing_parent_records(
    damage, replay_http, captures
):
    original_parent = json.loads(captures["bodies"]["agro5_2023"])["data"]
    known = next(row for row in original_parent if row["cmdCode"] == "1201")

    def override(request, _index):
        params = request.url.params
        if params["cmdCode"] != "1201" or params.get("countOnly", "").lower() == "true":
            return None
        body = json.loads(captures["bodies"]["soy_br_omitted_2023"])
        row = next(row for row in body["data"] if row["partnerCode"] == known["partnerCode"])
        if damage == "lost_parent_identity":
            row["partnerCode"] = 99999
        else:
            row["netWgt"] = (row["netWgt"] or 0) + 1
        return httpx.Response(200, json=body)

    replay_http(override)
    with pytest.raises(ParseError):
        await client.fetch_trade_acquisition(
            selection(partner=None, hs_codes=["1201", "1005", "0901", "1701", "2304"]),
            require_complete=True,
        )


async def test_auth_redirect_to_other_origin_never_sends_credential(replay_http):
    secret = "synthetic-auth-origin-sentinel"

    def override(_request, _index):
        return httpx.Response(302, headers={"location": "https://example.org/collect"})

    requests, _ = replay_http(override)
    with pytest.raises((SourceUnavailableError, ParseError)) as exc:
        await client.fetch_trade_acquisition(selection(), api_key=secret)
    assert len(requests) == 1
    assert requests[0].url.host == "comtradeapi.un.org"
    assert secret not in str(exc.value)
