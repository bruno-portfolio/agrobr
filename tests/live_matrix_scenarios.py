from __future__ import annotations

import os
import socket
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import pytest
import pytest_socket
import requests

from agrobr import datasets
from agrobr.antaq import client as antaq_client
from agrobr.exceptions import ParseError, SourceUnavailableError


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        item.own_markers[:] = [mark for mark in item.own_markers if mark.name != "enable_socket"]
        item.add_marker(pytest.mark.disable_socket)


@pytest.fixture(autouse=True)
def synthetic_live_sources(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    scenario = os.environ["AGROBR_TEST_LIVE_SCENARIO"]
    if scenario == "fixture_error":
        raise RuntimeError("Synthetic fixture bug")
    if scenario == "fixture_skip":
        pytest.skip("Synthetic unexpected fixture skip")
    monkeypatch.delenv("AGROBR_USDA_API_KEY", raising=False)
    if scenario == "usda_with_key":
        monkeypatch.setenv("AGROBR_USDA_API_KEY", "synthetic-not-a-credential")
    original_dataset = datasets.get_dataset

    async def synthetic_fetch(*_args: Any, **_kwargs: Any) -> tuple[pd.DataFrame, SimpleNamespace]:
        with pytest.warns(UserWarning), pytest.raises(pytest_socket.SocketBlockedError):
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if scenario in {"mixed", "bad_metadata", "teardown_error"}:
            frame = pd.DataFrame({"value": [1.0]})
            meta = SimpleNamespace(
                selected_source="synthetic",
                attempted_sources=["synthetic"],
                columns=frame.columns.tolist(),
                records_count=0 if scenario == "bad_metadata" else len(frame),
            )
            return frame, meta
        if scenario == "antaq_aggregated_text":
            raise SourceUnavailableError(
                source="movimentacao_portuaria/",
                errors=[("antaq", "parse", "antaq unavailable: Retriable status: 522")],
            )
        raise SourceUnavailableError(source="synthetic", last_error="Synthetic outage")

    def synthetic_dataset(name: str) -> Any:
        if name == "movimentacao_portuaria" and scenario in {
            "mixed",
            "known_only",
            "antaq_other_url",
            "antaq_503",
            "antaq_parse",
        }:
            return original_dataset(name)
        return SimpleNamespace(fetch=synthetic_fetch)

    def response(url: str, **_kwargs: Any) -> requests.Response:
        with pytest.warns(UserWarning), pytest.raises(pytest_socket.SocketBlockedError):
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = requests.Response()
        result.status_code = 503 if scenario == "antaq_503" else 522
        result.url = url
        result._content = b"Synthetic remote status; no network request"
        return result

    monkeypatch.setattr(datasets, "get_dataset", synthetic_dataset)
    monkeypatch.setattr(antaq_client.requests, "get", response)
    if scenario == "antaq_other_url":
        monkeypatch.setattr(antaq_client, "BULK_TXT_BASE", "https://example.invalid/other")
    if scenario == "antaq_parse":
        monkeypatch.setattr(
            antaq_client,
            "fetch_ano_zip",
            AsyncMock(side_effect=ParseError("antaq", 1, "Synthetic layout error 522")),
        )
    yield
    if scenario == "teardown_error":
        raise RuntimeError("Synthetic teardown bug")
