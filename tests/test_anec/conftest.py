from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from agrobr.utils import time as time_utils

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def category_2026_p1_payload() -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / "json" / "category_2026_p1.json").read_text(encoding="utf-8"))


@pytest.fixture
def no_next_data_html() -> str:
    return (FIXTURES_DIR / "html" / "no_next_data.html").read_text(encoding="utf-8")


@pytest.fixture
def malformed_json_html() -> str:
    return (FIXTURES_DIR / "html" / "malformed_json.html").read_text(encoding="utf-8")


def make_html(payload: dict[str, Any]) -> str:
    return (
        "<html><body>"
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(payload)
        + "</script></body></html>"
    )


@pytest.fixture
def html_factory():
    return make_html


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path / "agrobr_cache"))
    return tmp_path / "agrobr_cache"


@pytest.fixture(autouse=True)
def _reset_anec_list_cache():
    from agrobr.anec import client as _client
    from agrobr.anec.api import _parse_cache_clear

    _client._list_cache_clear()
    _parse_cache_clear()
    yield
    _client._list_cache_clear()
    _parse_cache_clear()


@pytest.fixture(autouse=True)
def reference_year(monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2026, 9, 8, 12, tzinfo=UTC))
