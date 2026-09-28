from __future__ import annotations

import httpx

from agrobr.http.settings import get_timeout


class TestGetTimeout:
    def test_returns_httpx_timeout(self):
        timeout = get_timeout()
        assert isinstance(timeout, httpx.Timeout)


def test_timeout_read_do_ambiente_vale_como_minimo(monkeypatch):
    assert get_timeout(read=120.0).read == 120.0
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "900")
    assert get_timeout(read=120.0).read == 900.0
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "10")
    assert get_timeout(read=120.0).read == 120.0
    assert get_timeout().read == 10.0
