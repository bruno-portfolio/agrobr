from __future__ import annotations

import httpx

from agrobr.constants import HTTPSettings
from agrobr.http.settings import get_timeout


class TestGetTimeout:
    def test_anp_diesel_allows_three_concurrent_requests(self):
        assert HTTPSettings().max_concurrent_anp_diesel == 3

    def test_returns_httpx_timeout(self):
        timeout = get_timeout()
        assert isinstance(timeout, httpx.Timeout)

    def test_default_values(self):
        timeout = get_timeout()
        assert timeout.connect == 10.0
        assert timeout.read == 30.0
        assert timeout.write == 10.0
        assert timeout.pool == 10.0

    def test_custom_settings(self):
        settings = HTTPSettings(timeout_connect=5.0, timeout_read=60.0)
        timeout = get_timeout(settings)
        assert timeout.connect == 5.0
        assert timeout.read == 60.0

    def test_from_env_vars(self, monkeypatch):
        monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_CONNECT", "2.5")
        monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "45.0")
        settings = HTTPSettings()
        timeout = get_timeout(settings)
        assert timeout.connect == 2.5
        assert timeout.read == 45.0

    def test_read_override(self):
        timeout = get_timeout(read=60.0)
        assert timeout.read == 60.0
        assert timeout.connect == 10.0

    def test_read_override_with_settings(self):
        settings = HTTPSettings(timeout_read=45.0)
        timeout = get_timeout(settings, read=120.0)
        assert timeout.read == 120.0
