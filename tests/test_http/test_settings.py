from __future__ import annotations

import httpx

from agrobr.constants import Fonte, HTTPSettings
from agrobr.http.settings import get_client_kwargs, get_rate_limit, get_timeout


class TestGetTimeout:
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


class TestGetRateLimit:
    def test_known_fonte(self):
        rate = get_rate_limit(Fonte.CEPEA)
        assert rate == 2.0

    def test_ibge_fonte(self):
        rate = get_rate_limit(Fonte.IBGE)
        assert rate == 1.0

    def test_anda_fonte(self):
        rate = get_rate_limit(Fonte.ANDA)
        assert rate == 3.0

    def test_custom_settings(self):
        settings = HTTPSettings(rate_limit_cepea=5.0)
        rate = get_rate_limit(Fonte.CEPEA, settings)
        assert rate == 5.0

    def test_default_rate_limit(self):
        settings = HTTPSettings()
        assert settings.rate_limit_default == 1.0


class TestGetClientKwargs:
    def test_returns_dict_with_timeout(self):
        kwargs = get_client_kwargs(Fonte.CEPEA)
        assert "timeout" in kwargs
        assert isinstance(kwargs["timeout"], httpx.Timeout)

    def test_returns_dict_with_headers(self):
        kwargs = get_client_kwargs(Fonte.CEPEA)
        assert "headers" in kwargs
        assert "Accept-Language" in kwargs["headers"]

    def test_follow_redirects(self):
        kwargs = get_client_kwargs(Fonte.CEPEA)
        assert kwargs["follow_redirects"] is True

    def test_extra_headers_merged(self):
        kwargs = get_client_kwargs(
            Fonte.CEPEA,
            extra_headers={"X-Custom": "test"},
        )
        assert kwargs["headers"]["X-Custom"] == "test"
        assert "Accept-Language" in kwargs["headers"]

    def test_extra_headers_override(self):
        kwargs = get_client_kwargs(
            Fonte.CEPEA,
            extra_headers={"Accept-Language": "en-US"},
        )
        assert kwargs["headers"]["Accept-Language"] == "en-US"

    def test_custom_settings_propagated(self):
        settings = HTTPSettings(timeout_read=99.0)
        kwargs = get_client_kwargs(Fonte.IBGE, settings)
        assert kwargs["timeout"].read == 99.0
