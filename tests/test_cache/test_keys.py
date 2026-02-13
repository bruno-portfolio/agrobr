from __future__ import annotations

from unittest.mock import patch

from agrobr.cache.keys import build_cache_key


class TestBuildCacheKey:
    def test_includes_version_and_schema(self):
        key = build_cache_key("cepea", {"produto": "soja"})
        assert "|v0.9.0|" in key
        assert key.endswith("|sv1.0")
        assert key.startswith("cepea|")

    def test_params_order_irrelevant(self):
        k1 = build_cache_key("ds", {"a": "1", "b": "2"})
        k2 = build_cache_key("ds", {"b": "2", "a": "1"})
        assert k1 == k2

    def test_none_param_normalized(self):
        k1 = build_cache_key("ds", {"x": None})
        k2 = build_cache_key("ds", {"x": ""})
        assert k1 == k2

    def test_different_params_different_keys(self):
        k1 = build_cache_key("cepea", {"produto": "soja"})
        k2 = build_cache_key("cepea", {"produto": "milho"})
        assert k1 != k2

    def test_different_schema_version(self):
        k1 = build_cache_key("ds", {"a": "1"}, schema_version="1.0")
        k2 = build_cache_key("ds", {"a": "1"}, schema_version="2.0")
        assert k1 != k2

    def test_different_lib_version(self):
        k1 = build_cache_key("ds", {"a": "1"})
        with patch("agrobr.__version__", "99.0.0"):
            k2 = build_cache_key("ds", {"a": "1"})
        assert k1 != k2

    def test_key_format(self):
        key = build_cache_key("ibge:pam", {"produto": "soja", "ano": 2024})
        parts = key.split("|")
        assert len(parts) == 4
        assert parts[0] == "ibge:pam"
        assert len(parts[1]) == 12
        assert parts[2].startswith("v")
        assert parts[3].startswith("sv")
