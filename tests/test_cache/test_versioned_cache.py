from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agrobr.cache.duckdb_store import DuckDBStore
from agrobr.cache.keys import (
    build_cache_key,
    is_legacy_key,
    legacy_key_prefix,
    parse_cache_key,
)
from agrobr.constants import CacheSettings, Fonte


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class TestParseCacheKey:
    def test_parses_valid_key(self):
        key = "cepea|abc123def456|v0.9.0|sv1.0"
        parsed = parse_cache_key(key)

        assert parsed["dataset"] == "cepea"
        assert parsed["params_hash"] == "abc123def456"
        assert parsed["lib_version"] == "0.9.0"
        assert parsed["schema_version"] == "1.0"

    def test_raises_on_legacy_key(self):
        with pytest.raises(ValueError, match="esperado 4 partes"):
            parse_cache_key("cepea_soja_2024")

    def test_raises_on_too_many_parts(self):
        with pytest.raises(ValueError, match="esperado 4 partes"):
            parse_cache_key("a|b|c|d|e")

    def test_roundtrip_with_build(self):
        from agrobr import __version__

        key = build_cache_key("ibge:pam", {"produto": "soja", "ano": 2024})
        parsed = parse_cache_key(key)
        assert parsed["dataset"] == "ibge:pam"
        assert parsed["lib_version"] == __version__


class TestIsLegacyKey:
    def test_legacy_key_no_pipes(self):
        assert is_legacy_key("cepea_soja_2024") is True

    def test_legacy_key_one_pipe(self):
        assert is_legacy_key("cepea|abc123") is True

    def test_versioned_key(self):
        key = build_cache_key("ds", {"a": "1"})
        assert is_legacy_key(key) is False


class TestLegacyKeyPrefix:
    def test_extracts_prefix(self):
        key = "cepea|abc123def456|v0.9.0|sv1.0"
        assert legacy_key_prefix(key) == "cepea|abc123def456"

    def test_returns_original_if_no_pipe(self):
        assert legacy_key_prefix("simple") == "simple"


@pytest.fixture()
def tmp_store(tmp_path: Path) -> DuckDBStore:
    settings = CacheSettings(cache_dir=tmp_path, db_name="test.duckdb")
    store = DuckDBStore(settings)
    yield store
    store.close()


@pytest.fixture()
def strict_store(tmp_path: Path) -> DuckDBStore:
    settings = CacheSettings(cache_dir=tmp_path, db_name="test.duckdb", strict_mode=True)
    store = DuckDBStore(settings)
    yield store
    store.close()


class TestLegacyCacheMigration:
    def test_migration_legacy_key_rewritten(self, tmp_store: DuckDBStore):
        legacy_key = "cepea|abc123def456"
        conn = tmp_store._get_conn()
        now = _utcnow()
        conn.execute(
            """
            INSERT INTO cache_entries
            (key, data, source, created_at, expires_at, last_accessed_at, hit_count, version, stale)
            VALUES (?, ?, ?, ?, ?, ?, 0, 1, FALSE)
            """,
            [legacy_key, b"legacy_data", "cepea", now, now + timedelta(hours=4), now],
        )

        versioned_key = "cepea|abc123def456|v0.9.0|sv1.0"
        data, stale = tmp_store.cache_get(versioned_key)

        assert data == b"legacy_data"
        assert stale is False

        old = conn.execute("SELECT * FROM cache_entries WHERE key = ?", [legacy_key]).fetchone()
        assert old is None

        new = conn.execute("SELECT * FROM cache_entries WHERE key = ?", [versioned_key]).fetchone()
        assert new is not None

    def test_migration_preserves_stale_flag(self, tmp_store: DuckDBStore):
        legacy_key = "cepea|abc123def456"
        conn = tmp_store._get_conn()
        now = _utcnow()
        conn.execute(
            """
            INSERT INTO cache_entries
            (key, data, source, created_at, expires_at, last_accessed_at, hit_count, version, stale)
            VALUES (?, ?, ?, ?, ?, ?, 0, 1, TRUE)
            """,
            [legacy_key, b"stale_data", "cepea", now, now + timedelta(hours=4), now],
        )

        versioned_key = "cepea|abc123def456|v0.9.0|sv1.0"
        data, stale = tmp_store.cache_get(versioned_key)

        assert data == b"stale_data"
        assert stale is True

    def test_no_migration_for_non_legacy_miss(self, tmp_store: DuckDBStore):
        versioned_key = build_cache_key("cepea", {"produto": "soja"})
        data, stale = tmp_store.cache_get(versioned_key)

        assert data is None
        assert stale is False


class TestStrictMode:
    def test_strict_mode_rejects_old_version(self, strict_store: DuckDBStore):
        old_key = "ds|abc123def456|v0.8.0|sv1.0"
        conn = strict_store._get_conn()
        now = _utcnow()
        conn.execute(
            """
            INSERT INTO cache_entries
            (key, data, source, created_at, expires_at, last_accessed_at, hit_count, version, stale)
            VALUES (?, ?, ?, ?, ?, ?, 0, 1, FALSE)
            """,
            [old_key, b"old_data", "cepea", now, now + timedelta(hours=4), now],
        )

        data, stale = strict_store.cache_get(old_key)

        assert data is None
        assert stale is False

    def test_strict_mode_accepts_current_version(self, strict_store: DuckDBStore):
        key = build_cache_key("ds", {"a": "1"})
        strict_store.cache_set(key, b"current", Fonte.CEPEA, ttl_seconds=3600)

        data, stale = strict_store.cache_get(key)

        assert data == b"current"
        assert stale is False

    def test_non_strict_accepts_old_version(self, tmp_store: DuckDBStore):
        old_key = "ds|abc123def456|v0.8.0|sv1.0"
        conn = tmp_store._get_conn()
        now = _utcnow()
        conn.execute(
            """
            INSERT INTO cache_entries
            (key, data, source, created_at, expires_at, last_accessed_at, hit_count, version, stale)
            VALUES (?, ?, ?, ?, ?, ?, 0, 1, FALSE)
            """,
            [old_key, b"old_data", "cepea", now, now + timedelta(hours=4), now],
        )

        data, stale = tmp_store.cache_get(old_key)

        assert data == b"old_data"
        assert stale is False


class TestConcurrentWrites:
    def test_concurrent_writes_no_corruption(self, tmp_path: Path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="test.duckdb")

        init_store = DuckDBStore(settings)
        init_store._get_conn()
        init_store.close()

        errors: list[Exception] = []

        def writer(thread_id: int):
            store = DuckDBStore(settings)
            try:
                for i in range(20):
                    key = f"t{thread_id}_k{i:03d}|hash|v0.9.0|sv1.0"
                    store.cache_set(
                        key, f"t{thread_id}_v{i}".encode(), Fonte.CEPEA, ttl_seconds=3600
                    )
            except Exception as e:
                errors.append(e)
            finally:
                store.close()

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent write errors: {errors}"

        verify_store = DuckDBStore(settings)
        for t in range(3):
            for i in range(20):
                key = f"t{t}_k{i:03d}|hash|v0.9.0|sv1.0"
                data, _ = verify_store.cache_get(key)
                assert data is not None, f"Key {key} missing after concurrent writes"
        verify_store.close()
