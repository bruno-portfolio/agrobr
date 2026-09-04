"""Tests for agrobr.cache.migrations."""

from __future__ import annotations

import duckdb

from agrobr.cache.migrations import SCHEMA_VERSION, get_current_version, migrate

SCHEMA_CACHE_MINIMAL = """
CREATE TABLE IF NOT EXISTS cache_entries (
    key TEXT PRIMARY KEY,
    data BLOB NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    last_accessed_at TIMESTAMP NOT NULL,
    version INTEGER DEFAULT 1
);
"""

SCHEMA_HISTORY_MINIMAL = """
CREATE TABLE IF NOT EXISTS history_entries (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL,
    data BLOB NOT NULL,
    source TEXT NOT NULL,
    data_date DATE NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    parser_version INTEGER NOT NULL
);
"""


def _fresh_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


def _seed_base_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(SCHEMA_CACHE_MINIMAL)
    conn.execute(SCHEMA_HISTORY_MINIMAL)


def _seed_version(conn: duckdb.DuckDBPyConnection, version: int) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version "
        "(version INTEGER PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.execute("INSERT INTO schema_version (version) VALUES (?)", [version])


class TestGetCurrentVersion:
    def test_no_table_returns_zero(self):
        conn = _fresh_conn()
        assert get_current_version(conn) == 0

    def test_existing_version(self):
        conn = _fresh_conn()
        _seed_version(conn, 2)
        assert get_current_version(conn) == 2

    def test_first_run_does_not_emit_traceback(self, capsys):
        conn = _fresh_conn()

        assert get_current_version(conn) == 0

        captured = capsys.readouterr()
        assert "Traceback" not in captured.out
        assert "Traceback" not in captured.err


class TestMigrate:
    def test_fresh_db(self):
        conn = _fresh_conn()
        _seed_base_tables(conn)
        migrate(conn)
        assert get_current_version(conn) == SCHEMA_VERSION

    def test_partial_upgrade(self):
        conn = _fresh_conn()
        _seed_base_tables(conn)
        _seed_version(conn, 2)
        migrate(conn)
        assert get_current_version(conn) == SCHEMA_VERSION

    def test_up_to_date_is_noop(self):
        conn = _fresh_conn()
        _seed_base_tables(conn)
        _seed_version(conn, SCHEMA_VERSION)
        migrate(conn)
        assert get_current_version(conn) == SCHEMA_VERSION

    def test_idempotent(self):
        conn = _fresh_conn()
        _seed_base_tables(conn)
        migrate(conn)
        migrate(conn)
        assert get_current_version(conn) == SCHEMA_VERSION

    def test_creates_expected_tables(self):
        conn = _fresh_conn()
        _seed_base_tables(conn)
        migrate(conn)

        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert "schema_version" in tables

    def test_migration_2_adds_columns(self):
        conn = _fresh_conn()
        _seed_base_tables(conn)
        _seed_version(conn, 1)
        migrate(conn)

        columns = {row[1] for row in conn.execute("PRAGMA table_info('cache_entries')").fetchall()}
        assert "hit_count" in columns
        assert "stale" in columns

    def test_migration_3_creates_indexes(self):
        conn = _fresh_conn()
        _seed_base_tables(conn)
        _seed_version(conn, 2)
        migrate(conn)

        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'history_entries'"
            ).fetchall()
        }
        assert "idx_history_key_date" in indexes
        assert "idx_history_parser" in indexes

    def test_migration_5_deletes_null_praca(self):
        conn = _fresh_conn()
        _seed_base_tables(conn)
        conn.execute(
            """
            CREATE SEQUENCE IF NOT EXISTS seq_indicadores_id START 1;
            CREATE TABLE IF NOT EXISTS indicadores (
                id INTEGER DEFAULT nextval('seq_indicadores_id') PRIMARY KEY,
                produto TEXT NOT NULL,
                praca TEXT,
                data DATE NOT NULL,
                valor DECIMAL(18,4) NOT NULL,
                unidade TEXT NOT NULL,
                fonte TEXT NOT NULL,
                metodologia TEXT,
                variacao_percentual DECIMAL(8,4),
                collected_at TIMESTAMP NOT NULL,
                parser_version INTEGER DEFAULT 1,
                UNIQUE(produto, praca, data, fonte)
            )
            """
        )
        conn.execute(
            "INSERT INTO indicadores (produto, praca, data, valor, unidade, fonte, collected_at) "
            "VALUES ('soja', NULL, '2024-01-01', 24.82, 'BRL/sc', 'cepea', CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "INSERT INTO indicadores (produto, praca, data, valor, unidade, fonte, collected_at) "
            "VALUES ('soja', 'Paranaguá/PR', '2024-01-01', 145.50, 'BRL/sc', 'cepea', CURRENT_TIMESTAMP)"
        )
        _seed_version(conn, 4)

        migrate(conn)

        rows = conn.execute("SELECT * FROM indicadores").fetchall()
        assert len(rows) == 1
        assert rows[0][2] == "Paranaguá/PR"
