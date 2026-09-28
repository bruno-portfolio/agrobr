"""Tests for agrobr.cache.migrations."""

from __future__ import annotations

from unittest import mock

import duckdb

from agrobr.cache import duckdb_store, migrations
from agrobr.cache.migrations import SCHEMA_VERSION, get_current_version, migrate
from tests.helpers import sem_excecao

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


class TestMigrate:
    def test_migration_2_skips_alter_for_indexed_current_schema(self):
        with _fresh_conn() as conn:
            conn.execute(duckdb_store.SCHEMA_CACHE)
            _seed_version(conn, 1)
            assert migrations._migration_sql(conn, 2) == ""
            indexes = conn.execute(
                "SELECT index_name, sql FROM duckdb_indexes() "
                "WHERE table_name = 'cache_entries' ORDER BY index_name"
            ).fetchall()
            observed = mock.Mock(wraps=conn)
            migrate(observed)
            assert not any(
                "ALTER TABLE" in call.args[0] for call in observed.execute.call_args_list
            )
            assert get_current_version(conn) == SCHEMA_VERSION
            assert (
                conn.execute(
                    "SELECT index_name, sql FROM duckdb_indexes() "
                    "WHERE table_name = 'cache_entries' ORDER BY index_name"
                ).fetchall()
                == indexes
            )

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


LEGACY_INDICADORES = """
CREATE TABLE indicadores (
    id INTEGER PRIMARY KEY,
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
);
"""


def test_migration_10_extends_existing_quarantine_before_data_migrations():
    with _fresh_conn() as conn:
        conn.execute(LEGACY_INDICADORES)
        conn.execute(
            "CREATE TABLE indicadores_quarentena AS SELECT *, "
            "CAST(NULL AS INTEGER) AS quarantine_migration, "
            "CAST(NULL AS TIMESTAMP) AS quarantined_at, "
            "CAST(NULL AS VARCHAR) AS quarantine_reason FROM indicadores WHERE false"
        )
        conn.execute(
            "INSERT INTO indicadores (id, produto, praca, data, valor, unidade, fonte, "
            "collected_at, parser_version) VALUES (1, 'leite', 'SP', DATE '2026-08-01', 2.5, "
            "'BRL/L', 'noticias_agricolas', CURRENT_TIMESTAMP, 3)"
        )
        _seed_version(conn, 8)
        migrate(conn)
        for table in ("indicadores", "indicadores_quarentena"):
            columns = {row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()}
            assert {"valor_usd", "peso_medio_kg"} <= columns
        assert conn.execute("SELECT count(*) FROM indicadores").fetchone() == (0,)
        assert conn.execute(
            "SELECT quarantine_migration FROM indicadores_quarentena"
        ).fetchall() == [(9,)]
        assert get_current_version(conn) == SCHEMA_VERSION


LEGACY_INDICADORES_10 = LEGACY_INDICADORES.replace(
    "    parser_version INTEGER DEFAULT 1,\n",
    "    parser_version INTEGER DEFAULT 1,\n    valor_usd DECIMAL(18,4),\n    peso_medio_kg DECIMAL(10,3),\n",
)


def test_migration_11_grava_a_marca_semanal_do_na_nas_linhas_antigas():
    with _fresh_conn() as conn:
        conn.execute(LEGACY_INDICADORES_10)
        conn.execute(
            "CREATE TABLE indicadores_quarentena AS SELECT *, "
            "CAST(NULL AS INTEGER) AS quarantine_migration, "
            "CAST(NULL AS TIMESTAMP) AS quarantined_at, "
            "CAST(NULL AS VARCHAR) AS quarantine_reason FROM indicadores WHERE false"
        )
        for ident, produto, fonte in [
            (1, "etanol_hidratado", "noticias_agricolas"),
            (2, "etanol_anidro", "noticias_agricolas"),
            (3, "soja", "noticias_agricolas"),
            (4, "etanol_hidratado", "cepea"),
        ]:
            conn.execute(
                "INSERT INTO indicadores (id, produto, praca, data, valor, unidade, fonte, "
                "collected_at, parser_version) VALUES (?, ?, 'São Paulo/SP', DATE '2026-09-25', "
                "2.5, 'BRL/L', ?, CURRENT_TIMESTAMP, 3)",
                [ident, produto, fonte],
            )
        _seed_version(conn, 10)
        migrate(conn)
        for table in ("indicadores", "indicadores_quarentena"):
            assert "anomalies" in {row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()}
        assert conn.execute("SELECT id, anomalies FROM indicadores ORDER BY id").fetchall() == [
            (1, '["media_semanal"]'),
            (2, '["media_semanal"]'),
            (3, "[]"),
            (4, "[]"),
        ]
        assert get_current_version(conn) == SCHEMA_VERSION == 11


def test_migration_11_sem_a_tabela_indicadores_nao_falha():
    with _fresh_conn() as conn:
        _seed_base_tables(conn)
        _seed_version(conn, 10)
        with sem_excecao():
            migrate(conn)
        assert get_current_version(conn) == SCHEMA_VERSION
