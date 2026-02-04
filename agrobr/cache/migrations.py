"""Schema migrations para DuckDB."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    import duckdb

logger = structlog.get_logger()

SCHEMA_VERSION = 3

MIGRATIONS: dict[int, str] = {
    1: """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT OR IGNORE INTO schema_version (version) VALUES (1);
    """,
    2: """
        ALTER TABLE cache_entries ADD COLUMN IF NOT EXISTS hit_count INTEGER DEFAULT 0;
        ALTER TABLE cache_entries ADD COLUMN IF NOT EXISTS stale BOOLEAN DEFAULT FALSE;
    """,
    3: """
        CREATE INDEX IF NOT EXISTS idx_history_key_date ON history_entries(key, data_date);
        CREATE INDEX IF NOT EXISTS idx_history_parser ON history_entries(parser_version);
    """,
}


def get_current_version(conn: duckdb.DuckDBPyConnection) -> int:
    """Retorna versão atual do schema."""
    try:
        result = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        return result[0] if result and result[0] else 0
    except Exception:
        return 0


def migrate(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Executa migrations pendentes.

    Migrations são idempotentes e podem ser re-executadas com segurança.
    """
    current = get_current_version(conn)

    if current >= SCHEMA_VERSION:
        logger.debug("schema_up_to_date", version=current)
        return

    logger.info("schema_migration_start", current=current, target=SCHEMA_VERSION)

    for version in range(current + 1, SCHEMA_VERSION + 1):
        if version in MIGRATIONS:
            try:
                for statement in MIGRATIONS[version].strip().split(";"):
                    statement = statement.strip()
                    if statement:
                        try:
                            conn.execute(statement)
                        except Exception as stmt_error:
                            if "already exists" in str(stmt_error).lower():
                                continue
                            if "duplicate" in str(stmt_error).lower():
                                continue
                            raise

                with contextlib.suppress(Exception):
                    conn.execute("INSERT INTO schema_version (version) VALUES (?)", [version])

                logger.info("migration_applied", version=version)
            except Exception as e:
                logger.error("migration_failed", version=version, error=str(e))
                raise

    logger.info("schema_migration_complete", version=SCHEMA_VERSION)
