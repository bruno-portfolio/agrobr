from __future__ import annotations

import contextlib

import duckdb
import structlog

logger = structlog.get_logger()

SCHEMA_VERSION = 5

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
    4: """
        CREATE TABLE IF NOT EXISTS health_checks (
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            category TEXT,
            latency_ms REAL NOT NULL,
            message TEXT,
            checked_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        );
        CREATE INDEX IF NOT EXISTS idx_hc_composite ON health_checks(source, checked_at, status);
    """,
    5: """
        DELETE FROM indicadores WHERE praca IS NULL;
    """,
}


def get_current_version(conn: duckdb.DuckDBPyConnection) -> int:
    try:
        result = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        return int(result[0]) if result and result[0] else 0
    except duckdb.Error as exc:
        logger.debug("schema_version_table_missing", error=str(exc))
        return 0


def migrate(conn: duckdb.DuckDBPyConnection) -> None:
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
                            err_msg = str(stmt_error).lower()
                            if "already exists" in err_msg:
                                continue
                            if "duplicate" in err_msg:
                                continue
                            if "depend on it" in err_msg:
                                continue
                            if "does not exist" in err_msg:
                                continue
                            raise

                with contextlib.suppress(Exception):
                    conn.execute("INSERT INTO schema_version (version) VALUES (?)", [version])

                logger.info("migration_applied", version=version)
            except Exception as e:
                logger.error("migration_failed", version=version, error=str(e))
                raise

    logger.info("schema_migration_complete", version=SCHEMA_VERSION)
