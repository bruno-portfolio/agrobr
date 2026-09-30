from __future__ import annotations

import contextlib

import duckdb

from agrobr import _log
from agrobr.exceptions import CacheMigrationError

logger = _log.get_logger(__name__)

SCHEMA_VERSION = 11

CACHE_COLUMN_MIGRATIONS: dict[str, str] = {
    "hit_count": "ALTER TABLE cache_entries ADD COLUMN hit_count INTEGER DEFAULT 0",
    "stale": "ALTER TABLE cache_entries ADD COLUMN stale BOOLEAN DEFAULT FALSE",
}

INDICADORES_COLUMN_MIGRATIONS: dict[str, str] = {
    "valor_usd": "ALTER TABLE {table} ADD COLUMN valor_usd DECIMAL(18,4)",
    "peso_medio_kg": "ALTER TABLE {table} ADD COLUMN peso_medio_kg DECIMAL(10,3)",
}

INDICADORES_ANOMALIAS_MIGRATIONS: dict[str, str] = {
    "anomalies": "ALTER TABLE {table} ADD COLUMN anomalies VARCHAR",
}

COLUMN_MIGRATIONS: dict[int, tuple[tuple[str, ...], dict[str, str]]] = {
    2: (("cache_entries",), CACHE_COLUMN_MIGRATIONS),
    10: (("indicadores", "indicadores_quarentena"), INDICADORES_COLUMN_MIGRATIONS),
    11: (("indicadores", "indicadores_quarentena"), INDICADORES_ANOMALIAS_MIGRATIONS),
}

NA_SEMANAIS = ("etanol_hidratado", "etanol_anidro")
COLUMN_BACKFILLS: dict[int, str] = {
    11: "UPDATE indicadores SET anomalies = CASE WHEN fonte = 'noticias_agricolas' AND produto IN "
    f"({', '.join(repr(produto) for produto in NA_SEMANAIS)}) "
    "THEN '[\"media_semanal\"]' ELSE '[]' END WHERE anomalies IS NULL",
}

NA_MILK_CONDITION = "produto = 'leite' AND fonte = 'noticias_agricolas'"
LEGACY_UNIT_CORRECTIONS: dict[str, str] = {
    "fonte = 'cepea' AND parser_version < 2 AND produto = 'trigo' "
    "AND unidade = 'BRL/sc60kg'": "BRL/ton",
    "fonte = 'cepea' AND parser_version < 2 AND produto = 'algodao' "
    "AND unidade = 'BRL/@'": "cBRL/lb",
}

QUARANTINE_RULES: dict[int, tuple[str, str]] = {
    5: ("praca IS NULL", "Praça ausente no histórico legado"),
    6: (
        "produto = 'suino' AND ("
        "(fonte = 'cepea' AND COALESCE(parser_version, 0) < 2) OR "
        "(fonte = 'noticias_agricolas' AND COALESCE(parser_version, 0) < 3))",
        "Praças de suíno anteriores à identificação regional",
    ),
    7: (
        "fonte = 'cepea' AND COALESCE(parser_version, 0) < 2 "
        "AND produto IN ('soja_parana', 'frango_resfriado', 'etanol_anidro', "
        "'acucar_refinado', 'leite', 'laranja_industria', 'laranja_in_natura')",
        "Seleção de tabela, unidade ou mês de referência do CEPEA legado",
    ),
    8: (
        "produto = 'acucar_refinado' AND fonte IN ('cepea', 'noticias_agricolas') "
        "AND unidade = 'BRL/sc50kg'",
        "Refinado por kg rotulado como saca de 50 kg pelo parser legado",
    ),
    9: (
        " OR ".join(
            f"({condition})" for condition in (NA_MILK_CONDITION, *LEGACY_UNIT_CORRECTIONS)
        ),
        "Data de publicação do leite NA ou rótulo legado de unidade de trigo/algodão CEPEA",
    ),
}

MIGRATIONS: dict[int, str] = {
    1: """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    2: ";".join(CACHE_COLUMN_MIGRATIONS.values()),
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
    **{
        version: f"DELETE FROM indicadores WHERE {condition}"
        for version, (condition, _) in QUARANTINE_RULES.items()
    },
    9: f"DELETE FROM indicadores WHERE {NA_MILK_CONDITION};"
    + ";".join(
        f"UPDATE indicadores SET unidade = '{unit}' WHERE {condition}"
        for condition, unit in LEGACY_UNIT_CORRECTIONS.items()
    ),
    10: ";".join(
        statement.format(table="indicadores")
        for statement in INDICADORES_COLUMN_MIGRATIONS.values()
    ),
}


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_catalog = current_database() AND table_schema = 'main' "
            "AND table_name = ?",
            [table],
        ).fetchone()
    )


def get_current_version(conn: duckdb.DuckDBPyConnection) -> int:
    if not _table_exists(conn, "schema_version"):
        return 0
    result = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
    return int(result[0]) if result and result[0] else 0


def _preserve_indicadores(conn: duckdb.DuckDBPyConnection, version: int) -> None:
    condition, reason = QUARANTINE_RULES[version]
    conn.execute("""
        CREATE TABLE IF NOT EXISTS indicadores_quarentena AS
        SELECT *, CAST(NULL AS INTEGER) AS quarantine_migration,
               CAST(NULL AS TIMESTAMP) AS quarantined_at,
               CAST(NULL AS VARCHAR) AS quarantine_reason
        FROM indicadores WHERE false
    """)
    conn.execute(
        "INSERT INTO indicadores_quarentena BY NAME "
        "SELECT *, ? AS quarantine_migration, CURRENT_TIMESTAMP AS quarantined_at, "
        f"? AS quarantine_reason FROM indicadores WHERE {condition}",
        [version, reason],
    )
    active = f"SELECT * FROM indicadores WHERE {condition}"
    preserved = (
        "SELECT * EXCLUDE (quarantine_migration, quarantined_at, quarantine_reason) "
        f"FROM indicadores_quarentena WHERE quarantine_migration = {version}"
    )
    for left, right in ((active, preserved), (preserved, active)):
        if conn.execute(f"SELECT 1 FROM ({left} EXCEPT ALL {right}) LIMIT 1").fetchone():
            raise CacheMigrationError(version, "Verificação da quarentena divergente")


def _missing_column_sql(conn: duckdb.DuckDBPyConnection, version: int) -> str:
    tables, statements = COLUMN_MIGRATIONS[version]
    pending: list[str] = []
    for table in tables:
        if not _table_exists(conn, table):
            continue
        columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_catalog = current_database() AND table_schema = 'main' "
                "AND table_name = ?",
                [table],
            ).fetchall()
        }
        pending.extend(
            statement.format(table=table)
            for name, statement in statements.items()
            if name not in columns
        )
    return ";".join(pending)


def _migration_sql(conn: duckdb.DuckDBPyConnection, version: int) -> str:
    if version in COLUMN_MIGRATIONS:
        passos = (_missing_column_sql(conn, version), COLUMN_BACKFILLS.get(version, ""))
        return ";".join(passo for passo in passos if passo)
    return MIGRATIONS[version]


def _apply_migration(conn: duckdb.DuckDBPyConnection, version: int) -> None:
    required_table = {2: "cache_entries", 3: "history_entries", 10: "indicadores"}.get(version)
    if version in COLUMN_BACKFILLS:
        required_table = "indicadores"
    if version in QUARANTINE_RULES:
        required_table = "indicadores"
    if required_table is None or _table_exists(conn, required_table):
        if version in QUARANTINE_RULES:
            _preserve_indicadores(conn, version)
        sql = _migration_sql(conn, version)
        if sql:
            conn.execute(sql)
    conn.execute("INSERT INTO schema_version (version) VALUES (?)", [version])


def migrate(conn: duckdb.DuckDBPyConnection) -> None:
    """Acréscimos de coluna são idempotentes e correm antes da transação principal:
    o DuckDB não altera uma tabela já modificada na mesma transação."""
    version = 0
    try:
        current = get_current_version(conn)
        if current >= SCHEMA_VERSION:
            logger.debug("schema_up_to_date", version=current)
            return
        logger.info("schema_migration_start", current=current, target=SCHEMA_VERSION)
        pending = range(current + 1, SCHEMA_VERSION + 1)
        for version in pending:
            if version in COLUMN_MIGRATIONS and (sql := _missing_column_sql(conn, version)):
                conn.execute(sql)
        conn.execute("BEGIN TRANSACTION")
        try:
            for version in pending:
                _apply_migration(conn, version)
            conn.execute("COMMIT")
        except BaseException:
            with contextlib.suppress(duckdb.Error):
                conn.execute("ROLLBACK")
            raise
    except (duckdb.Error, OSError) as exc:
        logger.error("migration_failed", version=version, error=str(exc))
        raise CacheMigrationError(version, str(exc)) from exc
    logger.info("schema_migration_complete", version=SCHEMA_VERSION)
