from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import structlog

from agrobr import constants

logger = structlog.get_logger()


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


SCHEMA_CACHE = """
CREATE TABLE IF NOT EXISTS cache_entries (
    key TEXT PRIMARY KEY,
    data BLOB NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    last_accessed_at TIMESTAMP NOT NULL,
    hit_count INTEGER DEFAULT 0,
    version INTEGER DEFAULT 1,
    stale BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_cache_source ON cache_entries(source);
CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache_entries(expires_at);
"""

SCHEMA_HISTORY = """
CREATE SEQUENCE IF NOT EXISTS seq_history_id START 1;

CREATE TABLE IF NOT EXISTS history_entries (
    id INTEGER DEFAULT nextval('seq_history_id') PRIMARY KEY,
    key TEXT NOT NULL,
    data BLOB NOT NULL,
    source TEXT NOT NULL,
    data_date DATE NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    parser_version INTEGER NOT NULL,
    fingerprint_hash TEXT,
    UNIQUE(key, data_date, collected_at)
);

CREATE INDEX IF NOT EXISTS idx_history_source ON history_entries(source);
CREATE INDEX IF NOT EXISTS idx_history_date ON history_entries(data_date);
CREATE INDEX IF NOT EXISTS idx_history_key ON history_entries(key);
"""

SCHEMA_INDICADORES = """
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
);

CREATE INDEX IF NOT EXISTS idx_ind_produto ON indicadores(produto);
CREATE INDEX IF NOT EXISTS idx_ind_data ON indicadores(data);
CREATE INDEX IF NOT EXISTS idx_ind_produto_data ON indicadores(produto, data);
"""

UPSERT_CHUNK_SIZE = 5000

_STAGING_DDL = """
CREATE TEMP TABLE IF NOT EXISTS _ind_staging (
    produto TEXT, praca TEXT, data DATE, valor DECIMAL(18,4),
    unidade TEXT, fonte TEXT, metodologia TEXT,
    variacao_percentual DECIMAL(8,4),
    collected_at TIMESTAMP, parser_version INTEGER
)
"""

_STAGING_INSERT = "INSERT INTO _ind_staging VALUES (?,?,?,?,?,?,?,?,?,?)"

_MERGE_SQL = """
INSERT INTO indicadores
(produto, praca, data, valor, unidade, fonte, metodologia,
 variacao_percentual, collected_at, parser_version)
SELECT * FROM _ind_staging
ON CONFLICT (produto, praca, data, fonte)
DO UPDATE SET
    valor = EXCLUDED.valor,
    variacao_percentual = EXCLUDED.variacao_percentual,
    collected_at = EXCLUDED.collected_at
"""


class DuckDBStore:
    def __init__(self, settings: constants.CacheSettings | None = None) -> None:
        self.settings = settings or constants.CacheSettings()
        self.db_path = self.settings.cache_dir / self.settings.db_name
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self.settings.cache_dir.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(str(self.db_path))
            self._init_schema()
        return self._conn

    def _init_schema(self) -> None:
        from agrobr.cache.migrations import migrate

        conn = self._conn
        if conn:
            conn.execute(SCHEMA_CACHE)
            conn.execute(SCHEMA_HISTORY)
            conn.execute(SCHEMA_INDICADORES)
            migrate(conn)

    def cache_get(self, key: str) -> tuple[bytes | None, bool]:
        from agrobr import __version__
        from agrobr.cache.keys import is_legacy_key, legacy_key_prefix, parse_cache_key

        with self._lock:
            conn = self._get_conn()
            now = _utcnow()

            result = conn.execute(
                "SELECT data, expires_at, stale, key FROM cache_entries WHERE key = ?",
                [key],
            ).fetchone()

            if result is not None:
                data, expires_at, stale, stored_key = result

                if self.settings.strict_mode and not is_legacy_key(stored_key):
                    try:
                        parsed = parse_cache_key(stored_key)
                        if parsed["lib_version"] != __version__:
                            logger.debug(
                                "cache_miss",
                                key=key,
                                reason="strict_version_mismatch",
                                cached_version=parsed["lib_version"],
                                current_version=__version__,
                            )
                            return None, False
                    except ValueError:
                        pass

                conn.execute(
                    "UPDATE cache_entries SET hit_count = hit_count + 1, last_accessed_at = ? WHERE key = ?",
                    [now, key],
                )

                if expires_at < now:
                    logger.debug("cache_hit", key=key, stale=True, reason="expired")
                    return data, True

                if stale:
                    logger.debug("cache_hit", key=key, stale=True, reason="marked_stale")
                    return data, True

                logger.debug("cache_hit", key=key, stale=False)
                return data, False

            if not is_legacy_key(key):
                prefix = legacy_key_prefix(key)
                legacy_result = conn.execute(
                    "SELECT key, data, source, expires_at, stale FROM cache_entries WHERE key LIKE ? || '%'",
                    [prefix],
                ).fetchone()

                if legacy_result is not None:
                    legacy_key, data, source, expires_at, stale = legacy_result

                    if is_legacy_key(legacy_key):
                        remaining_ttl = max(int((expires_at - now).total_seconds()), 0)
                        conn.execute("DELETE FROM cache_entries WHERE key = ?", [legacy_key])
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO cache_entries
                            (key, data, source, created_at, expires_at, last_accessed_at, hit_count, version, stale)
                            VALUES (?, ?, ?, ?, ?, ?, 0, 1, ?)
                            """,
                            [
                                key,
                                data,
                                source,
                                now,
                                now + timedelta(seconds=remaining_ttl),
                                now,
                                stale,
                            ],
                        )
                        logger.info(
                            "legacy_cache_migrated",
                            old_key=legacy_key,
                            new_key=key,
                        )

                        if expires_at < now:
                            return data, True
                        if stale:
                            return data, True
                        return data, False

        logger.debug("cache_miss", key=key, reason="not_found")
        return None, False

    def cache_set(
        self,
        key: str,
        data: bytes,
        source: constants.Fonte,
        ttl_seconds: int,
    ) -> None:
        with self._lock:
            conn = self._get_conn()
            now = _utcnow()
            expires_at = now + timedelta(seconds=ttl_seconds)

            conn.execute(
                """
                INSERT OR REPLACE INTO cache_entries
                (key, data, source, created_at, expires_at, last_accessed_at, hit_count, version, stale)
                VALUES (?, ?, ?, ?, ?, ?, 0, 1, FALSE)
                """,
                [key, data, source.value, now, expires_at, now],
            )

        logger.debug("cache_write", key=key, ttl_seconds=ttl_seconds)

    def cache_invalidate(self, key: str) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute("UPDATE cache_entries SET stale = TRUE WHERE key = ?", [key])

    def cache_delete(self, key: str) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute("DELETE FROM cache_entries WHERE key = ?", [key])

    def cache_clear(
        self,
        source: constants.Fonte | None = None,
        older_than_days: int | None = None,
    ) -> int:
        with self._lock:
            conn = self._get_conn()

            conditions = []
            params: list[Any] = []

            if source:
                conditions.append("source = ?")
                params.append(source.value)

            if older_than_days:
                cutoff = _utcnow() - timedelta(days=older_than_days)
                conditions.append("created_at < ?")
                params.append(cutoff)

            where = " AND ".join(conditions) if conditions else "1=1"
            result = conn.execute(f"DELETE FROM cache_entries WHERE {where} RETURNING *", params)

            count = len(result.fetchall()) if result else 0

        logger.info("cache_cleared", count=count, source=source, older_than_days=older_than_days)
        return count

    def history_save(
        self,
        key: str,
        data: bytes,
        source: constants.Fonte,
        data_date: datetime,
        parser_version: int,
        fingerprint_hash: str | None = None,
    ) -> None:
        if not self.settings.save_to_history:
            return

        with self._lock:
            conn = self._get_conn()
            now = _utcnow()

            try:
                conn.execute(
                    """
                    INSERT INTO history_entries
                    (key, data, source, data_date, collected_at, parser_version, fingerprint_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [key, data, source.value, data_date, now, parser_version, fingerprint_hash],
                )
                logger.debug("history_saved", key=key, data_date=data_date)
            except duckdb.ConstraintException:
                logger.debug("history_exists", key=key, data_date=data_date)

    def history_get(
        self,
        key: str,
        data_date: datetime | None = None,
    ) -> bytes | None:
        with self._lock:
            conn = self._get_conn()

            if data_date:
                result = conn.execute(
                    """
                    SELECT data FROM history_entries
                    WHERE key = ? AND data_date = ?
                    ORDER BY collected_at DESC LIMIT 1
                    """,
                    [key, data_date],
                ).fetchone()
            else:
                result = conn.execute(
                    """
                    SELECT data FROM history_entries
                    WHERE key = ?
                    ORDER BY data_date DESC, collected_at DESC LIMIT 1
                    """,
                    [key],
                ).fetchone()

            return result[0] if result else None

    def indicadores_query(
        self,
        produto: str,
        inicio: datetime | None = None,
        fim: datetime | None = None,
        praca: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            conn = self._get_conn()

            conditions = ["produto = ?"]
            params: list[Any] = [produto.lower()]

            if inicio:
                conditions.append("data >= ?")
                params.append(inicio)

            if fim:
                conditions.append("data <= ?")
                params.append(fim)

            if praca:
                conditions.append("praca = ?")
                params.append(praca)

            where = " AND ".join(conditions)

            result = conn.execute(
                f"""
                SELECT produto, praca, data, valor, unidade, fonte, metodologia,
                       variacao_percentual, collected_at, parser_version
                FROM indicadores
                WHERE {where}
                ORDER BY data DESC
                """,
                params,
            ).fetchall()

        columns = [
            "produto",
            "praca",
            "data",
            "valor",
            "unidade",
            "fonte",
            "metodologia",
            "variacao_percentual",
            "collected_at",
            "parser_version",
        ]

        indicadores = [dict(zip(columns, row)) for row in result]

        logger.debug(
            "indicadores_query",
            produto=produto,
            count=len(indicadores),
            inicio=inicio,
            fim=fim,
        )

        return indicadores

    @staticmethod
    def _to_row(ind: dict[str, Any], now: datetime) -> tuple[Any, ...]:
        return (
            ind.get("produto", "").lower(),
            ind.get("praca"),
            ind["data"],
            float(ind["valor"]),
            ind.get("unidade", "BRL/unidade"),
            ind.get("fonte", "unknown"),
            ind.get("metodologia"),
            ind.get("variacao_percentual"),
            now,
            ind.get("parser_version", 1),
        )

    def indicadores_upsert(self, indicadores: list[dict[str, Any]]) -> int:
        if not indicadores:
            return 0

        now = _utcnow()

        rows: list[tuple[Any, ...]] = []
        for ind in indicadores:
            try:
                rows.append(self._to_row(ind, now))
            except (KeyError, ValueError, TypeError) as e:
                logger.warning("indicador_row_invalid", data=ind.get("data"), error=str(e))

        if not rows:
            return 0

        with self._lock:
            conn = self._get_conn()
            count = 0
            conn.execute(_STAGING_DDL)

            for start in range(0, len(rows), UPSERT_CHUNK_SIZE):
                chunk = rows[start : start + UPSERT_CHUNK_SIZE]
                try:
                    conn.execute("DELETE FROM _ind_staging")
                    conn.executemany(_STAGING_INSERT, chunk)
                    conn.execute(_MERGE_SQL)
                    count += len(chunk)
                except duckdb.Error:
                    conn.execute("DELETE FROM _ind_staging")
                    for row in chunk:
                        try:
                            conn.execute(_STAGING_INSERT, list(row))
                        except duckdb.Error as e:
                            logger.warning("indicador_upsert_failed", data=row[2], error=str(e))
                    try:
                        conn.execute(_MERGE_SQL)
                        row_count = conn.execute("SELECT count(*) FROM _ind_staging").fetchone()
                        count += row_count[0] if row_count else 0
                    except duckdb.Error as e:
                        logger.warning("indicador_merge_failed", error=str(e))
                    conn.execute("DELETE FROM _ind_staging")

        logger.info("indicadores_upsert", count=count, total=len(indicadores))
        return count

    def indicadores_get_dates(
        self,
        produto: str,
        inicio: datetime | None = None,
        fim: datetime | None = None,
    ) -> set[datetime]:
        with self._lock:
            conn = self._get_conn()

            conditions = ["produto = ?"]
            params: list[Any] = [produto.lower()]

            if inicio:
                conditions.append("data >= ?")
                params.append(inicio)

            if fim:
                conditions.append("data <= ?")
                params.append(fim)

            where = " AND ".join(conditions)

            result = conn.execute(
                f"SELECT DISTINCT data FROM indicadores WHERE {where}",
                params,
            ).fetchall()

        dates = {row[0] for row in result}
        return dates

    def close(self) -> None:
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None


_store: DuckDBStore | None = None
_store_lock = threading.Lock()


def get_store() -> DuckDBStore:
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = DuckDBStore()
    return _store
