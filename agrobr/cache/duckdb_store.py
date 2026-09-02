from __future__ import annotations

import contextlib
import threading
from datetime import datetime
from typing import Any

import duckdb
import structlog

from agrobr import constants
from agrobr.normalize import regions
from agrobr.utils.time import utcnow

logger = structlog.get_logger()


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
        self._degraded = False
        self._lock = threading.Lock()

    def _get_conn(self) -> duckdb.DuckDBPyConnection | None:
        """Conexão lazy; falha de abertura (lock de outro processo, corrupção,
        permissão) degrada o cache para no-op permanente no processo — cache é
        otimização, nunca pode derrubar o fetch."""
        if self._conn is None and not self._degraded:
            try:
                self.settings.cache_dir.mkdir(parents=True, exist_ok=True)
                self._conn = duckdb.connect(str(self.db_path))
                self._init_schema()
            except (duckdb.Error, OSError) as e:
                if self._conn is not None:
                    with contextlib.suppress(duckdb.Error):
                        self._conn.close()
                    self._conn = None
                self._degraded = True
                logger.warning("cache_degraded", db_path=str(self.db_path), error=str(e))
        return self._conn

    def _init_schema(self) -> None:
        from agrobr.cache.migrations import migrate

        conn = self._conn
        if conn:
            conn.execute(SCHEMA_CACHE)
            conn.execute(SCHEMA_HISTORY)
            conn.execute(SCHEMA_INDICADORES)
            migrate(conn)

    def indicadores_query(
        self,
        produto: str,
        inicio: datetime | None = None,
        fim: datetime | None = None,
        praca: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            conn = self._get_conn()
            if conn is None:
                return []

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

        if praca:
            praca_slug = regions.slugificar_praca(praca)
            indicadores = [
                indicador
                for indicador in indicadores
                if indicador["praca"]
                and regions.slugificar_praca(str(indicador["praca"])) == praca_slug
            ]

        logger.debug(
            "indicadores_query",
            produto=produto,
            count=len(indicadores),
            inicio=inicio,
            fim=fim,
            praca=praca,
        )

        return indicadores

    @staticmethod
    def _to_row(ind: dict[str, Any], now: datetime) -> tuple[Any, ...]:
        return (
            ind.get("produto", "").lower(),
            ind.get("praca") or "",
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

        now = utcnow()

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
            if conn is None:
                return 0
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
