"""Storage DuckDB com separação cache/histórico."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import duckdb
import structlog

from agrobr import constants

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
CREATE TABLE IF NOT EXISTS history_entries (
    id INTEGER PRIMARY KEY,
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


class DuckDBStore:
    """Storage com DuckDB separando cache volátil e histórico permanente."""

    def __init__(self, settings: constants.CacheSettings | None = None) -> None:
        self.settings = settings or constants.CacheSettings()
        self.db_path = self.settings.cache_dir / self.settings.db_name
        self._conn: duckdb.DuckDBPyConnection | None = None

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
        """Busca entrada no cache. Retorna (dados, is_stale)."""
        conn = self._get_conn()
        now = datetime.utcnow()

        result = conn.execute(
            "SELECT data, expires_at, stale FROM cache_entries WHERE key = ?",
            [key],
        ).fetchone()

        if result is None:
            logger.debug("cache_miss", key=key, reason="not_found")
            return None, False

        data, expires_at, stale = result

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

    def cache_set(
        self,
        key: str,
        data: bytes,
        source: constants.Fonte,
        ttl_seconds: int,
    ) -> None:
        """Grava entrada no cache."""
        conn = self._get_conn()
        now = datetime.utcnow()
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
        """Marca entrada como stale."""
        conn = self._get_conn()
        conn.execute("UPDATE cache_entries SET stale = TRUE WHERE key = ?", [key])

    def cache_delete(self, key: str) -> None:
        """Remove entrada do cache."""
        conn = self._get_conn()
        conn.execute("DELETE FROM cache_entries WHERE key = ?", [key])

    def cache_clear(
        self,
        source: constants.Fonte | None = None,
        older_than_days: int | None = None,
    ) -> int:
        """Limpa cache com filtros opcionais. Retorna número de entradas removidas."""
        conn = self._get_conn()

        conditions = []
        params: list[Any] = []

        if source:
            conditions.append("source = ?")
            params.append(source.value)

        if older_than_days:
            cutoff = datetime.utcnow() - timedelta(days=older_than_days)
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
        """Salva dados no histórico permanente."""
        if not self.settings.save_to_history:
            return

        conn = self._get_conn()
        now = datetime.utcnow()

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
        """Busca dados no histórico. Se data_date não especificado, retorna mais recente."""
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
        """
        Busca indicadores no histórico local.

        Args:
            produto: Nome do produto
            inicio: Data inicial
            fim: Data final
            praca: Praça específica (opcional)

        Returns:
            Lista de dicts com dados dos indicadores
        """
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

    def indicadores_upsert(self, indicadores: list[dict[str, Any]]) -> int:
        """
        Salva indicadores no histórico (upsert).

        Args:
            indicadores: Lista de dicts com dados dos indicadores

        Returns:
            Número de indicadores salvos/atualizados
        """
        if not indicadores:
            return 0

        conn = self._get_conn()
        now = datetime.utcnow()
        count = 0

        for ind in indicadores:
            try:
                conn.execute(
                    """
                    INSERT INTO indicadores
                    (produto, praca, data, valor, unidade, fonte, metodologia,
                     variacao_percentual, collected_at, parser_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (produto, praca, data, fonte)
                    DO UPDATE SET
                        valor = EXCLUDED.valor,
                        variacao_percentual = EXCLUDED.variacao_percentual,
                        collected_at = EXCLUDED.collected_at
                    """,
                    [
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
                    ],
                )
                count += 1
            except Exception as e:
                logger.warning(
                    "indicador_upsert_failed",
                    data=ind.get("data"),
                    error=str(e),
                )

        logger.info("indicadores_upsert", count=count, total=len(indicadores))
        return count

    def indicadores_get_dates(
        self,
        produto: str,
        inicio: datetime | None = None,
        fim: datetime | None = None,
    ) -> set[datetime]:
        """
        Retorna conjunto de datas com indicadores no histórico.

        Args:
            produto: Nome do produto
            inicio: Data inicial
            fim: Data final

        Returns:
            Set de datas presentes no histórico
        """
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
        """Fecha conexão."""
        if self._conn:
            self._conn.close()
            self._conn = None


_store: DuckDBStore | None = None


def get_store() -> DuckDBStore:
    """Obtém instância global do store."""
    global _store
    if _store is None:
        _store = DuckDBStore()
    return _store
