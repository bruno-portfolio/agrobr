from __future__ import annotations

import contextlib
import json
import threading
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import structlog

from agrobr import constants
from agrobr.exceptions import CacheMigrationError
from agrobr.normalize import regions
from agrobr.utils.time import utcnow
from agrobr.utils.warnings import warn_once

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
    valor_usd DECIMAL(18,4),
    peso_medio_kg DECIMAL(10,3),
    anomalies VARCHAR,
    UNIQUE(produto, praca, data, fonte)
);

CREATE INDEX IF NOT EXISTS idx_ind_produto ON indicadores(produto);
CREATE INDEX IF NOT EXISTS idx_ind_data ON indicadores(data);
CREATE INDEX IF NOT EXISTS idx_ind_produto_data ON indicadores(produto, data);

CREATE TABLE IF NOT EXISTS cepea_series (
    produto TEXT PRIMARY KEY,
    publicada_ate DATE NOT NULL,
    baixada_em TIMESTAMP NOT NULL
);
"""

UPSERT_CHUNK_SIZE = 5000

SINAIS_DE_ARQUIVO_DANIFICADO = (
    "Could not read",
    "Corrupt database file",
    "not a valid DuckDB database file",
)

_STAGING_DDL = """
CREATE TEMP TABLE IF NOT EXISTS _ind_staging (
    produto TEXT, praca TEXT, data DATE, valor DECIMAL(18,4),
    unidade TEXT, fonte TEXT, metodologia TEXT,
    variacao_percentual DECIMAL(8,4),
    collected_at TIMESTAMP, parser_version INTEGER,
    valor_usd DECIMAL(18,4), peso_medio_kg DECIMAL(10,3), anomalies VARCHAR
)
"""

_STAGING_INSERT = "INSERT INTO _ind_staging VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

_MERGE_SQL = """
INSERT INTO indicadores
(produto, praca, data, valor, unidade, fonte, metodologia,
 variacao_percentual, collected_at, parser_version, valor_usd, peso_medio_kg, anomalies)
SELECT * FROM _ind_staging
QUALIFY row_number() OVER (
    PARTITION BY produto, praca, data, fonte ORDER BY rowid DESC
) = 1
ON CONFLICT (produto, praca, data, fonte)
DO UPDATE SET
    valor = EXCLUDED.valor,
    unidade = EXCLUDED.unidade,
    metodologia = EXCLUDED.metodologia,
    parser_version = EXCLUDED.parser_version,
    variacao_percentual = EXCLUDED.variacao_percentual,
    collected_at = EXCLUDED.collected_at,
    valor_usd = EXCLUDED.valor_usd,
    peso_medio_kg = EXCLUDED.peso_medio_kg,
    anomalies = EXCLUDED.anomalies
"""


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


class DuckDBStore:
    def __init__(self, settings: constants.CacheSettings | None = None) -> None:
        self.settings = settings or constants.CacheSettings()
        self.db_path = self.settings.cache_dir / self.settings.db_name
        self._degraded = False
        self._lock = threading.Lock()

    @contextlib.contextmanager
    def _conexao(self) -> Iterator[duckdb.DuckDBPyConnection | None]:
        """Uma conexão por operação, fechada ao fim: o arquivo fica livre para outro processo."""
        with self._lock:
            conn = self._get_conn()
            if conn is None:
                yield None
                return
            try:
                yield conn
            except duckdb.Error as erro:
                conn.close()
                self._degrade(erro)
                raise
            finally:
                conn.close()

    def _get_conn(self) -> duckdb.DuckDBPyConnection | None:
        """Abre uma conexão nova, que o chamador fecha.

        Falha de abertura (arquivo em uso por outro processo, corrupção, permissão) vira
        no-op nesta operação, e a próxima tenta de novo; falhas de migração interrompem o
        acesso para preservar o histórico.
        """
        conn: duckdb.DuckDBPyConnection | None = None
        try:
            self.settings.cache_dir.mkdir(parents=True, exist_ok=True)
            conn = duckdb.connect(str(self.db_path))
            self._init_schema(conn)
        except (CacheMigrationError, KeyboardInterrupt, SystemExit):
            _close_quietly(conn)
            raise
        except (duckdb.Error, OSError) as e:
            _close_quietly(conn)
            self._degrade(e)
            return None
        return conn

    def _degrade(self, error: Exception) -> None:
        if not self._degraded:
            logger.warning("cache_degraded", db_path=str(self.db_path), error=str(error))
        self._degraded = True
        movido = self._mover_danificado(error)
        if movido is not None:
            warn_once(
                f"cache_danificado:{movido}",
                f"agrobr: o cache em {self.db_path} está danificado ({error}); ele foi movido "
                f"para {movido}, e a próxima consulta cria um cache novo. Apague o arquivo "
                "movido quando não precisar mais dele.",
            )
            return
        warn_once(
            "cache_degradado",
            f"agrobr: cache indisponível em {self.db_path} ({type(error).__name__}: {error}); "
            "a consulta segue sem cache. Se outro processo estiver gravando, a próxima consulta "
            "tenta de novo; sem escrita na pasta, aponte a variável AGROBR_CACHE_CACHE_DIR para "
            "outra.",
        )

    def _mover_danificado(self, error: Exception) -> Path | None:
        """Move para o lado o banco (e o WAL) que o DuckDB não consegue ler.

        Arquivo em uso por outro processo, disco cheio e falha ao mover deixam o arquivo onde está.
        """
        if not _arquivo_danificado(error):
            return None
        sufixo = f".corrompido-{utcnow():%Y%m%d%H%M}"
        wal = self.db_path.with_name(f"{self.db_path.name}.wal")
        destino = self.db_path.with_name(self.db_path.name + sufixo)
        try:
            if wal.exists():
                wal.rename(wal.with_name(wal.name + sufixo))
            self.db_path.rename(destino)
        except OSError as falha:
            logger.warning("cache_move_failed", db_path=str(self.db_path), error=str(falha))
            return None
        return destino

    @staticmethod
    def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
        from agrobr.cache.migrations import migrate

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
        conditions = ["produto = ?"]
        params: list[Any] = [produto.lower()]

        if inicio:
            conditions.append("data >= ?")
            params.append(inicio)

        if fim:
            conditions.append("data <= ?")
            params.append(fim)

        where = " AND ".join(conditions)

        try:
            with self._conexao() as conn:
                if conn is None:
                    return []
                result = conn.execute(
                    f"""
                    SELECT produto, praca, data, valor, unidade, fonte, metodologia,
                           variacao_percentual, collected_at, parser_version,
                           valor_usd, peso_medio_kg, anomalies
                    FROM indicadores
                    WHERE {where}
                    ORDER BY data DESC
                    """,
                    params,
                ).fetchall()
        except duckdb.Error:
            return []

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
            "valor_usd",
            "peso_medio_kg",
            "anomalies",
        ]

        indicadores = [dict(zip(columns, row)) for row in result]
        for indicador in indicadores:
            texto = indicador["anomalies"]
            indicador["anomalies"] = json.loads(texto) if texto else None

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

    def indicadores_ultima_coleta(self, produto: str) -> datetime | None:
        try:
            with self._conexao() as conn:
                if conn is None:
                    return None
                coleta: datetime | None
                [(coleta,)] = conn.execute(
                    "SELECT max(collected_at) FROM indicadores WHERE produto = ?", [produto.lower()]
                ).fetchall()
        except duckdb.Error:
            return None
        return coleta

    def serie_registrar(self, produto: str, publicada_ate: date, baixada_em: datetime) -> None:
        with contextlib.suppress(duckdb.Error), self._conexao() as conn:
            if conn is None:
                return
            conn.execute(
                "INSERT OR REPLACE INTO cepea_series VALUES (?, ?, ?)",
                [produto.lower(), publicada_ate, baixada_em],
            )

    def serie_cobertura(self, produto: str) -> tuple[date, datetime] | None:
        """A última data que a série publicou e quando foi baixada; a página não mexe nisso."""
        try:
            with self._conexao() as conn:
                if conn is None:
                    return None
                linha = conn.execute(
                    "SELECT publicada_ate, baixada_em FROM cepea_series WHERE produto = ?",
                    [produto.lower()],
                ).fetchone()
        except duckdb.Error:
            return None
        return None if linha is None else (linha[0], linha[1])

    @staticmethod
    def _to_row(ind: dict[str, Any], now: datetime) -> tuple[Any, ...]:
        for field in ("produto", "data", "valor", "unidade", "fonte"):
            if field in ind and ind[field] is None:
                raise ValueError(f"{field} must not be null")
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
            _optional_float(ind.get("valor_usd")),
            _optional_float(ind.get("peso_medio_kg")),
            json.dumps(list(ind.get("anomalies") or []), ensure_ascii=False),
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

        try:
            with self._conexao() as conn:
                if conn is None:
                    return 0
                conn.execute(_STAGING_DDL)

                for start in range(0, len(rows), UPSERT_CHUNK_SIZE):
                    chunk = rows[start : start + UPSERT_CHUNK_SIZE]
                    try:
                        conn.execute("BEGIN TRANSACTION")
                        conn.executemany(_STAGING_INSERT, chunk)
                        conn.execute("COMMIT")
                    except duckdb.Error:
                        conn.execute("ROLLBACK")
                        for row in chunk:
                            try:
                                conn.execute(_STAGING_INSERT, list(row))
                            except duckdb.Error as e:
                                logger.warning("indicador_upsert_failed", data=row[2], error=str(e))

                [(staged,)] = conn.execute("SELECT count(*) FROM _ind_staging").fetchall()
                count = int(staged)
                try:
                    conn.execute(_MERGE_SQL)
                except duckdb.Error as e:
                    if _arquivo_danificado(e):
                        raise
                    logger.warning("indicador_merge_failed", error=str(e))
                    count = 0
        except duckdb.Error:
            return 0

        logger.info("indicadores_upsert", count=count, total=len(indicadores))
        return count

    def close(self) -> None:
        """Nada fica aberto entre operações; fica para quem fecha o store ao sair."""


def _arquivo_danificado(error: Exception) -> bool:
    return isinstance(error, duckdb.Error) and any(
        sinal in str(error) for sinal in SINAIS_DE_ARQUIVO_DANIFICADO
    )


def _close_quietly(conn: duckdb.DuckDBPyConnection | None) -> None:
    if conn is not None:
        with contextlib.suppress(duckdb.Error):
            conn.close()


_store: DuckDBStore | None = None
_store_lock = threading.Lock()


def get_store() -> DuckDBStore:
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = DuckDBStore()
    return _store
