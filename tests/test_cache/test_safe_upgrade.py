from __future__ import annotations

import duckdb
import pytest

from agrobr import constants
from agrobr.cache import duckdb_store, migrations
from agrobr.exceptions import CacheMigrationError
from tests.helpers import insert_cache_indicator, seed_cache_schema

AFFECTED = [
    "soja_parana",
    "frango_resfriado",
    "etanol_anidro",
    "acucar_refinado",
    "leite",
    "laranja_industria",
    "laranja_in_natura",
]
ORIGINALS = "* EXCLUDE (quarantine_migration, quarantined_at, quarantine_reason)"


def test_archive_content_mismatch_prevents_delete():
    with duckdb.connect(":memory:") as conn:
        seed_cache_schema(conn, 7)
        insert_cache_indicator(conn)
        conn.execute(
            "CREATE TABLE indicadores_quarentena AS SELECT *, 8 AS quarantine_migration, "
            "CURRENT_TIMESTAMP AS quarantined_at, 'anterior' AS quarantine_reason FROM indicadores"
        )
        with pytest.raises(CacheMigrationError, match="Verificação da quarentena divergente"):
            migrations.migrate(conn)
        assert conn.execute("SELECT count(*) FROM indicadores").fetchone() == (1,)
        assert conn.execute("SELECT count(*) FROM indicadores_quarentena").fetchone() == (1,)
        assert migrations.get_current_version(conn) == 7


def test_store_does_not_hide_failed_migration(tmp_path, monkeypatch):
    failure = CacheMigrationError(8, "falha simulada")
    with duckdb.connect(str(tmp_path / "agrobr.duckdb")) as conn:
        seed_cache_schema(conn, 7)
        insert_cache_indicator(conn)

    def fail(_conn):
        raise failure

    monkeypatch.setattr(migrations, "migrate", fail)
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path))
    with pytest.raises(CacheMigrationError):
        store.indicadores_query("soja")
    assert not store._degraded
    with duckdb.connect(str(tmp_path / "agrobr.duckdb"), read_only=True) as conn:
        assert conn.execute("SELECT count(*) FROM indicadores").fetchone() == (1,)


def test_version_read_failure_is_not_assumed_to_be_fresh_database():
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE schema_version (invalid INTEGER)")
        with pytest.raises(CacheMigrationError):
            migrations.migrate(conn)
