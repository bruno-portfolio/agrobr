from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import duckdb
import httpx
import pytest

from agrobr import constants
from agrobr.cache import duckdb_store, policies
from agrobr.cepea import api, client
from tests import helpers


@pytest.mark.parametrize("failure", ["503", "timeout"])
async def test_recuperacao_http_apos_falha_transitoria(
    monkeypatch: pytest.MonkeyPatch, failure: str
):
    attempts: list[str] = []
    factory = httpx.AsyncClient

    def respond(request: httpx.Request) -> httpx.Response:
        status = failure if not attempts else "200"
        attempts.append(status)
        if status == "timeout":
            raise httpx.ReadTimeout("interrupção transitória", request=request)
        return httpx.Response(int(status), content=b"recovered", request=request)

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(respond), **kwargs),
    )
    with helpers.collect_failures() as check:
        with check("resposta"):
            result = await client._fetch_with_httpx("https://example.invalid/cepea", {})
            assert result.html == "recovered"
            assert result.source == "cepea"
        with check("tentativas"):
            assert attempts == [failure, "200"]


@pytest.mark.parametrize(
    "instant,expected",
    [
        (datetime(2026, 9, 18, 20, 59), datetime(2026, 9, 18, 21)),
        (datetime(2026, 9, 18, 21), datetime(2026, 9, 21, 21)),
        (datetime(2026, 9, 18, 21, 1), datetime(2026, 9, 21, 21)),
    ],
)
async def test_expiracao_publica_no_horario_diario(
    monkeypatch: pytest.MonkeyPatch, instant: datetime, expected: datetime
):
    monkeypatch.setattr(policies, "utcnow", lambda: instant)
    monkeypatch.setattr(api, "utcnow", lambda: instant)
    monkeypatch.setattr(api, "_today", lambda: instant.date())
    store = mock.MagicMock()
    store.indicadores_query.return_value = [
        {
            "fonte": "cepea",
            "produto": "soja",
            "praca": "Paranaguá/PR",
            "data": date(2026, 9, 18),
            "valor": 150.0,
            "unidade": "BRL/sc60kg",
            "collected_at": instant,
        }
    ]
    monkeypatch.setattr(api, "get_store", lambda: store)
    _, meta = await api.indicador(
        "soja", inicio="2026-09-18", fim="2026-09-18", offline=True, return_meta=True
    )
    assert meta.cache_expires_at == expected.replace(tzinfo=UTC)


def test_migracao_opcionais_preserva_precisao_e_reexecucao(tmp_path: Path):
    settings = constants.CacheSettings(cache_dir=tmp_path)
    with duckdb.connect(str(tmp_path / settings.db_name)) as connection:
        connection.execute(duckdb_store.SCHEMA_CACHE)
        connection.execute(duckdb_store.SCHEMA_HISTORY)
        legacy = duckdb_store.SCHEMA_INDICADORES.replace(
            "    valor_usd DECIMAL(18,4),\n", ""
        ).replace("    peso_medio_kg DECIMAL(10,3),\n", "")
        connection.execute(legacy)
        connection.execute(
            "CREATE TABLE schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        connection.execute("INSERT INTO schema_version (version) VALUES (9)")
    values: dict[str, Any] = {
        "produto": "bezerro",
        "praca": "Mato Grosso do Sul",
        "data": date(2026, 9, 4),
        "valor": 3397.99,
        "unidade": "BRL/cabeca",
        "fonte": "cepea",
        "parser_version": 2,
        "valor_usd": 662.38,
        "peso_medio_kg": 211.69,
    }
    with helpers.collect_failures() as check:
        for iteration in range(2):
            with check(iteration):
                store = duckdb_store.DuckDBStore(settings)
                try:
                    store.indicadores_upsert([values, values])
                    rows = store.indicadores_query("bezerro")
                    assert len(rows) == 1
                    assert float(rows[0]["valor_usd"]) == 662.38
                    assert float(rows[0]["peso_medio_kg"]) == 211.69
                finally:
                    store.close()


@pytest.mark.parametrize("day", [15, 28])
def test_datetime_preserva_dia_civil(day: int):
    start, end = api._normalize_dates(datetime(2025, 1, day, 23, 59), datetime(2025, 2, day, 0, 1))
    assert start == date(2025, 1, day)
    assert end == date(2025, 2, day)


@pytest.mark.parametrize("force_refresh", [False, True])
async def test_offline_impede_aquisicao_mesmo_sem_cache(
    monkeypatch: pytest.MonkeyPatch, force_refresh: bool
):
    store = mock.MagicMock()
    store.indicadores_query.return_value = []
    fetch = mock.AsyncMock(return_value=api._FetchResult([], "cepea", "", 2, "", 0, 0))
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(api, "_today", lambda: date(2026, 9, 18))
    monkeypatch.setattr(api, "_fetch_and_parse", fetch)
    frame = await api.indicador(
        "soja", inicio="2026-09-18", fim="2026-09-18", offline=True, force_refresh=force_refresh
    )
    assert fetch.await_count == 0
    assert frame.empty
