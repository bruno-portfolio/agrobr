from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import constants, datasets
from agrobr.cache import duckdb_store
from agrobr.cepea import api
from agrobr.datasets.preco_diario import PrecoDiarioDataset
from agrobr.exceptions import SourceUnavailableError, StaleDataWarning
from agrobr.models import Indicador


def recent_days() -> tuple[date, date]:
    latest = date.today()
    while latest.weekday() > 4:
        latest -= timedelta(days=1)
    previous = latest - timedelta(days=1)
    while previous.weekday() > 4:
        previous -= timedelta(days=1)
    return previous, latest


def record(
    day: date,
    value: float,
    source: str = "cepea",
    praca: str | None = "Paranaguá/PR",
    produto: str = "soja",
) -> Indicador:
    return Indicador(
        fonte=constants.Fonte(source),
        produto=produto,
        praca=praca,
        data=day,
        valor=Decimal(str(value)),
        unidade="BRL/sc60kg",
        parser_version=3 if source == "noticias_agricolas" else 2,
    )


def fetched(records: list[Indicador]) -> api._FetchResult:
    source = records[0].fonte.value
    return api._FetchResult(records, source, "https://example.invalid/synthetic", 2, "", 0, 0)


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[duckdb_store.DuckDBStore]:
    database = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(api, "get_store", lambda: database)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: database)
    monkeypatch.setattr(
        api, "_fetch_and_parse", AsyncMock(side_effect=AssertionError("unexpected fetch"))
    )
    try:
        yield database
    finally:
        database.close()


async def test_revisao_da_mesma_fonte_atualiza_somente_sua_observacao(
    store: duckdb_store.DuckDBStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    previous, latest = recent_days()
    store.indicadores_upsert(
        api._indicadores_to_dicts(
            [record(previous, 149), record(previous, 145, "noticias_agricolas")]
        )
    )
    transport = AsyncMock(return_value=fetched([record(previous, 150), record(latest, 151)]))
    monkeypatch.setattr(api, "_fetch_and_parse", transport)
    monkeypatch.setattr(api, "_vencido", lambda _ultima_coleta: True)

    frame = await api.indicador("soja", inicio=previous, fim=latest)
    offline = await api.indicador("soja", inicio=previous, fim=latest, offline=True)

    pd.testing.assert_frame_equal(frame, offline)
    assert frame["valor"].tolist() == [150, 151]
    saved = store.indicadores_query("soja")
    assert len(saved) == 3
    assert sorted(float(row["valor"]) for row in saved) == [145, 150, 151]


def test_anomalies_json_preserva_acentos_delimitadores_e_nulo() -> None:
    _, latest = recent_days()
    flagged = record(latest, 150)
    flagged.anomalies = ['unidade: "inválida"', "valor; limite\nsegunda linha"]

    frame = api._to_dataframe([flagged, record(latest, 151)])

    assert json.loads(frame.iloc[0]["anomalies"]) == flagged.anomalies
    assert pd.isna(frame.iloc[1]["anomalies"])


async def test_force_refresh_preserva_semantica_de_ignorar_leitura_inicial(
    store: duckdb_store.DuckDBStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, latest = recent_days()
    store.indicadores_upsert(api._indicadores_to_dicts([record(latest, 150)]))
    transport = AsyncMock(return_value=fetched([record(latest, 145, "noticias_agricolas")]))
    monkeypatch.setattr(api, "_fetch_and_parse", transport)

    forced, forced_meta = await api.indicador(
        "soja", inicio=latest, fim=latest, force_refresh=True, return_meta=True
    )
    offline, offline_meta = await api.indicador(
        "soja", inicio=latest, fim=latest, offline=True, return_meta=True
    )

    assert forced["valor"].tolist() == [145]
    assert forced_meta.data_sources == ["noticias_agricolas"]
    assert offline["valor"].tolist() == [150]
    assert offline_meta.data_sources == ["cepea"]
    assert len(store.indicadores_query("soja")) == 2
    transport.assert_awaited_once_with("soja")


async def test_dataset_nao_confunde_fonte_primaria_de_outra_praca(
    store: duckdb_store.DuckDBStore,
) -> None:
    _, latest = recent_days()
    store.indicadores_upsert(
        api._indicadores_to_dicts(
            [record(latest, 160, praca="Paraná"), record(latest, 145, "noticias_agricolas")]
        )
    )

    frame, meta = await datasets.preco_diario(
        "soja", inicio=latest, fim=latest, offline=True, return_meta=True
    )

    assert frame["valor"].tolist() == [145]
    assert frame["praca"].tolist() == ["Paranaguá/PR"]
    assert meta.data_sources == ["noticias_agricolas"]


async def test_cache_apos_falha_de_fetch_aplica_precedencia(
    store: duckdb_store.DuckDBStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, latest = recent_days()
    store.indicadores_upsert(
        api._indicadores_to_dicts([record(latest, 145, "noticias_agricolas"), record(latest, 150)])
    )
    transport = AsyncMock(
        side_effect=SourceUnavailableError("cepea", last_error="synthetic outage")
    )
    monkeypatch.setattr(api, "_fetch_and_parse", transport)

    with pytest.warns(StaleDataWarning):
        frame, meta = await api.indicador(
            "soja", inicio=latest, fim=latest, force_refresh=True, return_meta=True
        )

    assert frame["valor"].tolist() == [150]
    assert meta.data_sources == ["cepea"]
    assert meta.selected_source == "cache"
    assert meta.attempted_sources == ["cepea", "cache"]
    assert meta.from_cache
    assert len(store.indicadores_query("soja")) == 2
    transport.assert_awaited_once_with("soja")


async def test_dataset_cache_direto_aplica_precedencia_e_proveniencia(
    store: duckdb_store.DuckDBStore,
) -> None:
    _, latest = recent_days()
    store.indicadores_upsert(
        api._indicadores_to_dicts([record(latest, 145, "noticias_agricolas"), record(latest, 150)])
    )
    frame, meta = await PrecoDiarioDataset().fetch(
        "soja", inicio=latest, fim=latest, offline=True, return_meta=True
    )

    assert frame["valor"].tolist() == [150]
    assert str(frame["valor"].dtype) == "float64"
    assert meta.data_sources == ["cepea"]
    assert meta.selected_source == "cache" and meta.from_cache
    assert len(store.indicadores_query("soja")) == 2


async def test_dataset_cache_direto_respeita_a_janela(store: duckdb_store.DuckDBStore) -> None:
    previous, latest = recent_days()
    store.indicadores_upsert(
        api._indicadores_to_dicts(
            [record(previous - timedelta(days=7), 140), record(previous, 149), record(latest, 150)]
        )
    )
    dataset = PrecoDiarioDataset()

    dia = await dataset.fetch("soja", inicio=previous, fim=previous, offline=True)
    periodo = await dataset.fetch("soja", inicio=previous, fim=latest, offline=True)

    assert dia["valor"].tolist() == [149]
    assert sorted(periodo["valor"].tolist()) == [149, 150]


async def test_api_preserva_pracas_e_dataset_escolhe_praca_canonica(
    store: duckdb_store.DuckDBStore,
) -> None:
    _, latest = recent_days()
    records = [
        record(latest, 145, "noticias_agricolas", "Paranaguá"),
        record(latest, 150, "cepea", "Paranaguá/PR"),
        record(latest, 160, "cepea", "Paraná"),
    ]
    store.indicadores_upsert(api._indicadores_to_dicts(records))

    frame = await api.indicador("soja", inicio=latest, fim=latest, offline=True)
    filtered = await api.indicador(
        "soja", praca="paranagua", inicio=latest, fim=latest, offline=True
    )
    dataset = await datasets.preco_diario("soja", inicio=latest, fim=latest, offline=True)
    requested = await datasets.preco_diario(
        "soja", praca="paranagua", inicio=latest, fim=latest, offline=True
    )

    assert sorted(frame["valor"].tolist()) == [150, 160]
    assert filtered["valor"].tolist() == dataset["valor"].tolist() == [150]
    assert dataset["praca"].tolist() == ["Paranaguá/PR"]
    pd.testing.assert_frame_equal(dataset, requested)
    assert len(store.indicadores_query("soja")) == 3
