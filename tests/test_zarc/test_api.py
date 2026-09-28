from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import duckdb
import pandas as pd
import pytest

from agrobr import constants, contracts, datasets
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    SourceUnavailableError,
)
from agrobr.zarc import api, cache, client, parser, store
from tests.helpers import zarc_csv


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"uf": "XX"},
        {"cultura": False},
        {"municipio": True},
        {"solo": 9},
        {"ciclo": 23},
        {"safra": "2026/2028"},
        {"as_polars": 1},
        {"return_meta": None},
        {"use_cache": 0},
        {"bbox": "ignored"},
    ],
)
async def test_invalid_parameters_precede_cache_and_http(zarc_replay, monkeypatch, kwargs):
    guarded = Mock(side_effect=AssertionError("cache accessed"))
    monkeypatch.setattr(cache, "get_catalog", guarded)
    with pytest.raises(InvalidParameterError):
        await api.zoneamento(**kwargs)
    assert not zarc_replay["requests"]
    guarded.assert_not_called()


@pytest.mark.asyncio
async def test_deterministic_precedes_cache_and_http(zarc_replay, monkeypatch):
    guarded = Mock(side_effect=AssertionError("cache accessed"))
    monkeypatch.setattr(cache, "get_catalog", guarded)
    async with deterministic("2026-09-07"):
        with pytest.raises(InvalidParameterError, match="deterministic"):
            await api.zoneamento()
    assert not zarc_replay["requests"]


@pytest.mark.asyncio
async def test_default_uses_latest_annual_resource(zarc_replay):
    _, meta = await api.zoneamento(return_meta=True)
    assert meta.source_details["effective_safra"] == "2026/2027"
    assert zarc_replay["requests"][-1].url.path.endswith("2026_2027.csv")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,column,expected",
    [
        ({"uf": " mt "}, "uf", "MT"),
        ({"cultura": " Soja "}, "cultura", "soja"),
        ({"municipio": 5107925}, "geocodigo", "5107925"),
        ({"municipio": "5107925"}, "geocodigo", "5107925"),
        ({"municipio": "(Nova)"}, "municipio", "Vila (Nova)"),
        ({"solo": 1}, "solo_codigo", 1),
        ({"ciclo": 20}, "ciclo_codigo", 20),
    ],
)
async def test_literal_selectors_reach_validated_rows(zarc_replay, kwargs, column, expected):
    zarc_replay["bodies"]["2026_2027"] = zarc_csv([{"municipio": "Vila (Nova)"}])
    frame = await api.zoneamento(**kwargs)
    assert len(frame) == 1
    assert frame[column].iloc[0] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "municipio,geocodigo,uf,nome,posicoes",
    [
        ("cerqueira césar", "3511409", "SP", "Cerqueira César", [9, 11, 12]),
        ("CERQUEIRA CÉSAR", "3511409", "SP", "Cerqueira César", [9, 11, 12]),
        ("cerqueira cesar", "3511409", "SP", "Cerqueira César", [9, 11, 12]),
        ("herval", "4316956", "RS", "Santa Maria do Herval", [18, 19]),
    ],
    ids=["caixa_baixa", "caixa_alta", "sem_acento", "parcial"],
)
async def test_municipio_por_nome_real_nos_tres_caminhos(
    zarc_replay, municipio, geocodigo, uf, nome, posicoes
):
    primeira, inicial = await api.zoneamento(municipio=municipio, return_meta=True)
    segunda, hit = await api.zoneamento(municipio=municipio, return_meta=True)
    terceira, bypass = await api.zoneamento(municipio=municipio, return_meta=True, use_cache=False)
    status = [meta.source_details["cache"]["status"] for meta in (inicial, hit, bypass)]
    assert status == ["store_miss", "store_hit", "bypass"]
    assert len(zarc_replay["requests"]) == 4
    for frame in (primeira, segunda, terceira):
        assert frame["registro_origem"].tolist() == posicoes
        assert set(zip(frame["geocodigo"], frame["uf"], frame["municipio"])) == {
            (geocodigo, uf, nome)
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch", [api.zoneamento, datasets.zoneamento_agricola])
async def test_cultura_fora_catalogo_falha_antes_da_rede(zarc_replay, monkeypatch, fetch):
    catalog = AsyncMock(side_effect=AssertionError("catalog accessed"))
    download = AsyncMock(side_effect=AssertionError("CSV downloaded"))
    cached = Mock(side_effect=AssertionError("cache accessed"))
    monkeypatch.setattr(client, "discover_catalog", catalog)
    monkeypatch.setattr(client, "download_acquisition", download)
    monkeypatch.setattr(cache, "get_catalog", cached)
    with pytest.raises(InvalidParameterError, match="abobrinha"):
        await fetch(cultura="abobrinha", uf="SP")
    catalog.assert_not_awaited()
    download.assert_not_awaited()
    cached.assert_not_called()
    assert not zarc_replay["requests"]


@pytest.mark.asyncio
async def test_cultura_catalogo_ausente_falha_apos_validar_tabua(zarc_replay):
    with pytest.raises(InvalidParameterError, match="use safra='perene'"):
        await api.zoneamento(cultura="sisal")
    assert len(zarc_replay["requests"]) == 2


@pytest.mark.asyncio
async def test_invalid_row_outside_filter_is_not_cached(zarc_replay):
    zarc_replay["bodies"]["2026_2027"] = zarc_csv([{}, {"UF": "XX"}])
    with pytest.raises(ParseError):
        await api.zoneamento(municipio="5107925", uf="MT")
    assert not store.path().exists()


@pytest.mark.asyncio
async def test_cache_hit_retains_acquisition_and_independent_outputs(zarc_replay):
    first, first_meta = await api.zoneamento(return_meta=True)
    second, second_meta = await api.zoneamento(return_meta=True)
    assert len(zarc_replay["requests"]) == 2
    assert not first_meta.from_cache and second_meta.from_cache
    assert first_meta.fetched_at == second_meta.fetched_at
    assert first_meta.fetch_timestamp == second_meta.fetch_timestamp == first_meta.fetched_at
    assert first_meta.cache_expires_at == second_meta.cache_expires_at
    assert first_meta.raw_content_hash == second_meta.raw_content_hash
    first.iloc[0, 0] = "modified"
    first_meta.source_details["resource"]["headers"]["etag"] = "modified"
    assert second.iloc[0, 0] != "modified"
    assert second_meta.source_details["resource"]["headers"]["etag"] != "modified"


@pytest.mark.asyncio
async def test_store_hit_skips_download_and_parser_with_identical_details(zarc_replay, monkeypatch):
    first, first_meta = await api.zoneamento(cultura="soja", uf="MT", return_meta=True)
    monkeypatch.setattr(
        client, "download_acquisition", AsyncMock(side_effect=AssertionError("download"))
    )
    monkeypatch.setattr(
        parser, "parse_tabua_risco_bundle", Mock(side_effect=AssertionError("parse"))
    )
    second, second_meta = await api.zoneamento(cultura="soja", uf="MT", return_meta=True)
    pd.testing.assert_frame_equal(first, second)
    assert first_meta.source_details["cache"]["status"] == "store_miss"
    assert second_meta.source_details["cache"]["status"] == "store_hit"
    assert first_meta.source_details["parser"] == second_meta.source_details["parser"]
    assert second_meta.source_details["parser"]["culturas_observadas"]
    assert len(zarc_replay["requests"]) == 2


@pytest.mark.asyncio
async def test_store_hit_rejects_absent_culture_without_another_acquisition(zarc_replay):
    await api.zoneamento()
    with pytest.raises(InvalidParameterError, match="use safra='perene'"):
        await api.zoneamento(cultura="sisal")
    assert len(zarc_replay["requests"]) == 2


@pytest.mark.asyncio
async def test_cache_and_bypass_preserve_filter_and_parser_details(zarc_replay):
    zarc_replay["bodies"]["2026_2027"] = zarc_csv(
        [
            {"municipio": "Vila (Nova)", "dec1": ""},
            {"municipio": "Vila (Nova)", "dec1": ""},
            {"Nome_cultura": "Arroz", "UF": "GO"},
        ]
    )
    selected = {"cultura": "soja", "uf": "MT", "municipio": "(nova)", "solo": 1, "ciclo": 20}
    first, initial = await api.zoneamento(**selected, return_meta=True)
    second, hit = await api.zoneamento(**selected, return_meta=True)
    third, bypass = await api.zoneamento(**selected, return_meta=True, use_cache=False)
    pd.testing.assert_frame_equal(first, second)
    pd.testing.assert_frame_equal(first, third)
    assert first["registro_origem"].tolist() == [1, 2]
    assert (
        initial.source_details["parser"]
        == hit.source_details["parser"]
        == bypass.source_details["parser"]
    )
    assert hit.source_details["parser"]["culturas_observadas"] == ["arroz", "soja"]


@pytest.mark.asyncio
async def test_corrupt_database_warns_and_returns_downloaded_table(zarc_replay, monkeypatch):
    store.path().parent.mkdir(parents=True, exist_ok=True)
    store.path().write_bytes(b"invalid DuckDB database")
    warning = Mock()
    monkeypatch.setattr(api.logger, "warning", warning)
    frame, meta = await api.zoneamento(return_meta=True)
    assert len(frame) == 21
    assert not meta.from_cache and meta.cache_key is None
    assert meta.source_details["cache"]["status"] == "store_error"
    assert [call.args[0] for call in warning.call_args_list] == [
        "zarc_store_read_failed",
        "zarc_store_write_failed",
    ]
    assert len(zarc_replay["requests"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [duckdb.IOException("database locked"), OSError("disk full")])
async def test_store_failure_does_not_fail_query(zarc_replay, monkeypatch, error):
    monkeypatch.setattr(store, "store", Mock(side_effect=error))
    warning = Mock()
    monkeypatch.setattr(api.logger, "warning", warning)
    frame, meta = await api.zoneamento(return_meta=True)
    assert len(frame) == 21 and not meta.from_cache
    assert meta.source_details["cache"]["status"] == "store_error"
    assert warning.call_args.args[0] == "zarc_store_write_failed"
    assert len(zarc_replay["requests"]) == 2


@pytest.mark.asyncio
async def test_bypass_reads_and_writes_neither_cache(zarc_replay, monkeypatch):
    await api.zoneamento()
    old = store.path().read_bytes()
    for name in ("get_catalog", "put_catalog"):
        monkeypatch.setattr(cache, name, Mock(side_effect=AssertionError(name)))
    for name in ("lookup", "query", "store"):
        monkeypatch.setattr(store, name, Mock(side_effect=AssertionError(name)))
    _, meta = await api.zoneamento(return_meta=True, use_cache=False)
    assert len(zarc_replay["requests"]) == 4
    assert not meta.from_cache and meta.cache_key is None
    assert meta.source_details["cache"]["status"] == "bypass"
    assert store.path().read_bytes() == old


@pytest.mark.asyncio
async def test_catalog_refresh_revision_requires_new_csv(zarc_replay, monkeypatch):
    _, old = await api.zoneamento(return_meta=True)
    monkeypatch.setattr(cache, "now", lambda: old.fetched_at + timedelta(hours=2))
    zarc_replay["catalog"]["result"]["resources"][1]["last_modified"] = "2026-09-07T02:00:00"
    _, new = await api.zoneamento(return_meta=True)
    assert len(zarc_replay["requests"]) == 4
    assert not new.from_cache and old.cache_key != new.cache_key


@pytest.mark.asyncio
async def test_unchanged_catalog_refresh_reuses_csv(zarc_replay, monkeypatch):
    _, old = await api.zoneamento(return_meta=True)
    monkeypatch.setattr(cache, "now", lambda: old.fetched_at + timedelta(hours=2))
    _, new = await api.zoneamento(return_meta=True)
    assert len(zarc_replay["requests"]) == 3
    assert new.from_cache and old.cache_key == new.cache_key
    assert old.cache_expires_at == new.cache_expires_at


@pytest.mark.asyncio
async def test_expired_csv_is_downloaded_without_announced_revision(zarc_replay, monkeypatch):
    _, old = await api.zoneamento(return_meta=True)
    monkeypatch.setattr(cache, "now", lambda: old.fetched_at + timedelta(hours=25))
    _, new = await api.zoneamento(return_meta=True)
    assert len(zarc_replay["requests"]) == 4
    assert not new.from_cache
    assert old.raw_content_hash == new.raw_content_hash
    assert new.fetched_at > old.fetched_at


@pytest.mark.asyncio
async def test_failed_revision_does_not_fallback_to_old_table(zarc_replay, monkeypatch):
    _, old = await api.zoneamento(return_meta=True)
    previous = store.path().read_bytes()
    monkeypatch.setattr(cache, "now", lambda: old.fetched_at + timedelta(hours=2))
    zarc_replay["catalog"]["result"]["resources"][1]["last_modified"] = "revision-new"
    zarc_replay["bodies"]["2026_2027"] = zarc_csv([{"dec1": "invalid"}])
    with pytest.raises(ParseError):
        await api.zoneamento()
    assert store.path().read_bytes() == previous


@pytest.mark.asyncio
async def test_concurrent_calls_share_download_but_not_frames(zarc_replay):
    first, second = await asyncio.gather(api.zoneamento(), api.zoneamento())
    assert len(zarc_replay["requests"]) == 2
    pd.testing.assert_frame_equal(first, second)
    assert first is not second


@pytest.mark.asyncio
async def test_cancelled_acquisition_does_not_publish_partial_cache(zarc_replay):
    zarc_replay["csv_started"] = asyncio.Event()
    zarc_replay["csv_release"] = asyncio.Event()
    pending = asyncio.create_task(api.zoneamento())
    await asyncio.wait_for(zarc_replay["csv_started"].wait(), 1)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not store.path().exists()
    zarc_replay["csv_release"].set()
    frame = await asyncio.wait_for(api.zoneamento(), 1)
    assert len(frame) == 21
    assert len(zarc_replay["requests"]) == 3


@pytest.mark.asyncio
async def test_expired_resource_http_error_does_not_use_stale_cache(zarc_replay, monkeypatch):
    _, old = await api.zoneamento(return_meta=True)
    previous = store.path().read_bytes()
    monkeypatch.setattr(cache, "now", lambda: old.fetched_at + timedelta(hours=25))
    zarc_replay["status"] = 400
    with pytest.raises(SourceUnavailableError, match="HTTP 400"):
        await api.zoneamento()
    assert store.path().read_bytes() == previous


@pytest.mark.asyncio
@pytest.mark.parametrize("return_meta", [False, True])
async def test_contract_is_mandatory(zarc_replay, monkeypatch, return_meta):
    original = contracts.validate_dataset

    def reject(frame, name):
        altered = frame.drop(columns="registro_origem")
        return original(altered, name)

    monkeypatch.setattr(contracts, "validate_dataset", reject)
    with pytest.raises(ContractViolationError):
        await api.zoneamento(return_meta=return_meta)
    assert not store.path().exists()
    assert len(zarc_replay["requests"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("return_meta", [False, True])
async def test_polars_schema_is_stable(zarc_replay, empty, return_meta):
    pl = pytest.importorskip("polars")
    response = await api.zoneamento(
        municipio="absent" if empty else None, as_polars=True, return_meta=return_meta
    )
    frame = response[0] if return_meta else response
    assert frame.width == 59 and frame.schema["cod_municipio"] == pl.Int64
    assert all(frame.schema[name] == pl.Int64 for name in constants.ZARC_INTEGER_COLUMNS)
    assert all(frame.schema[name] == pl.Utf8 for name in constants.ZARC_STRING_COLUMNS)
    assert (frame.height == 0) == empty
    assert len(zarc_replay["requests"]) == 2


@pytest.mark.asyncio
async def test_header_only_is_a_typed_empty_resource(zarc_replay):
    zarc_replay["bodies"]["2026_2027"] = zarc_csv([])
    frame, meta = await api.zoneamento(return_meta=True)
    assert frame.empty and len(frame.columns) == 59
    assert all(str(frame[name].dtype) == "Int64" for name in constants.ZARC_INTEGER_COLUMNS)
    assert meta.source_details["parser"]["validated_rows"] == 0


@pytest.mark.asyncio
async def test_nullable_risk_is_distinct_from_zero(zarc_replay):
    zarc_replay["bodies"]["2026_2027"] = zarc_csv([{"dec1": ""}, {"dec1": "0"}, {"dec1": "50"}])
    frame = await api.zoneamento()
    assert pd.isna(frame.loc[0, "dec1"])
    assert frame.loc[1:, "dec1"].tolist() == [0, 50]
    assert frame["registro_origem"].tolist() == [1, 2, 3]


@pytest.mark.asyncio
async def test_safras_disponiveis_uses_catalog(zarc_replay):
    assert await api.safras_disponiveis() == ["2016/2017", "2026/2027", "perene"]
    assert len(zarc_replay["requests"]) == 1


async def test_culturas_observadas_chegam_ao_dataset_antes_do_filtro(zarc_replay):
    zarc_replay["bodies"]["2026_2027"] = zarc_csv(
        [{"Nome_cultura": "Soja"}, {"Nome_cultura": "Arroz"}, {"Nome_cultura": "Trigo"}]
    )
    frame, meta = await datasets.zoneamento_agricola(
        cultura="soja", uf="MT", safra="2026/2027", return_meta=True
    )
    assert frame["cultura"].tolist() == ["soja"]
    assert meta.source_details["parser"]["culturas_observadas"] == ["arroz", "soja", "trigo"]
    assert meta.source_details["parser"]["validated_rows"] == 3
    assert meta.source_details["parser"]["selected_rows"] == meta.records_count == 1


def test_sync_uses_independent_event_loops(zarc_replay):
    from agrobr.sync import zarc

    first = zarc.zoneamento(safra="2016/2017")
    second = zarc.zoneamento(safra="2016/2017")
    pd.testing.assert_frame_equal(first, second)
    assert len(zarc_replay["requests"]) == 2
