from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import duckdb
import httpx
import pandas as pd
import pytest

from agrobr import constants, contracts
from agrobr.zarc import acquisition, cache, parser, query, store
from tests import helpers


@pytest.fixture
def table():
    raw = helpers.zarc_csv(
        [
            {"dec1": "", "municipio": "Vila (Nova)"},
            {"Nome_cultura": "Arroz", "dec1": "0"},
            {"dec1": "50", "UF": "GO", "municipio": "Straße"},
        ]
    )
    response = httpx.Response(
        200, content=raw, request=httpx.Request("GET", "https://example.org/table.csv")
    )
    captured = acquisition.from_response(response, str(response.url))
    bundle = parser.parse_tabua_risco_bundle(raw, expected_safra="2026/2027")
    details = store.StoredDetails(resource=captured.resource, parser=bundle.details)
    return captured, bundle, details


@pytest.mark.parametrize(
    "selectors",
    [
        {},
        {"produto": "soja"},
        {"uf": "GO"},
        {"municipio": "sorriso"},
        {"municipio": 5107925},
        {"solo": 1, "ciclo": 20},
        {"municipio": 5103403},
    ],
)
def test_store_query_matches_parser_values_types_and_order(table, selectors):
    captured, bundle, details = table
    selected = query.build_query(**selectors)
    expected = parser.parse_tabua_risco_bundle(
        captured.content, query=selected, expected_safra="2026/2027"
    )
    store.store("revision", captured.resource.sha256, "2026/2027", bundle.frame, details)
    entry = store.lookup("revision")
    assert entry is not None
    assert entry.details == details
    assert entry.raw_sha256 == captured.resource.sha256
    frame = store.query("revision", selected)
    pd.testing.assert_frame_equal(frame, expected.frame)
    contracts.validate_dataset(frame, "zoneamento_agricola")
    assert store.path().name == "zarc_tabuas.duckdb"
    assert not (store.path().parent / constants.CacheSettings().db_name).exists()


def test_absolute_ttl_is_not_renewed_by_store_hit(table, monkeypatch):
    captured, bundle, details = table
    store.store("revision", captured.resource.sha256, "2026/2027", bundle.frame, details)
    expiry = cache.expires_at(captured.resource.received_at)
    monkeypatch.setattr(cache, "now", lambda: expiry - timedelta(microseconds=1))
    assert store.lookup("revision") is not None
    monkeypatch.setattr(cache, "now", lambda: expiry)
    assert store.lookup("revision") is None
    with pytest.raises(KeyError, match="expirada"):
        store.query("revision", query.build_query())


def test_revision_limit_evicts_oldest_acquisition(table):
    captured, bundle, details = table
    keys = [f"revision-{i}" for i in range(constants.ZARC_STORE_MAX_REVISIONS + 2)]
    for index, key in enumerate(keys):
        receipt = captured.resource.model_copy(
            update={"received_at": captured.resource.received_at + timedelta(seconds=index)}
        )
        stored = details.model_copy(update={"resource": receipt})
        store.store(key, receipt.sha256, "2026/2027", bundle.frame, stored)
    assert [key for key in keys if store.lookup(key) is not None] == keys[
        -constants.ZARC_STORE_MAX_REVISIONS :
    ]
    with duckdb.connect(str(store.path())) as connection:
        assert (
            connection.execute("SELECT count(*) FROM tabuas").fetchone()[0]
            == len(bundle.frame) * constants.ZARC_STORE_MAX_REVISIONS
        )


def test_multiple_batches_preserve_nulls_and_positions(table, monkeypatch):
    captured, bundle, details = table
    monkeypatch.setattr(constants, "ZARC_STORE_BATCH_ROWS", 1)
    store.store("revision", captured.resource.sha256, "2026/2027", bundle.frame, details)
    frame = store.query("revision", query.build_query())
    pd.testing.assert_frame_equal(frame, bundle.frame)
    assert pd.isna(frame.loc[0, "dec1"])
    assert frame.loc[1:, "dec1"].tolist() == [0, 50]


def test_failed_transaction_preserves_previous_revision(table, monkeypatch):
    captured, bundle, details = table
    store.store("revision", captured.resource.sha256, "2026/2027", bundle.frame, details)

    def fail(_connection):
        raise duckdb.IOException("disk full")

    monkeypatch.setattr(store, "_evict", fail)
    with pytest.raises(duckdb.IOException, match="disk full"):
        store.store(
            "revision", captured.resource.sha256, "2026/2027", bundle.frame.iloc[:1], details
        )
    pd.testing.assert_frame_equal(store.query("revision", query.build_query()), bundle.frame)


def test_older_acquisition_cannot_replace_newer(table):
    captured, bundle, details = table
    store.store("revision", captured.resource.sha256, "2026/2027", bundle.frame, details)
    receipt = captured.resource.model_copy(
        update={"received_at": captured.resource.received_at - timedelta(hours=1)}
    )
    store.store(
        "revision",
        receipt.sha256,
        "2026/2027",
        bundle.frame.iloc[:1],
        details.model_copy(update={"resource": receipt}),
    )
    pd.testing.assert_frame_equal(store.query("revision", query.build_query()), bundle.frame)


def test_empty_table_and_clear_keep_typed_contract(table):
    captured, bundle, details = table
    empty = bundle.frame.iloc[:0]
    store.store("empty", captured.resource.sha256, "2026/2027", empty, details)
    pd.testing.assert_frame_equal(store.query("empty", query.build_query()), empty)
    store.clear()
    assert store.lookup("empty") is None
    assert store.path().is_file()


def test_concurrent_writers_keep_complete_revisions_within_limit(table):
    captured, bundle, details = table

    def publish(index):
        store.store(str(index), captured.resource.sha256, "2026/2027", bundle.frame, details)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(publish, range(8)))
    available = [str(index) for index in range(8) if store.lookup(str(index)) is not None]
    assert available == ["0", "1", "2"]
    for key in available:
        pd.testing.assert_frame_equal(store.query(key, query.build_query()), bundle.frame)
