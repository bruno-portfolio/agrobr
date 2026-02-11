from __future__ import annotations

from datetime import date, datetime
from unittest import mock

import pytest

from agrobr.cache.history import HistoryManager
from agrobr.constants import Fonte


def _make_mock_store():
    store = mock.MagicMock()
    store.history_save = mock.MagicMock()
    store.history_get = mock.MagicMock(return_value=None)
    store.history_query = mock.MagicMock(return_value=[])
    return store


class TestHistorySave:
    def test_save_new_record(self):
        store = _make_mock_store()
        mgr = HistoryManager(store=store)

        result = mgr.save(
            key="cepea:soja",
            data=b"payload",
            source=Fonte.CEPEA,
            data_date=date(2024, 6, 15),
            parser_version=1,
            fingerprint_hash="abc123",
        )

        assert result is True
        store.history_save.assert_called_once()
        call_kwargs = store.history_save.call_args
        assert call_kwargs.kwargs["key"] == "cepea:soja"
        assert call_kwargs.kwargs["data"] == b"payload"
        assert call_kwargs.kwargs["source"] is Fonte.CEPEA

    def test_save_duplicate_returns_false(self):
        store = _make_mock_store()
        store.history_save.side_effect = Exception("already exists")
        mgr = HistoryManager(store=store)

        result = mgr.save(
            key="cepea:soja",
            data=b"data",
            source=Fonte.CEPEA,
            data_date=date(2024, 6, 15),
            parser_version=1,
        )

        assert result is False

    def test_save_converts_date_to_datetime(self):
        store = _make_mock_store()
        mgr = HistoryManager(store=store)

        mgr.save("k", b"d", Fonte.CEPEA, date(2024, 3, 10), 1)

        call_kwargs = store.history_save.call_args.kwargs
        assert isinstance(call_kwargs["data_date"], datetime)
        assert call_kwargs["data_date"].year == 2024
        assert call_kwargs["data_date"].month == 3
        assert call_kwargs["data_date"].day == 10


class TestHistoryGet:
    def test_get_with_date(self):
        store = _make_mock_store()
        store.history_get.return_value = b"found"
        mgr = HistoryManager(store=store)

        result = mgr.get("cepea:soja", date(2024, 6, 15))

        assert result == b"found"
        call_args = store.history_get.call_args
        assert isinstance(call_args[0][1], datetime)

    def test_get_without_date_returns_latest(self):
        store = _make_mock_store()
        store.history_get.return_value = b"latest"
        mgr = HistoryManager(store=store)

        result = mgr.get("cepea:soja")

        assert result == b"latest"
        call_args = store.history_get.call_args
        assert call_args[0][1] is None

    def test_get_missing_returns_none(self):
        store = _make_mock_store()
        store.history_get.return_value = None
        mgr = HistoryManager(store=store)

        assert mgr.get("nonexistent") is None

    def test_get_latest_delegates(self):
        store = _make_mock_store()
        store.history_get.return_value = b"data"
        mgr = HistoryManager(store=store)

        result = mgr.get_latest("k")

        assert result == b"data"
        call_args = store.history_get.call_args
        assert call_args[0][1] is None


class TestHistoryQuery:
    def test_query_by_source(self):
        store = _make_mock_store()
        store.history_query.return_value = [
            {"key": "cepea:soja", "data_date": date(2024, 1, 1), "source": "cepea"},
        ]
        mgr = HistoryManager(store=store)

        results = mgr.query(source=Fonte.CEPEA)

        assert len(results) == 1
        store.history_query.assert_called_once()

    def test_query_by_date_range(self):
        store = _make_mock_store()
        store.history_query.return_value = []
        mgr = HistoryManager(store=store)

        results = mgr.query(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 6, 30),
        )

        assert results == []
        call_kwargs = store.history_query.call_args.kwargs
        assert isinstance(call_kwargs["start_date"], datetime)
        assert isinstance(call_kwargs["end_date"], datetime)

    def test_query_empty_returns_empty(self):
        store = _make_mock_store()
        store.history_query.return_value = []
        mgr = HistoryManager(store=store)

        assert mgr.query() == []

    def test_query_with_key_prefix_filters(self):
        store = _make_mock_store()
        store.history_query.return_value = [
            {"key": "cepea:soja", "data_date": date(2024, 1, 1)},
            {"key": "conab:milho", "data_date": date(2024, 1, 1)},
        ]
        mgr = HistoryManager(store=store)

        results = mgr.query(key_prefix="cepea")

        assert len(results) == 1
        assert results[0]["key"] == "cepea:soja"


class TestHistoryCount:
    def test_count_delegates_to_query(self):
        store = _make_mock_store()
        store.history_query.return_value = [
            {"key": "a"},
            {"key": "b"},
            {"key": "c"},
        ]
        mgr = HistoryManager(store=store)

        assert mgr.count() == 3

    def test_count_with_source(self):
        store = _make_mock_store()
        store.history_query.return_value = [{"key": "a"}]
        mgr = HistoryManager(store=store)

        assert mgr.count(source=Fonte.CEPEA) == 1


class TestHistoryCleanup:
    def test_cleanup_without_age_returns_zero(self):
        store = _make_mock_store()
        mgr = HistoryManager(store=store)

        assert mgr.cleanup() == 0

    def test_cleanup_with_age(self):
        store = _make_mock_store()
        mgr = HistoryManager(store=store)

        result = mgr.cleanup(older_than_days=90, source=Fonte.CEPEA)

        assert result == 0


class TestHistoryGetDates:
    def test_get_dates_extracts_dates(self):
        store = _make_mock_store()
        store.history_query.return_value = [
            {"key": "k", "data_date": datetime(2024, 1, 10)},
            {"key": "k", "data_date": datetime(2024, 3, 15)},
            {"key": "other", "data_date": datetime(2024, 5, 1)},
        ]
        mgr = HistoryManager(store=store)

        dates = mgr.get_dates("k")

        assert len(dates) == 2
        assert date(2024, 1, 10) in dates
        assert date(2024, 3, 15) in dates

    def test_get_dates_handles_date_objects(self):
        store = _make_mock_store()
        store.history_query.return_value = [
            {"key": "k", "data_date": date(2024, 1, 10)},
        ]
        mgr = HistoryManager(store=store)

        dates = mgr.get_dates("k")

        assert dates == [date(2024, 1, 10)]

    def test_get_dates_empty(self):
        store = _make_mock_store()
        store.history_query.return_value = []
        mgr = HistoryManager(store=store)

        assert mgr.get_dates("k") == []


class TestHistoryFindGaps:
    def test_find_gaps_identifies_missing_weekdays(self):
        store = _make_mock_store()
        store.history_query.return_value = [
            {"key": "k", "data_date": date(2024, 1, 8)},
            {"key": "k", "data_date": date(2024, 1, 10)},
        ]
        mgr = HistoryManager(store=store)

        gaps = mgr.find_gaps("k", date(2024, 1, 8), date(2024, 1, 12))

        assert date(2024, 1, 9) in gaps
        assert date(2024, 1, 11) in gaps
        assert date(2024, 1, 12) in gaps
        assert date(2024, 1, 8) not in gaps
        assert date(2024, 1, 10) not in gaps


class TestHistorySingleton:
    def test_store_lazy_init(self):
        mgr = HistoryManager(store=None)

        with mock.patch("agrobr.cache.duckdb_store.get_store") as mock_get_store:
            mock_store = _make_mock_store()
            mock_get_store.return_value = mock_store

            _ = mgr.store

            mock_get_store.assert_called_once()
