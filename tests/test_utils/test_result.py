from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result


@pytest.fixture()
def sample_df():
    return pd.DataFrame({"col": [1, 2, 3]})


@pytest.fixture()
def sample_meta():
    return {"source": "test", "records": 3}


class TestFinalizeResultPandas:
    def test_returns_df_by_default(self, sample_df):
        result = finalize_result(sample_df)
        pd.testing.assert_frame_equal(result, sample_df)

    def test_returns_df_with_meta(self, sample_df, sample_meta):
        df, meta = finalize_result(sample_df, sample_meta, return_meta=True)
        pd.testing.assert_frame_equal(df, sample_df)
        assert meta is sample_meta

    def test_returns_df_when_as_polars_false(self, sample_df, sample_meta):
        result = finalize_result(sample_df, sample_meta, as_polars=False, return_meta=False)
        pd.testing.assert_frame_equal(result, sample_df)

    def test_meta_none_no_return_meta(self, sample_df):
        result = finalize_result(sample_df, None, as_polars=False)
        pd.testing.assert_frame_equal(result, sample_df)

    def test_meta_none_with_return_meta(self, sample_df):
        df, meta = finalize_result(sample_df, None, return_meta=True)
        pd.testing.assert_frame_equal(df, sample_df)
        assert meta is None


class TestFinalizeResultPolars:
    def test_returns_polars_df(self, sample_df):
        pl = pytest.importorskip("polars")
        result = finalize_result(sample_df, as_polars=True)
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (3, 1)

    def test_returns_polars_with_meta(self, sample_df, sample_meta):
        pl = pytest.importorskip("polars")
        result_df, meta = finalize_result(sample_df, sample_meta, as_polars=True, return_meta=True)
        assert isinstance(result_df, pl.DataFrame)
        assert meta is sample_meta

    def test_polars_import_error_raises_with_install_hint(self, sample_df):
        with (
            patch.dict("sys.modules", {"polars": None}),
            pytest.raises(ImportError, match=r"pip install agrobr\[polars\]"),
        ):
            finalize_result(sample_df, as_polars=True)

    def test_polars_import_error_with_meta_raises(self, sample_df, sample_meta):
        with (
            patch.dict("sys.modules", {"polars": None}),
            pytest.raises(ImportError, match=r"pip install agrobr\[polars\]"),
        ):
            finalize_result(sample_df, sample_meta, as_polars=True, return_meta=True)


class TestBuildSourceMeta:
    def test_defaults(self, sample_df):
        meta = build_source_meta(
            "test_source",
            "https://example.com",
            "httpx",
            100,
            50,
            sample_df,
            1,
        )
        assert isinstance(meta, MetaInfo)
        assert meta.source == "test_source"
        assert meta.source_url == "https://example.com"
        assert meta.source_method == "httpx"
        assert meta.fetch_duration_ms == 100
        assert meta.parse_duration_ms == 50
        assert meta.records_count == len(sample_df)
        assert meta.columns == ["col"]
        assert meta.parser_version == 1
        assert meta.schema_version == "1.0"
        assert meta.attempted_sources == ["test_source"]
        assert meta.selected_source == "test_source"
        assert isinstance(meta.fetched_at, datetime)
        assert isinstance(meta.fetch_timestamp, datetime)
        assert meta.fetched_at == meta.fetch_timestamp

    def test_explicit_attempted_selected(self, sample_df):
        meta = build_source_meta(
            "src",
            "url",
            "httpx",
            0,
            0,
            sample_df,
            1,
            attempted_sources=["a", "b"],
            selected_source="b",
        )
        assert meta.attempted_sources == ["a", "b"]
        assert meta.selected_source == "b"

    def test_custom_schema_version(self, sample_df):
        meta = build_source_meta(
            "src",
            "url",
            "httpx",
            0,
            0,
            sample_df,
            1,
            schema_version="1.1",
        )
        assert meta.schema_version == "1.1"

    def test_parse_ms_zero(self, sample_df):
        meta = build_source_meta(
            "src",
            "url",
            "httpx",
            100,
            0,
            sample_df,
            1,
        )
        assert meta.parse_duration_ms == 0

    def test_multicolumn_df(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        meta = build_source_meta("src", "url", "httpx", 0, 0, df, 1)
        assert meta.records_count == 1
        assert meta.columns == ["a", "b", "c"]

    def test_empty_df(self):
        df = pd.DataFrame({"x": []})
        meta = build_source_meta("src", "url", "httpx", 0, 0, df, 1)
        assert meta.records_count == 0
        assert meta.columns == ["x"]
