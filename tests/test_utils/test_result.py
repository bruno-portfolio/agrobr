from __future__ import annotations

from datetime import datetime
from unittest import mock
from unittest.mock import patch

import pandas as pd
import pytest

from agrobr.models import MetaInfo
from agrobr.utils import result
from agrobr.utils.result import build_source_meta, finalize_result


@pytest.fixture()
def sample_df():
    return pd.DataFrame({"col": [1, 2, 3]})


@pytest.fixture()
def sample_meta():
    return {"source": "test", "records": 3}


class TestFinalizeResultPandas:
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


def test_polars_preserves_nullable_integer():
    pytest.importorskip("polars")
    frame = pd.DataFrame({"value": pd.Series([1, None], dtype="Int64")})
    assert result.finalize_result(frame, as_polars=True).to_dicts() == [
        {"value": 1},
        {"value": None},
    ]


@pytest.mark.parametrize("values", [[], [None, None], ["", None, "publicado"]])
def test_polars_declared_string_preserves_null_empty_and_text(values):
    pl = pytest.importorskip("polars")
    frame = pd.DataFrame(
        {
            "detail": pd.Series(values, dtype="object"),
            "count": pd.Series([1] * len(values), dtype="Int64"),
        }
    )
    before = frame.copy(deep=True)
    converted = result.finalize_result(frame, as_polars=True, string_columns=("detail",))
    assert converted["detail"].dtype == pl.Utf8
    assert converted["detail"].to_list() == values
    assert converted["count"].dtype == pl.Int64
    pd.testing.assert_frame_equal(frame, before)


def test_string_columns_keep_pandas_independent_of_polars():
    frame = pd.DataFrame({"detail": pd.Series([None], dtype="object")})
    with patch.dict("sys.modules", {"polars": None}):
        returned = result.finalize_result(frame, string_columns=("detail",))
    assert returned is frame
    assert str(returned["detail"].dtype) == "object"


def test_polars_conversion_preserves_dependency_error():
    pytest.importorskip("polars")
    with (
        mock.patch("polars.from_pandas", side_effect=ImportError("pyarrow is required")),
        pytest.raises(ImportError, match="pyarrow is required"),
    ):
        result.finalize_result(pd.DataFrame({"value": [1]}), as_polars=True)
