from __future__ import annotations

from unittest.mock import AsyncMock

import pandas as pd


def mock_source_meta(
    source_url: str = "http://test",
    parser_version: int = 1,
) -> AsyncMock:
    meta = AsyncMock()
    meta.source_url = source_url
    meta.fetched_at = None
    meta.parser_version = parser_version
    meta.from_cache = False
    meta.attempted_sources = []
    meta.selected_source = ""
    return meta


def make_source(
    df: pd.DataFrame,
    meta: AsyncMock | None = None,
    *,
    raises: Exception | None = None,
) -> AsyncMock:
    if raises is not None:
        return AsyncMock(side_effect=raises)
    return AsyncMock(return_value=(df, meta or mock_source_meta()))
