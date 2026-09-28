from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.cftc import api as cftc_api

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "cftc"
COT_URL = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"


def _load_golden() -> list[dict[str, str]]:
    with open(GOLDEN_DIR / "cot_sample.json", encoding="utf-8") as f:
        return json.load(f)


def _patch_fetch():
    return patch.object(
        cftc_api.client,
        "fetch_cot",
        new_callable=AsyncMock,
        return_value=(_load_golden(), COT_URL, (GOLDEN_DIR / "cot_sample.json").read_bytes()),
    )


class TestCotAsPolars:
    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        with _patch_fetch():
            result = await cftc_api.cot("soja", as_polars=True)

        assert isinstance(result, pl.DataFrame)
