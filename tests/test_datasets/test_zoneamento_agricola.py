from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.datasets import zoneamento_agricola as public_zoneamento
from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.zoneamento_agricola import ZoneamentoAgricolaDataset
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from tests.helpers import zarc_frame


@pytest.fixture
def supplied(monkeypatch):
    frame = pd.concat(
        [zarc_frame(dec10=None, registro_origem=105), zarc_frame(dec10=0, registro_origem=109)],
        ignore_index=True,
    )
    fetched = datetime(2026, 9, 7, tzinfo=UTC)
    meta = MetaInfo(
        source="zarc",
        source_url="https://dados.agricultura.gov.br/synthetic.csv",
        source_method="cache",
        fetched_at=fetched,
        fetch_timestamp=fetched,
        fetch_duration_ms=17,
        parse_duration_ms=23,
        raw_content_hash="d" * 64,
        raw_content_size=12345,
        records_count=len(frame),
        columns=frame.columns.tolist(),
        from_cache=True,
        cache_key="synthetic-resource-revision",
        cache_expires_at=fetched + timedelta(days=1),
        schema_version="2.0",
        contract_version="2.0",
        parser_version=2,
        attempted_sources=["zarc"],
        selected_source="zarc",
        source_details={
            "resources": [{"role": "csv", "sha256": "d" * 64}],
            "coverage": {"status": "unknown", "reason": "no_source_total"},
            "parsing": {"source_rows": 200, "validated_rows": 200, "selected_rows": 2},
        },
    )
    fetch = AsyncMock(return_value=(frame, meta))
    monkeypatch.setattr(ZoneamentoAgricolaDataset.info.sources[0], "fetch_fn", fetch)
    monkeypatch.setattr(
        datasets.registry._REGISTRY["zoneamento_agricola"].info.sources[0], "fetch_fn", fetch
    )
    return fetch, frame, meta


async def test_dataset_rejects_deterministic_before_source(supplied):
    fetch, _, _ = supplied
    async with deterministic("2026-09-06"):
        with pytest.raises(InvalidParameterError, match="deterministic"):
            await public_zoneamento(safra="2026/2027")
    fetch.assert_not_awaited()


@pytest.mark.parametrize("flag", ["use_cache", "as_polars", "return_meta"])
@pytest.mark.parametrize("value", [1, None, "true"])
async def test_dataset_rejects_non_boolean_before_source(supplied, flag, value):
    fetch, _, _ = supplied
    with pytest.raises(InvalidParameterError, match=flag):
        await public_zoneamento(**{flag: value})
    fetch.assert_not_awaited()


@pytest.mark.parametrize("keyword", ["uf_typo", "cultura", "snapshot", "limite"])
async def test_dataset_rejects_unknown_parameter_before_source(supplied, keyword):
    fetch, _, _ = supplied
    with pytest.raises(TypeError, match=keyword):
        await public_zoneamento(**{keyword: "unexpected"})
    fetch.assert_not_awaited()
