from __future__ import annotations

import copy
from unittest.mock import AsyncMock

import pandas as pd

from agrobr import contracts
from agrobr.datasets.balanco import BalancoDataset


async def test_empty_source_has_contract_columns_and_zero_metadata():
    dataset = BalancoDataset()
    dataset.info = copy.deepcopy(dataset.info)
    dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(pd.DataFrame(), None))
    frame, meta = await dataset.fetch("algodao", return_meta=True)
    assert frame.empty
    assert contracts.get_contract("balanco").validate(frame) == (True, [])
    assert meta.records_count == 0
