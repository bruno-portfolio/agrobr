from dataclasses import replace
from importlib import import_module
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest

from agrobr import datasets
from agrobr.contracts import get_contract
from agrobr.datasets.base import DatasetSource
from tests.helpers import desmatamento_meta
from tests.integration.test_datasets_live import LIVE_CASES


@pytest.mark.parametrize("name", datasets.list_datasets())
@pytest.mark.parametrize("as_polars", [False, True])
async def test_empty_source_preserves_dataset_contract(name, as_polars, monkeypatch):
    if as_polars:
        pl = pytest.importorskip("polars")
    dataset = datasets.get_dataset(name)
    meta = desmatamento_meta(0) if name == "desmatamento" else None
    fetch = AsyncMock(return_value=(pd.DataFrame(), meta))
    monkeypatch.setattr(
        dataset,
        "info",
        replace(
            dataset.info,
            sources=[DatasetSource("isolated_empty", 1, fetch)],
        ),
    )
    forbidden_http = None
    if name in {"custo_producao", "custo_sociobiodiversidade"}:
        module_name = "api" if name == "custo_producao" else "_sociobio_api"
        source = import_module(f"agrobr.conab.custo_producao.{module_name}")
        fetch = AsyncMock(return_value=(get_contract(name).empty_frame(), None))
        monkeypatch.setattr(source, name, fetch)
        forbidden_http = AsyncMock(side_effect=AssertionError("Unexpected HTTP in empty test"))
        monkeypatch.setattr(httpx.AsyncClient, "send", forbidden_http)
    args, kwargs = LIVE_CASES[name]
    try:
        df, meta = await dataset.fetch(*args, **kwargs, as_polars=as_polars, return_meta=True)
    except Exception as erro:
        raise AssertionError(f"{name}: fonte vazia derrubou o dataset: {erro!r}") from erro
    contract_name = dataset._contract_name(**kwargs)
    assert contract_name is not None
    contract = get_contract(contract_name)
    if as_polars:
        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()
        assert df.columns == contract.empty_frame().columns.tolist()
    else:
        assert contract.validate(df) == (True, [])
        assert df.empty
    assert meta.records_count == 0
    assert meta.columns == (df.columns if as_polars else df.columns.tolist())
    assert meta.schema_version == contract.version
    if forbidden_http is None and name != "desmatamento":
        assert (meta.selected_source, meta.attempted_sources) == (
            "isolated_empty",
            ["isolated_empty"],
        )
    if forbidden_http is not None:
        fetch.assert_awaited_once()
        forbidden_http.assert_not_awaited()
        if not as_polars:
            pd.testing.assert_frame_equal(df, contract.empty_frame())
