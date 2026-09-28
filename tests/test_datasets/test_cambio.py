from __future__ import annotations

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import bcb, datasets
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from tests.helpers import levanta_exatamente

FAMILIES = [
    ("cotacoes_cambio", "ptax", "bcb_ptax", "2.0", "intraday"),
    ("moedas_cambio", "ptax_moedas", "bcb_ptax_moedas", "1.0", "unknown"),
]


@pytest.fixture(params=FAMILIES, ids=[item[0] for item in FAMILIES])
def family(request):
    return request.param


@pytest.mark.parametrize("return_meta", [False, True])
async def test_quotes_invalid_frame_rejected_by_dataset_contract_without_metadata(
    return_meta, monkeypatch
):
    frame = pd.DataFrame({"cotacao_compra": [1.0]})
    monkeypatch.setattr(bcb, "ptax", AsyncMock(return_value=frame))
    with pytest.raises(ContractViolationError):
        await datasets.cotacoes_cambio(return_meta=return_meta)


@pytest.mark.parametrize(
    "kwargs,error", [({"produto": "USD"}, InvalidParameterError), ({"unknown": 1}, TypeError)]
)
async def test_cambio_registry_guards_before_http(family, kwargs, error, ptax_http):
    trace = ptax_http()
    with levanta_exatamente(error):
        await datasets.get_dataset(family[0]).fetch(**kwargs)
    assert trace["all"] == []


async def test_cambio_deterministic_rejected_before_http(family, ptax_http):
    trace = ptax_http()
    async with deterministic("2026-09-04"):
        with levanta_exatamente(InvalidParameterError, match="deterministic"):
            await getattr(datasets, family[0])()
    assert trace["all"] == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"top": 0},
        {"top": True},
        {"top": 1.5},
        {"top": "3"},
        {"as_polars": 1},
        {"return_meta": None},
    ],
)
async def test_cambio_invalid_flags_and_limits_before_source(family, kwargs, monkeypatch):
    name, source_name, *_ = family
    source = AsyncMock()
    monkeypatch.setattr(bcb, source_name, source)
    with pytest.raises(InvalidParameterError):
        await getattr(datasets, name)(**kwargs)
    source.assert_not_awaited()
