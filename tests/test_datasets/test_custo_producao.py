from importlib import import_module
from unittest.mock import AsyncMock

import pytest

from agrobr.contracts.conab_custos import CONAB_CUSTOS_V3
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente

module = import_module("agrobr.datasets.custo_producao")


@pytest.mark.asyncio
async def test_dataset_contract_required_without_metadata(monkeypatch):
    df = CONAB_CUSTOS_V3.empty_frame().drop(columns=["local"])
    monkeypatch.setattr(module, "_fetch_conab", AsyncMock(return_value=(df, None)))
    with levanta_exatamente(ContractViolationError):
        await module.custo_producao("soja", return_meta=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs", [{"tecnologia": "alta"}, {"unexpected": True}])
async def test_dataset_unknown_parameters_before_source(monkeypatch, kwargs):
    fetch = AsyncMock()
    monkeypatch.setattr(module, "_fetch_conab", fetch)
    with levanta_exatamente(TypeError):
        await module.custo_producao("soja", **kwargs)
    fetch.assert_not_awaited()
