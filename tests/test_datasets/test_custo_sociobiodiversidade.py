from importlib import import_module
from unittest.mock import AsyncMock

import pytest

from agrobr import contracts, datasets
from agrobr.contracts import conab_custos
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from tests import helpers
from tests.helpers import levanta_exatamente

module = import_module("agrobr.datasets.custo_sociobiodiversidade")


@pytest.mark.parametrize("product, year", [("carnauba", 2022), ("babacu", 2018)])
async def test_consulta_por_ano_resolve_safras_publicadas(monkeypatch, product, year):
    helpers.mock_sociobio_http(monkeypatch)
    try:
        frame, meta = await datasets.custo_sociobiodiversidade(product, ano=year, return_meta=True)
    except Exception as erro:
        raise AssertionError(f"consulta publicada quebrou: {erro!r}") from erro
    assert not frame.empty
    assert frame.ano.eq(year).all()
    contracts.validate_dataset(frame, "custo_sociobiodiversidade")
    assert str(frame.safra_publicada.dtype) == "string"
    assert frame.safra_publicada.dtype.storage == "python"
    assert meta.contract_version == "1.0"
    if product == "babacu":
        row = frame.loc[frame.aba.eq("Imperatriz-MA-2018")].iloc[0]
        assert row.safra_publicada == "2018/19"
    else:
        assert frame.safra_publicada.isna().all()


async def test_dataset_contrato_obrigatorio_sem_return_meta(monkeypatch):
    bad = conab_custos.CONAB_SOCIOBIO_V1.empty_frame().drop(columns=["unidade_valor"])
    monkeypatch.setattr(module, "_fetch_conab", AsyncMock(return_value=(bad, None)))
    with levanta_exatamente(ContractViolationError):
        await datasets.custo_sociobiodiversidade("acai")


@pytest.mark.parametrize(
    "kwargs", [{"ano": "2024"}, {"produto": "soja"}, {"use_cache": 1}, {"safra": "2024/25"}]
)
async def test_dataset_preflight_impede_consulta_invalida(monkeypatch, kwargs):
    fetch = AsyncMock()
    monkeypatch.setattr(module, "_fetch_conab", fetch)
    with levanta_exatamente((InvalidParameterError, TypeError)):
        await datasets.custo_sociobiodiversidade(**{"produto": "acai", **kwargs})
    fetch.assert_not_awaited()
