import asyncio
from unittest.mock import AsyncMock

import pytest

from agrobr import bcb, contracts, datasets
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


@pytest.mark.parametrize(
    "args,kwargs,error",
    [
        (("ipca",), {"codigo": 433}, InvalidParameterError),
        ((), {"codigo": 433, "unidade": "percentual"}, TypeError),
        ((), {}, InvalidParameterError),
    ],
    ids=["product", "unknown_selector", "missing_code"],
)
async def test_registry_rejects_invalid_selection_before_source(args, kwargs, error, monkeypatch):
    source = AsyncMock()
    monkeypatch.setattr(bcb, "sgs", source)
    dataset = datasets.get_dataset("series_economicas")
    with pytest.raises(error):
        await dataset.fetch(*args, **kwargs)
    source.assert_not_awaited()


async def test_concurrent_singleton_queries_and_contexts_are_independent(sgs_http):
    sgs_http()

    async def guarded():
        async with deterministic("2024-12-31"):
            with pytest.raises(InvalidParameterError):
                await datasets.series_economicas(1, ultimos=3)

    daily, monthly, _ = await asyncio.gather(
        datasets.series_economicas(1, ultimos=3, return_meta=True),
        datasets.series_economicas(
            "ipca", data_inicial="01/01/2024", data_final="31/12/2024", return_meta=True
        ),
        guarded(),
    )
    assert daily[0]["codigo"].eq(1).all() and monthly[0]["codigo"].eq(433).all()
    daily[1].source_details["resources"][0]["parameters"]["mutated"] = "yes"
    assert "mutated" not in monthly[1].source_details["resources"][0]["parameters"]


@pytest.mark.parametrize(
    "codigo,kwargs",
    [
        (True, {}),
        (None, {}),
        (0, {}),
        (-1, {}),
        (1.0, {}),
        (2**63, {}),
        ("1", {}),
        ("IPCA", {}),
        (" ipca ", {}),
        ("", {}),
        (1, {"ultimos": 0}),
        (1, {"ultimos": -1}),
        (1, {"ultimos": True}),
        (1, {"ultimos": "3"}),
        (1, {"ultimos": 1.5}),
        (1, {"data_inicial": "2024-01-01"}),
        (1, {"data_final": "31/02/2024"}),
        (1, {"data_inicial": 20240101}),
        (1, {"data_inicial": "02/01/2024", "data_final": "01/01/2024"}),
        (1, {"as_polars": 1}),
        (1, {"as_polars": None}),
        (1, {"return_meta": "yes"}),
        (1, {"return_meta": 0}),
    ],
)
async def test_invalid_arguments_fail_before_source(codigo, kwargs, monkeypatch):
    source = AsyncMock(side_effect=AssertionError("source must not run"))
    monkeypatch.setattr(bcb, "sgs", source)
    with levanta_exatamente(InvalidParameterError):
        await datasets.series_economicas(codigo, **kwargs)
    source.assert_not_awaited()


async def test_codigo_ausente_explica_no_dataset(monkeypatch):
    source = AsyncMock(side_effect=AssertionError("source must not run"))
    monkeypatch.setattr(bcb, "sgs", source)
    with levanta_exatamente(InvalidParameterError, match="exige codigo"):
        await datasets.series_economicas(None)
    source.assert_not_awaited()


def test_dataset_declara_a_versao_do_contrato_que_valida():
    assert datasets.info("series_economicas")["contract_version"] == (
        contracts.get_contract("bcb_sgs").version
    )
