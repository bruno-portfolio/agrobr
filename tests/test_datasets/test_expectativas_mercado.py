import asyncio
from unittest.mock import AsyncMock

import pytest

from agrobr import bcb, datasets
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


@pytest.mark.parametrize(
    "args,kwargs,error",
    [
        (("soja",), {}, InvalidParameterError),
        ((), {"fim": "2026-08-28"}, TypeError),
        ((), {"indicador": None}, InvalidParameterError),
    ],
)
async def test_registry_invalid_selection_fails_before_source(args, kwargs, error, monkeypatch):
    source = AsyncMock()
    monkeypatch.setattr(bcb, "focus", source)
    with pytest.raises(error):
        await datasets.get_dataset("expectativas_mercado").fetch(*args, **kwargs)
    source.assert_not_awaited()


async def test_concurrent_periodicities_and_contexts_do_not_mix(focus_http):
    focus_http()

    async def guarded():
        async with deterministic("2026-08-28"):
            with levanta_exatamente(InvalidParameterError):
                await datasets.expectativas_mercado()

    with pytest.warns(UserWarning):
        annual, monthly, _ = await asyncio.gather(
            datasets.expectativas_mercado(
                "Balança comercial",
                inicio="2026-08-28",
                top=6,
                max_registros=6,
                return_meta=True,
            ),
            datasets.expectativas_mercado(
                "IPCA",
                periodicidade="mensal",
                inicio="2026-08-28",
                top=6,
                max_registros=6,
                return_meta=True,
            ),
            guarded(),
        )
    assert (
        annual[0]["periodicidade"].eq("anual").all()
        and monthly[0]["periodicidade"].eq("mensal").all()
    )
    annual[1].source_details["resources"][0]["parameters"]["mutated"] = "yes"
    assert "mutated" not in monthly[1].source_details["resources"][0]["parameters"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"indicador": None},
        {"indicador": 1},
        {"indicador": ""},
        {"indicador": "  "},
        {"periodicidade": "trimestral"},
        {"periodicidade": "Anualx"},
        {"periodicidade": 1},
        {"periodicidade": None},
        {"top": None},
        {"top": 0},
        {"top": -1},
        {"top": True},
        {"top": 1.0},
        {"top": "3"},
        {"max_registros": 0},
        {"max_registros": -1},
        {"max_registros": True},
        {"max_registros": 1.5},
        {"max_registros": "3"},
        {"inicio": "28/08/26"},
        {"inicio": "2026-02-30"},
        {"inicio": 20260828},
        {"as_polars": 1},
        {"as_polars": None},
        {"return_meta": 0},
        {"return_meta": "yes"},
    ],
)
async def test_invalid_selection_fails_before_source(kwargs, monkeypatch):
    source = AsyncMock(side_effect=AssertionError("source must not run"))
    monkeypatch.setattr(bcb, "focus", source)
    with levanta_exatamente(InvalidParameterError):
        await datasets.expectativas_mercado(**kwargs)
    source.assert_not_awaited()
