from __future__ import annotations

import inspect
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr import datasets
from agrobr.exceptions import InvalidParameterError, ParseError, ResourceLimitError
from agrobr.icmbio import api
from tests.helpers import (
    collect_failures,
    fixture_instance,
    isolated_dataset_case,
    levanta_exatamente,
)

FIXTURE = (
    Path(__file__).parents[1]
    / "golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/icmbio/icmbio_national_007.csv"
)


def count_body(count: int) -> bytes:
    return f'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" numberOfFeatures="{count}"/>'.encode()


@pytest.fixture
def acquisition(monkeypatch):
    body = FIXTURE.read_bytes()
    fetch = AsyncMock(return_value=(body, "https://test/features"))
    count = AsyncMock(return_value=(count_body(347), "https://test/hits"))
    monkeypatch.setattr(api.client, "fetch_ucs", fetch)
    monkeypatch.setattr(api.client, "fetch_ucs_count", count)
    return fetch, count


_case_fixture_acquisition = inspect.unwrap(acquisition)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"uf": "ZZ"},
        {"uf": 1},
        {"grupo": "XX"},
        {"grupo": True},
        {"bioma": "Cerrado'"},
        {"bioma": 1},
        {"as_polars": 1},
        {"return_meta": "yes"},
        {"bbox": (1, 2, 0, 3)},
        {"bbox": (float("nan"), -10, 1, 2)},
        {"bbox": (True, -10, 1, 2)},
    ],
)
async def test_invalid_selection_fails_before_io(acquisition, kwargs):
    fetch, count = acquisition
    with levanta_exatamente((InvalidParameterError, ValueError)):
        await datasets.unidades_conservacao_federais(**kwargs)
    fetch.assert_not_awaited()
    count.assert_not_awaited()


@pytest.mark.asyncio
async def test_deterministic_is_rejected_before_io(acquisition):
    fetch, count = acquisition
    async with datasets.deterministic("2026-09-01"):
        with pytest.raises(InvalidParameterError, match="camada é corrente"):
            await datasets.unidades_conservacao_federais()
    fetch.assert_not_awaited()
    count.assert_not_awaited()


@pytest.mark.asyncio
async def test_product_is_rejected_before_io(acquisition):
    fetch, count = acquisition
    with pytest.raises(InvalidParameterError, match="não aceita produto"):
        await datasets.get_dataset("unidades_conservacao_federais").fetch("soja")
    fetch.assert_not_awaited()
    count.assert_not_awaited()


async def test_unidades_conservacao_federais_casos_2():
    with collect_failures() as check:
        case = "test_partial_count_fails_instead_of_returning_data"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(_case_fixture_acquisition, monkeypatch=monkeypatch) as acquisition,
        ):
            _, count = acquisition
            count.return_value = (count_body(348), "https://test/hits")
            with pytest.raises(ParseError, match="Contagem divergente"):
                await datasets.unidades_conservacao_federais()
        case = "test_limit_fails_before_feature_download"
        with (
            check(case),
            isolated_dataset_case(case) as monkeypatch,
            fixture_instance(_case_fixture_acquisition, monkeypatch=monkeypatch) as acquisition,
        ):
            fetch, count = acquisition
            count.return_value = (count_body(501), "https://test/hits")
            with pytest.raises(ResourceLimitError, match="excede o limite"):
                await datasets.unidades_conservacao_federais()
            fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_registry_argument_is_not_ignored(acquisition):
    fetch, count = acquisition
    with pytest.raises(TypeError, match="Argumentos desconhecidos"):
        await datasets.get_dataset("unidades_conservacao_federais").fetch(ano=2020)
    fetch.assert_not_awaited()
    count.assert_not_awaited()
