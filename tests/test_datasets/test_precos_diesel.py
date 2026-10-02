import typing
from unittest.mock import AsyncMock, patch

import pytest

from agrobr import datasets
from agrobr.alt.anp_diesel import api
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


@pytest.mark.asyncio
async def test_deterministic_recusado_antes_http():
    async with datasets.deterministic("2024-01-01"):
        with (
            patch.object(api.client, "fetch_precos_resource", new_callable=AsyncMock) as fetch,
            levanta_exatamente(InvalidParameterError, match="edição"),
        ):
            await datasets.precos_diesel(nivel="brasil")
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_kwarg_desconhecido_registry_recusado():
    dataset = datasets.get_dataset("precos_diesel")
    with (
        patch.object(api.client, "fetch_precos_resource", new_callable=AsyncMock) as fetch,
        levanta_exatamente(TypeError, match="desconhecidos"),
    ):
        await dataset.fetch(ano=2024)
    fetch.assert_not_awaited()


def test_municipio_aceita_codigo_ibge_na_anotacao_publica():
    dataset = type(datasets.get_dataset("precos_diesel"))
    alvos = [datasets.precos_diesel, *typing.get_overloads(datasets.precos_diesel), dataset.fetch]
    assert {alvo.__annotations__["municipio"] for alvo in alvos} == {"int | str | None"}
