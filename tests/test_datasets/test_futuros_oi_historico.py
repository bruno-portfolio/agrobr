from unittest import mock

import pytest

from agrobr import datasets
from agrobr.b3 import client
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


@pytest.mark.parametrize(
    "produto,inicio,fim,data",
    [
        (None, "2025-12-19", "2025-12-19", None),
        ("soja_fob", "2025-12-19", "2025-12-19", None),
        ("banana", "2025-12-19", "2025-12-19", None),
        ("boi", None, "2025-12-19", None),
        ("boi", "2025-12-19", None, None),
        ("boi", "2025-12-20", "2025-12-19", None),
        ("boi", "2025-02-30", "2025-12-19", None),
        ("boi", "19-12-2025", "2025-12-19", None),
        ("boi", "2025-12-19", "2025-12-19", "2025-12-19"),
    ],
)
async def test_futuros_oi_historico_parametros_invalidos_antes_da_rede(
    produto: str | None, inicio: str | None, fim: str | None, data: str | None
):
    with (
        mock.patch.object(client, "fetch_posicoes_abertas", mock.AsyncMock()) as fetch,
        levanta_exatamente(InvalidParameterError),
    ):
        await datasets.futuros_agricolas(
            produto, tipo="oi_historico", inicio=inicio, fim=fim, data=data
        )
    fetch.assert_not_awaited()
