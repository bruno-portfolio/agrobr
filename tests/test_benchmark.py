from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from agrobr.benchmark import benchmark_async, benchmark_sync
from agrobr.exceptions import InvalidParameterError


@pytest.mark.parametrize("iterations", [0, -1])
async def test_iterations_menor_que_um_e_recusado_antes_do_warmup(iterations):
    assincrona, sincrona = AsyncMock(), Mock()
    with pytest.raises(
        InvalidParameterError, match=f"iterations deve ser >= 1; recebido {iterations}"
    ):
        await benchmark_async("a", assincrona, iterations=iterations)
    with pytest.raises(
        InvalidParameterError, match=f"iterations deve ser >= 1; recebido {iterations}"
    ):
        benchmark_sync("s", sincrona, iterations=iterations)
    assincrona.assert_not_awaited()
    sincrona.assert_not_called()
