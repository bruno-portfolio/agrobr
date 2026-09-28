from __future__ import annotations

from unittest.mock import AsyncMock

from agrobr import conab
from agrobr.conab.custo_producao import _acquisition
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import time as time_utils
from tests.helpers import levanta_exatamente


async def test_ano_posterior_ao_corrente_e_recusado_antes_da_rede(monkeypatch):
    catalogo = AsyncMock()
    monkeypatch.setattr(_acquisition.Acquisition, "catalog", catalogo)
    ano = time_utils.utcnow().year + 1
    with levanta_exatamente(InvalidParameterError, match=f"ano {ano} posterior ao corrente"):
        await conab.custo_sociobiodiversidade("acai", ano=ano)
    catalogo.assert_not_awaited()
