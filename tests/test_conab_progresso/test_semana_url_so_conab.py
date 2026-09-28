from __future__ import annotations

from typing import Any

import httpx
import pytest

from agrobr import conab
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente

SEMANA = (
    "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra/"
    "acompanhamento-das-lavouras-14-09-a-20-09-26/acompanhamento-das-lavouras-14-09-a-20-09-26"
)


@pytest.fixture
def pedidos(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Todo pedido que chega ao transporte; a página da CONAB redireciona para fora."""
    vistos: list[str] = []

    async def responder(_transporte: Any, request: httpx.Request) -> httpx.Response:
        vistos.append(str(request.url))
        return httpx.Response(
            302, headers={"location": "https://interno.exemplo/planilha.xlsx"}, request=request
        )

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", responder)
    return vistos


@pytest.mark.parametrize(
    "semana_url",
    [
        "https://interno.exemplo/conab/semana",
        "http://www.gov.br/conab/semana",
        "https://www.gov.br@interno.exemplo/conab/semana",
        "https://www.gov.br:8443/conab/semana",
        "https://www.gov.br/conab/../ibama/semana",
        "https://www.gov.br/conabx/semana",
        "http://169.254.169.254/latest/meta-data/",
    ],
)
async def test_semana_url_fora_da_conab_recusada_antes_da_rede(pedidos, semana_url):
    with levanta_exatamente(InvalidParameterError, "pedido fora da CONAB recusado"):
        await conab.progresso_safra(semana_url=semana_url)

    assert pedidos == []


async def test_redirecionamento_para_fora_da_conab_recusado(pedidos):
    with levanta_exatamente(InvalidParameterError, "interno.exemplo/planilha.xlsx"):
        await conab.progresso_safra(semana_url=SEMANA)

    assert pedidos == [SEMANA]
