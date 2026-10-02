from __future__ import annotations

from typing import Any

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.mapbiomas_alerta import client
from tests.helpers import levanta_exatamente

ORIENTACAO = (
    " — confira AGROBR_MAPBIOMAS_ALERTA_TOKEN ou o argumento token=; o token é pessoal e expira "
    "(um novo sai da mutation signIn da API)"
)


def responder(monkeypatch: pytest.MonkeyPatch, status: int, corpo: bytes) -> list[str]:
    pedidos: list[str] = []
    real = httpx.AsyncClient

    def handler(request: httpx.Request) -> httpx.Response:
        pedidos.append(request.headers.get("authorization", ""))
        return httpx.Response(status, content=corpo, headers={"content-type": "application/json"})

    class Simulado(real):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(handler)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Simulado)
    return pedidos


@pytest.mark.parametrize("status", [401, 403])
async def test_credencial_recusada_aponta_a_variavel_e_o_argumento(monkeypatch, status):
    pedidos = responder(monkeypatch, status, b'{"message": "unauthorized segredo-123"}')
    with levanta_exatamente(SourceUnavailableError) as erro:
        await client._graphql_request("query { x }", {}, token="segredo-123")
    assert erro.value.last_error == f"HTTP {status}: credencial recusada{ORIENTACAO}"
    assert "segredo-123" not in str(erro.value)
    assert pedidos == ["Bearer segredo-123"]


async def test_token_invalido_no_graphql_ganha_a_orientacao(monkeypatch):
    responder(monkeypatch, 200, b'{"errors": [{"message": "Token de acesso inv\\u00e1lido"}]}')
    with levanta_exatamente(SourceUnavailableError) as erro:
        await client._graphql_request("query { x }", {}, token="segredo-123")
    assert erro.value.last_error == f"GraphQL error: Token de acesso inválido{ORIENTACAO}"


async def test_outro_erro_do_graphql_fica_sem_a_orientacao(monkeypatch):
    responder(monkeypatch, 200, b'{"errors": [{"message": "campo desconhecido"}]}')
    with levanta_exatamente(SourceUnavailableError) as erro:
        await client._graphql_request("query { x }", {}, token="segredo-123")
    assert erro.value.last_error == "GraphQL error: campo desconhecido"
