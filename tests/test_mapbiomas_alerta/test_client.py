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


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("consulta", ["fetch_alert_date_range", "fetch_last_publication"])
async def test_recusa_da_consulta_sem_token_nao_cita_o_token(monkeypatch, status, consulta):
    pedidos = responder(monkeypatch, status, b'{"message": "unauthorized"}')
    with levanta_exatamente(SourceUnavailableError) as erro:
        await getattr(client, consulta)()
    assert erro.value.last_error == f"HTTP {status}: acesso recusado (consulta sem token)"
    assert pedidos == [""]


async def test_erro_de_token_no_graphql_sem_token_nao_cita_a_variavel(monkeypatch):
    responder(monkeypatch, 200, b'{"errors": [{"message": "Token required"}]}')
    with levanta_exatamente(SourceUnavailableError) as erro:
        await client.fetch_last_publication()
    assert erro.value.last_error == "GraphQL error: Token required (consulta sem token)"


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
