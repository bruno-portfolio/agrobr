from __future__ import annotations

import httpx
import pytest

from agrobr import queimadas
from agrobr.exceptions import SourceUnavailableError
from agrobr.queimadas import client
from tests.helpers import levanta_exatamente

CSV_DIARIO = f"{client.BASE_URL}/diario/Brasil/focos_diario_br_20260926.csv"
CSV_MENSAL = f"{client.BASE_URL}/mensal/Brasil/focos_mensal_br_202609.csv"
ASYNC_CLIENT_REAL = httpx.AsyncClient


def _servir(monkeypatch: pytest.MonkeyPatch, vazios: set[str]) -> list[str]:
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        if str(request.url) in vazios:
            return httpx.Response(200, content=b"")
        return httpx.Response(404)

    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: ASYNC_CLIENT_REAL(transport=httpx.MockTransport(responder), **kwargs),
    )
    return pedidos


async def test_diario_com_corpo_vazio_diz_que_o_servidor_respondeu(monkeypatch):
    pedidos = _servir(monkeypatch, {CSV_DIARIO})
    with levanta_exatamente(SourceUnavailableError) as erro:
        await queimadas.focos(ano=2026, mes=9, dia=26)
    mensagem = str(erro.value)
    assert f"{CSV_DIARIO} respondeu HTTP 200 com corpo vazio (0 bytes)" in mensagem
    assert "404" not in mensagem
    assert pedidos == [CSV_DIARIO]


async def test_mensal_com_corpo_vazio_diz_que_o_servidor_respondeu(monkeypatch):
    pedidos = _servir(monkeypatch, {CSV_MENSAL})
    with levanta_exatamente(SourceUnavailableError) as erro:
        await queimadas.focos(ano=2026, mes=9)
    mensagem = str(erro.value)
    assert f"{CSV_MENSAL} respondeu HTTP 200 com corpo vazio (0 bytes)" in mensagem
    assert "nao encontrado" not in mensagem
    assert len(pedidos) == 3


async def test_mensal_sem_arquivo_segue_como_nao_encontrado(monkeypatch):
    _servir(monkeypatch, set())
    with levanta_exatamente(SourceUnavailableError, match="Focos mensal 202609 nao encontrado"):
        await queimadas.focos(ano=2026, mes=9)
