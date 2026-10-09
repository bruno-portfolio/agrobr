from __future__ import annotations

import base64
import logging
from pathlib import Path

import httpx
import pytest

from agrobr.conab import ceasa_precos
from agrobr.conab.ceasa import client, models
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import levanta_exatamente

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/conab_ceasa/precos_20260923"
USUARIO = "usuario_c43_falso"
SENHA = "senha_falsa"
CORPOS = {
    models.QUERY_PRECOS: (GOLDEN / "precos_response.json").read_bytes(),
}


def _instalar(monkeypatch, resposta) -> list[httpx.Request]:
    pedidos: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        return resposta(request)

    real = httpx.AsyncClient

    class Simulado(real):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Simulado)
    monkeypatch.setattr(client, "PENTAHO_AUTH", (USUARIO, SENHA))
    return pedidos


def _sem_credencial(texto: str) -> None:
    for trecho in (USUARIO, SENHA, "userid", "password"):
        assert trecho not in texto, trecho


async def test_credencial_vai_no_cabecalho_e_fica_fora_da_url_do_log_e_do_meta(
    monkeypatch, caplog, capsys
):
    pedidos = _instalar(
        monkeypatch,
        lambda request: httpx.Response(
            200,
            content=CORPOS[request.url.params["dataAccessId"]],
            headers={"content-type": "application/json"},
        ),
    )
    caplog.set_level(logging.INFO, logger="httpx")

    df, meta = await ceasa_precos(return_meta=True)

    esperado = "Basic " + base64.b64encode(f"{USUARIO}:{SENHA}".encode()).decode()
    assert len(pedidos) == 1
    assert [pedido.headers.get("authorization") for pedido in pedidos] == [esperado]
    assert not df.empty
    assert "HTTP Request: GET" in caplog.text
    for texto in [*(str(pedido.url) for pedido in pedidos), caplog.text, capsys.readouterr().out]:
        _sem_credencial(texto)
    _sem_credencial(meta.to_json())


@pytest.mark.parametrize(
    "resposta",
    [
        httpx.Response(500, content=b"erro"),
        httpx.Response(200, content=b"<html>login</html>", headers={"content-type": "text/html"}),
    ],
    ids=["status_5xx", "corpo_nao_json"],
)
async def test_falha_nao_expoe_a_credencial(monkeypatch, caplog, capsys, resposta):
    pedidos = _instalar(monkeypatch, lambda _request: resposta)
    caplog.set_level(logging.INFO)

    with levanta_exatamente(SourceUnavailableError) as erro:
        await client.fetch_precos()

    assert pedidos
    for texto in [str(erro.value), erro.value.url, caplog.text, capsys.readouterr().out]:
        _sem_credencial(texto)
