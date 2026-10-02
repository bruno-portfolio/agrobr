from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.usda import client
from tests.helpers import (
    levanta_exatamente,
    sem_excecao,
)

from .conftest import corpo, registros, simular_gateway

URL_BR = "https://api.fas.usda.gov/api/psd/commodity/2222000/country/BR/year/2024"


def test_chave_ausente_explicita_ou_do_ambiente(monkeypatch):
    monkeypatch.delenv("AGROBR_USDA_API_KEY", raising=False)
    with levanta_exatamente(SourceUnavailableError, r"api\.data\.gov/signup"):
        client._get_api_key(None)
    assert client._get_api_key("explicita") == "explicita"
    monkeypatch.setenv("AGROBR_USDA_API_KEY", "do-ambiente")
    assert client._get_api_key(None) == "do-ambiente"


async def test_rotas_e_chave_so_no_cabecalho(gateway):
    for arquivo in ("soja_BR_2024.json", "soja_world_2024.json", "soja_all_2024.json"):
        gateway.servir(arquivo)
    with sem_excecao():
        respostas = [
            await client.fetch_psd_country("2222000", "BR", 2024),
            await client.fetch_psd_world("2222000", 2024, api_key="explicita"),
            await client.fetch_psd_all_countries("2222000", 2024),
        ]
    urls = [
        URL_BR,
        "https://api.fas.usda.gov/api/psd/commodity/2222000/world/year/2024",
        "https://api.fas.usda.gov/api/psd/commodity/2222000/country/all/year/2024",
    ]
    assert [getattr(r, "url", None) for r in respostas] == urls
    assert [str(p.url) for p in gateway.pedidos] == urls
    assert respostas[0].corpo == corpo("soja_BR_2024.json")
    assert respostas[0].dados == registros("soja_BR_2024.json")
    assert [p.headers.get("X-Api-Key") for p in gateway.pedidos] == [
        "chave-de-teste",
        "explicita",
        "chave-de-teste",
    ]
    assert not any("api_key" in p.headers for p in gateway.pedidos)


@pytest.mark.parametrize(
    ("arquivo", "codigo"),
    [("sem_chave.json", "API_KEY_MISSING"), ("chave_invalida.json", "API_KEY_INVALID")],
)
async def test_chave_recusada_pelo_gateway(gateway, arquivo, codigo):
    gateway.servir(arquivo)
    with levanta_exatamente(SourceUnavailableError, f"HTTP 403, {codigo}") as erro:
        await client.fetch_psd_country("2222000", "BR", 2024)
    assert "chave-de-teste" not in str(erro.value)


async def test_chave_do_argumento_ecoada_em_corpo_nao_json_sai_mascarada(gateway):
    gateway.rotas[URL_BR] = (200, b"gateway rejected credential: chave-do-argumento")
    with levanta_exatamente(SourceUnavailableError, r"credential: \[REDACTED\]") as erro:
        await client.fetch_psd_country("2222000", "BR", 2024, api_key="chave-do-argumento")
    assert "chave-do-argumento" not in str(erro.value)
    assert len(gateway.pedidos) == 1


async def test_403_sem_codigo_de_chave_e_erro_http(gateway):
    gateway.rotas[URL_BR] = (403, b"<html>Access Denied</html>")
    with levanta_exatamente(SourceUnavailableError, "HTTP 403: a fonte recusou o pedido"):
        await client.fetch_psd_country("2222000", "BR", 2024)


@pytest.mark.parametrize(
    ("destino", "chave_no_destino"),
    [
        ("https://outro-host.example/coleta", None),
        ("http://api.fas.usda.gov/api/psd/commodity/2222000/country/BR/year/2024", None),
        ("https://api.fas.usda.gov/api/psd/commodity/2222000/country/BR/year/2025", "secreta"),
    ],
    ids=["outro_host", "mesmo_host_sem_tls", "mesmo_host"],
)
async def test_redirect_so_leva_a_chave_na_origem_da_api(monkeypatch, destino, chave_no_destino):
    pedidos: list[httpx.Request] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        if len(pedidos) == 1:
            return httpx.Response(302, headers={"Location": destino}, request=request)
        return httpx.Response(200, content=corpo("soja_BR_2024.json"), request=request)

    simular_gateway(monkeypatch, SimpleNamespace(responder=responder))
    with sem_excecao():
        await client.fetch_psd_country("2222000", "BR", 2024, api_key="secreta")
    assert [(str(p.url), p.headers.get("X-Api-Key")) for p in pedidos] == [
        (URL_BR, "secreta"),
        (destino, chave_no_destino),
    ]
