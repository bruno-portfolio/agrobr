from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from agrobr.usda import client

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/usda/psd_gateway_20260926"


def manifesto() -> dict[str, dict]:
    dados = json.loads((GOLDEN / "manifest.json").read_bytes())
    return {entrada["arquivo"]: entrada for entrada in dados["arquivos"]}


def corpo(arquivo: str) -> bytes:
    return (GOLDEN / arquivo).read_bytes()


def registros(arquivo: str) -> list[dict]:
    return json.loads(corpo(arquivo))


class Gateway:
    def __init__(self) -> None:
        self.rotas: dict[str, tuple[int, bytes]] = {}
        self.pedidos: list[httpx.Request] = []

    def servir(self, arquivo: str) -> dict:
        entrada = manifesto()[arquivo]
        self.rotas[entrada["url"]] = (entrada["status"], corpo(arquivo))
        return entrada

    def responder(self, request: httpx.Request) -> httpx.Response:
        self.pedidos.append(request)
        status, conteudo = self.rotas.get(str(request.url), (404, b""))
        return httpx.Response(
            status,
            content=conteudo,
            headers={"content-type": "application/json; charset=utf-8"},
            request=request,
        )


def simular_gateway(monkeypatch: pytest.MonkeyPatch, servidor: Any) -> None:
    transporte = httpx.MockTransport(servidor.responder)
    modulo = SimpleNamespace(**vars(httpx))
    modulo.AsyncClient = lambda **kwargs: httpx.AsyncClient(transport=transporte, **kwargs)
    monkeypatch.setattr(client, "httpx", modulo)


@pytest.fixture
def gateway(monkeypatch: pytest.MonkeyPatch) -> Gateway:
    servidor = Gateway()
    simular_gateway(monkeypatch, servidor)
    monkeypatch.setenv("AGROBR_USDA_API_KEY", "chave-de-teste")
    return servidor
