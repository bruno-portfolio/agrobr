from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr.mapbiomas_alerta import client, models

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/mapbiomas_alerta/oficial_20260926"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_bytes())
ORACULO = json.loads((GOLDEN / "oraculo_20260926.json").read_bytes())
INICIO, FIM = "2025-01-13", "2025-01-19"


def _chave(query: str, variables: dict[str, Any]) -> str:
    return json.dumps([query, variables], sort_keys=True)


def servir(
    monkeypatch: pytest.MonkeyPatch,
    trocar: Callable[[dict[str, Any], bytes], bytes] | None = None,
) -> dict[str, list[Any]]:
    """Troca o ``httpx.AsyncClient`` por um que responde com os corpos do golden.

    O pedido casa pela query e pelas variáveis exatas do recibo; fora do golden, a resposta é um
    erro GraphQL. ``trocar`` edita o corpo das páginas de alertas antes da resposta.
    """
    corpos = {
        _chave(recibo["query"], recibo["variables"]): (GOLDEN / recibo["arquivo"]).read_bytes()
        for recibo in MANIFESTO["recibos"]
        if recibo["arquivo"] and recibo["arquivo"] != "referencia_antes.json"
    }
    visto: dict[str, list[Any]] = {"servidos": [], "sem_golden": [], "autorizacao": []}

    def handler(request: httpx.Request) -> httpx.Response:
        pedido = json.loads(request.content)
        visto["autorizacao"].append(request.headers.get("authorization"))
        corpo = corpos.get(_chave(pedido["query"], pedido["variables"]))
        if corpo is None:
            visto["sem_golden"].append(pedido["variables"])
            corpo = b'{"errors": [{"message": "pedido fora do golden"}]}'
        else:
            visto["servidos"].append(pedido["variables"])
            if trocar is not None and pedido["query"] == models.ALERTS_QUERY:
                corpo = trocar(pedido["variables"], corpo)
        return httpx.Response(200, content=corpo, headers={"content-type": "application/json"})

    real = httpx.AsyncClient

    class Simulado(real):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(handler)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Simulado)
    monkeypatch.setattr(client, "PAGE_SIZE", 30)
    return visto
