from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx

from agrobr.imea import api, client
from scripts import reconciliar_imea as reconciliacao

GOLDEN = Path(__file__).parent / "golden_data" / "imea" / "duplicatas_20260926"
BASE = "https://api1.imea.com.br/api/v2/mobile/cadeias"
INDICADOR = "708192508838936580"


def _comparar(monkeypatch: Any, cotacoes: bytes, filtro: dict[str, str]) -> dict[str, Any]:
    corpos = {
        f"{BASE}/4/cotacoes": cotacoes,
        f"{BASE}/4/indicadores": (GOLDEN / "indicadores_4.json").read_bytes(),
    }

    def responder(request: httpx.Request) -> httpx.Response:
        corpo = corpos.get(str(request.url))
        if corpo is None:
            return httpx.Response(404, request=request)
        return httpx.Response(
            200, content=corpo, headers={"content-type": "application/json"}, request=request
        )

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(client, "httpx", namespace)
    with httpx.Client(transport=httpx.MockTransport(responder)) as cliente:
        return reconciliacao.comparar(cliente, 4, filtro)


def test_comparar_confere_as_linhas_unicas_e_as_duplicatas(monkeypatch):
    corpo = (GOLDEN / "cotacoes_4.json").read_bytes()

    completo = _comparar(monkeypatch, corpo, {})
    por_unidade = _comparar(monkeypatch, corpo, {"unidade": "R$/sc"})

    for resultado in (completo, por_unidade):
        assert (resultado["status"], resultado["problems"]) == ("ok", [])
        assert resultado["duplicatas_oficiais"] == 69
        assert resultado["linhas_publicadas"] == resultado["linhas_oficiais"]
    assert completo["linhas_oficiais"] == 67


def test_comparar_acusa_chave_repetida_com_valor_diferente(monkeypatch):
    registros = json.loads((GOLDEN / "cotacoes_4.json").read_bytes())
    ultima = max(
        i
        for i, r in enumerate(registros)
        if r["IndicadorFinalId"] == INDICADOR and r["Localidade"] == "Sorriso"
    )
    registros[ultima] = {**registros[ultima], "Valor": registros[ultima]["Valor"] + 1}

    resultado = _comparar(monkeypatch, json.dumps(registros).encode("utf-8"), {})

    assert resultado["status"] == "mismatch"
    assert resultado["problems"] == ["1 linhas repetem a chave com valores diferentes"]
    assert (
        resultado["linhas_oficiais"],
        resultado["linhas_publicadas"],
        resultado["duplicatas_oficiais"],
    ) == (68, 68, 68)


def test_comparar_acusa_saida_sem_colapso(monkeypatch):
    sem_colapso = {"linhas": 0, "indicadores": []}
    monkeypatch.setattr(
        api, "_colapsar_repetidos", lambda df, _cadeia: (df, sem_colapso, sem_colapso)
    )

    resultado = _comparar(monkeypatch, (GOLDEN / "cotacoes_4.json").read_bytes(), {})

    assert resultado["status"] == "mismatch"
    for problema in (
        "linhas: agrobr 136 × oficiais 67",
        "69 linhas repetidas, iguais em todas as colunas",
        "duplicatas colapsadas: agrobr 0 × oficiais 69",
    ):
        assert problema in resultado["problems"]
    assert not [p for p in resultado["problems"] if "valores diferentes" in p]
