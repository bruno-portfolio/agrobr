from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

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


def _cotacoes_com_chave_repetida() -> bytes:
    registros = json.loads((GOLDEN / "cotacoes_4.json").read_bytes())
    ultima = max(
        i
        for i, r in enumerate(registros)
        if r["IndicadorFinalId"] == INDICADOR and r["Localidade"] == "Sorriso"
    )
    registros[ultima] = {**registros[ultima], "Valor": registros[ultima]["Valor"] + 1}
    return json.dumps(registros).encode("utf-8")


def test_comparar_chave_repetida_avisada_pendente(monkeypatch):
    resultado = _comparar(monkeypatch, _cotacoes_com_chave_repetida(), {})

    assert resultado["status"] == "pendente"
    assert resultado["problems"] == []
    assert resultado["pendencias"] == [
        "2 linhas com chaves repetidas e valores diferentes, avisadas no MetaInfo"
    ]
    assert (
        resultado["linhas_oficiais"],
        resultado["linhas_publicadas"],
        resultado["duplicatas_oficiais"],
    ) == (68, 68, 68)


@pytest.mark.parametrize("defeito", ["sem_aviso", "outro_valor"])
def test_comparar_chave_repetida_nao_esconde_divergencia(monkeypatch, defeito):
    original = api._colapsar_repetidos

    def alterar(frame, cadeia):
        saida, colapsadas, repetidas = original(frame, cadeia)
        if defeito == "sem_aviso":
            repetidas = {"linhas": 0, "indicadores": []}
        else:
            indice = saida.index[saida["indicador_id"] != INDICADOR][0]
            saida.loc[indice, "valor"] += 1
        return saida, colapsadas, repetidas

    monkeypatch.setattr(api, "_colapsar_repetidos", alterar)
    resultado = _comparar(monkeypatch, _cotacoes_com_chave_repetida(), {})
    assert resultado["status"] == "mismatch"
    assert (
        "1 linhas repetem a chave com valores diferentes"
        if defeito == "sem_aviso"
        else "1 linhas divergentes"
    ) in resultado["problems"]


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


@pytest.mark.parametrize("status,codigo", [("pendente", 0), ("mismatch", 1)])
def test_run_pendencia_nao_e_mismatch(monkeypatch, tmp_path, capsys, status, codigo):
    monkeypatch.setattr(reconciliacao, "CADEIAS", {4: "soja"})
    monkeypatch.setattr(reconciliacao, "FILTROS", [])
    monkeypatch.setattr(reconciliacao, "catalogo", lambda _: {"status": "ok", "problems": []})
    monkeypatch.setattr(reconciliacao, "comparar", lambda *_: {"status": status, "problems": []})
    destino = tmp_path / "reconciliacao.json"
    assert reconciliacao.run(destino) == codigo
    assert json.loads(destino.read_text(encoding="utf-8"))["checks"]["soja"]["status"] == status
    assert ("0 mismatch / 1 pendente" if codigo == 0 else "1 mismatch / 0 pendente") in (
        capsys.readouterr().out
    )
