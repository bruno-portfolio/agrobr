from __future__ import annotations

import json
from datetime import date

import httpx
import pytest

from scripts import reconciliar_mapbiomas_alerta as reconciliacao
from tests.helpers import sem_excecao
from tests.test_mapbiomas_alerta.oficial import FIM, GOLDEN, INICIO, ORACULO, servir

TOKEN = "token-de-teste"


def _servir(
    monkeypatch: pytest.MonkeyPatch, editar=None, trocar=None, falha: str | None = None
) -> list[httpx.Request]:
    referencia = json.loads((GOLDEN / "referencia_antes.json").read_bytes())
    if editar is not None:
        editar(referencia["data"]["alerts"])
    pedidos: list[httpx.Request] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        if falha == "rede":
            raise httpx.ConnectError("sem rede", request=request)
        if falha == "graphql":
            return httpx.Response(200, json={"errors": [{"message": "x"}]}, request=request)
        variables = json.loads(request.content)["variables"]
        assert (variables["startDate"], variables["endDate"]) == (INICIO, FIM)
        return httpx.Response(200, json=referencia, request=request)

    monkeypatch.setattr(
        httpx.Client, "send", lambda _cliente, request, **_kwargs: responder(request)
    )
    servir(monkeypatch, trocar)
    monkeypatch.setenv("AGROBR_MAPBIOMAS_ALERTA_TOKEN", TOKEN)
    monkeypatch.setattr(
        reconciliacao,
        "recortes",
        lambda _hoje: {"semana_do_golden": {"startDate": INICIO, "endDate": FIM}},
    )
    return pedidos


def _rodar(tmp_path) -> tuple[int, dict]:
    saida = tmp_path / "mapbiomas.json"
    with sem_excecao():
        codigo = reconciliacao.run(saida)
    texto = saida.read_text(encoding="utf-8")
    assert TOKEN not in texto
    return codigo, json.loads(texto)["checks"]["semana_do_golden"]


def _total_84_na_pagina_2(variables, corpo):
    if variables["page"] != 2:
        return corpo
    dado = json.loads(corpo)
    dado["data"]["alerts"]["metadata"]["totalCount"] = 84
    return json.dumps(dado).encode()


def test_run_ok_com_o_token_so_no_cabecalho(monkeypatch, tmp_path):
    pedidos = _servir(monkeypatch)
    codigo, check = _rodar(tmp_path)

    assert codigo == 0
    assert check == {
        "status": "ok",
        "problems": [],
        "alertas": 83,
        "area_ha": ORACULO["area_ha_total"],
        "paginas_do_agrobr": 3,
        "revisados_desde_o_golden": False,
    }
    assert [json.loads(p.content)["variables"]["limit"] for p in pedidos] == [1, 83]
    assert {p.headers.get("authorization") for p in pedidos} == {f"Bearer {TOKEN}"}


@pytest.mark.parametrize(
    ("kwargs", "problema"),
    [
        (
            {"editar": lambda alerts: alerts["collection"][0].update(areaHa=0.5)},
            "saída (83 linhas) × referência (83)",
        ),
        (
            {"editar": lambda alerts: alerts["metadata"].update(totalCount=84)},
            "MetaInfo: 83 linhas × totalCount 84",
        ),
        ({"trocar": _total_84_na_pagina_2}, "avisos: ['totalCount mudou de 83 para 84"),
    ],
    ids=["area_diferente", "total_anunciado", "aviso_do_agrobr"],
)
def test_divergencia_vira_mismatch(monkeypatch, tmp_path, kwargs, problema):
    _servir(monkeypatch, **kwargs)
    codigo, check = _rodar(tmp_path)

    assert codigo == 1
    assert check["status"] == "mismatch"
    assert [p for p in check["problems"] if p.startswith(problema)] != []


def test_recorte_acima_da_referencia_fica_pendente(monkeypatch, tmp_path):
    _servir(monkeypatch)
    monkeypatch.setattr(reconciliacao, "LIMITE_REFERENCIA", 50)
    codigo, check = _rodar(tmp_path)

    assert codigo == 0
    assert check == {"status": "pendente", "problems": ["83 alertas: acima da referência"]}


@pytest.mark.parametrize(
    ("falha", "classe"),
    [("rede", "ConnectError"), ("graphql", "RuntimeError")],
    ids=["rede", "graphql"],
)
def test_falha_da_referencia_fica_indisponivel(monkeypatch, tmp_path, falha, classe):
    _servir(monkeypatch, falha=falha)
    codigo, check = _rodar(tmp_path)

    assert codigo == 0
    assert check == {"status": "indisponivel", "problems": [classe]}


def test_mes_de_um_ano_atras_e_o_mes_fechado_do_ano_anterior():
    recortes = reconciliacao.recortes(date(2025, 3, 1))

    assert recortes["mes_de_um_ano_atras"] == {"startDate": "2024-02-01", "endDate": "2024-02-29"}
    assert recortes["janeiro_2025_caixa"]["boundingBox"] == list(reconciliacao.CAIXA)
