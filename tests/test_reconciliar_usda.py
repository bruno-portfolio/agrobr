from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from agrobr.usda import models
from scripts import reconciliar_usda as reconciliacao
from tests.test_usda.conftest import simular_gateway

GOLDEN = Path(__file__).parent / "golden_data" / "usda" / "psd_gateway_20260926"
AMOSTRA = {"soja_BR_2024.json", "acucar_mundo_2024.json", "algodao_US_2025.json"}


def _servir(
    monkeypatch: pytest.MonkeyPatch, trocas: dict[str, bytes] | None = None
) -> list[httpx.Request]:
    manifesto = json.loads((GOLDEN / "manifest.json").read_bytes())
    corpos = {
        entrada["url"]: (GOLDEN / entrada["arquivo"]).read_bytes()
        for entrada in manifesto["arquivos"]
        if entrada.get("status") == 200
    }
    for catalogo in models.CATALOGOS.iterdir():
        corpos[f"{reconciliacao.GATEWAY}/{catalogo.stem}"] = catalogo.read_bytes()
    corpos.update(trocas or {})
    pedidos: list[httpx.Request] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        return httpx.Response(
            200 if str(request.url) in corpos else 404,
            content=corpos.get(str(request.url), b""),
            headers={"content-type": "application/json"},
            request=request,
        )

    monkeypatch.setattr(
        httpx.Client, "send", lambda _cliente, request, **_kwargs: responder(request)
    )
    simular_gateway(monkeypatch, SimpleNamespace(responder=responder))
    monkeypatch.setenv("AGROBR_USDA_API_KEY", "chave-de-teste")
    escolhidos = [c for c in reconciliacao.cortes() if c[-1] in AMOSTRA]
    monkeypatch.setattr(reconciliacao, "cortes", lambda: escolhidos)
    return pedidos


def test_cortes_sao_os_34_conferidos():
    cortes = reconciliacao.cortes()
    assert len(cortes) == 34
    assert ("acucar", "world", 2024, "acucar_mundo_2024.json") in cortes
    assert ("algodao", "US", 2025, "algodao_US_2025.json") in cortes
    assert {pais for _, pais, _, _ in cortes} == {"BR", "US", "world"}


def test_run_ok_com_a_chave_so_no_cabecalho(monkeypatch, tmp_path):
    pedidos = _servir(monkeypatch)
    saida = tmp_path / "usda.json"

    assert reconciliacao.run(saida) == 0

    relatorio = json.loads(saida.read_text(encoding="utf-8"))
    assert len(relatorio["checks"]) == 4
    assert {check["status"] for check in relatorio["checks"].values()} == {"ok"}
    assert relatorio["checks"]["acucar_world_2024"]["source_url"].endswith(
        "/0612000/world/year/2024"
    )
    assert len(pedidos) == 4 + 2 * 3
    assert {p.headers.get("X-Api-Key") for p in pedidos} == {"chave-de-teste"}
    assert not any(p.url.query for p in pedidos)
    assert "chave-de-teste" not in saida.read_text(encoding="utf-8")


def test_catalogo_novo_ou_renomeado_vira_mismatch(monkeypatch, tmp_path):
    atributos = json.loads((models.CATALOGOS / "commodityAttributes.json").read_bytes())
    atributos = [
        {**a, "attributeName": "Output"} if a["attributeId"] == 28 else a for a in atributos
    ] + [{"attributeId": 999, "attributeName": "Nova"}]
    _servir(
        monkeypatch,
        {f"{reconciliacao.GATEWAY}/commodityAttributes": json.dumps(atributos).encode()},
    )

    assert reconciliacao.run(tmp_path / "usda.json") == 1

    relatorio = json.loads((tmp_path / "usda.json").read_text(encoding="utf-8"))
    assert relatorio["checks"]["catalogos"]["problems"] == [
        "commodityAttributes: código novo 999 (Nova)",
        "commodityAttributes: 28 mudou de nome para Output",
    ]
    assert relatorio["checks"]["soja_BR_2024"]["problems"] == [
        "saída do agrobr (13 linhas) × corpo ao vivo (13 registros)"
    ]


def test_identidade_quebrada_e_revisao_desde_o_golden(monkeypatch, tmp_path):
    corpo = json.loads((GOLDEN / "soja_BR_2024.json").read_bytes())
    corpo = [{**r, "value": r["value"] + 1} if r["attributeId"] == 86 else r for r in corpo]
    url = "https://api.fas.usda.gov/api/psd/commodity/2222000/country/BR/year/2024"
    _servir(monkeypatch, {url: json.dumps(corpo).encode()})

    assert reconciliacao.run(tmp_path / "usda.json") == 1

    check = json.loads((tmp_path / "usda.json").read_text(encoding="utf-8"))["checks"][
        "soja_BR_2024"
    ]
    assert check["problems"] == ["oferta 202992.0 × Total Supply 202993.0"]
    assert check["revisados_desde_o_golden"] == [86]
    assert check["ultima_atualizacao"] == [["2026", "04"]]
