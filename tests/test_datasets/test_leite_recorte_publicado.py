from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import datasets, exceptions
from agrobr.ibge import agregados
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[2]
    / "tests/golden_data/reconciliacao_canais_ibge_20260918/agregados"
)


async def test_leite_industrial_preserva_recorte_de_uf_no_http(monkeypatch):
    dados = json.loads((GOLDEN / "agregados_014.json").read_bytes())
    periodos = json.loads((GOLDEN / "agregados_015.json").read_bytes())
    publicados = {
        variavel["id"]: (
            variavel["unidade"],
            next(
                serie["serie"]["202401"]
                for serie in variavel["resultados"][0]["series"]
                if serie["localidade"]["id"] == "31"
            ),
        )
        for variavel in dados
    }
    assert publicados == {
        "282": ("Mil litros", "1595643"),
        "283": ("Mil litros", "1593219"),
        "2522": ("Reais por litro", "2.22"),
    }
    pedidos: list[str] = []

    def responder(url: str, **_kwargs: Any):
        pedidos.append(str(url))
        endereco = httpx.URL(url)
        if endereco.host == "apisidra.ibge.gov.br":
            return helpers.make_mock_response(
                403,
                text="Just a moment",
                headers={"cf-mitigated": "challenge"},
                url=str(url),
            )
        assert endereco.host == "servicodados.ibge.gov.br"
        if endereco.path.endswith("/variaveis/282,283,2522"):
            recorte = endereco.params["localidades"]
            assert recorte in {"N3[31]", "N3[all]"}
            payload = json.loads((GOLDEN / "agregados_014.json").read_bytes())
            if recorte == "N3[31]":
                for variavel in payload:
                    for resultado in variavel["resultados"]:
                        resultado["series"] = [
                            serie
                            for serie in resultado["series"]
                            if serie["localidade"]["id"] == "31"
                        ]
        else:
            assert endereco.path.endswith("/1086/periodos")
            payload = periodos
        return helpers.make_mock_response(
            json_data=payload,
            content=json.dumps(payload).encode(),
            headers={"content-type": "application/json"},
            url=str(url),
        )

    cliente = helpers.make_mock_async_client()
    cliente.get.side_effect = responder
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: cliente)
    monkeypatch.setattr(agregados, "_periodos_cache", {})

    with pytest.warns(exceptions.SourceFallbackWarning, match="SIDRA indisponível"):
        frame, meta = await datasets.leite_industrial("2024T1", uf="MG", return_meta=True)

    assert frame["localidade_cod"].tolist() == [31]
    assert frame["localidade"].tolist() == ["Minas Gerais"]
    assert frame["trimestre"].tolist() == ["202401"]
    assert frame["leite_adquirido"].tolist() == [1_595_643.0]
    assert frame["leite_industrializado"].tolist() == [1_593_219.0]
    assert frame["preco_medio"].tolist() == [2.22]
    assert meta.records_count == 1
    assert any(httpx.URL(url).params.get("localidades") == "N3[31]" for url in pedidos)
