from __future__ import annotations

import httpx
import pytest

from agrobr import bruto
from agrobr.alt.sicar import client
from agrobr.exceptions import ParseError
from tests import helpers
from tests.test_bruto import conftest as bruto_helpers


@pytest.mark.parametrize(
    ("corpo", "mensagem"),
    [
        pytest.param(b"<FeatureCollection", "contagem WFS ilegível", id="xml_ilegivel"),
        pytest.param(
            b'<!DOCTYPE FeatureCollection><FeatureCollection numberMatched="0"/>',
            "contagem WFS com DOCTYPE",
            id="doctype",
        ),
        pytest.param(
            b'<FeatureCollection numberReturned="0"/>',
            "contagem WFS sem numberMatched",
            id="sem_number_matched",
        ),
        pytest.param(
            b'<FeatureCollection numberMatched="-1"/>',
            "numberMatched inválido na contagem",
            id="number_matched_invalido",
        ),
    ],
)
async def test_coleta_contagem_invalida_nao_publica_cobertura(
    monkeypatch, tmp_path, corpo, mensagem
):
    pedidos = []

    def responder(request):
        pedidos.append(request)
        return bruto_helpers.resposta(200, corpo, {"Content-Type": "text/xml"})

    monkeypatch.setattr(
        client,
        "make_session",
        lambda: httpx.AsyncClient(transport=httpx.MockTransport(responder)),
    )

    with helpers.levanta_exatamente(ParseError, match=mensagem):
        await bruto.coletar(
            "sicar", "imoveis", destino=tmp_path, uf="DF", tamanho_pagina=5, compactar=False
        )

    assert len(pedidos) == 1
    assert pedidos[0].url.params["resultType"] == "hits"
    (registro,) = bruto_helpers.manifesto(tmp_path)
    assert registro["status"] == "erro"
    assert registro["cobertura"]["completa"] is False
