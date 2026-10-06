from __future__ import annotations

from unittest import mock

import httpx
import pytest

from agrobr import exceptions
from scripts import reconciliacao_semanal, reconciliar_ana


@pytest.mark.parametrize("etapa", ["ids", "features"])
@pytest.mark.parametrize("status", [200, 403, 503])
def test_html_oficial_vira_indisponivel_antes_do_json(etapa, status):
    resposta = httpx.Response(
        status, text="<html>Serviço em manutenção</html>", headers={"content-type": "text/html"}
    )
    with mock.patch.object(resposta, "json", side_effect=AssertionError("JSON prematuro")):

        def responder(request):
            if etapa == "features" and request.url.params.get("returnIdsOnly"):
                return httpx.Response(200, json={"objectIds": [7]})
            return resposta

        with (
            httpx.Client(transport=httpx.MockTransport(responder)) as cliente,
            pytest.raises(exceptions.SourceUnavailableError) as erro,
        ):
            reconciliar_ana.official(cliente, "hidrografia", {}, "json")

    estado, _, _ = reconciliacao_semanal.classificar(
        1, None, f"{type(erro.value).__name__}: {erro.value}"
    )
    assert estado == "indisponível"


@pytest.mark.parametrize("etapa", ["ids", "features"])
@pytest.mark.parametrize(
    "corpo",
    [b"{incompleto", b"[]", b'{"error":{"code":503,"message":"Unavailable"}}'],
)
def test_json_oficial_invalido_vira_indisponivel(etapa, corpo):
    def responder(request):
        if etapa == "features" and request.url.params.get("returnIdsOnly"):
            return httpx.Response(200, json={"objectIds": [7]})
        return httpx.Response(200, content=corpo, headers={"content-type": "application/json"})

    with (
        httpx.Client(transport=httpx.MockTransport(responder)) as cliente,
        pytest.raises(exceptions.SourceUnavailableError),
    ):
        reconciliar_ana.official(cliente, "hidrografia", {}, "json")


@pytest.mark.parametrize("tipo", ["application/json", "application/geo+json", "text/plain"])
@pytest.mark.parametrize("formato,atributos", [("json", "attributes"), ("geojson", "properties")])
def test_resposta_oficial_json_preserva_ids_e_atributos(tipo, formato, atributos):
    feature = {atributos: {"OBJECTID": 7, "NORIOCOMP": "Rio publicado"}}
    pedidos = []

    def responder(request):
        pedidos.append(dict(request.url.params))
        payload = {"objectIds": [7]} if len(pedidos) == 1 else {"features": [feature]}
        return httpx.Response(200, json=payload, headers={"content-type": f"{tipo}; charset=utf-8"})

    with httpx.Client(transport=httpx.MockTransport(responder)) as cliente:
        ids, features = reconciliar_ana.official(cliente, "hidrografia", {}, formato)

    assert ids == [7]
    assert features == {7: feature}
    assert pedidos[1]["objectIds"] == "7"
    assert pedidos[1]["f"] == formato
