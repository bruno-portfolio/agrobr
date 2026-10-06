from __future__ import annotations

import json
from urllib import parse

from agrobr import ana
from agrobr.ana import client
from agrobr.exceptions import ParseError
from tests import helpers
from tests.test_ana import oficial


async def test_massas_dagua_recusa_fid_booleano_antes_da_pagina(monkeypatch):
    documento = json.loads((oficial.MASSAS / "barragem_df/faixa_0.json").read_bytes())
    feicao = documento["features"][0]
    feicao["attributes"]["FID"] = 1
    pedidos = []
    http = helpers.make_mock_async_client()

    def responder(url):
        params = dict(parse.parse_qsl(parse.urlsplit(str(url)).query))
        pedidos.append(params)
        if params.get("returnCountOnly") == "true":
            corpo = {"count": 1}
        elif params.get("returnIdsOnly") == "true":
            corpo = {"objectIdFieldName": "FID", "objectIds": [True]}
        else:
            corpo = {"features": [feicao]}
        return helpers.make_mock_response(
            content=json.dumps(corpo).encode(),
            json_data=corpo,
            headers={"content-type": "application/json"},
            url=str(url),
        )

    http.get.side_effect = responder
    monkeypatch.setattr(client.httpx, "AsyncClient", lambda **_kwargs: http)

    with helpers.levanta_exatamente(ParseError, "returnIdsOnly sem lista de FID") as erro:
        await ana.massas_dagua(uf="DF")

    assert erro.value.source == "ana"
    assert len(pedidos) == 2
    assert pedidos[0]["returnCountOnly"] == "true"
    assert pedidos[1]["returnIdsOnly"] == "true"
