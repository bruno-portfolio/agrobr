from __future__ import annotations

import json

import httpx
import pytest

from agrobr import exceptions
from agrobr.utils import geo
from tests import helpers


@pytest.fixture
def camada():
    return {
        "service_path": "Hidrografia/MapServer/0",
        "max_record_count": 2,
        "oid_field": "OBJECTID",
        "fields": "OBJECTID",
        "rename_map": {},
        "colunas_saida": ["OBJECTID"],
        "required_cols": {"OBJECTID"},
    }


def _instalar_paginas(monkeypatch, paginas, formato):
    chave = "attributes" if formato == "json" else "properties"
    respostas = [helpers.make_mock_response(json_data={"count": 4})]
    for oids in paginas:
        corpo = json.dumps({"features": [{chave: {"OBJECTID": oid}} for oid in oids]}).encode()
        respostas.append(helpers.make_mock_response(content=corpo))
    cliente = helpers.make_mock_async_client()
    cliente.get.side_effect = respostas
    monkeypatch.setattr(geo.httpx, "AsyncClient", lambda **_kwargs: cliente)
    return cliente


@pytest.mark.parametrize("formato", ["json", "geojson"])
@pytest.mark.parametrize(
    ("paginas", "chamadas"),
    [
        pytest.param([[10, 20], [20, 30]], 3, id="sobreposicao_com_avanco"),
        pytest.param([[10, 20], [5, 30]], 3, id="retrocesso_com_avanco"),
        pytest.param([[10, 10], [30, 40]], 2, id="duplicata_na_primeira"),
        pytest.param([[20, 10], [30, 40]], 2, id="desordem_na_primeira"),
        pytest.param([[10, 20], [30, 30]], 3, id="duplicata_na_segunda"),
        pytest.param([[10, 20], [40, 30]], 3, id="desordem_na_segunda"),
    ],
)
async def test_keyset_recusa_chaves_fora_de_ordem_estrita(
    monkeypatch, camada, paginas, chamadas, formato
):
    cliente = _instalar_paginas(monkeypatch, paginas, formato)
    with pytest.raises(exceptions.SourceUnavailableError, match="OBJECTID"):
        await geo.fetch_arcgis_layer(
            "https://example.test",
            camada,
            source="ana",
            timeout=httpx.Timeout(10),
            f=formato,
        )
    assert cliente.get.await_count == chamadas


@pytest.mark.parametrize("formato", ["json", "geojson"])
async def test_keyset_aceita_chaves_crescentes_com_lacunas(monkeypatch, camada, formato):
    cliente = _instalar_paginas(monkeypatch, [[10, 20], [30, 40]], formato)
    paginas, primeira_url = await geo.fetch_arcgis_layer(
        "https://example.test",
        camada,
        source="ana",
        timeout=httpx.Timeout(10),
        f=formato,
    )
    assert len(paginas) == 2
    assert cliente.get.await_count == 3
    pedidos = [httpx.URL(chamada.args[0]) for chamada in cliente.get.await_args_list]
    assert primeira_url == str(pedidos[1])
    assert pedidos[1].params["orderByFields"] == "OBJECTID"
    assert pedidos[2].params["where"] == "(1=1) AND OBJECTID > 20"
    assert pedidos[2].params["orderByFields"] == "OBJECTID"
