from __future__ import annotations

import gzip
import json
import warnings
from pathlib import Path
from unittest.mock import Mock

import pytest

from agrobr import bcb, contracts, datasets
from agrobr.bcb import client
from agrobr.exceptions import ParseError
from tests import helpers

GOLDEN = Path(__file__).parents[1] / "golden_data/bcb"


def servir(monkeypatch, payload):
    http = helpers.make_mock_async_client()
    response = helpers.make_mock_response(content=json.dumps(payload).encode())
    response.json.return_value = payload
    http.get.return_value = response
    monkeypatch.setattr(client.httpx, "AsyncClient", Mock(return_value=http))
    return http


@pytest.mark.parametrize(
    "payload",
    [{}, {"odata.metadata": "..."}, None, [], {"value": None}, {"value": {}}, {"value": [1]}],
)
@pytest.mark.parametrize(
    "consulta,args",
    [
        (bcb.credito_rural, ("soja",)),
        (bcb.credito_rural_total, ()),
        (datasets.credito_rural, ("soja",)),
    ],
)
async def test_envelope_invalido_nao_e_recorte_vazio(monkeypatch, payload, consulta, args):
    http = servir(monkeypatch, payload)
    with pytest.raises(ParseError, match="Envelope SICOR inválido") as erro:
        await consulta(*args, safra="2024/25")
    assert erro.value.__cause__ is not None
    assert erro.value.errors
    http.get.assert_awaited_once()


@pytest.mark.parametrize(
    "consulta,agregacao",
    [
        (bcb.credito_rural, "uf"),
        (bcb.credito_rural, "programa"),
        (bcb.credito_rural, "registro"),
        (datasets.credito_rural, "uf"),
        (datasets.credito_rural, "registro"),
    ],
)
@pytest.mark.parametrize("as_polars", [False, True])
async def test_credito_vazio_tem_tipos_e_colunas_do_recorte_oficial(
    monkeypatch, consulta, agregacao, as_polars
):
    if as_polars:
        pl = pytest.importorskip("polars")
    path = (
        GOLDEN.parent
        / "reconciliacao_mercados_credito_20260918/sicor/sicor_custeio_soja_2024_2025_MT.json"
    )
    payload = json.loads(path.read_bytes())
    servir(monkeypatch, payload)
    cheio = await consulta(
        "soja", safra="2024/25", uf="MT", agregacao=agregacao, as_polars=as_polars
    )
    assert len(cheio) > 0
    assert cheio["valor"].sum() == pytest.approx(sum(row["VlCusteio"] for row in payload["value"]))
    assert cheio["qtd_contratos"].sum() == sum(row["QtdCusteio"] for row in payload["value"])
    servir(monkeypatch, {"value": []})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        vazio, meta = await consulta(
            "soja",
            safra="2024/25",
            uf="MT",
            agregacao=agregacao,
            as_polars=as_polars,
            return_meta=True,
        )
    assert len(vazio) == 0 and meta.records_count == 0
    if as_polars:
        assert cheio.schema == vazio.schema
        assert pl.Null not in vazio.schema.values()
        return
    assert cheio.dtypes.equals(vazio.dtypes)
    contrato = "bcb_credito_rural_registro" if agregacao == "registro" else "credito_rural"
    assert vazio.dtypes.equals(contracts.get_contract(contrato).empty_frame().dtypes)


@pytest.mark.parametrize("agregacao", ["uf", "programa"])
@pytest.mark.parametrize("as_polars", [False, True])
async def test_total_vazio_preserva_tipos_do_total_publicado(monkeypatch, agregacao, as_polars):
    if as_polars:
        pl = pytest.importorskip("polars")
    path = sorted((GOLDEN / "sicor_regiao_uf_20260925").glob("RegiaoUF_*.gz"))[0]
    payload = json.loads(gzip.decompress(path.read_bytes()))
    servir(monkeypatch, payload)
    cheio = await bcb.credito_rural_total(
        agregacao=agregacao, finalidade="custeio", as_polars=as_polars
    )
    assert len(cheio) > 0
    assert cheio["valor"].sum() == pytest.approx(sum(row["VlCusteio"] for row in payload["value"]))
    servir(monkeypatch, {"value": []})
    vazio, meta = await bcb.credito_rural_total(
        agregacao=agregacao, as_polars=as_polars, return_meta=True
    )
    assert len(vazio) == 0 and meta.records_count == 0
    if as_polars:
        assert cheio.schema == vazio.schema
        assert pl.Null not in vazio.schema.values()
    else:
        assert cheio.dtypes.equals(vazio.dtypes)
