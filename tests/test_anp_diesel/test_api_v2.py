from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.alt.anp_diesel import api
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from tests.helpers import make_anp_precos_resource


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query",
    [
        {"as_polars": "yes"},
        {"return_meta": 1},
        {"nivel": []},
        {"agregacao": {}},
        {"produto": None},
        {"uf": True},
        {"uf": ""},
        {"municipio": []},
        {"municipio": " "},
        {"nivel": "brasil", "uf": "MT"},
        {"nivel": "uf", "municipio": "CUIABA"},
        {"inicio": datetime(2024, 1, 1)},
        {"inicio": True},
        {"inicio": "20240101"},
        {"inicio": "2024-02-30"},
        {"inicio": "0001-01-01"},
        {"inicio": "9999-01-01"},
    ],
)
async def test_argumentos_invalidos_antes_http(query):
    with (
        patch.object(api.client, "fetch_precos_resource", new_callable=AsyncMock) as fetch,
        pytest.raises(InvalidParameterError),
    ):
        await api.precos_diesel(**query)
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_polars_ausente_antes_http():
    with (
        patch.object(api.importlib, "import_module", side_effect=ImportError),
        patch.object(api.client, "fetch_precos_resource", new_callable=AsyncMock) as fetch,
        pytest.raises(ImportError, match="polars"),
    ):
        await api.precos_diesel(as_polars=True)
    fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch", [api.precos_diesel, datasets.precos_diesel])
async def test_agregado_invalido_recusado_antes_da_saida(fetch):
    resource = make_anp_precos_resource(nivel="brasil")
    aggregate = api.parser.agregar_mensal

    def invalid_monthly(frame):
        result = aggregate(frame)
        result["n_postos"] = pd.array([1] * len(result), dtype="Int64")
        return result

    with (
        patch.object(api.client, "fetch_precos_resource", return_value=resource),
        patch.object(api.parser, "agregar_mensal", side_effect=invalid_monthly),
        pytest.raises(ContractViolationError, match="somente"),
    ):
        await fetch(nivel="brasil", agregacao="mensal")


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch", [api.precos_diesel, datasets.precos_diesel])
@pytest.mark.parametrize("agregacao", ["semanal", "mensal"])
@pytest.mark.parametrize("inicio", ["2024-01-01", "2030-01-01"])
async def test_polars_preserva_schema_e_nulos(fetch, agregacao, inicio):
    pl = pytest.importorskip("polars")
    resource = make_anp_precos_resource(nivel="brasil")
    with patch.object(api.client, "fetch_precos_resource", return_value=resource):
        frame, meta = await fetch(
            nivel="brasil",
            agregacao=agregacao,
            inicio=inicio,
            as_polars=True,
            return_meta=True,
        )
    assert frame.schema["data"] == pl.Datetime("ns")
    assert frame.schema["n_postos"] == pl.Int64
    assert frame.schema["preco_compra"] == pl.Float64
    assert frame.schema["municipio"] == pl.Utf8
    assert frame["preco_compra"].null_count() == len(frame)
    assert meta.validation_passed
