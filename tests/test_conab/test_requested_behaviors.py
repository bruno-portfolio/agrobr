from __future__ import annotations

import json
import sys
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import httpx
import pandas as pd
import pytest

from agrobr.conab import api
from agrobr.conab._custo_producao import _acquisition, _sociobio_api
from agrobr.conab._custo_producao import api as cost_api
from agrobr.conab._serie_historica import api as history_api
from agrobr.conab._serie_historica import client as history_client
from agrobr.exceptions import InvalidParameterError
from tests import helpers

R4_MANIFEST = json.loads(
    (
        Path(helpers.__file__).parent / "golden_data/reconciliacao_r4_20260918/manifest.json"
    ).read_text(encoding="utf-8")
)


@pytest.fixture
def transient_http(monkeypatch: pytest.MonkeyPatch, failure: str) -> list[str]:
    attempts: list[str] = []
    factory = httpx.AsyncClient

    def respond(request: httpx.Request) -> httpx.Response:
        status = failure if not attempts else "200"
        attempts.append(status)
        if status == "timeout":
            raise httpx.ReadTimeout("interrupção transitória", request=request)
        return httpx.Response(int(status), content=b"recovered", request=request)

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(respond), **kwargs),
    )
    return attempts


@pytest.mark.parametrize("failure", ["503", "timeout"])
async def test_serie_historica_recupera_resposta_transitoria(
    transient_http: list[str], failure: str
):
    with (
        helpers.isolated_dataset_case(("serie_retry", failure)),
        helpers.collect_failures() as check,
    ):
        with check("resposta"):
            body, meta = await history_client.download_xls("soja")
            assert body.getvalue() == b"recovered"
            assert meta["produto"] == "soja"
            assert meta["size_bytes"] == len(b"recovered")
        with check("tentativas"):
            assert transient_http == [failure, "200"]


@pytest.mark.parametrize("failure", ["503", "timeout"])
async def test_aquisicao_custos_recupera_resposta_transitoria(
    transient_http: list[str], failure: str
):
    with (
        helpers.isolated_dataset_case(("custos_retry", failure)),
        helpers.collect_failures() as check,
    ):
        acquired = _acquisition.Acquisition()
        with check("resposta"):
            assert await acquired.get(_acquisition.CATALOG_URL, "catalog") == b"recovered"
        with check("tentativas"):
            assert transient_http == [failure, "200"]
            assert len(acquired.receipts) == 2
            assert acquired.receipts[-1]["status"] == 200
            assert acquired.receipts[-1]["closed"] and acquired.receipts[-1]["eof"]


@pytest.fixture
def requested_conversion(monkeypatch: pytest.MonkeyPatch) -> object:
    converted = object()
    monkeypatch.setitem(
        sys.modules,
        "polars",
        SimpleNamespace(from_pandas=lambda _frame, **_kwargs: converted),
    )

    def finalize(frame: pd.DataFrame, meta: Any, as_polars: bool, return_meta: bool) -> Any:
        output = converted if as_polars else frame
        return (output, meta) if return_meta else output

    monkeypatch.setattr(cost_api, "finalize_output", finalize)
    monkeypatch.setattr(_sociobio_api, "finalize_output", finalize)
    return converted


@pytest.mark.parametrize("endpoint", ["safras", "balanco", "brasil_total"])
async def test_fontes_encaminham_conversao_solicitada(requested_conversion: object, endpoint: str):
    with helpers.collect_failures() as check:
        for return_meta in (False, True):
            with (
                check(return_meta),
                helpers.isolated_dataset_case((endpoint, return_meta)) as monkeypatch,
            ):
                parser = mock.MagicMock(version=3)
                parser.parse_safra_produto.return_value = []
                parser.parse_suprimento.return_value = []
                parser.parse_brasil_total.return_value = []
                monkeypatch.setattr(api, "ConabParserV1", mock.Mock(return_value=parser))
                monkeypatch.setattr(
                    api.client,
                    "fetch_safra_xlsx",
                    mock.AsyncMock(return_value=(BytesIO(b"data"), {"safra": "2025/26"})),
                )
                arguments = () if endpoint == "brasil_total" else ("soja",)
                result = await getattr(api, endpoint)(
                    *arguments, as_polars=True, return_meta=return_meta
                )
                output = result[0] if return_meta else result
                assert output is requested_conversion
                if return_meta:
                    assert result[1].selected_source == "conab"


async def test_serie_historica_encaminha_conversao_solicitada(requested_conversion: object):
    with helpers.collect_failures() as check:
        for return_meta in (False, True):
            with (
                check(return_meta),
                helpers.isolated_dataset_case(("serie_polars", return_meta)) as monkeypatch,
            ):
                monkeypatch.setattr(
                    history_client,
                    "download_xls",
                    mock.AsyncMock(return_value=(BytesIO(b"data"), {})),
                )
                monkeypatch.setattr(history_api, "parse_serie_historica", lambda **_kwargs: [])
                result = await history_api.serie_historica(
                    "soja", as_polars=True, return_meta=return_meta
                )
                output = result[0] if return_meta else result
                assert output is requested_conversion
                if return_meta:
                    assert result[1].selected_source == "conab_serie_historica"


@pytest.mark.parametrize("endpoint", ["custo_producao", "catalogo_custos"])
async def test_custos_encaminham_conversao_solicitada(requested_conversion: object, endpoint: str):
    case = next(item for item in R4_MANIFEST["cases"] if item["id"] == "milho_8608f61a51d3")
    selection = dict(case["selection"])
    product = selection.pop("produto")
    with helpers.collect_failures() as check:
        for return_meta in (False, True):
            with (
                check(return_meta),
                helpers.isolated_dataset_case((endpoint, return_meta)) as monkeypatch,
            ):
                _acquisition.clear()
                helpers.install_reconciliacao_r4_http(monkeypatch)
                result = await getattr(cost_api, endpoint)(
                    product,
                    **(selection if endpoint == "custo_producao" else {}),
                    as_polars=True,
                    return_meta=return_meta,
                    use_cache=False,
                )
                output = result[0] if return_meta else result
                assert output is requested_conversion


@pytest.mark.parametrize("endpoint", ["custo_sociobiodiversidade", "catalogo_sociobiodiversidade"])
async def test_sociobio_encaminha_conversao_solicitada(requested_conversion: object, endpoint: str):
    case = next(item for item in R4_MANIFEST["cases"] if item["id"] == "murumuru_48eead0b3c5d")
    with helpers.collect_failures() as check:
        for return_meta in (False, True):
            with (
                check(return_meta),
                helpers.isolated_dataset_case((endpoint, return_meta)) as monkeypatch,
            ):
                _acquisition.clear()
                helpers.install_reconciliacao_r4_http(monkeypatch)
                result = await getattr(_sociobio_api, endpoint)(
                    **(case["selection"] if endpoint == "custo_sociobiodiversidade" else {}),
                    as_polars=True,
                    return_meta=return_meta,
                    use_cache=False,
                )
                output = result[0] if return_meta else result
                assert output is requested_conversion


async def test_safras_recusa_produto_desconhecido_antes_da_rede(monkeypatch: pytest.MonkeyPatch):
    visto = helpers.install_replay_http(
        monkeypatch, {"requests": []}, Path(helpers.__file__).parent
    )
    with helpers.levanta_exatamente(
        InvalidParameterError, match="Produto CONAB 'cafe' inválido"
    ) as erro:
        await api.safras("cafe", safra="2025/26")
    assert visto == {"served": [], "unmatched": []}
    assert "'soja'" in str(erro.value) and "'gergelim'" in str(erro.value)


async def test_balanco_recusa_produto_desconhecido_antes_da_rede(monkeypatch: pytest.MonkeyPatch):
    visto = helpers.install_replay_http(
        monkeypatch, {"requests": []}, Path(helpers.__file__).parent
    )
    with helpers.levanta_exatamente(
        InvalidParameterError, match="Produto do balanço CONAB 'cafe' inválido"
    ) as erro:
        await api.balanco("cafe", safra="2025/26")
    assert visto == {"served": [], "unmatched": []}
    assert str(erro.value).endswith(
        "Válidos: ['soja', 'milho', 'arroz', 'feijao', 'trigo', 'algodao']"
    )
