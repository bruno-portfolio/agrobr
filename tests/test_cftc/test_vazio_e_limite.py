from __future__ import annotations

import json
import warnings
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import cftc, datasets
from agrobr.cftc import client
from tests.helpers import conferir_corpo, make_mock_async_client, make_mock_response

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/cftc/spreads_20260925"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
CORPO = (GOLDEN / MANIFESTO["corpos"]["soja_recent"]["arquivo"]).read_bytes()
CONSULTA = MANIFESTO["corpos"]["soja_recent"]["requested_url"]


def _resposta(corpo):
    return make_mock_response(200, content=corpo, json_data=json.loads(corpo), url=CONSULTA)


def _instalar_resposta(monkeypatch):
    transporte = make_mock_async_client()
    transporte.get = AsyncMock(return_value=_resposta(CORPO))
    monkeypatch.setattr(client.httpx, "AsyncClient", lambda **_kwargs: transporte)
    return transporte


@pytest.mark.parametrize("dataset", [False, True])
@pytest.mark.parametrize("as_polars", [False, True])
async def test_cot_vazio_e_cheio_preservam_schema_e_valores_publicados(
    monkeypatch, dataset, as_polars
):
    if as_polars:
        pl = pytest.importorskip("polars")
    transporte = _instalar_resposta(monkeypatch)
    consultar = datasets.posicionamento_fundos if dataset else cftc.cot
    cheio, meta = await consultar("soja", return_meta=True, as_polars=as_polars)
    abertas = "posicoes_abertas" if dataset else "open_interest"
    saldo = "fundos_saldo" if dataset else "managed_money_net"
    outros = "outros_spread" if dataset else "other_spread"
    produto = "produto" if dataset else "commodity"
    assert cheio[abertas][0] == 1027541
    assert cheio[saldo][0] == 234920
    assert cheio["swap_spread"][0] == 8355
    assert cheio[outros][0] == 70779
    assert str(cheio["data"][0])[:10] == "2026-09-01"
    assert cheio["codigo_cftc"][0] == "005602"
    assert cheio[produto][0] == "soja"
    conferir_corpo(meta, CORPO)
    contagens = set(cheio.columns) - {"data", produto, "contrato", "codigo_cftc"}
    assert len(contagens) == 18
    if as_polars:
        assert all(cheio.schema[coluna] == pl.Int64 for coluna in contagens)
        assert cheio.schema["data"] == pl.Datetime("ns")
    else:
        assert all(str(cheio[coluna].dtype) == "Int64" for coluna in contagens)
        assert str(cheio["data"].dtype) == "datetime64[ns]"
        assert cheio[produto].dtype == pd.Series(["soja"]).dtype

    transporte.get.return_value = _resposta(b"[]")
    vazio, meta_vazio = await consultar("soja", return_meta=True, as_polars=as_polars)
    assert len(vazio) == 0
    assert list(vazio.columns) == list(cheio.columns)
    if as_polars:
        assert vazio.schema == cheio.schema
    else:
        assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert meta_vazio.records_count == 0
    assert meta_vazio.validation_warnings == []
    conferir_corpo(meta_vazio, b"[]")


@pytest.mark.parametrize("dataset", [False, True])
@pytest.mark.parametrize("return_meta", [False, True])
async def test_cot_no_teto_avisa_incerteza_e_preserva_proveniencia(
    monkeypatch, dataset, return_meta
):
    _instalar_resposta(monkeypatch)
    monkeypatch.setattr(client, "MAX_ROWS", 3)
    consultar = datasets.posicionamento_fundos if dataset else cftc.cot
    with pytest.warns(UserWarning, match="limite de 3") as avisos:
        resultado = await consultar("soja", return_meta=return_meta)
    assert len(avisos) == 1
    if return_meta:
        quadro, meta = resultado
        assert meta.validation_warnings == [str(avisos[0].message)]
        assert meta.source_details["row_limit"] == 3
        assert meta.source_details["completeness"] == "unknown"
        assert "expected_count" not in meta.source_details
        conferir_corpo(meta, CORPO)
    else:
        quadro = resultado
    assert len(quadro) == 3
    assert quadro["posicoes_abertas" if dataset else "open_interest"][0] == 1027541


async def test_cot_abaixo_do_teto_nao_avisa(monkeypatch):
    _instalar_resposta(monkeypatch)
    monkeypatch.setattr(client, "MAX_ROWS", 4)
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        _, meta = await cftc.cot("soja", return_meta=True)
    assert not avisos
    assert meta.validation_warnings == []
