from __future__ import annotations

import re
import warnings

import httpx
import pytest

from agrobr.alt.antt_pedagio import api
from agrobr.exceptions import ParseError, SourceUnavailableError
from tests.helpers import install_anttpedagio_http, install_anttpedagio_source

TRAFEGO = (
    b"concessionaria;praca;mes_ano;categoria;tipo_de_veiculo;tipo_cobranca;sentido;volume_total\n"
    b"Concessionaria;P1;01/01/2023;Categoria 4;Comercial;N/I;Crescente;423,00\n"
)
PRACAS = (
    b"concessionaria;praca_de_pedagio;rodovia;uf;municipio\nConcessionaria;P1;BR-163;MT;Municipio\n"
)


async def test_fluxo_cadastro_indisponivel_preserva_trafego_e_recibo(monkeypatch):
    calls, bodies = install_anttpedagio_source(
        monkeypatch,
        {"volume-2023.csv": TRAFEGO},
        plazas=PRACAS,
        faults={"https://dados.antt.gov.br/pracas.csv": 404},
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            frame, meta = await api.fluxo_pedagio(ano=2023, return_meta=True)
        except Exception as exc:
            frame = exc
    assert not isinstance(frame, Exception), frame
    assert any(re.search("Cadastro ANTT indisponível", str(item.message)) for item in caught), (
        caught
    )
    assert frame["volume"].tolist() == [423]
    assert frame[["rodovia", "uf", "municipio"]].isna().all().all()
    assert len(calls) == 4
    failed = meta.source_details["acquisitions"][0]
    assert failed["role"] == "pracas_failed"
    assert failed["acquisition"]["complete"] is False
    assert failed["acquisition"]["files"] == []
    last = failed["acquisition"]["attempts"][-1]
    assert last["status"] == 404 and last["closed"]
    assert last["error_type"] == "SourceUnavailableError"
    assert meta.source_details["received_bytes"] == sum(
        len(bodies[str(request.url)])
        if str(request.url) != "https://dados.antt.gov.br/pracas.csv"
        else len(b"failure")
        for request in calls
    )
    assert meta.source_details["data_file_bytes"] == len(TRAFEGO)
    assert meta.validation_warnings
    assert meta.source_details["enrichment"]["status"] == "unavailable"


async def test_fluxo_filtro_geografico_nao_degrada_sem_cadastro(monkeypatch):
    calls, _ = install_anttpedagio_source(
        monkeypatch,
        {"volume-2023.csv": TRAFEGO},
        plazas=PRACAS,
        faults={"https://dados.antt.gov.br/pracas.csv": 404},
    )
    try:
        await api.fluxo_pedagio(ano=2023, uf="MT")
    except Exception as exc:
        caught = exc
    else:
        caught = None
    assert isinstance(caught, SourceUnavailableError) and "HTTP 404" in str(caught), caught
    assert len(calls) == 2
    assert all("volume-2023.csv" not in str(request.url) for request in calls)


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b'{"success": "true", "result": {}}',
        b'{"success": true, "result": {"id": "wrong", "name": "wrong", "resources": []}}',
    ],
)
async def test_fluxo_catalogo_cadastro_invalido_nao_degrada(monkeypatch, body):
    calls = install_anttpedagio_http(
        monkeypatch, lambda _request: httpx.Response(200, content=body)
    )
    try:
        await api.fluxo_pedagio(ano=2026)
    except Exception as exc:
        caught = exc
    else:
        caught = None
    assert isinstance(caught, ParseError), caught
    assert len(calls) == 1
    assert calls[0].url.params["id"] == "praca-de-pedagio"
    acquired = caught.antt_acquisition
    assert acquired["complete"] is False and acquired["files"] == []
    attempt = acquired["attempts"][0]
    assert attempt["status"] == 200 and attempt["complete_body"] and attempt["closed"]
    assert caught.antt_completed_sources == []
