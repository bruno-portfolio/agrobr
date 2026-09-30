from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from agrobr import cftc, datasets
from agrobr.cftc import client, parser
from agrobr.cftc.models import resolve_contract_codes
from agrobr.constants import URLS, Fonte
from tests.helpers import conferir_corpo, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/cftc/spreads_20260925"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
COMPRADAS = ["producer_long", "swap_long", "managed_money_long", "other_long", "nonreportable_long"]
VENDIDAS = [
    "producer_short",
    "swap_short",
    "managed_money_short",
    "other_short",
    "nonreportable_short",
]
SPREADS = ["swap_spread", "managed_money_spread", "other_spread"]


@pytest.mark.parametrize("corpo", sorted(MANIFESTO["corpos"]))
def test_spreads_de_swap_e_other_saem_do_socrata_e_fecham_o_open_interest(corpo):
    oficial = json.loads((GOLDEN / f"{corpo}.json").read_text(encoding="utf-8"))
    with sem_excecao():
        frame = parser.parse_cot(oficial)
    assert {"swap_spread", "other_spread"} <= set(frame.columns)
    publicados = [
        [int(registro["swap__positions_spread_all"]), int(registro["other_rept_positions_spread"])]
        for registro in sorted(oficial, key=lambda registro: registro["report_date_as_yyyy_mm_dd"])
    ]
    assert frame[["swap_spread", "other_spread"]].values.tolist() == publicados
    folga = 1 if MANIFESTO["corpos"][corpo]["combined"] else 0
    spreads = frame[SPREADS].sum(axis=1)
    assert (frame["open_interest"] - frame[COMPRADAS].sum(axis=1) - spreads).abs().max() <= folga
    assert (frame["open_interest"] - frame[VENDIDAS].sum(axis=1) - spreads).abs().max() <= folga


async def test_cot_traz_a_consulta_com_os_filtros_e_o_corpo_da_resposta(monkeypatch):
    conteudo = (GOLDEN / MANIFESTO["corpos"]["arroz_recent"]["arquivo"]).read_bytes()
    real = httpx.AsyncClient
    pedidos: list[httpx.URL] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(request.url)
        return httpx.Response(200, content=conteudo, request=request)

    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: real(transport=httpx.MockTransport(responder), **kwargs),
    )
    with sem_excecao():
        _, meta = await cftc.cot("arroz", inicio="2026-09-15", fim="2026-09-15", return_meta=True)

    conferir_corpo(meta, conteudo)
    assert meta.source_url == str(pedidos[0])
    consulta = httpx.URL(meta.source_url)
    assert str(consulta.copy_with(query=None)) == URLS[Fonte.CFTC]["disaggregated_futures"]
    filtro = consulta.params["$where"]
    assert all(f"'{codigo}'" in filtro for codigo in resolve_contract_codes("arroz"))
    assert "report_date_as_yyyy_mm_dd >= '2026-09-15T00:00:00.000'" in filtro
    assert "report_date_as_yyyy_mm_dd <= '2026-09-15T00:00:00.000'" in filtro


async def test_cot_e_dataset_declaram_o_contrato_com_os_spreads(monkeypatch):
    corpo = MANIFESTO["corpos"]["soja_recent"]
    oficial = json.loads((GOLDEN / corpo["arquivo"]).read_text(encoding="utf-8"))
    monkeypatch.setattr(
        client,
        "fetch_cot",
        AsyncMock(
            return_value=(oficial, corpo["requested_url"], (GOLDEN / corpo["arquivo"]).read_bytes())
        ),
    )
    with sem_excecao():
        frame, meta = await cftc.cot("soja", return_meta=True)
        dataset, meta_dataset = await datasets.posicionamento_fundos("soja", return_meta=True)
    assert {"swap_spread", "other_spread"} <= set(frame.columns)
    assert {"swap_spread", "outros_spread"} <= set(dataset.columns)
    assert meta.schema_version == "1.1"
    assert meta_dataset.contract_version == "2.0"
