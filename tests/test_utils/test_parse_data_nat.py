from __future__ import annotations

import warnings

import pandas as pd
import pytest

from agrobr import b3, bcb, cftc, datasets, desmatamento, mapbiomas_alerta
from agrobr.alt import antt_pedagio
from agrobr.exceptions import InvalidParameterError

NAT = pd.NaT

CHAMADAS = {
    "b3.ajustes": lambda: b3.ajustes(data=NAT),
    "b3.historico inicio": lambda: b3.historico(contrato="milho", inicio=NAT, fim="2024-01-05"),
    "b3.historico fim": lambda: b3.historico(contrato="milho", inicio="2024-01-02", fim=NAT),
    "b3.posicoes_abertas": lambda: b3.posicoes_abertas(data=NAT),
    "b3.posicoes_abertas_historico": lambda: b3.posicoes_abertas_historico(
        contrato="milho", inicio=NAT, fim="2024-01-05"
    ),
    "cftc.cot inicio": lambda: cftc.cot("soja", inicio=NAT),
    "cftc.cot fim": lambda: cftc.cot("soja", inicio="2024-01-02", fim=NAT),
    "bcb.focus": lambda: bcb.focus("IPCA", inicio=NAT),
    "bcb.ptax data": lambda: bcb.ptax(data=NAT),
    "bcb.ptax inicio": lambda: bcb.ptax(inicio=NAT, fim="2024-01-05"),
    "bcb.sgs inicio": lambda: bcb.sgs(432, inicio=NAT, fim="2024-01-05"),
    "bcb.sgs fim": lambda: bcb.sgs(432, inicio="2024-01-02", fim=NAT),
    "futuros_agricolas data": lambda: datasets.futuros_agricolas("milho", data=NAT),
    "futuros_agricolas historico": lambda: datasets.futuros_agricolas(
        "milho", tipo="historico", inicio=NAT, fim="2024-01-05"
    ),
    "posicionamento_fundos": lambda: datasets.posicionamento_fundos("soja", inicio=NAT),
    "mapbiomas_alerta.alertas": lambda: mapbiomas_alerta.alertas(inicio=NAT, fim="2024-01-31"),
    "desmatamento.deter inicio": lambda: desmatamento.deter(inicio=NAT, fim="2024-01-31"),
    "desmatamento.deter fim": lambda: desmatamento.deter(inicio="2024-01-01", fim=NAT),
    "antt_pedagio.fluxo_pedagio": lambda: antt_pedagio.fluxo_pedagio(ano=2024, inicio=NAT),
}


@pytest.mark.parametrize("chamada", CHAMADAS.values(), ids=CHAMADAS.keys())
async def test_nat_recusado_antes_da_rede(chamada):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with pytest.raises(InvalidParameterError, match=r"(inicio|fim|data) é NaT"):
            await chamada()
