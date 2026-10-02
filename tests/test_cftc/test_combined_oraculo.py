from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from agrobr import cftc, datasets
from agrobr.cftc import models
from agrobr.constants import URLS, Fonte
from agrobr.contracts.datasets import POSICIONAMENTO_FUNDOS_COLUNAS_V2
from tests.helpers import assert_replay_served, install_replay_http, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/cftc/cot_20260923"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
ONDE = (
    "cftc_contract_market_code in('005602') AND report_date_as_yyyy_mm_dd >= '2026-06-01T00:00:00.000'"
    " AND report_date_as_yyyy_mm_dd <= '2026-06-30T00:00:00.000'"
)


async def test_cot_combined_confere_o_relatorio_oficial(monkeypatch):
    caso = {
        "requests": [
            {
                "match": {
                    "path": URLS[Fonte.CFTC]["disaggregated_combined"],
                    "params": {
                        "$where": ONDE,
                        "$order": "report_date_as_yyyy_mm_dd,cftc_contract_market_code",
                    },
                    "skip": 0,
                },
                "file": "cftc_soja_202606_combined.json",
                "content_type": "application/json",
            }
        ]
    }
    visto = install_replay_http(monkeypatch, caso, GOLDEN)
    with sem_excecao():
        frame = await cftc.cot("soja", inicio="2026-06-01", fim="2026-06-30", combined=True)
        pelo_dataset = await datasets.posicionamento_fundos(
            "soja", inicio="2026-06-01", fim="2026-06-30", combinado=True
        )
    assert_replay_served(visto)
    assert isinstance(pelo_dataset, pd.DataFrame)
    pd.testing.assert_frame_equal(
        pelo_dataset, frame.rename(columns=POSICIONAMENTO_FUNDOS_COLUNAS_V2)
    )
    oficial = json.loads((GOLDEN / "cftc_soja_202606_combined.json").read_text(encoding="utf-8"))
    esperado = [
        {
            "data": registro["report_date_as_yyyy_mm_dd"][:10],
            "commodity": "soja",
            **{
                destino: registro[origem]
                if destino in ("contrato", "codigo_cftc")
                else int(float(registro[origem]))
                for origem, destino in models.COLUMN_MAP.items()
                if destino != "data"
            },
            "managed_money_net": int(float(registro["m_money_positions_long_all"]))
            - int(float(registro["m_money_positions_short_all"])),
        }
        for registro in oficial
    ]
    publicado = [
        {
            **{coluna: valor for coluna, valor in linha.items() if coluna != "data"},
            "data": linha["data"].date().isoformat(),
        }
        for linha in frame.to_dict("records")
    ]
    assert len(publicado) == len(esperado) == 5
    assert {
        coluna: str(frame[coluna].dtype) for coluna in models.POSITION_COLUMNS
    } == dict.fromkeys(models.POSITION_COLUMNS, "Int64")
    assert [{k: linha[k] for k in esperado[0]} for linha in publicado] == esperado


def test_codigos_cftc_apontam_os_mercados_oficiais():
    esperado = MANIFESTO["mercado_esperado"]
    for arquivo in (
        "cftc_nomes_disaggregated_futures_20260630.json",
        "cftc_nomes_disaggregated_combined_20260630.json",
    ):
        oficial = {
            registro["cftc_contract_market_code"]: registro["market_and_exchange_names"]
            for registro in json.loads((GOLDEN / arquivo).read_text(encoding="utf-8"))
        }
        assert set(oficial) == set(models.CFTC_CONTRACTS)
        assert {
            commodity: [oficial[c] for c in models.resolve_contract_codes(commodity)]
            for commodity in esperado
        } == {commodity: [mercado] for commodity, mercado in esperado.items()}
