from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from agrobr import nasa_power
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/nasa_power/reconciliacao_fontes_parametros_20260926"
)


async def test_cobertura_por_parametro_exclui_os_dias_sem_medicao(monkeypatch):
    manifesto = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
    corpo = manifesto["corpos"]["misto_30_dias"]
    caminho, params, salto = helpers.replay_signature(corpo["recibo"]["url"])
    pedido = {
        "match": {"path": caminho, "params": dict(params), "skip": salto},
        "file": corpo["arquivo"],
        "content_type": "application/json",
    }
    visto = helpers.install_replay_http(monkeypatch, {"requests": [pedido]}, GOLDEN)
    with helpers.sem_excecao():
        frame, meta = await nasa_power.clima_ponto(
            -15.6,
            -47.71,
            date(2026, 8, 27),
            date(2026, 9, 25),
            parameters=["T2M", "PRECTOTCORR"],
            return_meta=True,
        )
    helpers.assert_replay_served(visto)
    assert meta.source_details["observed_coverage"] == {
        codigo: {
            "returned_days": 30,
            "valid_days": 27,
            "first_valid_day": "20260827",
            "last_valid_day": "20260922",
        }
        for codigo in ("T2M", "PRECTOTCORR")
    }
    assert frame["data"].dt.strftime("%Y%m%d").tolist()[-4:] == [
        "20260922",
        "20260923",
        "20260924",
        "20260925",
    ]
    ultimo_valido = frame.loc[frame["data"].dt.strftime("%Y%m%d").eq("20260922")].iloc[0]
    assert ultimo_valido["temp_media"] == pytest.approx(29.23)
    assert ultimo_valido["precip_mm"] == 0.0
    assert frame.tail(3)[["temp_media", "precip_mm"]].isna().all().all()
