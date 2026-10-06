from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr import nasa_power
from agrobr.nasa_power import client, models
from tests.helpers import assert_replay_served, install_replay_http, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data"
R10 = GOLDEN / "reconciliacao_clima_20260918/nasa_power/nasa_power_mt_2025.json"
EXTRAS = GOLDEN / "nasa_power/parametros_20260924/mt_202501_extras.json"
CODIGOS = ["GWETROOT", "GWETTOP", "PS", "T2MDEW", "WS10M"]


def test_unidades_do_catalogo_conferem_a_nasa_power():
    oficial = {
        **json.loads(R10.read_text(encoding="utf-8"))["parameters"],
        **json.loads(EXTRAS.read_text(encoding="utf-8"))["parameters"],
    }
    publicado = nasa_power.parametros()
    assert sorted(publicado["codigo"]) == sorted(oficial)
    assert {linha.codigo: linha.unidade for linha in publicado.itertuples()} == {
        codigo: dados["units"] for codigo, dados in oficial.items()
    }


async def test_parametros_fora_do_padrao_conferem_o_corpo_oficial(monkeypatch):
    caso = {
        "requests": [
            {
                "match": {
                    "path": client.BASE_URL,
                    "params": {
                        "parameters": ",".join(CODIGOS),
                        "community": "AG",
                        "longitude": "-56.1",
                        "latitude": "-12.6",
                        "start": "20250101",
                        "end": "20250131",
                        "format": "JSON",
                        "time-standard": "LST",
                    },
                    "skip": 0,
                },
                "file": "nasa_power/parametros_20260924/mt_202501_extras.json",
                "content_type": "application/json",
            }
        ]
    }
    visto = install_replay_http(monkeypatch, caso, GOLDEN)
    periodo = (-12.6, -56.1, "2025-01-01", "2025-01-31")
    with sem_excecao():
        diario = await nasa_power.clima_ponto(*periodo, parameters=CODIGOS)
        mensal = await nasa_power.clima_ponto(*periodo, parameters=CODIGOS, agregacao="mensal")
    assert_replay_served(visto)
    oficial = json.loads(EXTRAS.read_text(encoding="utf-8"))["properties"]["parameter"]
    diarias = {item.codigo: item.coluna for item in models.CATALOGO}
    mensais = {item.codigo: item.coluna_mensal for item in models.CATALOGO}
    assert len(diario) == 31
    assert mensal["mes"].tolist() == [pd.Timestamp("2025-01-01")]
    for codigo in CODIGOS:
        publicado = dict(
            zip(diario["data"].dt.strftime("%Y%m%d"), diario[diarias[codigo]], strict=True)
        )
        esperado = {
            dia: (None if valor == -999 else valor) for dia, valor in oficial[codigo].items()
        }
        assert {
            dia: (None if valor != valor else valor) for dia, valor in publicado.items()
        } == esperado, codigo
        validos = [valor for valor in oficial[codigo].values() if valor != -999]
        assert mensal[mensais[codigo]].iloc[0] == pytest.approx(sum(validos) / len(validos)), codigo
