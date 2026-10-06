from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from agrobr import nasa_power
from tests.helpers import assert_replay_served, install_replay_http, replay_signature, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/nasa_power"
R45 = GOLDEN / "reconciliacao_fontes_parametros_20260926"
COBERTURA = GOLDEN / "cobertura_mensal_20260925"


async def _consultar(monkeypatch, pasta: Path, corpo: str):
    manifesto = json.loads((pasta / "manifest.json").read_text(encoding="utf-8"))
    caminho, params, salto = replay_signature(manifesto["corpos"][corpo]["recibo"]["url"])
    pedido = {
        "match": {"path": caminho, "params": dict(params), "skip": salto},
        "file": manifesto["corpos"][corpo]["arquivo"],
        "content_type": "application/json",
    }
    visto = install_replay_http(monkeypatch, {"requests": [pedido]}, pasta)
    params = dict(params)
    with sem_excecao():
        frame, meta = await nasa_power.clima_ponto(
            float(params["latitude"]),
            float(params["longitude"]),
            date.fromisoformat(
                f"{params['start'][:4]}-{params['start'][4:6]}-{params['start'][6:]}"
            ),
            date.fromisoformat(f"{params['end'][:4]}-{params['end'][4:6]}-{params['end'][6:]}"),
            parameters=params["parameters"].split(","),
            return_meta=True,
        )
    assert_replay_served(visto)
    return frame, meta


async def test_janela_so_merra2_nao_avisa(monkeypatch):
    frame, meta = await _consultar(monkeypatch, R45, "merra2_jan2025")
    assert str(frame["data"].dtype) == "datetime64[ns]"
    assert meta.source_details["fontes"] == ["MERRA2"]
    assert meta.source_details["fontes_por_bloco"] == [
        {"inicio": "2025-01-01", "fim": "2025-01-31", "fontes": ["MERRA2"]}
    ]
    assert meta.source_details["periodos_baixa_latencia"] == []
    assert not any("baixa latência" in aviso for aviso in meta.validation_warnings)


@pytest.mark.parametrize(
    "corpo,inicio,fim,origem,exato",
    [
        ("geosit_set2026", "2026-09-01", "2026-09-22", "cabecalho", True),
        ("misto_30_dias", "2026-09-01", "2026-09-25", "cabecalho+regra_mensal_nasa", True),
        ("misto_3_meses", "2026-06-26", "2026-09-25", "cabecalho", False),
    ],
)
async def test_trecho_do_geosit_sai_no_detalhe_e_no_aviso(
    monkeypatch, corpo, inicio, fim, origem, exato
):
    _, meta = await _consultar(monkeypatch, R45, corpo)
    assert meta.source_details["periodos_baixa_latencia"] == [
        {"fonte": "GEOSIT", "inicio": inicio, "fim": fim, "origem": origem, "exato": exato}
    ]
    dias = f"dias de {date.fromisoformat(inicio):%d/%m/%Y} a {date.fromisoformat(fim):%d/%m/%Y}"
    avisos = [aviso for aviso in meta.validation_warnings if "GEOS-IT" in aviso]
    assert len(avisos) == 1
    aviso = avisos[0]
    assert aviso.startswith(dias)
    assert "a NASA recomenda parar 2 meses antes" in aviso
    assert ("não separa por dia" in aviso) is not exato


async def test_flashflux_da_radiacao_tambem_avisa(monkeypatch):
    _, meta = await _consultar(monkeypatch, COBERTURA, "current")
    assert meta.source_details["fontes"] == ["FLASHFLUX", "GEOSIT", "POWER"]
    periodos = {
        periodo["fonte"]: periodo for periodo in meta.source_details["periodos_baixa_latencia"]
    }
    assert periodos["FLASHFLUX"] == {
        "fonte": "FLASHFLUX",
        "inicio": "2026-09-01",
        "fim": "2026-09-10",
        "origem": "cabecalho",
        "exato": True,
    }
    avisos = [aviso for aviso in meta.validation_warnings if "FLASHFlux" in aviso]
    assert len(avisos) == 1
    aviso = avisos[0]
    assert aviso.startswith("dias de 01/09/2026 a 10/09/2026 vêm do FLASHFlux")
    assert "substitui pelo SYN1deg" in aviso
