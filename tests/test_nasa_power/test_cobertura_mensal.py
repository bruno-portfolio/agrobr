from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import nasa_power
from tests.helpers import assert_replay_served, install_replay_http, replay_signature, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/nasa_power/cobertura_mensal_20260925"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
COBERTURA = {"dias", "data_inicio", "data_fim"}


def _servir(monkeypatch, corpo: str, pasta: Path = GOLDEN) -> tuple[dict, dict[str, str]]:
    caminho, params, salto = replay_signature(MANIFESTO["corpos"][corpo]["recibo"]["url"])
    pedido = {
        "match": {"path": caminho, "params": dict(params), "skip": salto},
        "file": MANIFESTO["corpos"][corpo]["arquivo"],
        "content_type": "application/json",
    }
    return install_replay_http(monkeypatch, {"requests": [pedido]}, pasta), dict(params)


def _data(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


async def _mensal(params: dict[str, str]) -> tuple[pd.DataFrame, object]:
    with sem_excecao():
        return await nasa_power.clima_ponto(
            float(params["latitude"]),
            float(params["longitude"]),
            _data(params["start"]),
            _data(params["end"]),
            agregacao="mensal",
            return_meta=True,
        )


@pytest.mark.parametrize("corpo", ["partial", "complete", "current"])
async def test_mensal_traz_os_dias_e_o_intervalo_cobertos_em_cada_mes(monkeypatch, corpo):
    visto, params = _servir(monkeypatch, corpo)
    frame, meta = await _mensal(params)
    assert_replay_served(visto)
    assert meta.schema_version == "1.2"
    assert set(frame.columns) >= COBERTURA
    esperado = MANIFESTO["corpos"][corpo]["meses"]
    assert frame["mes"].dt.strftime("%Y%m").tolist() == sorted(esperado)
    for linha in frame.itertuples():
        mes = esperado[linha.mes.strftime("%Y%m")]
        assert linha.dias == mes["dias"]
        assert linha.data_inicio == pd.Timestamp(mes["data_inicio"])
        assert linha.data_fim == pd.Timestamp(mes["data_fim"])
        assert linha.precip_acum_mm == pytest.approx(mes["precip_acum_mm"], abs=1e-9)


async def test_dia_sem_nenhum_valor_valido_fica_fora_da_cobertura(monkeypatch, tmp_path):
    oficial = json.loads(
        (GOLDEN / MANIFESTO["corpos"]["partial"]["arquivo"]).read_text(encoding="utf-8")
    )
    preenchimento = oficial["header"]["fill_value"]
    chuva_do_dia = oficial["properties"]["parameter"]["PRECTOTCORR"]["20250131"]
    for valores in oficial["properties"]["parameter"].values():
        valores["20250131"] = preenchimento
    (tmp_path / MANIFESTO["corpos"]["partial"]["arquivo"]).write_text(
        json.dumps(oficial), encoding="utf-8"
    )
    visto, params = _servir(monkeypatch, "partial", tmp_path)
    frame, _meta = await _mensal(params)
    assert_replay_served(visto)
    janeiro = frame.loc[frame["mes"].eq(pd.Timestamp("2025-01-01"))].iloc[0]
    esperado = MANIFESTO["corpos"]["partial"]["meses"]["202501"]
    assert janeiro["dias"] == esperado["dias"] - 1
    assert janeiro["data_inicio"] == pd.Timestamp(esperado["data_inicio"])
    assert janeiro["data_fim"] == pd.Timestamp("2025-01-30")
    assert janeiro["precip_acum_mm"] == pytest.approx(
        esperado["precip_acum_mm"] - chuva_do_dia, abs=1e-9
    )


def _fevereiro_sem_valor() -> dict:
    oficial = json.loads(
        (GOLDEN / MANIFESTO["corpos"]["partial"]["arquivo"]).read_text(encoding="utf-8")
    )
    for valores in oficial["properties"]["parameter"].values():
        for dia in valores:
            if dia.startswith("202502"):
                valores[dia] = oficial["header"]["fill_value"]
    return oficial


@pytest.mark.parametrize("consulta", ["ponto", "uf"])
async def test_mensal_em_polars_mantem_datas_e_dias_inteiros(monkeypatch, consulta):
    polars = pytest.importorskip("polars")
    monkeypatch.setattr(
        "agrobr.nasa_power.client.fetch_daily", AsyncMock(return_value=_fevereiro_sem_valor())
    )

    async def consultar(**kwargs):
        if consulta == "uf":
            return await nasa_power.clima_uf("DF", 2025, **kwargs)
        return await nasa_power.clima_ponto(
            -15.79, -47.88, "2025-01-15", "2025-02-05", agregacao="mensal", **kwargs
        )

    with sem_excecao():
        esperado = await consultar()
        frame = await consultar(as_polars=True)
    assert frame.columns == list(esperado.columns)
    for coluna in ("mes", "data_inicio", "data_fim"):
        assert frame.schema[coluna] == polars.Datetime("ns")
        assert frame[coluna].to_list() == [
            None if pd.isna(valor) else valor.to_pydatetime() for valor in esperado[coluna]
        ]
    assert frame.schema["dias"] == polars.Int64
    assert frame["dias"].to_list() == esperado["dias"].tolist() == [17, 0]
    assert ("uf" in frame.columns) == (consulta == "uf")
    if consulta == "uf":
        assert frame.schema["uf"] == polars.Utf8
    assert frame.schema["precip_acum_mm"] == polars.Float64
