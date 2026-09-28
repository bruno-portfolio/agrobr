from __future__ import annotations

import json
import warnings
from pathlib import Path

import pandas as pd
import pytest

from agrobr import datasets, inmet
from agrobr.inmet import client
from tests.helpers import assert_replay_served, install_replay_http

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/inmet/chuva_mes_completo_20260925"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
COBERTURA = {"estacoes_chuva", "estacoes_chuva_parciais", "dias", "data_inicio", "data_fim"}


def _servir(monkeypatch) -> dict:
    monkeypatch.setattr(client, "_historico_zip_cache", None)
    corpo = MANIFESTO["corpo"]
    pedido = {
        "match": {"path": corpo["url"], "params": {}, "skip": 0},
        "file": corpo["arquivo"],
        "content_type": "application/zip",
    }
    return install_replay_http(monkeypatch, {"requests": [pedido]}, GOLDEN)


def _meses(uf: str) -> dict[str, dict]:
    return {grupo["mes"]: grupo for grupo in MANIFESTO["meses"].values() if grupo["uf"] == uf}


def _aviso(uf: str) -> str:
    meses = [
        f"{uf} {mes}"
        for mes, grupo in _meses(uf).items()
        if not grupo["completas"] and grupo["parciais"]
    ]
    return f"Chuva mensal nula em {', '.join(meses)}: nenhuma estação com o mês completo."


def _avisos_de_chuva(avisos: list[warnings.WarningMessage]) -> list[str]:
    return [str(aviso.message) for aviso in avisos if "Chuva mensal" in str(aviso.message)]


@pytest.mark.parametrize("uf", ["DF", "GO", "MS", "RS"])
async def test_chuva_mensal_da_uf_usa_so_estacoes_com_o_mes_completo(monkeypatch, uf):
    visto = _servir(monkeypatch)
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame, meta = await inmet.historico_uf(uf, 2001, return_meta=True)
    assert_replay_served(visto)
    assert set(frame.columns) >= COBERTURA
    esperado = _meses(uf)
    assert frame["mes"].dt.strftime("%Y-%m").tolist() == sorted(esperado)
    for linha in frame.itertuples():
        grupo = esperado[linha.mes.strftime("%Y-%m")]
        assert linha.num_estacoes == len(grupo["estacoes"])
        assert linha.estacoes_chuva == len(grupo["completas"])
        assert linha.estacoes_chuva_parciais == len(grupo["parciais"])
        if grupo["precip_acum_mm"] is None:
            assert pd.isna(linha.precip_acum_mm)
        else:
            assert linha.precip_acum_mm == pytest.approx(grupo["precip_acum_mm"], abs=1e-6)
        assert linha.dias == grupo["cobertura"]["dias"]
        assert linha.data_inicio == pd.Timestamp(grupo["cobertura"]["data_inicio"])
        assert linha.data_fim == pd.Timestamp(grupo["cobertura"]["data_fim"])
    mensagens = _avisos_de_chuva(avisos)
    assert len(mensagens) == 1
    assert mensagens[0].startswith(_aviso(uf))
    assert mensagens[0] in meta.validation_warnings


async def test_clima_da_uf_repassa_a_contagem_das_estacoes_e_o_aviso(monkeypatch):
    visto = _servir(monkeypatch)
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame, meta = await datasets.clima("RS", 2001, fonte="inmet_historico", return_meta=True)
    assert_replay_served(visto)
    assert set(frame.columns) >= COBERTURA
    novembro = frame.loc[frame["mes"].eq(pd.Timestamp("2001-11-01"))].iloc[0]
    grupo = _meses("RS")["2001-11"]
    assert (novembro["estacoes_chuva"], novembro["estacoes_chuva_parciais"]) == (1, 3)
    assert novembro["precip_acum_mm"] == pytest.approx(grupo["precip_acum_mm"], abs=1e-6)
    assert novembro["precip_acum_mm"] != pytest.approx(grupo["precip_regra_1_1_0"], abs=1e-6)
    assert [mensagem[: len(_aviso("RS"))] for mensagem in _avisos_de_chuva(avisos)] == [
        _aviso("RS")
    ]
    assert any(aviso.startswith(_aviso("RS")) for aviso in meta.validation_warnings)
    assert meta.contract_version == "3.1"
