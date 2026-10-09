from __future__ import annotations

import copy
import json
import warnings
from pathlib import Path

import pandas as pd
import pytest

from agrobr import datasets
from agrobr import desmatamento as desmatamento_source
from agrobr.normalize import municipalities, regions
from tests.helpers import (
    assert_replay_samples,
    assert_replay_served,
    assert_replay_structure,
    install_replay_http,
    sem_excecao,
)

GOLDEN = (
    Path(__file__).resolve().parents[1] / "golden_data/reconciliacao_uso_solo_ambiente_20260918"
)
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
PARAMETROS_2_0 = {"data_inicio": "inicio", "data_fim": "fim", "geocodigo": "municipio"}


def _na_api_2_0(case: dict) -> dict:
    """O caso com os nomes da 2.0: parâmetros de data e de município e, no MapBiomas, a UF."""
    case = copy.deepcopy(case)
    case["selection"] = {PARAMETROS_2_0.get(k, k): v for k, v in case["selection"].items()}
    if case["dataset"] == "uso_do_solo":
        case["columns"] = ["uf" if c == "estado" else c for c in case["columns"]]
        for sample in case["samples"]:
            sample["key"] = {"uf" if k == "estado" else k: v for k, v in sample["key"].items()}
        for item in case["structure"]:
            if item["destino"] == "estado":
                item["destino"] = "uf"
    return case


CASES = {case["id"]: _na_api_2_0(case) for case in MANIFEST["cases"]}

AGREGADOS = [case["id"] for case in MANIFEST["cases"] if case["dataset"] == "desmatamento"]
FEICOES = [case["id"] for case in MANIFEST["cases"] if case["dataset"].startswith("desmatamento.")]
FOCOS = [case["id"] for case in MANIFEST["cases"] if case["dataset"] == "queimadas"]
USO_DO_SOLO = [case["id"] for case in MANIFEST["cases"] if case["dataset"] == "uso_do_solo"]


@pytest.mark.parametrize("case_id", AGREGADOS)
async def test_desmatamento_agregado(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    with sem_excecao():
        frame, meta = await datasets.desmatamento(**case["selection"], return_meta=True)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    assert_replay_structure(frame.drop(columns="cod_municipio", errors="ignore"), case)
    assert_replay_samples(frame, case)
    if "cod_municipio" in frame:
        colunas = ["municipio_id", "municipio", "uf", "cod_municipio"]
        for municipio_id, municipio, uf, codigo in frame[colunas].itertuples(index=False):
            if not pd.isna(municipio_id):
                assert codigo == int(municipio_id)
                continue
            info = municipalities.ibge_para_municipio(int(codigo))
            assert info is not None
            assert (regions.remover_acentos(info["nome"]).upper(), info["uf"]) == (
                regions.remover_acentos(municipio).upper(),
                uf,
            )
    assert meta.selected_source == f"terrabrasilis_{case['selection']['tipo']}"
    coverage = meta.source_details["coverage"]
    assert coverage["status"] == "reconciled"
    assert coverage["count_reconciled"] is True
    assert coverage["truncated"] is False
    assert coverage["expected_rows"] == case["period"]["feicoes_de_entrada"]
    aggregation = meta.source_details["aggregation"]
    assert aggregation["input_occurrences"] == case["period"]["feicoes_de_entrada"]
    assert aggregation["discarded_occurrences"] == 0
    assert aggregation["official_rate"] is False
    for entry in case.get("null_columns", []):
        assert frame[entry["column"]].isna().all()


@pytest.mark.parametrize("case_id", FEICOES)
async def test_desmatamento_feicoes(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    produto = case["dataset"].split(".")[1]
    fetch = desmatamento_source.prodes if produto == "prodes" else desmatamento_source.deter
    with sem_excecao(), warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame, meta = await fetch(**case["selection"], return_meta=True)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    assert frame["feature_id"].iloc[0] == case["period"]["primeira_feicao"]
    assert frame["feature_id"].iloc[-1] == case["period"]["ultima_feicao"]
    assert_replay_structure(frame, case)
    assert_replay_samples(frame, case)
    assert meta.selected_source == f"terrabrasilis_{produto}"
    assert not [aviso for aviso in avisos if issubclass(aviso.category, UserWarning)]
    assert [
        texto for texto in meta.validation_warnings if "snapshot transacional" not in texto
    ] == []
    assert meta.source_details["coverage"]["ambiguities"] == []
    assert "geometry" not in meta.source_details


@pytest.mark.parametrize("case_id", FOCOS)
async def test_queimadas_focos(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    with sem_excecao():
        frame, meta = await datasets.queimadas(**case["selection"], return_meta=True)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    assert "cod_municipio" in frame
    assert_replay_structure(frame.drop(columns="cod_municipio"), case)
    assert frame["cod_municipio"].astype(object).where(
        frame["cod_municipio"].notna(), None
    ).tolist() == [None if pd.isna(codigo) else int(codigo) for codigo in frame["municipio_id"]]
    assert_replay_samples(frame, case)
    assert meta.selected_source == "inpe"
    assert meta.source_url == case["requests"][0]["match"]["path"]
    assert frame["hora_gmt"].str.fullmatch(r"\d{2}:\d{2}").all()
    primeira, ultima = frame.iloc[0], frame.iloc[-1]
    assert f"{primeira['data']:%Y-%m-%d} {primeira['hora_gmt']}" == case["period"]["primeira_linha"]
    assert f"{ultima['data']:%Y-%m-%d} {ultima['hora_gmt']}" == case["period"]["ultima_linha"]


@pytest.mark.parametrize("case_id", USO_DO_SOLO)
async def test_uso_do_solo(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    with sem_excecao():
        frame, meta = await datasets.uso_do_solo(**case["selection"], return_meta=True)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    assert_replay_structure(frame.drop(columns="cod_municipio", errors="ignore"), case)
    assert_replay_samples(frame, case)
    if "cod_municipio" in frame:
        municipal = frame["cod_municipio"].notna()
        assert municipal.any()
        assert frame.loc[municipal, "cod_municipio"].tolist() == [
            int(codigo) for codigo in frame.loc[municipal, "geocodigo"]
        ]
    rotulo = "ano" if case["selection"]["tipo"] == "cobertura" else "periodo"
    assert sorted(frame[rotulo].unique().tolist()) == sorted(case["period"]["rotulos"])
    esperado = "mapbiomas_oficial" if case["selection"]["colecao"] == 11 else "mapbiomas_dataverse"
    assert meta.selected_source == esperado
    assert meta.data_sources == [f"mapbiomas_colecao_{case['selection']['colecao']}"]
