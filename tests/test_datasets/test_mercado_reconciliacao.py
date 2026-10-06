from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.contracts.datasets import POSICIONAMENTO_FUNDOS_COLUNAS_V2
from tests.helpers import (
    assert_replay_samples,
    assert_replay_served,
    assert_replay_structure,
    collect_failures,
    install_replay_http,
    isolated_dataset_case,
    replay_matches,
)


def _golden(rel: str) -> Path:
    return (GOLDEN / rel).resolve()


GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/reconciliacao_mercados_credito_20260918"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
CASES = {case["id"]: case for case in MANIFEST["cases"]}


@pytest.mark.parametrize(
    "case_id", ["ptax_usd_dia", "ptax_usd_periodo", "ptax_eur_periodo", "ptax_usd_historico_1994"]
)
async def test_cotacoes_cambio_from_official_bodies(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    selection = {
        {"data_inicial": "inicio", "data_final": "fim"}.get(nome, nome): valor
        for nome, valor in case["selection"].items()
    }
    frame, meta = await datasets.cotacoes_cambio(**selection, return_meta=True)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows_fechamento"]
    for column in ("cotacao_compra", "cotacao_venda", "paridade_compra", "paridade_venda"):
        assert str(frame[column].dtype) == "float64", column
    assert_replay_structure(frame, case)
    assert_replay_samples(frame, case)
    assert meta.selected_source == "bcb_ptax"
    todos, _ = await datasets.cotacoes_cambio(**selection, boletim="todos", return_meta=True)
    assert len(todos) == case["period"]["rows_all"]
    assert sorted(todos["tipo_boletim"].astype(str).unique()) == case["period"]["boletins"]
    assert_replay_served(seen)


async def test_moedas_cambio_from_official_catalog(monkeypatch: pytest.MonkeyPatch):
    case = CASES["ptax_moedas"]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    frame, meta = await datasets.moedas_cambio(**case["selection"], return_meta=True)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    assert frame["moeda"].iloc[0] == case["period"]["first"]
    assert frame["moeda"].iloc[-1] == case["period"]["last"]
    assert_replay_structure(frame, case)
    assert_replay_samples(frame, case)
    assert meta.selected_source == "bcb_ptax_moedas"


@pytest.mark.parametrize("case_id", ["sgs_1_diaria", "sgs_433_mensal", "sgs_22083_trimestral"])
async def test_series_economicas_from_official_bodies(
    case_id: str, monkeypatch: pytest.MonkeyPatch
):
    case = CASES[case_id]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    selection = {
        {"data_inicial": "inicio", "data_final": "fim"}.get(nome, nome): valor
        for nome, valor in case["selection"].items()
    }
    codigo = selection.pop("codigo")
    frame, meta = await datasets.series_economicas(codigo, **selection, return_meta=True)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    assert str(frame["valor"].dtype) == "float64"
    assert frame["data"].is_monotonic_increasing
    assert_replay_structure(frame, case)
    assert_replay_samples(frame, case)
    assert meta.selected_source == "bcb_sgs"


@pytest.mark.parametrize(
    "case_id", ["focus_pib_agro_anual", "focus_balanca_anual", "focus_ipca_mensal"]
)
async def test_expectativas_mercado_from_official_bodies(
    case_id: str, monkeypatch: pytest.MonkeyPatch
):
    case = CASES[case_id]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    selection = {
        {"data_inicial": "inicio", "data_final": "fim"}.get(nome, nome): valor
        for nome, valor in case["selection"].items()
    }
    indicador = selection.pop("indicador")
    frame, meta = await datasets.expectativas_mercado(indicador, **selection, return_meta=True)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    for column in ("media", "mediana", "minimo", "maximo"):
        assert str(frame[column].dtype) == "float64", column
    assert_replay_structure(frame, case)
    assert_replay_samples(frame, case)
    assert meta.selected_source == "bcb_focus"


@pytest.mark.parametrize("case_id", ["sicor_custeio_soja_mt_uf", "sicor_custeio_soja_mt_programa"])
async def test_credito_rural_from_official_body(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    selection = dict(case["selection"])
    produto = selection.pop("produto")
    frame, meta = await datasets.credito_rural(produto, **selection, return_meta=True)
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    assert str(frame["valor"].dtype) == "float64"
    assert_replay_structure(frame, case)
    assert_replay_samples(frame, case)
    assert meta.selected_source == "bcb"


async def test_posicionamento_fundos_from_official_body(monkeypatch: pytest.MonkeyPatch):
    case = CASES["cftc_soja_202606"]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    selection = dict(case["selection"])
    produto = selection.pop("produto")
    selection = {"inicio": selection.pop("start"), "fim": selection.pop("end"), **selection}
    frame, meta = await datasets.posicionamento_fundos(produto, **selection, return_meta=True)
    pt = POSICIONAMENTO_FUNDOS_COLUNAS_V2
    case = {
        **case,
        "structure": [
            {**item, "destino": pt.get(item["destino"], item["destino"])}
            for item in case["structure"]
        ],
        "samples": [
            {**item, "column": pt.get(item["column"], item["column"])} for item in case["samples"]
        ],
    }
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    assert sorted(frame["data"].dt.strftime("%Y-%m-%d")) == case["period"]["datas"]
    assert_replay_structure(frame, case)
    assert_replay_samples(frame, case)
    assert meta.selected_source == "cftc"


@pytest.mark.parametrize(
    "case_id", [case_id for case_id in CASES if case_id.startswith("ceasa_precos_")]
)
async def test_preco_atacado_from_official_pivot(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    result = await datasets.preco_atacado(**case["selection"], return_meta=True)
    assert isinstance(result, tuple)
    frame, meta = result
    assert_replay_served(seen)
    assert len(frame) == case["period"]["rows"]
    if not case["selection"]:
        assert frame["produto"].nunique() == case["period"]["produtos"]
        assert frame["ceasa"].nunique() == case["period"]["ceasas"]
        assert sorted(frame["data"].dt.strftime("%Y-%m-%d").unique()) == case["period"]["datas"]
    assert_replay_structure(frame, case)
    assert_replay_samples(frame, case)
    assert meta.dataset == "preco_atacado"
    assert meta.selected_source == "conab_ceasa"
    assert meta.attempted_sources == ["conab_ceasa"]


async def test_mercado_reconciliacao_r9_casos_1():
    with collect_failures() as check:
        for case_id in [case_id for case_id in CASES if case_id.startswith("b3_ajustes_")]:
            case = f"test_futuros_agricolas_ajustes_from_official_zip[{(case_id,)!r}]"
            with check(case), isolated_dataset_case(case) as monkeypatch:
                case = CASES[case_id]
                seen = install_replay_http(monkeypatch, case, GOLDEN)
                selection = dict(case["selection"])
                produto = selection.pop("produto")
                frame, meta = await datasets.futuros_agricolas(
                    produto, **selection, return_meta=True
                )
                assert_replay_served(seen)
                assert len(frame) == case["period"]["rows"], sorted(
                    frame["data"].astype(str).unique()
                )
                assert sorted(frame["data"].dt.strftime("%Y-%m-%d").unique()) == ["2026-09-17"]
                assert not frame.duplicated(["ticker", "vencimento_codigo"]).any()
                assert_replay_structure(frame, case)
                assert_replay_samples(frame, case)
                assert meta.selected_source == "b3"
        for case_id in [case_id for case_id in CASES if case_id.startswith("b3_posicoes_")]:
            case = f"test_futuros_agricolas_posicoes_from_official_csv[{(case_id,)!r}]"
            with check(case), isolated_dataset_case(case) as monkeypatch:
                case = CASES[case_id]
                seen = install_replay_http(monkeypatch, case, GOLDEN)
                selection = dict(case["selection"])
                produto = selection.pop("produto")
                frame, meta = await datasets.futuros_agricolas(
                    produto, **selection, return_meta=True
                )
                assert_replay_served(seen)
                assert len(frame) == case["period"]["rows"]
                assert frame["ticker"].nunique() == 1
                assert_replay_structure(frame, case)
                assert_replay_samples(frame, case)
                assert meta.selected_source == "b3"


@pytest.mark.parametrize(
    "mutation", ["valor", "uma_unidade", "chave", "coluna_chave", "periodo", "coluna_saida"]
)
def test_mutated_output_or_manifest_fails(mutation: str):
    case = json.loads(json.dumps(CASES["sgs_433_mensal"]))
    frame = pd.DataFrame(
        {
            "codigo": [433, 433],
            "data": pd.to_datetime(["2024-01-01", "2024-12-01"]),
            "valor": [
                case["samples"][0]["value"],
                case["samples"][-1]["value"],
            ],
            "nome_serie": ["IPCA", "IPCA"],
        }
    )
    assert_replay_samples(frame, case)
    assert_replay_structure(frame, case)
    if mutation == "valor":
        case["samples"][0]["value"] += 0.01
    elif mutation == "uma_unidade":
        frame.loc[0, "valor"] += 0.01
    elif mutation == "chave":
        case["samples"][0]["key"]["data"] = "2023-01-01"
    elif mutation == "coluna_chave":
        frame = frame.drop(columns=["codigo"])
    elif mutation == "periodo":
        frame = frame.iloc[:1]
    else:
        frame = frame.drop(columns=["valor"])
    with pytest.raises(AssertionError):
        assert_replay_samples(frame, case)
        assert_replay_structure(frame, case)


def test_float_sum_tolerance_rejects_one_unit():
    sample = {"tolerance": "float_sum", "terms": 277}
    total = 1.0e10
    assert replay_matches(total + 1e-6, total, sample)
    assert not replay_matches(total + 1.0, total, sample)
    assert not replay_matches(None, total, sample)


@pytest.mark.parametrize("mutation", ["troca_de_ordem", "nome_fora_do_catalogo"])
async def test_preco_atacado_catalog_identity(
    mutation: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    from agrobr import exceptions

    case = json.loads(json.dumps(CASES["ceasa_precos_20260213"]))
    catalog_request = next(r for r in case["requests"] if r["role"] == "ceasas")
    body = json.loads(_golden(catalog_request["file"]).read_text(encoding="utf-8"))
    if mutation == "troca_de_ordem":
        body["resultset"][0], body["resultset"][1] = body["resultset"][1], body["resultset"][0]
    else:
        body["resultset"][0][1] = "CEASA INEXISTENTE"
    mutated = tmp_path / f"catalog_{mutation}.json"
    mutated.write_text(json.dumps(body), encoding="utf-8")
    catalog_request["file"] = str(mutated)
    install_replay_http(monkeypatch, case, GOLDEN)
    if mutation == "troca_de_ordem":
        frame = await datasets.preco_atacado()
        assert len(frame) == case["period"]["rows"]
        assert_replay_samples(frame, case)
    else:
        with pytest.raises(exceptions.ParseError) as failure:
            await datasets.preco_atacado()
        assert "CEASA" in str(failure.value)
