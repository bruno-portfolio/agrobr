from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from agrobr import contracts, datasets, ibge
from agrobr.exceptions import ParseError
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_censos_producao_ibge_conab_20260918"
)
MANIFEST = json.loads((GOLDEN / "lot2_manifest.json").read_text(encoding="utf-8"))
CASES = {case["api"]: case for case in MANIFEST["cases"]}


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda item: item["id"])
async def test_trimestrais_replay_local_com_proveniencia_limitada(monkeypatch, case):
    calls = helpers.install_reconciliacao_r5_quarter_http(monkeypatch, case, MANIFEST)
    source, source_meta = await getattr(ibge, case["api"])(
        **case["source_selection"], return_meta=True
    )
    frame, meta = await getattr(datasets, case["dataset"])(**case["selection"], return_meta=True)
    helpers.assert_reconciliation_case(source, case)
    helpers.assert_reconciliation_case(frame, case)
    assert len(calls) == 2
    dataset_source = "ibge" if case["dataset"] == "pib_agro" else case["source"]
    assert source_meta.attempted_sources == [case["source"]]
    assert meta.attempted_sources == [dataset_source]
    assert meta.selected_source == dataset_source
    assert meta.records_count == len(frame)
    assert case["coverage"]["N2_source_certified"] is False


@pytest.mark.parametrize("api", ["abate", "leite_trimestral"])
async def test_trimestrais_rejeitam_observacao_duplicada_antes_do_join(monkeypatch, api):
    case = CASES[api]
    with (GOLDEN / case["file"]).open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    rows.append({**rows[0], "V": "1"})
    helpers.install_reconciliacao_r5_quarter_http(monkeypatch, case, MANIFEST, rows)
    with pytest.raises(ParseError, match="duplicad"):
        await getattr(ibge, api)(**case["source_selection"])


@pytest.mark.parametrize(
    ("anomalia", "mensagem"),
    [
        ("variavel_fora_do_abate", "sem as variáveis 284 e 285"),
        ("variavel_irreconhecivel", "sem coluna de variável"),
        ("sem_trimestre_nem_localidade", "sem trimestre nem localidade"),
    ],
)
async def test_abate_rejeita_layout_anomalo_nao_vazio(monkeypatch, anomalia, mensagem):
    case = CASES["abate"]
    with (GOLDEN / case["file"]).open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if anomalia == "variavel_fora_do_abate":
        rows = [{**row, "D2C": "151", "D2N": "Número de informantes"} for row in rows]
    elif anomalia == "variavel_irreconhecivel":
        rows = [{**row, "D2C": "999", "D2N": "Variável desconhecida"} for row in rows]
    else:
        rows = [
            {k: v for k, v in row.items() if k not in {"D1C", "D1N", "D3C", "D3N"}} for row in rows
        ]
    helpers.install_reconciliacao_r5_quarter_http(monkeypatch, case, MANIFEST, rows)
    with pytest.raises(ParseError, match=mensagem):
        await ibge.abate(**case["source_selection"])


async def test_abate_entrega_medidas_float64_como_o_contrato(monkeypatch):
    case = CASES["abate"]
    helpers.install_reconciliacao_r5_quarter_http(monkeypatch, case, MANIFEST)
    frame = await datasets.abate_trimestral(**case["selection"])
    flutuantes = [
        coluna.name
        for coluna in contracts.get_contract(case["dataset"]).columns
        if coluna.type is contracts.ColumnType.FLOAT
    ]
    assert flutuantes == ["peso_carcacas"]
    assert str(frame["animais_abatidos"].dtype) == "Int64"
    assert {nome: str(frame[nome].dtype) for nome in flutuantes} == dict.fromkeys(
        flutuantes, "float64"
    )
    helpers.assert_reconciliation_case(frame, case)
