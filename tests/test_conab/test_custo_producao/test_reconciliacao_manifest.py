from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.conab._custo_producao import _sociobio_api, api
from agrobr.exceptions import ParseError
from tests import helpers

GOLDEN = Path(__file__).resolve().parents[2] / "golden_data/reconciliacao_custos_conab_20260918"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
ACCEPTED = [case for case in MANIFEST["cases"] if case["status"] == "accepted"]
REFUSED = [case for case in MANIFEST["cases"] if case["status"] == "refused"]
RECUSAS = {
    "soja_8653452454e9": "Contexto não unívoco ou incompleto na aba P. do Leste-MT-1997",
    "soja_959da8ae1110": "Medida inválida em 1-Ijuí-RS-2007!R39C4",
    "acai_bf0c4107b9cf": "Igarapé-Miri-PA-2008: Medida sem descrição na linha 49",
    "acai_719457b5481f": "Igarapé-Miri-PA-2008: Medida sem descrição na linha 49",
    "pinhao_36c66436aaa4": "São Joaquim-SC-2015: Coluna sem mapeamento em R8C4: '1 kg'",
    "baru_1b21392b1930": "Amêndoa-Iporá-GO-2010: Safra/local não reconhecidos: 'SAFRA'",
    "piacava_fa0d6efebd54": "Belmonte-BA-2011: Cabeçalhos monetários não reconhecidos: 3",
    "trigo_a22b9d4b369a": "Coluna duplicada valor_unidade_produto na aba Toledo-PR-2002",
}


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(case, marks=pytest.mark.slow)
        if case["id"] not in {"milho_8608f61a51d3", "murumuru_48eead0b3c5d"}
        else case
        for case in ACCEPTED
    ],
    ids=lambda case: case["id"],
)
async def test_custos_manifesto_na_api_e_dataset_publicos(monkeypatch, case):
    calls = helpers.install_reconciliacao_custos_http(monkeypatch)
    selection = dict(case["selection"])
    product = selection.pop("produto")
    source = (
        _sociobio_api.custo_sociobiodiversidade
        if case["dataset"] == "custo_sociobiodiversidade"
        else api.custo_producao
    )
    source_frame, source_meta = await source(
        product, **selection, return_meta=True, use_cache=False
    )
    frame, meta = await getattr(datasets, case["dataset"])(
        product, **selection, return_meta=True, use_cache=True
    )
    helpers.assert_reconciliation_case(source_frame, case)
    helpers.assert_reconciliation_case(frame, case)
    helpers.assert_custo_meta(frame, meta, case, MANIFEST)
    helpers.assert_custo_meta(source_frame, source_meta, case, MANIFEST, source=True)
    for field, expected in case["fixed_key"].items():
        assert frame[field].eq(expected).all()
    assert calls
    assert set(source_frame.columns) == set(frame.columns)
    pd.testing.assert_frame_equal(source_frame, frame)


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(case, marks=pytest.mark.slow)
        if case["id"] in {"soja_8653452454e9", "soja_959da8ae1110", "trigo_a22b9d4b369a"}
        else case
        for case in REFUSED
    ],
    ids=lambda case: case["id"],
)
async def test_custos_recusas_reais_nao_retornam_quadro_parcial(monkeypatch, case):
    helpers.install_reconciliacao_custos_http(monkeypatch)
    selection = dict(case["selection"])
    product = selection.pop("produto")
    with pytest.raises(ParseError, match=re.escape(RECUSAS[case["id"]])):
        await getattr(datasets, case["dataset"])(product, **selection, use_cache=False)
