from __future__ import annotations

import copy
import csv
import hashlib
import json
from pathlib import Path

import pytest

from agrobr import datasets, ibge
from agrobr.exceptions import ParseError
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_censos_producao_ibge_conab_20260918"
)
MANIFEST = json.loads((GOLDEN / "lot3_manifest.json").read_text(encoding="utf-8"))
CASE = MANIFEST["cases"][0]
SYNTHETIC = json.loads((GOLDEN / "lot3_synthetic_regressions.json").read_text(encoding="utf-8"))
with (GOLDEN / CASE["file"]).open(encoding="utf-8-sig", newline="") as stream:
    RAW_ROWS = list(csv.DictReader(stream))


async def test_censo_efetivo_replay_local_preserva_celulas_e_chaves(monkeypatch):
    assert (
        hashlib.sha256((GOLDEN / CASE["file"]).read_bytes()).hexdigest()
        == MANIFEST["files"][0]["sha256"]
    )
    calls = helpers.install_reconciliacao_censos_agro_http(
        monkeypatch, {"6907": RAW_ROWS, "323": []}, {"6907": "10010,2209", "323": "105"}
    )
    source, source_meta = await ibge.censo_agro("efetivo_rebanho", ano=2017, return_meta=True)
    frame, meta = await datasets.censo_agropecuario("efetivo_rebanho", return_meta=True)
    helpers.assert_reconciliation_case(source, CASE)
    helpers.assert_reconciliation_case(frame, CASE)
    assert len(calls) == 3
    assert meta.selected_source == "ibge_censo_agro"
    assert meta.attempted_sources == ["ibge_censo_agro"]
    assert meta.records_count == 10
    assert source_meta.parser_version == meta.parser_version == 3
    assert CASE["coverage"]["N2_source_certified"] is False


async def test_censo_dataset_sem_meta_preserva_celulas(monkeypatch):
    calls = helpers.install_reconciliacao_censos_agro_http(
        monkeypatch, {"6907": RAW_ROWS, "323": []}, {"6907": "10010,2209", "323": "105"}
    )
    frame = await datasets.censo_agropecuario("efetivo_rebanho", ano=2017)
    assert len(calls) == 1
    helpers.assert_reconciliation_case(frame, CASE)


async def test_censo_colisao_entre_tabelas_complementares_e_rejeitada(monkeypatch):
    responses = copy.deepcopy(SYNTHETIC["lavoura_1995"])
    responses["492"][0].update(
        {
            "D2C": "214",
            "D2N": "Quantidade produzida",
        }
    )
    helpers.install_reconciliacao_censos_agro_http(
        monkeypatch, responses, {"497": "214", "492": "151", "503": "216"}
    )
    with pytest.raises(ParseError, match="duplicad"):
        await ibge.censo_agro("lavoura_temporaria", ano=1995)
