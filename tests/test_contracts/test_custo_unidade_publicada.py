from __future__ import annotations

import json
from pathlib import Path

import pytest

from agrobr import conab, contracts, exceptions
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[2] / "tests/golden_data/reconciliacao_custos_conab_20260918"
)


async def test_contrato_rejeita_unidade_de_saca_no_custo_por_hectare(monkeypatch):
    calls = helpers.install_reconciliacao_r4_http(monkeypatch)
    manifest = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
    case = next(item for item in manifest["cases"] if item["id"] == "milho_8608f61a51d3")
    selection = dict(case["selection"])
    product = selection.pop("produto")
    frame = await conab.custo_producao(product, **selection, use_cache=False)

    helpers.assert_reconciliation_case(frame, case)
    contracts.validate_dataset(frame, "custo_producao")
    selected = frame["linha"].eq(12)
    row = frame.loc[selected].iloc[0]
    assert row["unidade"] == "CUSTO POR HA"
    assert row["valor_ha"] == pytest.approx(420.84)
    assert row["unidade_produto"] == "CUSTO /  60 kg"
    assert row["valor_unidade_produto"] == pytest.approx(5.26052)
    assert calls

    invalid = frame.copy()
    invalid.loc[selected, "unidade"] = row["unidade_produto"]
    with pytest.raises(
        exceptions.ContractViolationError,
        match="Unidade publicada de custo por hectare não reconhecida",
    ):
        contracts.validate_dataset(invalid, "custo_producao")
