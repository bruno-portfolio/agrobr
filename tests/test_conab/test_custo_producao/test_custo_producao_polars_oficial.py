from __future__ import annotations

import pytest

from agrobr import conab
from agrobr.conab._custo_producao import _acquisition
from tests import helpers


@pytest.mark.asyncio
async def test_custo_producao_polars_preserva_valor_e_unidade_oficiais(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pl = pytest.importorskip("polars")
    _acquisition.clear()
    calls = helpers.install_reconciliacao_r4_http(monkeypatch)
    try:
        frame, meta = await conab.custo_producao(
            "soja",
            planilha="serie-historica-custos-soja-1997-a-2025.xls",
            aba="Barreiras-BA-2011",
            as_polars=True,
            return_meta=True,
            use_cache=False,
        )
    finally:
        _acquisition.clear()

    total = frame.filter(pl.col("linha") == 72)
    assert total.height == 1
    row = total.row(0, named=True)
    assert row["item"] == "CUSTO TOTAL (H+I=J)"
    assert (row["local"], row["uf"], row["safra"]) == ("Barreiras", "BA", "2011/12")
    assert row["unidade"] == "CUSTO POR HA"
    assert row["unidade_produto"] == "CUSTO / 60KG"
    assert row["valor_ha"] == pytest.approx(1630.41, rel=0, abs=1e-9)
    assert row["valor_unidade_produto"] == pytest.approx(33.96, rel=0, abs=1e-9)
    assert row["participacao_cv_pct"] == pytest.approx(168.22, rel=0, abs=1e-9)
    assert row["participacao_ct_pct"] == pytest.approx(100.0, rel=0, abs=1e-9)
    assert frame.schema["valor_ha"] == pl.Float64
    assert frame.schema["valor_unidade_produto"] == pl.Float64
    assert frame.schema["linha"] == pl.Int64
    assert frame.schema["data_referencia"] == pl.Datetime("ns")
    assert meta.source_details["output_dtypes"]["valor_ha"] == "Float64"
    assert meta.records_count == frame.height
    assert any(
        "serie-historica-custos-soja-1997-a-2025.xls/@@download/file" in url for url in calls
    )
