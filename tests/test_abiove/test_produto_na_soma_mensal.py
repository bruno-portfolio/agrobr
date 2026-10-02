from __future__ import annotations

import warnings
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.abiove import api
from agrobr.exceptions import InvalidParameterError

EDICAO = Path(__file__).resolve().parents[1] / "golden_data/abiove/edicao_202608/exp_202608.xlsx"


async def _exportacao(**kwargs) -> pd.DataFrame:
    resposta = (EDICAO.read_bytes(), "http://test/exp_202608.xlsx", "2026-08")
    with (
        patch.object(api.client, "fetch_exportacao_excel", AsyncMock(return_value=resposta)),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("ignore")
        return await api.exportacao(2025, **kwargs)


async def test_total_no_detalhado_falha_antes_da_rede():
    baixar = AsyncMock(side_effect=AssertionError("produto='total' chegou à rede"))
    with (
        patch.object(api.client, "fetch_exportacao_excel", baixar),
        pytest.raises(InvalidParameterError, match="agregacao='mensal'"),
    ):
        await api.exportacao(2025, produto="total")
    baixar.assert_not_awaited()


async def test_total_na_soma_mensal_e_o_total_dos_produtos():
    total = await _exportacao(produto="total", agregacao="mensal")
    pd.testing.assert_frame_equal(total, await _exportacao(agregacao="mensal"))
    assert len(total) == 12
    assert set(total["produto"]) == {"total"}


async def test_soma_mensal_filtrada_sai_com_o_produto_filtrado():
    grao = await _exportacao(produto="grao", agregacao="mensal")
    detalhado = await _exportacao(produto="grao")
    assert set(grao["produto"]) == {"grao"}
    assert grao["volume_ton"].sum() == pytest.approx(detalhado["volume_ton"].sum())
    assert grao["volume_ton"].sum() < (await _exportacao(agregacao="mensal"))["volume_ton"].sum()
