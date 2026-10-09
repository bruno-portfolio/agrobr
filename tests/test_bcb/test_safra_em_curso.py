from __future__ import annotations

import json
import warnings
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr import datasets
from agrobr.bcb import api
from agrobr.utils import time as time_utils

SICOR_CUSTEIO_SOJA_MT = (
    Path(__file__).parents[1]
    / "golden_data/reconciliacao_mercados_credito_20260918/sicor/sicor_custeio_soja_2024_2025_MT.json"
)
REGISTROS = json.loads(SICOR_CUSTEIO_SOJA_MT.read_bytes())["value"]


async def _consultar(
    monkeypatch: pytest.MonkeyPatch,
    registros: list[dict],
    agora: datetime,
    consulta=api.credito_rural,
):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: agora)
    monkeypatch.setattr(
        api.client,
        "fetch_credito_rural_with_fallback",
        AsyncMock(return_value=(registros, "odata")),
    )
    with warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        frame, meta = await consulta("soja", uf="MT", return_meta=True)
    return frame, meta, [str(aviso.message) for aviso in emitidos]


@pytest.mark.parametrize(
    "consulta", [api.credito_rural, datasets.credito_rural], ids=["fonte", "dataset"]
)
async def test_safra_em_curso_sai_marcada_com_os_meses_cobertos(monkeypatch, consulta):
    ate_agosto = [r for r in REGISTROS if (r["AnoEmissao"], r["MesEmissao"]) <= ("2024", "08")]
    meses = sorted({f"{r['AnoEmissao']}-{r['MesEmissao']}" for r in ate_agosto})

    frame, meta, emitidos = await _consultar(
        monkeypatch, ate_agosto, datetime(2024, 9, 15, 12, tzinfo=UTC), consulta
    )

    assert frame["safra"].tolist() == ["2024/25"]
    assert meses == ["2024-07", "2024-08"]
    assert meta.source_details.get("safra_em_curso") == "2024/25"
    assert meta.source_details.get("meses_cobertos") == meses
    esperado = "credito_rural: a safra 2024/25 está em curso (julho a junho); o resultado cobre de 2024-07 a 2024-08"
    assert [aviso for aviso in meta.validation_warnings if aviso.startswith(esperado)]
    assert [aviso for aviso in emitidos if aviso.startswith(esperado)]


async def test_safra_fechada_nao_recebe_aviso(monkeypatch):
    _, meta, emitidos = await _consultar(
        monkeypatch, REGISTROS, datetime(2026, 9, 15, 12, tzinfo=UTC)
    )

    assert "safra_em_curso" not in meta.source_details
    assert not [aviso for aviso in meta.validation_warnings if "em curso" in aviso]
    assert not [aviso for aviso in emitidos if "em curso" in aviso]
