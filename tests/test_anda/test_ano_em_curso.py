from __future__ import annotations

import warnings
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from agrobr import datasets
from agrobr.anda import api
from agrobr.utils import time as time_utils

from .test_api import _mock_parsed_df

ABRIL_2024 = datetime(2024, 4, 10, 12, tzinfo=UTC)
ABRIL_2025 = datetime(2025, 4, 10, 12, tzinfo=UTC)


async def _consultar(monkeypatch: pytest.MonkeyPatch, agora: datetime, consulta):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: agora)
    with (
        patch.object(
            api.client,
            "fetch_entregas_pdf",
            new_callable=AsyncMock,
            return_value=(b"pdf", 2024, {"url": "https://anda.org.br/x.pdf", "text": "Dados 2024"}),
        ),
        patch.object(api.parser, "parse_entregas_pdf", return_value=_mock_parsed_df()),
        patch.object(api.parser, "edicao_impressa", return_value="Janeiro a Março"),
        warnings.catch_warnings(record=True) as emitidos,
    ):
        warnings.simplefilter("always")
        _, meta = await consulta()
    return meta, [str(aviso.message) for aviso in emitidos]


@pytest.mark.parametrize(
    "consulta",
    [
        lambda: api.entregas(2024, return_meta=True),
        lambda: datasets.fertilizante("total", return_meta=True),
    ],
    ids=["fonte", "dataset_sem_ano"],
)
async def test_ano_em_curso_sai_marcado_com_os_meses_cobertos(monkeypatch, consulta):
    meta, emitidos = await _consultar(monkeypatch, ABRIL_2024, consulta)

    esperado = "anda: o ano 2024 está em curso; o boletim cobre de janeiro a 03/2024"
    assert meta.source_details.get("ano_em_curso") == 2024
    assert meta.source_details.get("meses_cobertos") == [1, 2, 3]
    assert [aviso for aviso in meta.validation_warnings if aviso.startswith(esperado)]
    assert [aviso for aviso in emitidos if aviso.startswith(esperado)]


async def test_ano_fechado_nao_recebe_aviso(monkeypatch):
    meta, emitidos = await _consultar(
        monkeypatch, ABRIL_2025, lambda: api.entregas(2024, return_meta=True)
    )

    assert "ano_em_curso" not in meta.source_details
    assert not [aviso for aviso in emitidos if "em curso" in aviso]
