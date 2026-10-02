from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from agrobr.bcb import focus_client, focus_query, sgs_client
from agrobr.utils import time as time_utils


@pytest.mark.parametrize(
    "instante,dia,inicio",
    [
        (datetime(2026, 1, 1, 1, tzinfo=UTC), date(2025, 12, 31), "31/12/2015"),
        (datetime(2024, 3, 1, 1, tzinfo=UTC), date(2024, 2, 29), "28/02/2014"),
    ],
)
async def test_sgs_limites_padrao_seguem_data_civil_brasileira(monkeypatch, instante, dia, inicio):
    class Relogio(datetime):
        @classmethod
        def now(cls, tz=None):
            return instante.astimezone(tz) if tz is not None else instante.replace(tzinfo=None)

    monkeypatch.setattr(sgs_client, "datetime", Relogio)
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: instante)
    adquirir = AsyncMock(
        return_value=SimpleNamespace(
            records=[], resources=[SimpleNamespace(url="https://example.invalid/sgs")]
        )
    )
    monkeypatch.setattr(sgs_client, "fetch_sgs_acquisition", adquirir)
    assert sgs_client._default_start_date() == inicio
    await sgs_client.fetch_sgs(1)
    consulta = adquirir.await_args.args[0]
    assert consulta.reference_date == consulta.fim == dia
    assert consulta.inicio.strftime("%d/%m/%Y") == inicio


@pytest.mark.parametrize(
    "limite,cobertura,texto", [(6, "unknown", "não comprovada"), (2, "partial", "parcial")]
)
async def test_focus_aviso_de_limite_legivel_sem_alterar_cobertura(
    focus_http, focus_captures, limite, cobertura, texto
):
    corpo = focus_captures["bodies"]["annual_api_ge"]
    focus_http(lambda _request, _index: httpx.Response(200, content=corpo))
    consulta = focus_query.build_query(
        "Balança comercial", data_inicial="2026-08-28", top=6, max_registros=limite
    )
    resultado = await focus_client.fetch_focus_acquisition(consulta)
    assert resultado.coverage.completeness == cobertura
    assert resultado.coverage.expected_count is None
    assert len(resultado.records) == limite
    aviso = resultado.warnings[0]
    assert f"cobertura {texto}" in aviso
    assert "total não informado pela fonte" in aviso
    assert "unknown" not in aviso and "None" not in aviso
