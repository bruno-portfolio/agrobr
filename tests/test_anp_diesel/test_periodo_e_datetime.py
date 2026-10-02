from __future__ import annotations

from datetime import date, datetime
from unittest.mock import AsyncMock

import pytest

from agrobr.alt.anp_diesel import api
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from tests.helpers import levanta_exatamente


@pytest.fixture(autouse=True)
def hoje_fixo(monkeypatch):
    monkeypatch.setattr(api.time_utils, "hoje", lambda: date(2026, 10, 1))


def _consulta(nivel: str = "brasil", **kwargs):
    return api.normalize_price_query(
        kwargs.get("uf"),
        None,
        "DIESEL S10",
        kwargs.get("inicio"),
        kwargs.get("fim"),
        "semanal",
        nivel,
    )


def test_datetime_vira_a_data_civil():
    consulta = _consulta(inicio=datetime(2025, 1, 6, 23, 59), fim=datetime(2025, 2, 3))
    assert consulta["inicio"] == date(2025, 1, 6)
    assert type(consulta["inicio"]) is date
    assert consulta["fim"] == date(2025, 2, 3)


@pytest.mark.parametrize("nivel", ["brasil", "uf"])
def test_periodo_inteiramente_futuro_e_parametro_invalido(nivel):
    with levanta_exatamente(InvalidParameterError, "2026-10-01"):
        _consulta(nivel, uf="SP" if nivel == "uf" else None, inicio=date(2026, 10, 2))


def test_fim_futuro_com_inicio_no_passado_segue_valido():
    assert _consulta(inicio=date(2026, 9, 1), fim=date(2026, 12, 31))["fim"] == date(2026, 12, 31)


@pytest.mark.parametrize("limite", [date(2021, 12, 31), date(2027, 1, 4)])
async def test_municipio_fora_do_dominio_e_parametro_invalido_antes_da_rede(monkeypatch, limite):
    catalogo = AsyncMock(side_effect=AssertionError("rede antes da validação"))
    monkeypatch.setattr(api.client, "fetch_precos_catalog", catalogo)
    consulta = {"nivel": "municipio", "inicio": None, "fim": limite}
    with levanta_exatamente(InvalidParameterError, "2022 a 2026"):
        await api._resolve_price_urls(consulta)
    catalogo.assert_not_awaited()


def test_ano_valido_sem_arquivo_no_catalogo_segue_indisponivel():
    catalogo = {"2022-2023": "https://exemplo/2022-2023.xlsx", "2025": "https://exemplo/2025.xlsx"}
    with levanta_exatamente(SourceUnavailableError, "Ano 2024"):
        api._periodos_municipios(date(2023, 6, 1), date(2025, 6, 1), catalogo)
