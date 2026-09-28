from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from agrobr.cache.policies import (
    POLICIES,
    calculate_expiry,
    format_ttl,
    get_next_update_info,
    get_policy,
)
from agrobr.constants import Fonte
from agrobr.exceptions import InvalidParameterError
from tests import helpers


class TestFormatTTL:
    def test_seconds(self):
        assert format_ttl(30) == "30 segundos"

    def test_one_minute(self):
        assert format_ttl(60) == "1 minuto"

    def test_one_hour(self):
        assert format_ttl(3600) == "1 hora"

    def test_one_day(self):
        assert format_ttl(86400) == "1 dia"


def test_get_next_update_info_cepea():
    with patch("agrobr.cache.policies.utcnow", return_value=datetime(2026, 9, 23, 12, 0)):
        info = get_next_update_info(Fonte.CEPEA)
    assert info == {
        "type": "smart",
        "expires_at": "2026-09-23 21:00",
        "description": "Expira às 18h BRT (atualização CEPEA)",
    }


@pytest.mark.parametrize(
    ("fonte", "endpoint"),
    [
        ("cepea_diario", None),
        ("cepea", None),
        (Fonte.CEPEA, None),
        (Fonte.CEPEA, "diario"),
        (Fonte.CEPEA, "inexistente"),
    ],
)
def test_get_policy_cepea(fonte, endpoint):
    assert get_policy(fonte, endpoint) is POLICIES["cepea_diario"]


@pytest.mark.parametrize(
    ("fonte", "endpoint", "nome"),
    [
        (Fonte.CONAB, None, "conab"),
        ("conab", "balanco", "conab"),
        (Fonte.NOTICIAS_AGRICOLAS, None, "noticias_agricolas"),
        ("ibge_pib", None, "ibge_pib"),
        ("fonte_inexistente", None, "fonte_inexistente"),
    ],
)
def test_get_policy_fonte_sem_cache_levanta(fonte, endpoint, nome):
    with pytest.raises(InvalidParameterError, match=f"'{nome}' não tem cache no agrobr"):
        get_policy(fonte, endpoint)


@pytest.mark.parametrize(
    ("fonte", "agora", "esperado"),
    [
        ("cepea_diario", datetime(2026, 9, 23, 12, 0), datetime(2026, 9, 23, 21, 0)),
        ("cepea_diario", datetime(2026, 9, 23, 22, 0), datetime(2026, 9, 24, 21, 0)),
    ],
)
def test_calculate_expiry_ttl_e_virada_das_18h(fonte, agora, esperado):
    with patch("agrobr.cache.policies.utcnow", return_value=agora):
        assert calculate_expiry(fonte) == esperado
    assert esperado - agora <= timedelta(days=1)


@pytest.mark.parametrize(
    ("fonte", "desde", "esperado"),
    [
        ("cepea_diario", datetime(2026, 9, 25, 22, 0), datetime(2026, 9, 28, 21, 0)),
        ("cepea_diario", datetime(2026, 9, 26, 10, 0), datetime(2026, 9, 28, 21, 0)),
        ("cepea_diario", datetime(2026, 9, 23, 21, 0), datetime(2026, 9, 24, 21, 0)),
        (
            "cepea_diario",
            datetime(2026, 9, 23, 18, 30, tzinfo=timezone(timedelta(hours=-3))),
            datetime(2026, 9, 24, 21, 0),
        ),
    ],
    ids=["sexta_apos_virada", "sabado", "na_virada", "brt_com_fuso"],
)
def test_calculate_expiry_desde_a_coleta_em_dia_util(fonte, desde, esperado):
    with (
        patch("agrobr.cache.policies.utcnow", return_value=datetime(2030, 1, 1)),
        helpers.sem_excecao(),
    ):
        resultado = calculate_expiry(fonte, desde=desde)
    assert resultado == esperado
