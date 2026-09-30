from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agrobr.desmatamento import query
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import time as time_utils


@pytest.mark.parametrize(
    "field,value",
    [
        ("ano", 2022),
        ("inicio", "2026-02-29"),
        ("inicio", "2026-1-01"),
        ("fim", "2026-01-01T00:00:00"),
        ("fim", True),
        ("classe", 1),
        ("classe", "   "),
        ("bioma", "Pampa"),
    ],
)
def test_deter_query_invalid(field, value):
    args = {
        "product": "DETER",
        "include_geometry": False,
        "bioma": "Amazônia",
        "max_registros": None,
    }
    args[field] = value
    with pytest.raises(InvalidParameterError):
        query.build_query(**args)


def test_query_rejects_inverted_dates():
    with pytest.raises(InvalidParameterError):
        query.build_query(
            product="DETER",
            include_geometry=False,
            bioma="Amazônia",
            max_registros=None,
            inicio="2026-02-01",
            fim="2026-01-01",
        )


def test_query_geometry_page_limit():
    with pytest.raises(InvalidParameterError):
        query.build_query(
            product="PRODES",
            include_geometry=True,
            bioma="Amazônia",
            max_registros=None,
            tamanho_pagina=501,
        )


def test_query_model_copy_is_revalidated():
    original = query.build_query(
        product="PRODES", include_geometry=False, bioma="Amazônia", max_registros=None
    )
    with pytest.raises(InvalidParameterError):
        query.revalidate(original.model_copy(update={"page_size": 0}))


@pytest.mark.parametrize(
    ("agora_utc", "aceito"),
    [
        (datetime(2027, 1, 1, 2, 59, tzinfo=UTC), False),
        (datetime(2027, 1, 1, 3, 0, tzinfo=UTC), True),
    ],
    ids=["ainda_2026_em_brasilia", "ja_2027_em_brasilia"],
)
def test_prodes_ano_corrente_e_o_de_brasilia(agora_utc, aceito, monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: agora_utc)
    args = {"product": "PRODES", "include_geometry": False, "bioma": "Cerrado"}
    if aceito:
        assert query.build_query(**args, max_registros=None, ano=2027).year == 2027
    else:
        with pytest.raises(InvalidParameterError, match=r"posterior ao corrente \(2026\)"):
            query.build_query(**args, max_registros=None, ano=2027)
