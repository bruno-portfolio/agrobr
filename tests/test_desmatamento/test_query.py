from __future__ import annotations

import pytest

from agrobr.desmatamento import query
from agrobr.exceptions import InvalidParameterError


@pytest.mark.parametrize(
    "field,value",
    [
        ("ano", 2022),
        ("data_inicio", "2026-02-29"),
        ("data_inicio", "2026-1-01"),
        ("data_fim", "2026-01-01T00:00:00"),
        ("data_fim", True),
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
            data_inicio="2026-02-01",
            data_fim="2026-01-01",
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
