from __future__ import annotations

import pytest

from agrobr.embrapa_solos import query
from agrobr.exceptions import InvalidParameterError


@pytest.mark.parametrize(
    "kwargs",
    [
        {"product": "other"},
        {"include_geometry": 1},
        {"uf": "XX"},
        {"uf": 1},
        {"ordem": 2},
        {"ordem": "x"},
        {"max_registros": True},
        {"max_registros": 0},
        {"max_registros": 2.0},
        {"max_registros": "3"},
        {"tamanho_pagina": False},
        {"tamanho_pagina": 0},
        {"tamanho_pagina": 1001},
        {"include_geometry": True, "tamanho_pagina": 101},
        {"bbox": (0, 0, 1, 1), "tamanho_pagina": 101},
        {"bbox": (False, -1, 2, 3)},
        {"bbox": ("1", -1, 2, 3)},
        {"bbox": (1, 2, 3)},
        {"bbox": (float("nan"), -1, 2, 3)},
        {"bbox": (0, -1, float("inf"), 3)},
        {"bbox": (2, -1, 2, 3)},
        {"bbox": (-181, -1, 2, 3)},
        {"bbox": (0, -91, 2, 3)},
    ],
)
def test_invalid_selection(kwargs):
    values = {"product": "perfis", "include_geometry": False, "max_registros": 10, **kwargs}
    with pytest.raises(InvalidParameterError):
        query.build_query(**values)


def test_nested_requested_drift_rejected():
    original = query.build_query(product="perfis", include_geometry=False, max_registros=10)
    original.requested["max_registros"] = 2
    with pytest.raises(InvalidParameterError):
        query.validate_query(original)


@pytest.mark.parametrize(
    ("campo", "valor"),
    [("output_crs", "EPSG:4326"), ("fetch_geometry", True), ("fetch_crs", "EPSG:4326")],
)
def test_modo_adulterado_recusado_pela_consulta_canonica(campo, valor):
    original = query.build_query(
        product="perfis", include_geometry=False, max_registros=10, tamanho_pagina=10
    )
    adulterada = original.model_copy(update={campo: valor})
    with pytest.raises(InvalidParameterError, match="divergem da consulta canônica"):
        query.validate_query(adulterada)
