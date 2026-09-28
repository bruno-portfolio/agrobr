from __future__ import annotations

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.funai import query


@pytest.mark.parametrize(
    "kwargs",
    [
        {"include_geometry": 1},
        {"uf": 1},
        {"uf": "XX"},
        {"fase": []},
        {"fase": "Regularizada "},
        {"fase": "regularizada"},
        {"fase": ""},
        {"max_registros": True},
        {"max_registros": 0},
        {"max_registros": 2.0},
        {"max_registros": "3"},
        {"tamanho_pagina": False},
        {"tamanho_pagina": 0},
        {"tamanho_pagina": 1001},
        {"include_geometry": True, "tamanho_pagina": 101},
        {"bbox": (0, 0, 1, 1), "tamanho_pagina": 101},
        {"bbox": (False, 0, 1, 1)},
        {"bbox": ("0", 0, 1, 1)},
        {"bbox": (0, 1, 2)},
        {"bbox": (float("nan"), 0, 1, 1)},
        {"bbox": (0, 0, float("inf"), 1)},
        {"bbox": (2, 0, 1, 1)},
        {"bbox": (-181, 0, 1, 1)},
        {"bbox": (0, -91, 1, 1)},
    ],
)
def test_invalid_selectors(kwargs):
    with pytest.raises(InvalidParameterError):
        query.build_query(**{"include_geometry": False, "max_registros": 10, **kwargs})


@pytest.mark.parametrize("mutation", ["value", "unknown", "missing"])
def test_requested_mutation_rejected(mutation):
    original = query.build_query(include_geometry=False, max_registros=10)
    if mutation == "value":
        original.requested["max_registros"] = 2
    elif mutation == "unknown":
        original.requested["CQL_FILTER"] = "gid>0"
    else:
        del original.requested["uf"]
    with pytest.raises(InvalidParameterError):
        query.validate_query(original)


@pytest.mark.parametrize(
    ("campo", "valor"),
    [("output_crs", "EPSG:4326"), ("fetch_geometry", True), ("fetch_crs", "EPSG:4326")],
)
def test_modo_adulterado_recusado_pela_consulta_canonica(campo, valor):
    original = query.build_query(include_geometry=False, max_registros=10, tamanho_pagina=10)
    adulterada = original.model_copy(update={campo: valor})
    with pytest.raises(InvalidParameterError, match="divergem da consulta canônica"):
        query.validate_query(adulterada)
