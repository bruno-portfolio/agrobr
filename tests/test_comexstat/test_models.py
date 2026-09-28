from __future__ import annotations

import pytest
from pydantic import ValidationError

from agrobr.comexstat import models


@pytest.fixture
def raw() -> dict[str, str]:
    return dict(
        zip(
            (
                "CO_ANO",
                "CO_MES",
                "CO_NCM",
                "CO_UNID",
                "CO_PAIS",
                "SG_UF_NCM",
                "CO_VIA",
                "CO_URF",
                "QT_ESTAT",
                "KG_LIQUIDO",
                "VL_FOB",
            ),
            (
                "2026",
                "01",
                "12019000",
                "10",
                "000",
                "EX",
                "00",
                "0000000",
                "9007199254740993",
                "0",
                "-0",
            ),
            strict=True,
        )
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("CO_NCM", "1201900"),
        ("CO_NCM", "１２019000"),
        ("CO_PAIS", "0"),
        ("CO_UNID", "0"),
        ("CO_VIA", "0"),
        ("CO_URF", "000000"),
        ("SG_UF_NCM", "ex"),
        ("SG_UF_NCM", " EX"),
        ("CO_MES", "13"),
        ("CO_ANO", "1996"),
        ("CO_MES", "1.0"),
        ("CO_ANO", True),
        ("KG_LIQUIDO", "-1"),
        ("KG_LIQUIDO", "1.0"),
        ("KG_LIQUIDO", " 1"),
        ("KG_LIQUIDO", "9223372036854775808"),
        ("QT_ESTAT", None),
        ("VL_FOB", "-1"),
        ("VL_FOB", "NaN"),
        ("VL_FOB", "Infinity"),
        ("VL_FOB", "1,2"),
        ("VL_FOB", "1e309"),
        ("VL_FOB", "1e-400"),
        ("VL_FOB", "1e" + "9" * 100),
        ("VL_FOB", " 0"),
        ("VL_FOB", 1.0),
    ],
)
def test_invalid_external_field_rejected(raw, field, value):
    raw[field] = value
    with pytest.raises(ValidationError):
        models.ExportRecord.model_validate(raw)


def test_unknown_external_member_rejected(raw):
    with pytest.raises(ValidationError):
        models.ExportRecord.model_validate(raw | {"unknown": "x"})


@pytest.mark.parametrize("produto", ["quinoa", 12, None])
def test_unknown_product_rejected(produto):
    with pytest.raises(ValueError):
        models.resolve_ncm(produto)
