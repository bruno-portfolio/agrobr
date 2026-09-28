from __future__ import annotations

from agrobr.defensivos import models


def test_rename_maps_and_derived_columns_cover_public_tables():
    formulated = set(models.FORMULADOS_RENAME.values()) | {"composicao_texto"}
    assert set(models.FORMULADOS_PRODUCT_COLS) <= formulated
    assert set(models.AUTORIZACOES_COLS) <= formulated
    technical = set(models.TECNICOS_RENAME.values()) | {"composicao_texto"}
    assert set(models.TECNICOS_COLS) <= technical
    assert "situacao" not in models.TECNICOS_COLS
    assert "concentracao_valor" not in models.FORMULADOS_PRODUCT_COLS
    assert "concentracao_valor" not in models.TECNICOS_COLS
