from __future__ import annotations

import pytest

from scripts import update_conab_explorer_data as explorer


@pytest.mark.parametrize(
    "crop,season,year",
    [("soja", "2024/25", 2025), ("cafe", "2025/26", 2025), ("trigo", "2025/26", 2025)],
)
def test_ano_respeita_semantica_da_serie(crop, season, year):
    assert explorer.panel_year(crop, season) == year


def test_nacional_preserva_metricas_nulas():
    assert explorer.national_metrics(
        {"MT": [100, 20, 5000], "AM": [None, 10, None]}, ["MT", "AM"]
    ) == [None, 30, None]


def test_nacional_deriva_rendimento_sem_somar_taxas():
    assert explorer.national_metrics(
        {"MT": [100, 20, 5000], "AM": [50, 10, 5000]}, ["MT", "AM"]
    ) == [150, 30, 5000]


@pytest.mark.parametrize("current_year", [2026, 2027])
def test_historico_ausencia_uf_e_area_cafe(current_year):
    capture = {
        "request": {"params": {"produto": "cafe"}},
        "records_sha256": "test",
        "meta": {},
        "records": [
            {
                "produto": "cafe",
                "safra": f"{year}/{str(year + 1)[2:]}",
                "uf": "MT",
                "producao_mil_ton": 10,
                "area_em_producao_mil_ha": 5,
                "area_plantada_mil_ha": 8,
                "produtividade_kg_ha": 2000,
            }
            for year in range(2018, current_year)
        ],
    }
    result = explorer.build_history([capture], {"MT", "AM"}, year=current_year)
    assert str(current_year - 1) in result["history"]
    assert str(current_year) not in result["history"]
    assert list(result["current"]) == [str(current_year)]
    assert result["history"]["2025"]["cafe"]["MT"] == [10000, 5000, 2000]
    assert result["history"]["2025"]["cafe"]["AM"] == [None, None, None]
    assert result["history"]["2025"]["cafe"]["BR"] == [10000, 5000, 2000]
    assert (
        result["historyMeta"]["2025"]["cafe"]["coverage"]["nationalMethod"] == "sum_available_ufs"
    )


@pytest.mark.parametrize("year", [2026, 2027])
def test_atual_converte_milhares(year):
    capture = {
        "request": {"params": {"produto": "soja"}},
        "records_sha256": "test",
        "meta": {},
        "records": [
            {
                "produto": "soja",
                "safra": f"{year - 1}/{year % 100:02d}",
                "uf": "MT",
                "producao": 51611.5,
                "area_plantada": 13000,
                "produtividade": 3970,
                "levantamento": 11,
            }
        ],
    }
    production = {"sourceCalls": [], "current": {str(year): {}}, "currentMeta": {str(year): {}}}
    explorer.add_current(production, [capture], {"MT", "AM"}, year=year)
    assert production["current"][str(year)]["soja"]["MT"] == [51611500, 13000000, 3970]
    assert production["current"][str(year)]["soja"]["AM"] == [None, None, None]
