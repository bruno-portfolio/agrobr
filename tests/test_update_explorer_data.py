from __future__ import annotations

from typing import Any

import pytest

from scripts import update_explorer_data as audit


def lspa_rows() -> list[dict[str, Any]]:
    return [
        {
            "ano": 2026,
            "mes": 7,
            "produto": product,
            "variavel_cod": code,
            "unidade": unit,
            "valor": value,
        }
        for product, production, area in [("milho_1", 100, 10), ("milho_2", 50, 20)]
        for code, unit, value in [
            (35, "Toneladas", production),
            (216, "Hectares", area),
            (112, "Quilogramas por Hectare", 99999),
        ]
    ]


def test_lspa_produtividade_derivada():
    assert audit.lspa_metrics(lspa_rows(), "milho") == {"202607": [150, 30, 5000]}


def test_lspa_preserva_nulo():
    rows = lspa_rows()
    rows[0]["valor"] = None
    assert audit.lspa_metrics(rows, "milho") == {"202607": [None, 30, None]}


def test_lspa_rejeita_componente_ausente():
    with pytest.raises(ValueError, match="Esperadas 2"):
        audit.lspa_metrics(lspa_rows()[3:], "milho")


def test_lspa_rejeita_duplicacao():
    rows = lspa_rows()
    with pytest.raises(ValueError, match="duplicada"):
        audit.lspa_metrics(rows + rows[:1], "milho")


def test_cobertura_soma_biomas_e_estados():
    rows = [
        {"ano": 2024, "uf": uf, "bioma": biome, "classe_id": code, "area_ha": area}
        for uf, biome, code, area in [
            ("MT", "Amazonia", 3, 20),
            ("MT", "Cerrado", 3, 10),
            ("MT", "Cerrado", 39, 5),
            ("AM", "Amazonia", 15, 2),
            ("AM", "Amazonia", 33, 8),
        ]
    ]
    coverage, classes = audit.coverage_metrics(rows)
    assert coverage["2024"]["MT"] == [30, 5, 0, 0]
    assert coverage["2024"]["BR"] == [30, 5, 2, 8]
    assert sum(classes["2024"]["BR"].values()) == 45


def test_cobertura_rejeita_classe_agregada():
    with pytest.raises(ValueError, match="dupla contagem"):
        audit.coverage_metrics(
            [{"ano": 2024, "uf": "MT", "bioma": "Cerrado", "classe_id": 18, "area_ha": 1}]
        )


def test_totais_rejeita_uf_ausente():
    with pytest.raises(ValueError, match="territorial incompleta"):
        audit.verify_totals({"2024": {"BR": [1, 1, 1]}}, {"MT"})


def test_totais_rejeita_divergencia():
    with pytest.raises(ValueError, match="nacional divergente"):
        audit.verify_totals({"2024": {"BR": [10, 1, 1], "MT": [1, 1, 1]}}, {"MT"})


def test_lspa_rejeita_componente_trocado():
    rows = lspa_rows()
    rows[0]["produto"] = "soja"
    with pytest.raises(ValueError, match="Componente LSPA inesperado"):
        audit.lspa_metrics(rows, "milho")


def test_cobertura_rejeita_observacao_duplicada():
    row = {"ano": 2024, "uf": "MT", "bioma": "Cerrado", "classe_id": 3, "area_ha": 1}
    with pytest.raises(ValueError, match="duplicada"):
        audit.coverage_metrics([row, row])


def test_comparacao_distingue_precisao_de_alteracao_real():
    original = {"crops": {}, "estimates": {}, "coverage": {"2024": {"MT": [30, 5, 2, 8]}}}
    updated = original | {
        "coverage": {"2024": {"MT": [30.000000001, 5, None, 10]}},
        "audit": {
            "verified_at": "test",
            "calls": [],
            "production_records": 0,
            "coverage_records": 1,
        },
    }
    report = audit.comparison_report(original, updated)
    assert report["scalar_changes_before_tolerance"] == 3
    assert report["material_scalar_changes"] == 2
    assert report["new_null_values"] == 1
    assert report["differences"][0]["path"] == ["coverage", "2024", "MT", 2]
