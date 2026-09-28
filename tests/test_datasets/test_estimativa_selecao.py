from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr import conab, contracts, datasets, ibge
from agrobr.datasets.estimativa_safra import _normalize_lspa
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    SourceUnavailableError,
)
from agrobr.models import MetaInfo
from tests.helpers import levanta_exatamente


def _conab_frame(levantamento: int = 3) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fonte": "conab",
                "produto": "soja",
                "safra": "2024/25",
                "uf": "MT",
                "area_plantada": 1.2,
                "area_colhida": float("nan"),
                "produtividade": 3000.0,
                "producao": 3.0,
                "levantamento": levantamento,
                "data_publicacao": pd.Timestamp("2025-01-15"),
            }
        ]
    )


def _lspa_frame(
    produtos: tuple[str, ...] = ("soja",),
    meses: tuple[int, ...] = (3,),
    *,
    ano: int = 2025,
    uf: str | None = "MT",
) -> pd.DataFrame:
    measurements = [(1200.0, 1000.0, 3000.0), (2200.0, 2000.0, 10000.0), (700.0, 500.0, 1000.0)]
    records = []
    for index, product in enumerate(produtos):
        for month in meses:
            for variable, unit, value in zip(
                ("Área plantada", "Área colhida", "Produção"),
                ("Hectares", "Hectares", "Toneladas"),
                measurements[index],
                strict=True,
            ):
                records.append(
                    {
                        "ano": ano,
                        "mes": month,
                        "localidade": "Mato Grosso" if uf else "Brasil",
                        "localidade_cod": 51 if uf else 1,
                        "produto": product,
                        "variavel": variable,
                        "unidade": unit,
                        "valor": value,
                        "fonte": "ibge_lspa",
                    }
                )
    return pd.DataFrame(records)


def _meta(source: str) -> MetaInfo:
    return MetaInfo(
        source=source,
        source_url=f"https://example.test/{source}",
        source_method="httpx",
        fetched_at=datetime(2025, 6, 1, tzinfo=UTC),
        selected_source=source,
        attempted_sources=[source],
    )


@pytest.mark.parametrize(
    "selectors,expected_source,expected_levantamento,expected_mes",
    [
        ({}, "conab", None, None),
        ({"fonte": "conab"}, "conab", None, None),
        ({"levantamento": 6}, "conab", 6, None),
        ({"fonte": "conab", "levantamento": 6}, "conab", 6, None),
        ({"mes": 3}, "ibge_lspa", None, 3),
        ({"mes": "03"}, "ibge_lspa", None, 3),
        ({"fonte": "ibge_lspa"}, "ibge_lspa", None, None),
        ({"fonte": "ibge_lspa", "mes": "03"}, "ibge_lspa", None, 3),
    ],
)
async def test_selection_routes_to_matching_source(
    selectors: dict[str, Any],
    expected_source: str,
    expected_levantamento: int | None,
    expected_mes: int | None,
):
    singleton = datasets.get_dataset("estimativa_safra")
    original_sources = tuple(singleton.info.sources)
    with (
        patch.object(
            conab,
            "safras",
            new_callable=AsyncMock,
            return_value=(_conab_frame(expected_levantamento or 3), _meta("conab")),
        ) as conab_api,
        patch.object(
            ibge,
            "lspa",
            new_callable=AsyncMock,
            return_value=(_lspa_frame(meses=(1, 3)), _meta("ibge_lspa")),
        ) as lspa_api,
    ):
        frame, meta = await datasets.estimativa_safra("soja", "2024/25", "MT", True, **selectors)

    assert tuple(singleton.info.sources) == original_sources
    selected, other = (conab_api, lspa_api) if expected_source == "conab" else (lspa_api, conab_api)
    selected.assert_awaited_once()
    other.assert_not_awaited()
    assert meta.attempted_sources == [expected_source]
    assert meta.selected_source == expected_source
    assert meta.contract_version == "3.1"
    assert set(frame["fonte"]) == {expected_source}
    if expected_source == "conab":
        assert selected.call_args.kwargs.get("levantamento") == expected_levantamento
        assert frame["ano_lspa"].isna().all()
        assert frame["mes_lspa"].isna().all()
    else:
        assert selected.call_args.kwargs["ano"] == 2025
        passed_month = selected.call_args.kwargs.get("mes")
        assert (int(passed_month) if passed_month is not None else None) == expected_mes
        assert frame["ano_lspa"].tolist() == [2025]
        assert frame["mes_lspa"].tolist() == [3]
        assert frame["levantamento"].isna().all()
    contracts.validate_dataset(frame, "estimativa_safra")


@pytest.mark.parametrize(
    "selectors,source",
    [
        ({"levantamento": 6}, "conab"),
        ({"fonte": "conab"}, "conab"),
        ({"mes": 3}, "ibge_lspa"),
        ({"fonte": "ibge_lspa"}, "ibge_lspa"),
    ],
)
async def test_explicit_selection_never_falls_back(selectors: dict[str, Any], source: str):
    with (
        patch.object(
            conab,
            "safras",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("CONAB indisponível"),
        ) as conab_api,
        patch.object(
            ibge,
            "lspa",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("LSPA indisponível"),
        ) as lspa_api,
        pytest.raises(SourceUnavailableError),
    ):
        await datasets.estimativa_safra("soja", safra="2024/25", **selectors)
    selected, other = (conab_api, lspa_api) if source == "conab" else (lspa_api, conab_api)
    selected.assert_awaited_once()
    other.assert_not_awaited()


@pytest.mark.parametrize(
    "damage",
    [
        "missing_component",
        "missing_variable",
        "duplicate",
        "mixed_year",
        "wrong_year",
        "mixed_locality",
        "wrong_locality",
        "wrong_unit",
        "wrong_product",
    ],
)
def test_normalization_rejects_incomplete_or_incompatible_dimensions(damage: str):
    raw = _lspa_frame(("milho_1", "milho_2"))
    if damage == "missing_component":
        raw = raw[raw["produto"] == "milho_1"]
    elif damage == "missing_variable":
        raw = raw[~((raw["produto"] == "milho_2") & (raw["variavel"] == "Produção"))]
    elif damage == "duplicate":
        raw = pd.concat([raw, raw.iloc[[0]]], ignore_index=True)
    elif damage == "mixed_year":
        raw.loc[0, "ano"] = 2024
    elif damage == "wrong_year":
        raw["ano"] = 2024
    elif damage == "mixed_locality":
        raw.loc[0, ["localidade_cod", "localidade"]] = [41, "Paraná"]
    elif damage == "wrong_locality":
        raw["localidade_cod"], raw["localidade"] = 41, "Paraná"
    elif damage == "wrong_unit":
        raw.loc[0, "unidade"] = "Quilômetros quadrados"
    else:
        raw["produto"] = "soja"
    with pytest.raises(ContractViolationError):
        _normalize_lspa(raw, "milho", "2024/25", "MT")


@pytest.mark.parametrize(
    "variable,column",
    [
        ("Área plantada", "area_plantada"),
        ("Área colhida", "area_colhida"),
        ("Produção", "producao"),
    ],
)
def test_normalization_explicit_missing_value_preserves_independent_metrics(
    variable: str, column: str
):
    raw = _lspa_frame(("milho_1", "milho_2"))
    raw.loc[(raw["produto"] == "milho_2") & (raw["variavel"] == variable), "valor"] = float("nan")
    row = _normalize_lspa(raw, "milho", "2024/25", "MT").iloc[0]

    assert pd.isna(row[column])
    for other_column, expected in {
        "area_plantada": 3.4,
        "area_colhida": 3.0,
        "producao": 13.0,
    }.items():
        if other_column != column:
            assert row[other_column] == expected
    if column == "area_plantada":
        assert row["produtividade"] == pytest.approx(13000 / 3)
    else:
        assert pd.isna(row["produtividade"])


@pytest.mark.parametrize(
    "selectors",
    [
        {"levantamento": 1, "mes": 1},
        {"fonte": "conab", "mes": "01"},
        {"fonte": "ibge_lspa", "levantamento": 1},
        {"fonte": "other"},
        {"fonte": True},
        {"levantamento": 0},
        {"levantamento": 13},
        {"levantamento": True},
        {"levantamento": 1.5},
        {"levantamento": "1"},
        {"mes": 0},
        {"mes": 13},
        {"mes": True},
        {"mes": 1.5},
        {"mes": "202501"},
        {"mes": "January"},
    ],
)
async def test_invalid_selection_rejected_before_source_io(selectors: dict[str, Any]):
    with (
        patch.object(conab, "safras", new_callable=AsyncMock) as conab_api,
        patch.object(ibge, "lspa", new_callable=AsyncMock) as lspa_api,
        levanta_exatamente(InvalidParameterError),
    ):
        await datasets.estimativa_safra("soja", safra="2024/25", **selectors)
    conab_api.assert_not_awaited()
    lspa_api.assert_not_awaited()
