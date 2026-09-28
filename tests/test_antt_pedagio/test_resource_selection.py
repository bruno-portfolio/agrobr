from __future__ import annotations

import json
from itertools import permutations
from pathlib import Path

import pytest
from pydantic import ValidationError

from agrobr.alt.antt_pedagio import catalog
from agrobr.exceptions import SourceUnavailableError


def outcome(*arguments: object) -> object:
    try:
        return catalog.select_traffic(*arguments)
    except Exception as exc:
        return exc


def resource(filename: str, **fields: object) -> catalog.CatalogResource:
    return catalog.CatalogResource.model_validate(
        {
            "id": filename,
            "name": filename,
            "url": f"https://dados.antt.gov.br/{filename}",
            "format": "CSV",
            **fields,
        }
    )


@pytest.mark.parametrize("year", range(2010, 2027))
@pytest.mark.parametrize("reverse", [False, True])
def test_official_catalog_monthly_selection(year: int, reverse: bool):
    payload = json.loads(
        (Path(__file__).parent / "fixtures/ckan_trafego_20260906.json").read_bytes()
    )
    resources = [
        catalog.CatalogResource.model_validate(item) for item in payload["result"]["resources"]
    ]
    if reverse:
        resources.reverse()
    selected = outcome(resources, year, "mensal")
    suffix = "_mensal_consolidado" if year >= 2024 else ""
    assert not isinstance(selected, Exception), selected
    assert selected.url.endswith(f"/volume-trafego-praca-pedagio-{year}{suffix}.csv")


@pytest.mark.parametrize("year", [2024, 2025, 2026])
def test_official_catalog_daily_selection(year: int):
    payload = json.loads(
        (Path(__file__).parent / "fixtures/ckan_trafego_20260906.json").read_bytes()
    )
    resources = [
        catalog.CatalogResource.model_validate(item) for item in payload["result"]["resources"]
    ]
    selected = outcome(resources, year, "diaria")
    assert not isinstance(selected, Exception), selected
    assert selected.url.endswith(f"{year}_diario.csv")


def test_monthly_preference_independent_of_daily_revision():
    monthly = resource("2025_mensal.csv", last_modified="2025-01-01")
    consolidated = resource("2025_mensal_consolidado.csv", last_modified="2024-01-01")
    daily = resource("2025_diario.csv", last_modified="2026-01-01")
    for order in permutations([monthly, consolidated, daily]):
        assert outcome(list(order), 2025, "mensal") == consolidated
        assert outcome(list(order), 2025, "diaria") == daily


@pytest.mark.parametrize("revision", [None, "2026-01-01", "invalid", "2026-01-01T10:00:00Z"])
def test_same_priority_ambiguous_even_with_revision(revision: str | None):
    first = resource("a_2025_mensal.csv", last_modified=revision)
    second = resource("b_2025_mensal.csv", last_modified="2027-01-01")
    with pytest.raises(SourceUnavailableError, match="ambíguo"):
        catalog.select_traffic([first, second], 2025, "mensal")


@pytest.mark.parametrize(
    "candidate",
    [
        resource("volume_20250_mensal.csv"),
        resource("volume_x2025_mensal.csv"),
        resource("volume_2025x_mensal.csv"),
        resource("volume_2024.csv", name="Volume 2025"),
        resource("volume.csv", url="https://dados.antt.gov.br/2025-uuid/download/volume.csv"),
        resource("volume.csv", url="https://dados.antt.gov.br/volume.csv?ano=2025"),
        resource("volume_2025_mensal.json", format="JSON"),
        resource("volume_2025_mensal.pdf", format=""),
        resource("volume_2025_mensal.csv", url=""),
        resource("volume_2025_diario.csv", name="Volume 2025 mensal"),
        resource("volume_2024_2025_mensal.csv"),
    ],
)
def test_wrong_year_or_frequency_or_format_rejected(candidate: catalog.CatalogResource):
    selected = outcome([candidate], 2025, "mensal")
    assert isinstance(selected, SourceUnavailableError) and "ausente" in str(selected), selected


def test_unknown_2024_layout_does_not_substitute_monthly():
    with pytest.raises(SourceUnavailableError, match="ausente"):
        catalog.select_traffic([resource("volume_2024.csv")], 2024, "mensal")


def test_plazas_only_csv_and_no_revision_tiebreaker():
    item = resource("plazas.csv")
    assert catalog.select_plazas([item, resource("plazas.xlsx", format="XLSX")]) == item
    with pytest.raises(SourceUnavailableError, match="ausente"):
        catalog.select_plazas([resource("plazas.xlsx", format="XLSX")])
    with pytest.raises(SourceUnavailableError, match="ambíguo"):
        catalog.select_plazas([item, resource("other.csv", last_modified="2099-01-01")])


@pytest.mark.parametrize(
    "url",
    [
        "http://dados.antt.gov.br/data.csv",
        "https://other.gov.br/data.csv",
        "https://dados.antt.gov.br:444/data.csv",
        "https://user@dados.antt.gov.br/data.csv",
        "https://dados.antt.gov.br/data.csv#fragment",
    ],
)
def test_download_origin_guard(url: str):
    with pytest.raises(SourceUnavailableError):
        catalog.validate_download_url(url)


def test_catalog_duplicate_ids_and_unsuccessful_envelope_rejected():
    item = {"id": "a", "name": "a", "url": "https://dados.antt.gov.br/a.csv", "format": "CSV"}
    cases = [
        (
            catalog.CatalogPackage,
            {"id": "p", "name": "p", "resources": [item, item]},
            "IDs duplicados",
        ),
        (
            catalog.CatalogEnvelope,
            {"success": False, "result": {"id": "p", "name": "p", "resources": []}},
            "success=false",
        ),
    ]
    for model, payload, message in cases:
        try:
            model.model_validate(payload)
        except Exception as exc:
            caught = exc
        else:
            caught = None
        assert isinstance(caught, ValidationError) and message in str(caught), caught


@pytest.mark.parametrize(
    "candidate,frequency",
    [
        (resource("volume_2025_diario.csv", name="Volume 2025 mensal"), "diaria"),
        (resource("volume_2025_diario.csv"), "mensal"),
    ],
)
def test_frequency_never_borrows_other_family(candidate: catalog.CatalogResource, frequency: str):
    selected = outcome([candidate], 2025, frequency)
    assert isinstance(selected, SourceUnavailableError), selected


def test_percent_encoded_filename_is_decoded():
    item = resource(
        "arquivo", name="arquivo", url="https://dados.antt.gov.br/volume%2D2025_mensal.csv"
    )
    assert outcome([item], 2025, "mensal") == item


def test_accented_daily_name_selected():
    item = resource("volume_2025.csv", name="Volume 2025 Diário")
    assert outcome([item], 2025, "diaria") == item
