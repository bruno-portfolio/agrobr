from __future__ import annotations

import csv
import io

import pydantic
import pytest

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.zarc import models
from agrobr.zarc.models import extract_safras, match_safra_resource
from tests.helpers import zarc_csv


def test_match_safra_resource():
    resources = [
        {"name": "Tabua de Risco - Safra 2025/2026", "url": "https://x/2025.csv", "format": "CSV"},
        {"name": "Tabua de Risco - Safra 2024/2025", "url": "https://x/2024.csv", "format": "CSV"},
        {"name": "Dicionario", "url": "https://x/dict.pdf", "format": "PDF"},
    ]
    assert match_safra_resource(resources, "2025/2026") == "https://x/2025.csv"
    assert match_safra_resource(resources, "2024/2025") == "https://x/2024.csv"
    assert match_safra_resource(resources, "2023/2024") is None


def test_match_safra_perene():
    resources = [
        {"name": "Tabua de Risco - Safra perene", "url": "https://x/perene.csv", "format": "CSV"},
        {"name": "Tabua de Risco - Safra 2025/2026", "url": "https://x/2025.csv", "format": "CSV"},
    ]
    assert match_safra_resource(resources, "perene") == "https://x/perene.csv"


def test_extract_safras_sorted_perene_last():
    resources = [
        {"name": "Safra perene", "url": "https://x/p.csv", "format": "CSV"},
        {"name": "Safra 2025/2026", "url": "https://x/25.csv", "format": "CSV"},
        {"name": "Safra 2016/2017", "url": "https://x/16.csv", "format": "CSV"},
        {"name": "Safra 2020/2021", "url": "https://x/20.csv", "format": "CSV"},
        {"name": "Dicionario", "url": "https://x/d.pdf", "format": "PDF"},
    ]
    result = extract_safras(resources)
    assert result == ["2016/2017", "2020/2021", "2025/2026", "perene"]


def test_duplicate_resource_selection_is_ambiguous():
    resources = [
        {"name": "Safra 2026/2027", "url": "https://x/one.csv", "format": "CSV"},
        {"name": "Safra 2026/2027", "url": "https://x/two.csv", "format": "CSV"},
    ]
    with pytest.raises(ParseError, match="ambígua"):
        match_safra_resource(resources, "2026/2027")


def test_one_resource_two_seasons_is_ambiguous():
    resources = [
        {"name": "Safras 2026/2027 e 2025/2026", "url": "https://x/a.csv", "format": "CSV"}
    ]
    with pytest.raises(ParseError, match="ambígua"):
        extract_safras(resources)


@pytest.mark.parametrize("name", ["Safra 2026/2028", "Safra ２０２６/２０２７", "Safra 12026/2027"])
def test_nonconsecutive_or_non_ascii_resource_not_selected(name):
    assert extract_safras([{"name": name, "url": "https://x/a.csv", "format": "CSV"}]) == []


def test_empty_format_requires_csv_url():
    resources = [{"name": "Safra 2026/2027", "url": "https://x/a.pdf", "format": ""}]
    assert match_safra_resource(resources, "2026/2027") is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("solo_codigo", True),
        ("solo_codigo", 1),
        ("ciclo_codigo", 20.0),
        ("cultura_original", None),
        ("nm_codigo", 0),
        ("produtividade_texto", 0.75),
        ("riscos", tuple([False] * 36)),
        ("riscos", tuple([0] * 36)),
        ("riscos", tuple([None] * 36)),
        ("riscos", tuple(["0"] * 35)),
        ("riscos", tuple(["0"] * 37)),
    ],
)
def test_external_record_rejects_unpublished_types(field, value):
    header, row = list(csv.reader(io.StringIO(zarc_csv().decode("utf-8-sig")), delimiter=";"))
    published = dict(zip(header, row, strict=True))
    values = {
        constants.ZARC_CSV_TO_OUTPUT[name]: raw
        for name, raw in published.items()
        if name not in constants.ZARC_RISK_COLUMNS
    }
    values["riscos"] = tuple(published[name] for name in constants.ZARC_RISK_COLUMNS)
    values[field] = value
    with pytest.raises(pydantic.ValidationError):
        models.ZarcRecord.model_validate(values)
