from __future__ import annotations

import json
import warnings
from pathlib import Path

import pandas as pd
import pytest

from agrobr import embrapa_solos
from agrobr.embrapa_solos import parser
from agrobr.exceptions import ParseError
from tests.helpers import embrapa_solos_features, install_embrapa_solos_wfs

GOLDEN = Path(__file__).parents[1] / "golden_data/embrapa_solos/official_20260907"


@pytest.fixture
def page():
    return {"type": "FeatureCollection", "features": embrapa_solos_features()[:1]}


@pytest.fixture
def parse():
    return lambda value, product="perfis", geo=False: parser.parse_page(
        json.dumps(value).encode(), product=product, include_geometry=geo
    )


@pytest.mark.parametrize("field", ["numberMatched", "numberReturned", "totalFeatures"])
def test_counts_reject_negative_zero_token(field):
    content = ('{"type":"FeatureCollection","features":[],"' + field + '":-0}').encode()
    with pytest.raises(ParseError):
        parser.parse_page(content, product="perfis", include_geometry=False)


@pytest.mark.parametrize("value", [None, "", " ", "\t\n", 1, True])
def test_feature_id_rejects_unsupported_representation(page, parse, value):
    page["features"][0]["id"] = value
    with pytest.raises(ParseError):
        parse(page)


@pytest.mark.parametrize(
    "value,expected", [(" sc ", "SC"), ("", None), (None, None), ("NULL", None), ("XX", None)]
)
def test_uf_original_and_normalized_are_separate(page, parse, value, expected):
    page["features"][0]["properties"]["uf"] = value
    result = parse(page)
    frame = parser.build_frame(result.records, product="perfis")
    assert (
        pd.isna(frame.loc[0, "uf_original"])
        if value is None
        else frame.loc[0, "uf_original"] == value
    )
    assert pd.isna(frame.loc[0, "uf"]) if expected is None else frame.loc[0, "uf"] == expected
    assert ("unknown_uf" in result.diagnostics) == (expected is None)


def test_diagnostic_counts_complete_examples_bounded(page, parse):
    page["features"][0]["properties"].update(uf="XX", gcs_latitu=91, gcs_longit=-181)
    page["features"] *= 31
    result = parse(page)
    for name in ("unknown_uf", "latitude_outside_range", "longitude_outside_range"):
        assert result.diagnostics[name] == {
            "count": 31,
            "examples": [{"row_index": n} for n in range(10)],
            "examples_omitted": 21,
        }
    assert len(result.records) == 31


@pytest.mark.parametrize(
    "field,value",
    [
        ("numberReturned", 2),
        ("numberMatched", 0),
        ("numberMatched", "1"),
        ("totalFeatures", None),
        ("numberReturned", True),
    ],
)
def test_envelope_invalid_count_rejected(page, parse, field, value):
    page[field] = value
    with pytest.raises(ParseError):
        parse(page)


def test_conflicting_next_links_rejected(page, parse):
    page.update(
        next="https://example.test/1", links=[{"rel": "next", "href": "https://example.test/2"}]
    )
    with pytest.raises(ParseError):
        parse(page)


@pytest.mark.parametrize(
    ("column", "value"),
    [("ano", "1997/1998"), ("ano", ""), ("data_colet", "2024-02-30"), ("data_colet", "08/02/2006")],
)
def test_calendario_invalido_nao_vira_ausente(page, parse, column, value):
    page["features"][0]["properties"][column] = value
    records = parse(page).records
    with pytest.raises(ParseError):
        parser.build_frame(records, product="perfis")


@pytest.mark.parametrize("funcao", [embrapa_solos.perfis, embrapa_solos.perfis_geo])
async def test_data_colet_com_ano_fora_de_1900_2099_vira_nat_com_aviso(funcao, monkeypatch):
    features = embrapa_solos_features()
    if funcao is embrapa_solos.perfis_geo:
        pytest.importorskip("geopandas")
        for feature in features:
            propriedades = feature["properties"]
            feature["geometry"] = {
                "type": "Point",
                "coordinates": [propriedades["gcs_longit"], propriedades["gcs_latitu"]],
            }
    features[1]["properties"]["data_colet"] = "0982-11-01"
    features[4]["properties"]["data_colet"] = "1892-07-14"
    esperadas = {
        feature["id"]: None if indice in (1, 4) else feature["properties"]["data_colet"]
        for indice, feature in enumerate(features)
    }
    install_embrapa_solos_wfs(monkeypatch, features)
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        frame, meta = await funcao(max_registros=None, tamanho_pagina=2, return_meta=True)
    avisos = [str(aviso.message) for aviso in capturados if "data_colet" in str(aviso.message)]
    assert avisos == [
        "embrapa_solos: 2 valor(es) de data_colet viraram NaT (data ilegível ou com ano fora de "
        "1900–2099)."
    ]
    assert [aviso for aviso in meta.validation_warnings if "data_colet" in aviso] == avisos
    assert str(frame["data_colet"].dtype) == "datetime64[ns]"
    lidas = [None if pd.isna(data) else f"{data:%Y-%m-%d}" for data in frame["data_colet"].tolist()]
    assert dict(zip(frame["feature_id"], lidas)) == esperadas
