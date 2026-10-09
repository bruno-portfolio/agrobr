from __future__ import annotations

import json
import warnings
from pathlib import Path

import pandas as pd
import pytest

from agrobr.desmatamento import api
from tests.helpers import assert_replay_served, install_replay_http, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data/desmatamento/geo_20260923"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
ALIASES = {
    "year": "ano",
    "state": "estado_original",
    "main_class": "classe",
    "area_km": "area_km2",
    "satellite": "satelite",
    "view_date": "data",
    "classname": "classe",
    "municipality": "municipio",
    "mun_geocod": "municipio_id",
    "areamunkm": "area_km2",
    "uf": "uf_original",
}
DATES = {"image_date", "publish_year", "view_date", "publish_month", "created_date", "pub_date"}
PARAMETROS = {"data_inicio": "inicio", "data_fim": "fim"}


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda case: case["id"])
async def test_geo_oficial_preserva_geometria_crs_e_atributos(case, monkeypatch):
    shapely_geometry = pytest.importorskip("shapely.geometry")
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    with sem_excecao():
        selection = {PARAMETROS.get(nome, nome): valor for nome, valor in case["selection"].items()}
        frame, meta = await getattr(api, case["api"])(**selection, return_meta=True)
    assert_replay_served(seen)
    raw = json.loads((GOLDEN / case["requests"][1]["file"]).read_bytes())
    oracle = case["oracle"]
    assert len(frame) == len(raw["features"]) == oracle["numberMatched"]
    assert frame.crs.to_epsg() == 4326
    assert meta.source_details["geometry"]["declared_crs_verified"] is True
    assert meta.selected_source == f"terrabrasilis_{case['api'].removesuffix('_geo')}_geo"
    assert frame["feature_id"].tolist() == oracle["feature_ids"]
    for index, feature in enumerate(raw["features"]):
        published = json.loads(json.dumps(shapely_geometry.mapping(frame.geometry.iloc[index])))
        assert published == feature["geometry"], (index, feature["id"])
        for name, expected in feature["properties"].items():
            observed = frame.iloc[index][ALIASES.get(name, name)]
            if expected is None:
                assert pd.isna(observed), (index, name)
            elif name in DATES:
                assert observed == pd.Timestamp(expected), (index, name)
            elif name == "scene_id":
                assert observed == str(expected), (index, name)
            else:
                assert observed == expected, (index, name)


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda case: case["id"])
async def test_geometria_invalida_publicada_vira_aviso_sem_reparo(case, monkeypatch):
    shapely_geometry = pytest.importorskip("shapely.geometry")
    install_replay_http(monkeypatch, case, GOLDEN)
    raw = json.loads((GOLDEN / case["requests"][1]["file"]).read_bytes())
    invalidas = sum(not shapely_geometry.shape(f["geometry"]).is_valid for f in raw["features"])
    selection = {PARAMETROS.get(nome, nome): valor for nome, valor in case["selection"].items()}
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await getattr(api, case["api"])(**selection, return_meta=True)
    publicados = [str(aviso.message) for aviso in avisos if "geometrias" in str(aviso.message)]
    assert int((~frame.geometry.is_valid).sum()) == invalidas
    if not invalidas:
        assert publicados == [] and "geometrias_invalidas" not in meta.source_details
        return
    produto = case["api"].removesuffix("_geo").upper()
    aviso = f"{produto}: {invalidas} de {len(raw['features'])} geometrias inválidas como publicadas pela fonte; use make_valid antes de operações espaciais."
    assert publicados == [aviso] and meta.validation_warnings.count(aviso) == 1
    assert (meta.source_details["geometrias_invalidas"], meta.source_details["geometrias"]) == (
        invalidas,
        len(raw["features"]),
    )
