from __future__ import annotations

import hashlib
import json
import warnings
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from agrobr import funai
from agrobr.funai import api

from .test_oficial import GEO_1, chamar, instalar, na_feicao, pagina

gpd = pytest.importorskip("geopandas")

RECORTE = Path(__file__).parents[1] / "golden_data" / "funai" / "area_declarada_ac_20260926"
AREA_DO_POLIGONO = {8601: 36362.0208, 31301: 31967.6367, 73878: 543430.106}
AVISO = "funai: "


def _recorte_do_ac() -> gpd.GeoDataFrame:
    manifesto = json.loads((RECORTE / "manifest.json").read_bytes())
    corpo = (RECORTE / manifesto["file"]).read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == manifesto["sha256"]
    frame = gpd.GeoDataFrame.from_features(json.loads(corpo)["features"], crs="EPSG:4326")
    return frame.rename(
        columns={
            "terrai_codigo": "codigo",
            "terrai_nome": "nome",
            "superficie_perimetro_ha": "area_ha",
        }
    ).astype({"codigo": "Int64", "nome": "string"})


def test_recorte_do_ac_avisa_as_3_terras_fora_da_tolerancia():
    meta = SimpleNamespace(validation_warnings=[], source_details={})
    with warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        api._avisar_area_divergente(_recorte_do_ac(), meta)

    terras = meta.source_details.get("area_divergente", [])
    assert [(terra["codigo"], terra["area_ha"]) for terra in terras] == [
        (8601, 32623.6443),
        (31301, 27263.5212),
        (73878, 421.0),
    ]
    assert {terra["codigo"]: terra["area_poligono_ha"] for terra in terras} == pytest.approx(
        AREA_DO_POLIGONO, abs=1e-3
    )
    assert len(meta.validation_warnings) == 1
    aviso = meta.validation_warnings[0]
    assert aviso.startswith(
        f"{AVISO}3 terra(s) com area_ha, a área declarada pela FUNAI, a mais de 5%"
    )
    assert "Mashco do Rio Chandless (73878): 421.0 ha declarados × 543430.1 ha no polígono" in aviso
    assert [str(item.message) for item in emitidos] == [aviso]


async def test_geo_com_area_declarada_divergente_avisa_no_meta(monkeypatch: pytest.MonkeyPatch):
    instalar(monkeypatch, pagina(na_feicao(0, "superficie_perimetro_ha", 421.0)))

    resultado, emitidos = await chamar(funai.terras_indigenas_geo, return_meta=True, **GEO_1)

    assert isinstance(resultado, tuple), resultado
    frame, meta = resultado
    terras = meta.source_details.get("area_divergente", [])
    assert frame["area_ha"].tolist() == [421.0]
    assert [(terra["codigo"], terra["area_ha"]) for terra in terras] == [
        (int(frame["codigo"].iloc[0]), 421.0)
    ]
    avisos = [aviso for aviso in meta.validation_warnings if aviso.startswith(AVISO)]
    assert len(avisos) == 1
    assert [aviso for aviso in emitidos if aviso.startswith(AVISO)] == avisos


async def test_geo_com_area_declarada_coerente_nao_avisa(monkeypatch: pytest.MonkeyPatch):
    instalar(monkeypatch)

    resultado, emitidos = await chamar(funai.terras_indigenas_geo, return_meta=True, **GEO_1)

    assert isinstance(resultado, tuple), resultado
    _, meta = resultado
    assert meta.source_details.get("area_divergente") is None
    assert not [aviso for aviso in meta.validation_warnings if aviso.startswith(AVISO)]
    assert not [aviso for aviso in emitidos if aviso.startswith(AVISO)]


async def test_tabela_sem_geometria_nao_compara_area(monkeypatch: pytest.MonkeyPatch):
    instalar(monkeypatch, pagina(na_feicao(0, "superficie_perimetro_ha", 421.0)))

    resultado, _ = await chamar(
        funai.terras_indigenas, return_meta=True, max_registros=60, tamanho_pagina=25
    )

    assert isinstance(resultado, tuple), resultado
    assert isinstance(resultado[0], pd.DataFrame)
    assert resultado[1].source_details.get("area_divergente") is None
