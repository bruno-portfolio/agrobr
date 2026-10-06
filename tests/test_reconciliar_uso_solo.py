from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from agrobr.desmatamento import models as desmatamento_models
from agrobr.queimadas import models as queimadas_models
from scripts import reconciliar_uso_solo as reconciliation

MANIFEST = json.loads(
    (
        Path(__file__).parent / "golden_data/reconciliacao_uso_solo_ambiente_20260918/manifest.json"
    ).read_text(encoding="utf-8")
)


def described(product: str, biome: str, layer: str) -> dict[str, Any]:
    geometry = desmatamento_models.layout_geometry_column(product, biome)
    properties = [
        {
            "name": name,
            "localType": sorted(reconciliation.TIPOS_PUBLICADOS[product][name])[0],
        }
        for name in desmatamento_models.layout_properties(product, biome)
    ]
    properties.append({"name": geometry, "localType": "MultiPolygon"})
    return {"featureTypes": [{"typeName": layer, "properties": properties}]}


@pytest.mark.parametrize(
    ("product", "biome", "layer"),
    [
        ("PRODES", "Cerrado", "yearly_deforestation"),
        ("PRODES", "Amazônia", "yearly_deforestation_biome"),
        ("DETER", "Amazônia", "deter_amz"),
        ("DETER", "Cerrado", "deter_cerrado"),
    ],
)
def test_compare_wfs_layer_aceita_a_camada_publicada(product: str, biome: str, layer: str):
    result = reconciliation.compare_wfs_layer(
        product, biome, layer, described(product, biome, layer), 200
    )
    assert result["status"] == "ok"
    assert result["propriedades_declaradas"] == len(
        desmatamento_models.layout_properties(product, biome)
    )


@pytest.mark.parametrize(
    "mutacao",
    [
        "propriedade_nova",
        "propriedade_removida",
        "tipo_trocado",
        "camada_ausente",
        "sem_geometria",
        "sem_propriedades",
        "propriedades_ausentes",
        "geometria_string",
    ],
)
def test_compare_wfs_layer_recusa_deriva_de_layout(mutacao: str):
    payload = described("PRODES", "Cerrado", "yearly_deforestation")
    properties = payload["featureTypes"][0]["properties"]
    if mutacao == "sem_propriedades":
        payload["featureTypes"][0]["properties"] = []
    elif mutacao == "propriedades_ausentes":
        del payload["featureTypes"][0]["properties"]
    elif mutacao == "geometria_string":
        next(item for item in properties if item["name"] == "geom")["localType"] = "string"
    elif mutacao == "propriedade_nova":
        properties.append({"name": "novo_campo", "localType": "string"})
    elif mutacao == "propriedade_removida":
        payload["featureTypes"][0]["properties"] = [
            item for item in properties if item["name"] != "area_km"
        ]
    elif mutacao == "tipo_trocado":
        next(item for item in properties if item["name"] == "area_km")["localType"] = "string"
    elif mutacao == "camada_ausente":
        payload["featureTypes"][0]["typeName"] = "outra_camada"
    else:
        payload["featureTypes"][0]["properties"] = [
            item for item in properties if item["name"] != "geom"
        ]
    result = reconciliation.compare_wfs_layer(
        "PRODES", "Cerrado", "yearly_deforestation", payload, 200
    )
    assert result["status"] == "mismatch"
    assert result["problems"]


def test_compare_wfs_layer_recusa_resposta_sem_descricao():
    assert (
        reconciliation.compare_wfs_layer("DETER", "Cerrado", "deter_cerrado", None, 503)["status"]
        == "mismatch"
    )


def test_compare_focos_header_aceita_o_cabecalho_declarado():
    result = reconciliation.compare_focos_header(list(queimadas_models.COLUNAS_CSV))
    assert result["status"] == "ok"
    assert result["colunas_publicadas"] == len(queimadas_models.COLUNAS_CSV)


@pytest.mark.parametrize("mutacao", ["coluna_nova", "coluna_removida", "ordem", "vazio"])
def test_compare_focos_header_recusa_mudanca_de_colunas(mutacao: str):
    header = list(queimadas_models.COLUNAS_CSV)
    if mutacao == "coluna_nova":
        header.append("confianca")
    elif mutacao == "coluna_removida":
        header.remove("frp")
    elif mutacao == "ordem":
        header[0], header[1] = header[1], header[0]
    else:
        header = []
    assert reconciliation.compare_focos_header(header)["status"] == "mismatch"


def test_compare_workbook_identity_usa_os_bytes_da_captura():
    recorded = next(
        entry
        for entry in MANIFEST["files"]
        if entry["file"] == "../mapbiomas/collection11_official/response.xlsx"
    )
    headers = {"content-length": str(recorded["corpo_original_bytes"]), "etag": '"abc"'}
    result = reconciliation.compare_workbook_identity(headers, 200, recorded)
    assert result["status"] == "ok"
    assert "identidade" in result["nota"]
    assert result["etag"] == '"abc"'
    trocado = {**headers, "content-length": str(recorded["corpo_original_bytes"] + 1)}
    assert reconciliation.compare_workbook_identity(trocado, 200, recorded)["status"] == "mismatch"
    assert reconciliation.compare_workbook_identity(headers, 404, recorded)["status"] == "mismatch"
    assert reconciliation.compare_workbook_identity({}, 200, recorded)["status"] == "mismatch"
