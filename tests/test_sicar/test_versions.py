from __future__ import annotations

import copy
import hashlib
import itertools
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import contracts, datasets
from agrobr.alt.sicar import api, client, parser
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures, sicar_feature_collection

FeatureCaptures = dict[str, list[dict[str, Any]]]

FIXTURE = Path(__file__).parents[1] / "golden_data/sicar/versoes_20260916"


@pytest.fixture
def official_features() -> dict[str, list[dict[str, Any]]]:
    manifest = json.loads((FIXTURE / "metadata.json").read_text(encoding="utf-8"))
    result = {}
    for resource in manifest["resources"]:
        content = (FIXTURE / resource["file"]).read_bytes()
        assert hashlib.sha256(content).hexdigest() == resource["sha256"]
        result[resource["file"]] = json.loads(content)["features"]
    return result


def instante(texto: str | None) -> str | None:
    if texto is None:
        return None
    return datetime.fromisoformat(texto).isoformat().replace("+00:00", "Z")


def descarte(descartada: dict[str, Any], mantida: dict[str, Any], criterio: str) -> dict[str, Any]:
    valores = descartada["properties"]
    return {
        "cod_imovel": valores["cod_imovel"],
        "feature_id": descartada["id"],
        "feature_id_mantida": mantida["id"],
        "criterio": criterio,
        "data_atualizacao": instante(valores.get("data_atualizacao")),
        "data_criacao": instante(valores["dat_criacao"]),
    }


@pytest.mark.parametrize("entrypoint", [api.imoveis, datasets.cadastro_rural])
@pytest.mark.parametrize(
    "uf,criterion,winner",
    [
        ("GO", "data_atualizacao", "2026-09-16T11:41:07.464Z"),
        ("RS", "data_criacao", "2026-09-16T18:32:25.534Z"),
    ],
)
async def test_versoes_reais_selecionadas_e_auditadas(
    monkeypatch: pytest.MonkeyPatch,
    official_features: FeatureCaptures,
    entrypoint: Any,
    uf: str,
    criterion: str,
    winner: str,
):
    features = official_features[f"{uf.lower()}_duplicates.json"]
    fetch = AsyncMock(
        side_effect=[b'<FeatureCollection numberMatched="2"/>', sicar_feature_collection(features)]
    )
    monkeypatch.setattr(client, "fetch_wfs", fetch)

    frame, meta = await entrypoint(
        uf, municipio=features[0]["properties"]["cod_municipio_ibge"], return_meta=True
    )

    assert len(frame) == meta.records_count == 1
    assert frame.iloc[0][criterion] == pd.Timestamp(winner)
    assert frame["cod_imovel"].is_unique and "feature_id" not in frame
    assert meta.source_details.get("sicar") == {
        "anunciados": 2,
        "features_unicas": 2,
        "codigos_colapsados": 1,
        "versoes_descartadas_total": 1,
        "versoes_descartadas": [descarte(features[0], features[1], criterion)],
        "versoes_descartadas_truncadas": False,
        "criterios": {"data_atualizacao": 0, "data_criacao": 0, "feature_id": 0} | {criterion: 1},
    }
    assert len(meta.validation_warnings) == 1
    assert "1 codigos" in meta.validation_warnings[0]
    assert fetch.await_count == 2
    contracts.validate_dataset(frame, "cadastro_rural")


def test_criterio_do_grupo_misto_independe_da_ordem(official_features: FeatureCaptures):
    features = [copy.deepcopy(official_features["go_duplicates.json"][0]) for _ in range(3)]
    dates = [
        ("2026-01-01T00:00:00Z", "2026-05-01T00:00:00Z"),
        ("2026-03-01T00:00:00Z", "2026-04-01T00:00:00Z"),
        ("2026-02-01T00:00:00Z", None),
    ]
    for index, (feature, (created, updated)) in enumerate(zip(features, dates, strict=True), 1):
        feature["id"] = f"sicar_imoveis_go.{index}"
        feature["properties"].update(dat_criacao=created, data_atualizacao=updated, area=index)
    with collect_failures() as check:
        for order in itertools.permutations(range(3)):
            with check(order):
                details: dict[str, Any] = {}
                frame = parser.parse_imoveis_json(
                    [sicar_feature_collection([features[index] for index in order])],
                    source_details=details,
                )
                assert len(frame) == 1 and frame.iloc[0]["area_ha"] == 2.0
                assert details["features_unicas"] == 3 and details["codigos_colapsados"] == 1
                assert details["versoes_descartadas_total"] == 2
                assert details["criterios"] == {
                    "data_atualizacao": 0,
                    "data_criacao": 2,
                    "feature_id": 0,
                }
                assert [item["feature_id"] for item in details["versoes_descartadas"]] == [
                    "sicar_imoveis_go.1",
                    "sicar_imoveis_go.3",
                ]


def test_desempate_compara_sufixo_numerico_e_registra_criterio(official_features: FeatureCaptures):
    with collect_failures() as check:
        for scenario in ["update_tie", "creation_tie", "no_dates", "missing_creation"]:
            with check(scenario):
                features = copy.deepcopy(official_features["go_duplicates.json"])
                for feature, suffix in zip(features, [9, 10], strict=True):
                    feature["id"] = f"sicar_imoveis_go.{suffix}"
                    feature["properties"].update(
                        area=suffix,
                        dat_criacao="2026-01-01T00:00:00Z",
                        data_atualizacao="2026-05-01T00:00:00Z"
                        if scenario == "update_tie"
                        else None,
                    )
                    if scenario == "no_dates" or (scenario == "missing_creation" and suffix == 10):
                        feature["properties"]["dat_criacao"] = None
                if scenario == "update_tie":
                    features[1]["properties"]["data_atualizacao"] = "2026-04-30T21:00:00-03:00"
                details: dict[str, Any] = {}

                frame = parser.parse_imoveis_json(
                    [sicar_feature_collection(features)], source_details=details
                )

                assert frame.iloc[0]["area_ha"] == 10.0
                assert details["versoes_descartadas"][0]["feature_id"] == "sicar_imoveis_go.9"
                assert details["versoes_descartadas"][0]["criterio"] == "feature_id"
                assert details["criterios"] == {
                    "data_atualizacao": 0,
                    "data_criacao": 0,
                    "feature_id": 1,
                }


def test_atualizacao_prevalece_sobre_criacao_e_id(official_features: FeatureCaptures):
    features = copy.deepcopy(official_features["go_duplicates.json"])
    features[0]["properties"].update(data_atualizacao="2026-10-01T00:00:00Z", area=100)
    details: dict[str, Any] = {}

    frame = parser.parse_imoveis_json([sicar_feature_collection(features)], source_details=details)

    assert frame.iloc[0]["area_ha"] == 100.0
    assert details["versoes_descartadas"][0]["feature_id"] == features[1]["id"]
    assert details["criterios"]["data_atualizacao"] == 1


def test_lista_limitada_preserva_contagem_total_dos_descartes(official_features: FeatureCaptures):
    with collect_failures() as check:
        for amount in (1001, 1002):
            with check(amount):
                features = [
                    copy.deepcopy(official_features["go_duplicates.json"][0]) for _ in range(amount)
                ]
                for index, feature in enumerate(features, 1):
                    feature["id"] = f"sicar_imoveis_go.{index}"
                    feature["properties"]["area"] = index
                details: dict[str, Any] = {}

                frame = parser.parse_imoveis_json(
                    [sicar_feature_collection(features)], source_details=details
                )

                assert len(frame) == 1 and frame.iloc[0]["area_ha"] == float(amount)
                assert details["codigos_colapsados"] == 1
                assert (
                    details["versoes_descartadas_total"]
                    == details["criterios"]["feature_id"]
                    == amount - 1
                )
                assert len(details["versoes_descartadas"]) == 1000
                assert details["versoes_descartadas_truncadas"] == (amount > 1001)


async def test_varredura_mg_repetindo_feature_real_continua_falhando(
    monkeypatch: pytest.MonkeyPatch, official_features: FeatureCaptures
):
    before = official_features["mg_page_before.json"]
    after = official_features["mg_page_after.json"]
    assert before[0]["id"] == after[0]["id"] == "sicar_imoveis_mg.13828149"
    fetch = AsyncMock(
        side_effect=[
            b'<FeatureCollection numberMatched="2"/>',
            sicar_feature_collection(before, number_matched=2),
            sicar_feature_collection(after, number_matched=2),
        ]
    )
    monkeypatch.setattr(client, "PAGE_SIZE", 1)
    monkeypatch.setattr(client, "fetch_wfs", fetch)

    with pytest.raises(ParseError, match=r"Varredura inconsistente.*13828149.*repita a consulta"):
        await client.fetch_imoveis("MG")

    assert fetch.await_count == 3


def test_parser_recusa_feature_repetida_sem_colapsar(official_features: FeatureCaptures):
    features = official_features["mg_page_before.json"]
    one = sicar_feature_collection(features)
    with collect_failures() as check:
        for pages in ([one, one], [sicar_feature_collection(features + features)]):
            with (
                check(len(pages)),
                pytest.raises(ParseError, match=r"Varredura inconsistente.*repita a consulta"),
            ):
                parser.parse_imoveis_json(pages)


async def test_consulta_vazia_preserva_contrato_e_informa_zeros(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        client, "fetch_wfs", AsyncMock(side_effect=[b'<FeatureCollection numberMatched="0"/>'])
    )

    frame, meta = await api.imoveis("GO", municipio=5205802, return_meta=True)

    assert frame.empty
    assert meta.source_details.get("sicar") == {
        "anunciados": 0,
        "features_unicas": 0,
        "codigos_colapsados": 0,
        "versoes_descartadas_total": 0,
        "versoes_descartadas": [],
        "versoes_descartadas_truncadas": False,
        "criterios": {"data_atualizacao": 0, "data_criacao": 0, "feature_id": 0},
    }
    assert not meta.validation_warnings
    contracts.validate_dataset(frame, "sicar_imoveis")


async def test_resumo_municipal_agrega_ocorrencias_selecionadas(
    monkeypatch: pytest.MonkeyPatch, official_features: FeatureCaptures
):
    features = official_features["go_duplicates.json"]
    monkeypatch.setattr(
        client,
        "fetch_wfs",
        AsyncMock(
            side_effect=[
                b'<FeatureCollection numberMatched="2"/>',
                sicar_feature_collection(features),
            ]
        ),
    )

    frame, meta = await api.resumo("GO", municipio=5205802, return_meta=True)

    assert frame.iloc[0]["total"] == 1
    assert frame.iloc[0]["area_total_ha"] == 164.4076
    assert meta.source_details["sicar"]["versoes_descartadas_total"] == 1
    assert len(meta.validation_warnings) == 1


async def test_codigo_entre_paginas_com_drift_confere_features_antes_da_selecao(
    monkeypatch: pytest.MonkeyPatch, official_features: FeatureCaptures
):
    features = copy.deepcopy(official_features["go_duplicates.json"])
    third = copy.deepcopy(features[1])
    third["id"] = "sicar_imoveis_go.99999999"
    third["properties"]["cod_imovel"] += "-B"
    fetch = AsyncMock(
        side_effect=[
            b'<FeatureCollection numberMatched="2"/>',
            sicar_feature_collection(features[:1], number_matched=2),
            sicar_feature_collection(features[1:], number_matched=3),
            sicar_feature_collection([third], number_matched=3),
        ]
    )
    monkeypatch.setattr(client, "PAGE_SIZE", 1)
    monkeypatch.setattr(client, "fetch_wfs", fetch)

    frame, meta = await datasets.cadastro_rural("GO", municipio=5205802, return_meta=True)

    assert len(frame) == 2 and fetch.await_count == 4
    assert meta.source_details["sicar"]["anunciados"] == 3
    assert meta.source_details["sicar"]["features_unicas"] == 3
    assert meta.source_details["sicar"]["versoes_descartadas_total"] == 1
    assert len(meta.validation_warnings) == 2
    assert "2 para 3" in meta.validation_warnings[0]
    assert "1 codigos" in meta.validation_warnings[1]


def test_criterios_contam_cada_descarte_contra_vencedor_final(official_features: FeatureCaptures):
    features = [copy.deepcopy(official_features["go_duplicates.json"][0]) for _ in range(5)]
    for index, feature in enumerate(features, 1):
        feature["id"] = f"sicar_imoveis_go.{index}"
        feature["properties"]["area"] = index
        if index in (2, 3):
            feature["properties"]["data_atualizacao"] = "2026-10-01T00:00:00Z"
        if index > 3:
            feature["properties"]["cod_imovel"] += "-B"
            feature["properties"].update(dat_criacao=None, data_atualizacao=None)
    details: dict[str, Any] = {}

    frame = parser.parse_imoveis_json([sicar_feature_collection(features)], source_details=details)

    assert frame["area_ha"].tolist() == [3.0, 5.0]
    assert details["codigos_colapsados"] == 2 and details["versoes_descartadas_total"] == 3
    assert details["criterios"] == {"data_atualizacao": 1, "data_criacao": 0, "feature_id": 2}
    assert [item["criterio"] for item in details["versoes_descartadas"]] == [
        "data_atualizacao",
        "feature_id",
        "feature_id",
    ]
