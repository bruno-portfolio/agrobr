from __future__ import annotations

import gzip
import json
from functools import cache
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.alt import sicar
from agrobr.alt.sicar import parser
from scripts import reconciliar_sicar as reconciliation
from tests import helpers
from tests.helpers import collect_failures

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/sicar"
)
MANIFEST = json.loads((GOLDEN / "manifest.json").read_bytes())
SEM_COLAPSO = {
    "codigos_colapsados": 0,
    "versoes_descartadas_total": 0,
    "versoes_descartadas": [],
    "versoes_descartadas_truncadas": False,
    "criterios": {"data_atualizacao": 0, "data_criacao": 0, "feature_id": 0},
}


def resource_for(name: str) -> dict[str, Any]:
    return next(item for item in MANIFEST["resources"] if item["id"] == name)


@cache
def expected_rows(name: str) -> list[dict[str, Any]]:
    resource = resource_for(name)
    with gzip.open(GOLDEN / resource["oracle"]["file"], "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def assert_publication(frame: pd.DataFrame, name: str) -> None:
    resource = resource_for(name)
    columns = resource["oracle"]["columns"]
    rows = expected_rows(name)
    assert list(frame.columns) == [*columns, "cod_municipio"]
    assert len(frame) == len(rows) == resource["statistics"]["rows"]
    expected = [tuple(row["values"][field] for field in columns) for row in rows]
    actual = [
        tuple(
            None
            if pd.isna(value)
            else value.isoformat()
            if isinstance(value, pd.Timestamp)
            else value
            for value in row
        )
        for row in frame[columns].itertuples(index=False, name=None)
    ]
    assert actual == expected, name
    for field in ("data_criacao", "data_atualizacao"):
        assert str(frame[field].dtype) == "datetime64[ns, UTC]"
    for field in ("area_ha", "modulos_fiscais"):
        assert str(frame[field].dtype) == "float64"
    assert str(frame["cod_municipio_ibge"].dtype) == "Int64"


@pytest.mark.parametrize(
    ("case", "layer"),
    [
        pytest.param(case, layer, id=f"{case['id']}-{layer}")
        for case in MANIFEST["cases"]
        for layer in ("source", "dataset")
        if (case["id"], layer) != ("sicar_df_full", "dataset")
    ],
)
async def test_sicar_selecao_completa_todas_as_celulas(
    monkeypatch: pytest.MonkeyPatch, case: dict[str, Any], layer: str
):
    seen = helpers.install_replay_http(monkeypatch, case, GOLDEN)
    fetch = sicar.imoveis if layer == "source" else datasets.cadastro_rural
    consulta = {
        ("municipio" if chave == "cod_municipio" else chave): valor
        for chave, valor in case["query"].items()
    }
    try:
        frame, meta = await fetch(**consulta, return_meta=True)
    finally:
        helpers.assert_replay_served(seen)
    contagem_repetida = sum(
        pedido["match"]["params"].get("resultType") == "hits"
        and pedido["match"].get("occurrence", 0) > 1
        for pedido in case["requests"]
    )
    assert len(seen["served"]) == len(case["requests"]) - contagem_repetida
    assert_publication(frame, case["resource"])
    source = "sicar_wfs" if layer == "source" else "sicar"
    assert (meta.attempted_sources, meta.selected_source) == ([source], source)
    assert (meta.schema_version, meta.parser_version, meta.from_cache) == ("2.1", 2, False)
    assert meta.records_count == len(frame)
    assert meta.validation_warnings == []
    announced = case["announced_features"]
    assert meta.source_details.get("sicar") == {
        "anunciados": announced,
        "features_unicas": announced,
        **SEM_COLAPSO,
    }


@pytest.mark.parametrize("name", ["historical_go_page", "historical_rs_page"])
def test_sicar_pagina_historica_integral_e_escolha_da_versao(name: str):
    resource = resource_for(name)
    pages = [gzip.decompress((GOLDEN / file).read_bytes()) for file in resource["pages"]]
    details: dict[str, Any] = {}
    warnings: list[str] = []
    frame = parser.parse_imoveis_json(pages, source_details=details, validation_warnings=warnings)
    assert_publication(frame.sort_values("cod_imovel").reset_index(drop=True), name)
    statistics = resource["statistics"]
    assert details["features_unicas"] == statistics["features"] == 10000
    assert details["codigos_colapsados"] == statistics["repeated_codes"] == 1
    assert details["versoes_descartadas_total"] == len(statistics["discarded"]) == 1
    assert details["criterios"] == statistics["criteria"]
    for actual, expected in zip(
        details["versoes_descartadas"], statistics["discarded"], strict=True
    ):
        assert actual["feature_id"] == expected["discarded_feature"]
        assert actual["feature_id_mantida"] == expected["retained_feature"]
        assert actual["criterio"] == expected["criterion"]
    assert details["anunciados"] > details["features_unicas"]
    assert warnings


def pagina_n1(mutacao: str) -> bytes:
    resource = resource_for("sicar_df_full")
    payload = json.loads(gzip.decompress((GOLDEN / resource["pages"][-1]).read_bytes()))
    record = payload["features"][-1]["properties"]
    if mutacao == "fuso":
        record["dat_criacao"] = "2026-09-18T00:00:00"
    elif mutacao == "propriedade_nova":
        record["campo_novo"] = "1"
    elif mutacao == "id_repetido":
        payload["features"][-1] = payload["features"][0]
    elif mutacao == "contagem":
        payload["numberReturned"] += 1
    elif mutacao == "area_negativa":
        record["area"] = -1
    return json.dumps(payload).encode()


def test_sicar_n1_confere_schemas_e_recusa_deriva_da_pagina():
    df = next(item for item in MANIFEST["schemas"] if item["uf"] == "DF")
    with collect_failures() as check:
        for schema in MANIFEST["schemas"]:
            with check(schema["uf"]):
                raw = gzip.decompress((GOLDEN / schema["file"]).read_bytes())
                status = reconciliation.schema_reconciliation.compare_schema(raw, schema)
                assert status["status"] == "ok"
        for mutacao in (
            "original",
            "fuso",
            "propriedade_nova",
            "id_repetido",
            "contagem",
            "area_negativa",
        ):
            with check(mutacao):
                status = reconciliation.compare_page(pagina_n1(mutacao), df)["status"]
                assert status == ("ok" if mutacao == "original" else "mismatch")
