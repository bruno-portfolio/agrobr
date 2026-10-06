from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.zarc import api, cache
from scripts import reconciliar_zarc
from tests import helpers

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/zarc"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_bytes())


def consulta(case: dict[str, Any]) -> dict[str, Any]:
    return {
        ("produto" if nome == "cultura" else nome): valor for nome, valor in case["query"].items()
    }


ORACLE = json.loads((GOLDEN / "oracle.json").read_bytes())
LEGACY = json.loads((GOLDEN / "legacy_crops.json").read_bytes())
RESOURCES = {
    resource["original"]["family"].split(":", 1)[1]: resource for resource in MANIFEST["resources"]
}
CATALOG_URL = "https://dados.agricultura.gov.br/api/3/action/package_show?id=tabua-de-risco-zoneamento-agricola-de-risco-climatico"


def _request(url: str, path: Path, content_type: str) -> dict[str, Any]:
    address, params, skip = helpers.replay_signature(url)
    return {
        "match": {"path": address, "params": dict(params), "skip": skip},
        "file": str(path),
        "content_type": content_type,
    }


def install_replay(
    monkeypatch: Any, resource: dict[str, Any], body: Path | None = None
) -> dict[str, list[str]]:
    cache.clear()
    return helpers.install_replay_http(
        monkeypatch,
        {
            "requests": [
                _request(CATALOG_URL, GOLDEN / "catalog.json", "application/json"),
                _request(
                    resource["original"]["requested_url"],
                    body or GOLDEN / resource["derived_file"],
                    "text/csv",
                ),
            ]
        },
        ROOT,
    )


def expected_rows(case: dict[str, Any], *, original: bool = False) -> list[dict[str, Any]]:
    resource = RESOURCES[case["resource"]]
    rows = [
        copy.deepcopy(ORACLE[case["resource"]][index - 1])
        for index in case["expected_derived_positions"]
    ]
    if original:
        for row in rows:
            row["registro_origem"] = resource["selected_original_records"][
                row["registro_origem"] - 1
            ]
    return rows


def assert_values(frame: pd.DataFrame, expected: list[dict[str, Any]]) -> None:
    assert frame.columns.tolist() == [*MANIFEST["columns"], "cod_municipio"]
    assert frame["cod_municipio"].astype(object).where(
        frame["cod_municipio"].notna(), None
    ).tolist() == [int(codigo) for codigo in frame["geocodigo"]]
    publicado = frame[MANIFEST["columns"]]
    actual = publicado.astype(object).where(publicado.notna(), None).to_dict("records")
    assert actual == expected
    for column in [
        "solo_codigo",
        "ciclo_codigo",
        "registro_origem",
        *[f"dec{i}" for i in range(1, 37)],
    ]:
        assert str(frame[column].dtype) == "Int64"


def assert_meta(meta: Any, resource: dict[str, Any], rows: int, *, original: bool = False) -> None:
    receipt = resource["original"]
    assert meta.selected_source == "zarc"
    assert meta.attempted_sources == ["zarc"]
    assert meta.schema_version == meta.contract_version == "2.1"
    assert meta.source_url == receipt["requested_url"]
    assert meta.raw_content_hash == (receipt["sha256"] if original else resource["derived_sha256"])
    assert meta.raw_content_size == (
        receipt["size_bytes"] if original else resource["derived_bytes"]
    )
    assert meta.fetched_at.utcoffset() == timedelta(0)
    details = meta.source_details["parser"]
    assert details["validated_rows"] == (
        resource["source_rows"] if original else len(resource["selected_original_records"])
    )
    assert details["selected_rows"] == rows
    assert details["eof_reached"] is True
    assert details["origin_position"]["scope"] == "CSV body SHA256"
    assert details["risk_statistics"]["dec36"]
    assert meta.source_details["coverage"]["status"] == "size_checked"
    assert meta.source_details["coverage"].get("published_size_bytes") == meta.raw_content_size


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "layer", "mode"),
    [
        pytest.param(case, layer, mode, id=f"{mode}-{layer}-{case['id']}")
        for case in MANIFEST["cases"]
        for layer in ("source", "dataset")
        for mode in ("bypass", "warm")
        if layer == "source"
        or case["resource"] == "2026_2027"
        or (case["id"] == "2016_2017_selection" and mode == "bypass")
    ],
)
async def test_reconciliacao_zarc_publica(monkeypatch, case, layer, mode):
    resource = RESOURCES[case["resource"]]
    seen = install_replay(monkeypatch, resource)
    fetch = api.zoneamento if layer == "source" else datasets.zoneamento_agricola
    with helpers.sem_excecao():
        frame, meta = await fetch(**consulta(case), use_cache=mode != "bypass", return_meta=True)
    assert_values(frame, expected_rows(case))
    assert_meta(meta, resource, len(frame))
    assert not meta.from_cache
    assert meta.source_details["cache"]["status"] == (
        "bypass" if mode == "bypass" else "store_miss"
    )
    if mode == "warm":
        with helpers.sem_excecao():
            second, cached = await fetch(**consulta(case), use_cache=True, return_meta=True)
        assert_values(second, expected_rows(case))
        assert_meta(cached, resource, len(second))
        assert cached.from_cache
        assert cached.fetched_at == meta.fetched_at
        assert cached.cache_expires_at == meta.cache_expires_at
        assert cached.source_details["resource"] == meta.source_details["resource"]
    assert len(seen["served"]) == 2
    helpers.assert_replay_served(seen)
    cache.clear()


@pytest.mark.parametrize("resource", MANIFEST["resources"], ids=lambda item: item["derived_file"])
def test_zarc_golden_hash_e_ultimo_registro(resource):
    body = (GOLDEN / resource["derived_file"]).read_bytes()
    assert hashlib.sha256(body).hexdigest() == resource["derived_sha256"]
    rows = list(csv.reader(io.StringIO(body.decode("utf-8")), delimiter=";"))
    assert rows[0] == resource["headers"]
    assert rows[-1] == resource["last_cells"]
    assert resource["selected_original_records"][-1] == resource["last_original_record"]
    assert len(rows) - 1 == len(resource["selected_original_records"])
    assert len(resource["column_decisions"]) == 55


@pytest.mark.parametrize("resource", MANIFEST["resources"], ids=lambda item: item["derived_file"])
def test_zarc_n1_csv_controle(resource):
    assert (
        reconciliar_zarc.compare_csv(GOLDEN / resource["derived_file"], resource)["status"] == "ok"
    )


@pytest.mark.parametrize(
    "mutation", ["header", "width", "culture", "risk", "period", "cycle", "soil"]
)
def test_zarc_n1_detecta_deriva(tmp_path, mutation):
    resource = RESOURCES["2026_2027"]
    rows = list(
        csv.reader(
            io.StringIO((GOLDEN / resource["derived_file"]).read_text(encoding="utf-8")),
            delimiter=";",
        )
    )
    if mutation == "header":
        rows[0][-1] = "dec37"
    elif mutation == "width":
        rows[-1].pop()
    else:
        field, value = {
            "culture": ("Nome_cultura", "Cultura nova"),
            "risk": ("dec36", "90"),
            "period": ("SafraFin", "2028"),
            "cycle": ("Cod_Ciclo", "99"),
            "soil": ("Cod_Solo", "99"),
        }[mutation]
        rows[-1][rows[0].index(field)] = value
    path = tmp_path / "drift.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        csv.writer(stream, delimiter=";", lineterminator="\n").writerows(rows)
    assert reconciliar_zarc.compare_csv(path, resource)["status"] == "mismatch"


def test_zarc_n1_catalogo_controle():
    payload = json.loads((GOLDEN / "catalog.json").read_bytes())
    assert (
        reconciliar_zarc.compare_catalog(payload, MANIFEST["catalog_decisions"])["status"] == "ok"
    )


@pytest.mark.parametrize("mutation", ["new", "missing", "duplicate", "name", "url"])
def test_zarc_n1_catalogo_detecta_deriva(mutation):
    payload = json.loads((GOLDEN / "catalog.json").read_bytes())
    resources = payload["result"]["resources"]
    if mutation == "new":
        resources.append({**resources[-1], "id": "new-resource"})
    elif mutation == "missing":
        resources.pop()
    elif mutation == "duplicate":
        resources.append(copy.deepcopy(resources[-1]))
    else:
        resources[-1][mutation] += " changed"
    assert (
        reconciliar_zarc.compare_catalog(payload, MANIFEST["catalog_decisions"])["status"]
        == "mismatch"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "label", ["Arroz Sequeiro", "arroz_sequeiro", "Trigo Sequeiro", "trigo_sequeiro"]
)
@pytest.mark.parametrize("mode", ["bypass", "warm"])
async def test_zarc_cultura_legada_publicada(monkeypatch, label, mode):
    canonical = LEGACY["aliases"].get(label, label)
    seen = install_replay(monkeypatch, RESOURCES["2016_2017"], GOLDEN / LEGACY["derived_file"])
    fetch = api.zoneamento
    expected = [row for row in LEGACY["expected"] if row["cultura"] == canonical]
    with helpers.sem_excecao():
        frame, meta = await fetch(
            produto=label, safra="2016/2017", use_cache=mode != "bypass", return_meta=True
        )
    assert_values(frame, expected)
    assert meta.raw_content_hash == LEGACY["derived_sha256"]
    assert not meta.from_cache
    if mode == "warm":
        with helpers.sem_excecao():
            second, cached = await fetch(
                produto=label, safra="2016/2017", use_cache=True, return_meta=True
            )
        assert_values(second, expected)
        assert cached.from_cache
        assert cached.fetched_at == meta.fetched_at
        assert cached.raw_content_hash == meta.raw_content_hash
    assert len(seen["served"]) == 2
    helpers.assert_replay_served(seen)
    cache.clear()


def test_zarc_suplemento_legado_hash_e_multiplicidade():
    body = (GOLDEN / LEGACY["derived_file"]).read_bytes()
    assert hashlib.sha256(body).hexdigest() == LEGACY["derived_sha256"]
    assert len(LEGACY["expected"]) == len(LEGACY["selected_original_records"]) == 15
    assert LEGACY["selected_original_records"][-1] == RESOURCES["2016_2017"]["source_rows"]
