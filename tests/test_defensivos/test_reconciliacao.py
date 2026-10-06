from __future__ import annotations

import copy
import csv
import gzip
import hashlib
import io
import json
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import constants, datasets, defensivos
from agrobr.exceptions import ParseError
from scripts import reconciliar_agrofit as reconciliation
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/agrofit"
)
MANIFEST = json.loads((GOLDEN / "manifest.json").read_bytes())
FILTER_REGISTERS = {
    "0314",
    "14017/Pré-Mistura",
    "658688",
    "07697",
    "08124",
    "22516",
    "12516",
    "9815",
}
FILTER_CASES = [
    case for case in MANIFEST["cases"] if case["query"]["nr_registro"] in FILTER_REGISTERS
]


def resource_for(kind: str) -> dict[str, Any]:
    return next(
        item for item in MANIFEST["resources"] if item["original"]["family"] == f"agrofit_{kind}"
    )


def install(
    monkeypatch: pytest.MonkeyPatch, replacements: dict[str, Path] | None = None
) -> dict[str, list[str]]:
    manifest = copy.deepcopy(MANIFEST)
    for request in manifest["requests"]:
        if replacements and request["file"] in replacements:
            request["file"] = str(replacements[request["file"]])
    return helpers.install_replay_http(monkeypatch, manifest, GOLDEN)


def frame_cells(frame: pd.DataFrame, columns: list[str]) -> Counter[tuple[Any, ...]]:
    return Counter(
        tuple(None if pd.isna(value) else value for value in row)
        for row in frame[columns].itertuples(index=False, name=None)
    )


def assert_records(frame: pd.DataFrame, rows: list[dict[str, Any]]) -> None:
    assert rows
    columns = list(rows[0])
    assert set(frame.columns) == set(columns)
    assert len(frame) == len(rows)
    expected = Counter(tuple(row[name] for name in columns) for row in rows)
    actual = frame_cells(frame, columns)
    assert (sum((expected - actual).values()), sum((actual - expected).values())) == (0, 0)


def assert_oracle(frame: pd.DataFrame, oracle: dict[str, Any], *, partial: bool = False) -> None:
    columns = oracle["columns"]
    assert set(columns) <= set(frame.columns) if partial else set(columns) == set(frame.columns)
    assert len(frame) == oracle["rows"]
    with gzip.open(GOLDEN / oracle["file"], "rt", encoding="utf-8") as stream:
        values = (json.loads(line)["values"] for line in stream)
        expected = Counter(tuple(row[name] for name in columns) for row in values)
    expected_rows = oracle.get("subset_rows", oracle["rows"])
    assert sum(expected.values()) == expected_rows
    if "subset_rows" in oracle:
        assert 0 < expected_rows <= oracle["rows"]
    actual = frame_cells(frame, columns)
    missing = sum((expected - actual).values())
    unexpected = 0 if "subset_rows" in oracle else sum((actual - expected).values())
    assert (missing, unexpected) == (0, 0)


@pytest.mark.parametrize(
    "layer,kind",
    [
        pytest.param("source", "tecnicos", id="source-tecnicos"),
        pytest.param("source", "formulados", id="source-formulados"),
        pytest.param("dataset", "tecnicos", id="dataset-tecnicos"),
        pytest.param("dataset", "formulados", marks=pytest.mark.slow, id="dataset-formulados"),
    ],
)
async def test_agrofit_populacao_integral_relacoes_e_componentes(
    monkeypatch: pytest.MonkeyPatch, kind: str, layer: str
):
    seen = install(monkeypatch)
    resource = resource_for(kind)
    product_api = (
        getattr(defensivos, kind) if layer == "source" else getattr(datasets, f"defensivos_{kind}")
    )
    product, meta = await product_api(use_cache=True, return_meta=True)
    assert_oracle(product, resource["oracles"]["products"], partial=kind == "tecnicos")
    repeated, cached = await product_api(use_cache=True, return_meta=True)
    assert_oracle(repeated, resource["oracles"]["products"], partial=kind == "tecnicos")
    assert product.dtypes.equals(repeated.dtypes)
    component_api = defensivos.composicao if layer == "source" else datasets.composicao_defensivos
    components = await component_api(tipo=kind, use_cache=True)
    expected_component_rows = 5678 if kind == "formulados" else 2993
    assert len(components) == meta.source_details["component_rows"] == expected_component_rows
    cohort_registers = resource["cohort_registers"]
    selected_components = components[components["nr_registro"].isin(cohort_registers)]
    assert_oracle(selected_components, resource["oracles"]["selected_components"])
    for case in MANIFEST["cases"]:
        if case["kind"] == kind:
            selected_product = product[product["nr_registro"] == case["query"]["nr_registro"]]
            assert_records(selected_product, [case["product"]])
    if kind == "formulados":
        authorization_api = (
            defensivos.autorizacoes if layer == "source" else datasets.autorizacoes_defensivos
        )
        authorizations = await authorization_api(use_cache=True)
        assert_oracle(authorizations, resource["oracles"]["autorizacoes"])
        assert len(authorizations) == resource["source_rows"]
    assert meta.raw_content_hash == cached.raw_content_hash == resource["original"]["sha256"]
    assert meta.raw_content_size == resource["original"]["bytes"]
    assert meta.source_details["source_rows"] == resource["source_rows"]
    assert meta.source_details["product_rows"] == resource["unique_identities"]
    assert meta.fetched_at == cached.fetched_at
    assert meta.fetched_at.utcoffset() == timedelta(0)
    ttl = timedelta(seconds=constants.DEFENSIVOS_CACHE_TTL_SECONDS)
    assert meta.cache_expires_at == cached.cache_expires_at == meta.fetched_at + ttl
    assert not meta.from_cache and cached.from_cache
    if layer == "source":
        assert (meta.source_method, cached.source_method) == ("httpx+csv", "cache")
    assert len(seen["served"]) == 1
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize(
    "case",
    [
        pytest.param(case, marks=pytest.mark.slow) if case["kind"] == "formulados" else case
        for case in FILTER_CASES
    ],
    ids=lambda case: case["id"],
)
async def test_agrofit_coorte_completa_filtro_publico(
    monkeypatch: pytest.MonkeyPatch, case: dict[str, Any], layer: str
):
    seen = install(monkeypatch)
    kind = case["kind"]
    fetch = (
        getattr(defensivos, kind) if layer == "source" else getattr(datasets, f"defensivos_{kind}")
    )
    product = await fetch(**case["query"], use_cache=True)
    assert_records(product, [case["product"]])
    component_api = defensivos.composicao if layer == "source" else datasets.composicao_defensivos
    components, meta = await component_api(
        tipo=kind, **case["query"], use_cache=True, return_meta=True
    )
    assert_records(components, case["components"])
    assert (meta.schema_version, meta.contract_version) == ("1.0", "1.0")
    if case["query"]["nr_registro"] in {"12516", "9815"}:
        assert meta.validation_warnings
        assert any(
            issue["nr_registro"] == case["query"]["nr_registro"]
            for issue in meta.source_details["unparsed_components"]
        )
    if kind == "formulados":
        authorization_api = (
            defensivos.autorizacoes if layer == "source" else datasets.autorizacoes_defensivos
        )
        frame = await authorization_api(**case["query"], use_cache=True)
        assert_records(frame, case["authorizations"])
    assert len(seen["served"]) == 1
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize(
    "kind",
    ["tecnicos", pytest.param("formulados", marks=pytest.mark.slow)],
)
async def test_agrofit_populacao_sem_cache(monkeypatch: pytest.MonkeyPatch, layer: str, kind: str):
    seen = install(monkeypatch)
    fetch = (
        getattr(defensivos, kind) if layer == "source" else getattr(datasets, f"defensivos_{kind}")
    )
    frame, meta = await fetch(use_cache=False, return_meta=True)
    assert_oracle(frame, resource_for(kind)["oracles"]["products"], partial=kind == "tecnicos")
    assert not meta.from_cache and meta.cache_key is None
    assert len(seen["served"]) == 1
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize("resource", MANIFEST["resources"], ids=lambda item: item["golden_file"])
def test_agrofit_corpos_e_oraculos_integros(resource: dict[str, Any]):
    digest = hashlib.sha256()
    size = 0
    with gzip.open(GOLDEN / resource["golden_file"], "rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    assert digest.hexdigest() == resource["original"]["sha256"]
    assert size == resource["original"]["bytes"]
    for oracle in resource["oracles"].values():
        assert (
            hashlib.sha256((GOLDEN / oracle["file"]).read_bytes()).hexdigest() == oracle["sha256"]
        )


def mutated_technical(mutation: str, target: Path) -> Path:
    resource = resource_for("tecnicos")
    raw = gzip.decompress((GOLDEN / resource["golden_file"]).read_bytes())
    rows = list(csv.reader(io.StringIO(raw.decode("utf-8-sig")), delimiter=";", strict=True))
    if mutation == "value":
        column = rows[0].index("INGREDIENTE_ATIVO(GRUPO_QUIMICI)(CONCENTRACAO)")
        assert "929.6" in rows[-1][column]
        rows[-1][column] = rows[-1][column].replace("929.6", "929.7")
    elif mutation == "unit":
        column = rows[0].index("INGREDIENTE_ATIVO(GRUPO_QUIMICI)(CONCENTRACAO)")
        rows[-1][column] = rows[-1][column].replace("g/kg", "g/L")
    elif mutation == "unknown_unit":
        column = rows[0].index("INGREDIENTE_ATIVO(GRUPO_QUIMICI)(CONCENTRACAO)")
        rows[-1][column] = rows[-1][column].replace("g/kg", "UNIDADE_NOVA")
    elif mutation == "key":
        rows[-1][0] += "0"
    elif mutation == "last_record":
        rows.pop()
    elif mutation == "conflict":
        row = list(rows[-1])
        row[1] += " ALTERADO"
        rows.append(row)
    elif mutation == "unknown_column":
        rows[0].append("CAMPO_NOVO")
        for row in rows[1:]:
            row.append("1")
    elif mutation == "width":
        rows[-1].append("EXTRA")
    elif mutation != "noop":
        raise ValueError(mutation)
    stream = io.StringIO(newline="")
    csv.writer(stream, delimiter=";", lineterminator="\n").writerows(rows)
    target.write_bytes(gzip.compress(stream.getvalue().encode("utf-8-sig"), mtime=0))
    return target


@pytest.mark.parametrize(
    "resource",
    [
        pytest.param(item, marks=pytest.mark.slow)
        if item["golden_file"] == "agrofit_formulados_02.csv.gz"
        else item
        for item in MANIFEST["resources"]
    ],
    ids=lambda item: item["golden_file"],
)
def test_n1_agrofit_csv_integral(resource: dict[str, Any], tmp_path: Path):
    target = tmp_path / "published.csv"
    with gzip.open(GOLDEN / resource["golden_file"], "rb") as stream, target.open("wb") as output:
        while chunk := stream.read(1024 * 1024):
            output.write(chunk)
    result = reconciliation.compare_csv(target, resource)
    assert result["status"] == "ok"
    assert result["source_rows"] == resource["source_rows"]
    assert result["unique_products"] == resource["unique_identities"]


@pytest.mark.parametrize("mutation", ["unknown_column", "unknown_unit", "width"])
def test_n1_agrofit_deriva_de_layout_e_concentracao(mutation: str, tmp_path: Path):
    zipped = mutated_technical(mutation, tmp_path / "changed.csv.gz")
    target = tmp_path / "changed.csv"
    target.write_bytes(gzip.decompress(zipped.read_bytes()))
    assert reconciliation.compare_csv(target, resource_for("tecnicos"))["status"] == "mismatch"


def test_n1_agrofit_catalogo_completo():
    original = json.loads(gzip.decompress((GOLDEN / "agrofit_catalog_00.json.gz").read_bytes()))
    assert reconciliation.compare_catalog(original, original)["status"] == "ok"


@pytest.mark.parametrize("mutation", ["missing", "new", "duplicate", "url", "format"])
def test_n1_agrofit_catalogo_mutado(mutation: str):
    original = json.loads(gzip.decompress((GOLDEN / "agrofit_catalog_00.json.gz").read_bytes()))
    changed = copy.deepcopy(original)
    resources = changed["result"]["resources"]
    if mutation == "missing":
        resources.pop()
    elif mutation == "new":
        resources.append({"id": "new", "url": "https://example.test/new.csv", "format": ".CSV"})
    elif mutation == "duplicate":
        resources.append(resources[-1])
    elif mutation == "url":
        resources[-1]["url"] = "https://example.test/replaced.csv"
    else:
        resources[-1]["format"] = "XLSX"
    assert reconciliation.compare_catalog(changed, original)["status"] == "mismatch"


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize("mutation", ["conflict", "width"])
async def test_agrofit_inconsistencia_antes_dos_filtros(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, layer: str, mutation: str
):
    resource = resource_for("tecnicos")
    target = mutated_technical(mutation, tmp_path / "mutated.csv.gz")
    seen = install(monkeypatch, {resource["golden_file"]: target})
    fetch = defensivos.tecnicos if layer == "source" else datasets.defensivos_tecnicos
    with pytest.raises(ParseError, match="divergentes|quantidade de campos") as caught:
        await fetch(nr_registro="4215", use_cache=False)
    if layer == "dataset":
        assert caught.value.errors and all(kind == "parse" for _, kind, _ in caught.value.errors)
        assert isinstance(caught.value.__cause__, ParseError)
    helpers.assert_replay_served(seen)
