from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import datasets, icmbio
from agrobr.exceptions import ParseError
from scripts import reconciliar_icmbio as reconciliation
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/icmbio"
)
MANIFEST = json.loads((GOLDEN / "manifest.json").read_bytes())


def resource_for(case: dict[str, Any]) -> dict[str, Any]:
    return next(item for item in MANIFEST["resources"] if item["id"] == case["resource"])


def assert_publication(frame: pd.DataFrame, case: dict[str, Any]) -> None:
    resource = resource_for(case)
    columns = resource["oracle"]["columns"]
    assert list(frame.columns) == columns
    rows = json.loads((GOLDEN / resource["oracle"]["file"]).read_bytes())
    assert len(frame) == len(rows) == case["expected_rows"]
    expected = Counter(tuple(row["values"][name] for name in columns) for row in rows)
    actual = Counter(
        tuple(None if pd.isna(value) else value for value in row)
        for row in frame[columns].itertuples(index=False, name=None)
    )
    assert actual == expected, "ICMBio: células diferentes do oráculo independente"
    assert frame["codigo"].iloc[0] == resource["statistics"]["first_code"]
    assert frame["codigo"].iloc[-1] == resource["statistics"]["last_code"]
    dtypes = dict.fromkeys(columns, str(pd.Series([""]).dtype))
    dtypes.update(area_ha="float64", ano_criacao="Int64")
    assert {column: str(dtype) for column, dtype in frame.dtypes.items()} == dtypes


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda case: case["id"])
async def test_icmbio_todas_as_celulas_e_proveniencia(
    monkeypatch: pytest.MonkeyPatch, case: dict[str, Any], layer: str
):
    seen = helpers.install_replay_http(monkeypatch, case, GOLDEN)
    fetch = icmbio.ucs if layer == "source" else datasets.unidades_conservacao_federais
    frame, meta = await fetch(**case["query"], return_meta=True)
    assert_publication(frame, case)
    resource = resource_for(case)
    assert meta.raw_content_hash == resource["original"]["sha256"]
    assert meta.raw_content_size == resource["original"]["bytes"]
    assert meta.fetched_at.utcoffset() == timedelta(0)
    assert meta.fetch_timestamp == meta.fetched_at
    assert meta.records_count == len(frame)
    assert meta.selected_source == "icmbio_wfs"
    assert meta.attempted_sources == ["icmbio_wfs"]
    assert meta.source_details["query"] == {
        "uf": case["query"].get("uf"),
        "grupo": None,
        "bioma": case["query"].get("bioma"),
        "bbox": None,
    }
    coverage = meta.source_details["coverage"]
    assert coverage["expected"] == coverage["returned"] == len(frame)
    assert coverage["status"] == "count_reconciled"
    assert coverage["transactional_snapshot"] is False
    assert meta.source_details["edition"] is None
    assert meta.source_details["count"]["order"] == "before_feature_acquisition"
    assert len(seen["served"]) == 2
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize("item", MANIFEST["files"], ids=lambda item: item["file"])
def test_icmbio_corpos_oficiais_integrais(item: dict[str, Any]):
    raw = (GOLDEN / item["file"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == item["original"]["sha256"]
    assert len(raw) == item["original"]["bytes"]


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda case: case["id"])
def test_icmbio_oraculo_integral_e_n1_csv(case: dict[str, Any]):
    resource = resource_for(case)
    raw = (GOLDEN / resource["oracle"]["file"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == resource["oracle"]["sha256"]
    assert (
        reconciliation.compare_csv((GOLDEN / resource["file"]).read_bytes(), resource)["status"]
        == "ok"
    )


def changed_csv(mutation: str) -> bytes:
    resource = resource_for(MANIFEST["cases"][0])
    reader = csv.reader(io.StringIO((GOLDEN / resource["file"]).read_text(encoding="utf-8")))
    rows = list(reader)
    field, replacement = {
        "area": ("areahaalb", "1.0"),
        "key": ("cnuc", "CNUC_ALTERADO"),
        "label": ("nomeuc", "UC ALTERADA"),
        "year": ("criacaoano", "1900"),
        "invalid_number": ("areahaalb", "NaN"),
    }.get(mutation, ("", ""))
    if field:
        rows[-1][rows[0].index(field)] = replacement
    elif mutation == "last_record":
        rows.pop()
    elif mutation == "column":
        rows[0][rows[0].index("cnuc")] = "codigo_novo"
    elif mutation == "width":
        rows[-1].append("extra")
    elif mutation != "noop":
        raise ValueError(mutation)
    output = io.StringIO(newline="")
    csv.writer(output, lineterminator="\n").writerows(rows)
    return output.getvalue().encode("utf-8")


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize("mutation", ["noop", "area", "key", "label", "year", "last_record"])
async def test_icmbio_mutacao_nos_bytes_e_detectada(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, layer: str, mutation: str
):
    case = copy.deepcopy(MANIFEST["cases"][0])
    target = tmp_path / "mutated.csv"
    target.write_bytes(changed_csv(mutation))
    for request in case["requests"]:
        if request["file"].endswith(".csv"):
            request["file"] = str(target)
    seen = helpers.install_replay_http(monkeypatch, case, GOLDEN)
    fetch = icmbio.ucs if layer == "source" else datasets.unidades_conservacao_federais
    if mutation == "last_record":
        with pytest.raises(ParseError, match="Contagem divergente") as caught:
            await fetch()
        if layer == "dataset":
            assert caught.value.errors and all(
                kind == "parse" for _, kind, _ in caught.value.errors
            )
            assert isinstance(caught.value.__cause__, ParseError)
    else:
        frame = await fetch()
        if mutation == "noop":
            assert_publication(frame, case)
        else:
            with pytest.raises(AssertionError, match="ICMBio: células diferentes do oráculo"):
                assert_publication(frame, case)
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize("mutation", ["column", "width", "invalid_number"])
def test_icmbio_n1_csv_rejeita_estrutura_incompativel(mutation: str):
    resource = resource_for(MANIFEST["cases"][0])
    assert reconciliation.compare_csv(changed_csv(mutation), resource)["status"] == "mismatch"


def test_icmbio_n1_schema_completo():
    schema = MANIFEST["schema"]
    assert reconciliation.compare_schema((GOLDEN / schema["file"]).read_bytes(), schema) == {
        "status": "ok",
        "problems": [],
        "properties": 22,
    }


@pytest.mark.parametrize("mutation", ["extra", "removed", "type", "nullable", "empty"])
def test_icmbio_n1_schema_rejeita_deriva(mutation: str):
    schema = MANIFEST["schema"]
    root = ET.fromstring((GOLDEN / schema["file"]).read_bytes())
    sequence = root.find(".//{http://www.w3.org/2001/XMLSchema}sequence")
    assert sequence is not None
    if mutation == "extra":
        ET.SubElement(sequence, "{http://www.w3.org/2001/XMLSchema}element", name="nova")
    elif mutation == "removed":
        sequence.remove(sequence[-1])
    elif mutation == "empty":
        sequence.clear()
    elif mutation == "type":
        sequence[-1].set("type", "xsd:string")
    else:
        sequence[0].set("nillable", "true")
    assert reconciliation.compare_schema(ET.tostring(root), schema)["status"] == "mismatch"
