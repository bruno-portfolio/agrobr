from __future__ import annotations

import copy
import csv
import gzip
import hashlib
import io
import json
from collections import Counter
from datetime import timedelta
from functools import cache
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import datasets, rnc
from scripts import reconciliar_rnc as reconciliation
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/rnc"
)
MANIFEST = json.loads((GOLDEN / "manifest.json").read_bytes())


def resource_for(kind: str) -> dict[str, Any]:
    return next(
        item for item in MANIFEST["resources"] if item["original"]["family"] == f"rnc_{kind}"
    )


@cache
def expected_rows(kind: str) -> list[dict[str, Any]]:
    with gzip.open(GOLDEN / resource_for(kind)["oracle"]["file"], "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def assert_publication(frame: pd.DataFrame, case: dict[str, Any]) -> None:
    columns = resource_for(case["kind"])["oracle"]["columns"]
    assert set(frame.columns) == set(columns)
    rows = expected_rows(case["kind"])
    if case.get("selection") != "all":
        positions = set(case["expected_original_records"])
        rows = [row for row in rows if row["original_record"] in positions]
    assert len(frame) == len(rows) == case["expected_rows"]
    expected = Counter(
        tuple(helpers.rnc_saida_do_publicado(name, row["values"][name]) for name in columns)
        for row in rows
    )
    actual = Counter(
        tuple(
            None
            if pd.isna(value)
            else value.date().isoformat()
            if isinstance(value, pd.Timestamp)
            else value
            for value in row
        )
        for row in frame[columns].itertuples(index=False, name=None)
    )
    assert actual == expected, (
        case["id"],
        sum((expected - actual).values()),
        sum((actual - expected).values()),
    )


def assert_parser_statistics(details: dict[str, Any], kind: str) -> None:
    resource = resource_for(kind)
    statistics = resource["statistics"]
    rows = [row["values"] for row in expected_rows(kind)]
    dates = [
        decision["target"]
        for decision in resource["column_decisions"]
        if decision["interpretation"].startswith("Civil")
    ]
    texts = [name for name in resource["oracle"]["columns"] if name not in dates]
    empty = {name: sum(row[name] is None for row in rows) for name in dates}
    if kind == "protegidas":
        empty["termino_protecao"] = sum(row["termino_protecao_texto"] == "" for row in rows)

    def identifiers(name: str) -> dict[str, int]:
        counts = Counter(row[name] for row in rows if row[name] != "")
        repeated = [count for count in counts.values() if count > 1]
        return {
            "unique_nonempty": len(counts),
            "blank_count": sum(row[name] == "" for row in rows),
            "repeated_groups": len(repeated),
            "rows_in_repeated_groups": sum(repeated),
        }

    assert details["family"] == kind
    assert details["source_headers"] == resource["headers"]
    assert details["encoding"] == resource["encoding"]
    assert details["primary_key"] == [statistics["primary_key"]]
    assert details["source_rows"] == details["validated_rows"] == details["output_rows"]
    assert details["output_rows"] == len(rows) and details["blank_physical_rows"] == 0
    assert details["field_statistics"] == {
        name: {"blank_count": sum(row[name] == "" for row in rows)} for name in texts
    }
    assert details["date_statistics"] == {
        name: {
            "null_count": sum(row[name] is None for row in rows),
            "minimum": min(row[name] for row in rows if row[name] is not None),
            "maximum": max(row[name] for row in rows if row[name] is not None),
            "empty_count": empty[name],
        }
        for name in dates
    }
    assert details["identifier_statistics"] == {
        name: identifiers(name) for name in texts if name.startswith("nr_")
    }
    assert details["situacoes"] == statistics["situacoes"]
    secondary = statistics["secondary_identifier"]
    identifier = details["identifier_statistics"][secondary["field"]]
    assert identifier["repeated_groups"] == secondary["repeated_groups"]
    assert identifier["rows_in_repeated_groups"] == secondary["repeated_occurrences"]
    for name, count in statistics["null_dates"].items():
        assert details["date_statistics"][name]["null_count"] == count
    if kind == "protegidas":
        assert details["termination_counts"] == {
            "dated": sum(row["termino_protecao"] is not None for row in rows),
            "conditional": statistics["conditional_ends"],
            "blank": empty["termino_protecao"],
        }
        assert statistics["conditional_ends"] == sum(
            row["termino_protecao_texto"] == reconciliation.CONDITIONAL_END for row in rows
        )


def install(
    monkeypatch: pytest.MonkeyPatch, replacements: dict[str, Path] | None = None
) -> dict[str, list[str]]:
    manifest = copy.deepcopy(MANIFEST)
    for request in manifest["requests"]:
        if replacements and request["file"] in replacements:
            request["file"] = str(replacements[request["file"]])
    return helpers.install_replay_http(monkeypatch, manifest, GOLDEN)


@pytest.mark.parametrize(
    "case,layer",
    [
        pytest.param(
            case,
            layer,
            marks=pytest.mark.slow
            if case["kind"] == "registradas"
            and (case["id"], layer) != ("rnc_registradas_blank_form", "source")
            else (),
            id=f"{case['id']}-{layer}",
        )
        for case in MANIFEST["cases"]
        for layer in ["source", "dataset"]
    ],
)
async def test_rnc_celulas_publicadas_e_cache(
    monkeypatch: pytest.MonkeyPatch, case: dict[str, Any], layer: str
):
    seen = install(monkeypatch)
    kind = case["kind"]
    fetch = getattr(rnc, kind) if layer == "source" else getattr(datasets, f"cultivares_{kind}")
    frame, meta = await fetch(**case["query"], use_cache=True, return_meta=True)
    assert_publication(frame, case)
    assert frame.index.equals(pd.RangeIndex(len(frame)))
    second, cached = await fetch(**case["query"], use_cache=True, return_meta=True)
    assert_publication(second, case)
    assert frame.dtypes.equals(second.dtypes)
    resource = resource_for(kind)
    assert meta.raw_content_hash == resource["decoded_sha256"]
    assert meta.raw_content_size == resource["decoded_bytes"]
    assert meta.fetched_at.utcoffset() == timedelta(0)
    if layer == "source":
        assert meta.fetch_timestamp == meta.fetched_at
        assert (meta.source_method, cached.source_method) == ("httpx+csv", "cache")
    assert meta.selected_source == f"rnc_{kind}"
    assert meta.attempted_sources == [f"rnc_{kind}"]
    assert meta.records_count == len(frame)
    coverage = meta.source_details["coverage"]
    assert coverage["reported_total"] == coverage["source_rows"] == resource["source_rows"]
    assert coverage["output_rows"] == len(frame)
    assert coverage["status"] == "count_matched"
    assert not coverage["transactional_snapshot"]
    assert meta.source_details["selection"] == {"filters": case["query"], "output_rows": len(frame)}
    assert isinstance(meta.source_details["filter_duration_ms"], int)
    assert not meta.from_cache and cached.from_cache
    assert cached.raw_content_hash == meta.raw_content_hash
    assert cached.fetched_at == meta.fetched_at
    assert (meta.source_details["cache_status"], cached.source_details["cache_status"]) == (
        "stored",
        "hit",
    )
    for current in (meta, cached):
        assert current.cache_key == f"rnc/{kind}/parser2"
        assert current.cache_expires_at == current.fetched_at + timedelta(days=1)
    assert len(seen["served"]) == 4
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize("kind", ["registradas", "protegidas"])
async def test_rnc_oraculo_integral_sem_cache(
    monkeypatch: pytest.MonkeyPatch, kind: str, layer: str
):
    seen = install(monkeypatch)
    case = next(
        item
        for item in MANIFEST["cases"]
        if item["kind"] == kind and item.get("selection") == "all"
    )
    fetch = getattr(rnc, kind) if layer == "source" else getattr(datasets, f"cultivares_{kind}")
    frame, meta = await fetch(use_cache=False, return_meta=True)
    assert_publication(frame, case)
    assert_parser_statistics(meta.source_details["parser"], kind)
    assert not meta.from_cache
    assert meta.source_details["cache_status"] == "bypassed"
    assert len(seen["served"]) == 4
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize("resource", MANIFEST["resources"], ids=lambda item: item["golden_file"])
def test_rnc_corpo_integral_oraculo_e_ultima_linha(resource: dict[str, Any]):
    raw = gzip.decompress((GOLDEN / resource["golden_file"]).read_bytes())
    assert hashlib.sha256(raw).hexdigest() == resource["decoded_sha256"]
    assert len(raw) == resource["decoded_bytes"]
    if resource.get("masked"):
        assert resource["decoded_sha256"] != resource["original"]["sha256"]
    else:
        assert resource["decoded_sha256"] == resource["original"]["sha256"]
        assert resource["decoded_bytes"] == resource["original"]["bytes"]
    kind = resource["original"]["family"].removeprefix("rnc_")
    rows = expected_rows(kind)
    assert len(rows) == resource["source_rows"]
    assert rows[-1]["original_record"] == resource["source_rows"]
    key = resource["statistics"]["primary_key"]
    column = resource["headers"].index(resource["identity_field"])
    assert rows[-1]["values"][key] == resource["last_record"]["cells"][column].strip()
    oracle = (GOLDEN / resource["oracle"]["file"]).read_bytes()
    assert hashlib.sha256(oracle).hexdigest() == resource["oracle"]["sha256"]


def mutated_csv(mutation: str, target: Path) -> Path:
    resource = resource_for("protegidas")
    raw = gzip.decompress((GOLDEN / resource["golden_file"]).read_bytes())
    rows = list(csv.reader(io.StringIO(raw.decode("utf-8-sig")), delimiter=",", strict=True))
    if mutation == "label":
        rows[-1][rows[0].index("CULTIVAR")] += " ALTERADO"
    elif mutation == "date":
        rows[-1][rows[0].index("INÍCIO DA PROTEÇÃO")] = "11/02/2021"
    elif mutation == "key":
        rows[-1][rows[0].index("Nº PROCESSO")] += "0"
    elif mutation == "last_record":
        rows.pop()
    elif mutation == "duplicate":
        rows.append(rows[-1])
    elif mutation == "invalid_date":
        rows[-1][rows[0].index("INÍCIO DA PROTEÇÃO")] = "31/02/2021"
    elif mutation == "unknown_column":
        rows[0].append("CAMPO_NOVO")
        for row in rows[1:]:
            row.append("1")
    elif mutation != "noop":
        raise ValueError(mutation)
    stream = io.StringIO(newline="")
    csv.writer(stream, delimiter=",", lineterminator="\n").writerows(rows)
    target.write_bytes(gzip.compress(stream.getvalue().encode("utf-8-sig"), mtime=0))
    return target


@pytest.mark.parametrize("resource", MANIFEST["resources"], ids=lambda item: item["golden_file"])
def test_n1_rnc_csv_integral(resource: dict[str, Any], tmp_path: Path):
    target = tmp_path / "published.csv"
    target.write_bytes(gzip.decompress((GOLDEN / resource["golden_file"]).read_bytes()))
    result = reconciliation.compare_csv(target, resource)
    assert result["status"] == "ok"
    assert result["rows"] == resource["source_rows"]


@pytest.mark.parametrize("mutation", ["duplicate", "invalid_date", "unknown_column"])
def test_n1_rnc_mutacao_csv(mutation: str, tmp_path: Path):
    zipped = mutated_csv(mutation, tmp_path / "mutated.csv.gz")
    target = tmp_path / "mutated.csv"
    target.write_bytes(gzip.decompress(zipped.read_bytes()))
    assert reconciliation.compare_csv(target, resource_for("protegidas"))["status"] == "mismatch"


def search_body() -> tuple[dict[str, Any], bytes]:
    item = next(
        item for item in MANIFEST["files"] if item["golden_file"] == "rnc_protegidas_08.html.gz"
    )
    return item, gzip.decompress((GOLDEN / item["golden_file"]).read_bytes())


def mutated_search(mutation: str) -> tuple[dict[str, Any], bytes, bytes]:
    item, raw = search_body()
    old, new = {
        "total": (b"Sua pesquisa retornou 5424 registros", b"Sua pesquisa retornou 5423 registros"),
        "missing_total": (b"Sua pesquisa retornou 5424 registros", b"Contagem indisponivel"),
        "export_format": (b'value="csv"', b'value="xls"'),
    }[mutation]
    assert old in raw
    return item, raw, raw.replace(old, new)


def test_n1_rnc_html_total_e_exportacao():
    item, raw = search_body()
    assert (
        reconciliation.compare_html(raw, raw, item["original"]["requested_url"], csv_rows=5424)[
            "status"
        ]
        == "ok"
    )


@pytest.mark.parametrize("mutation", ["total", "missing_total", "export_format"])
def test_n1_rnc_html_mutado(mutation: str):
    item, raw, changed = mutated_search(mutation)
    assert (
        reconciliation.compare_html(changed, raw, item["original"]["requested_url"], csv_rows=5424)[
            "status"
        ]
        == "mismatch"
    )


@pytest.mark.slow
@pytest.mark.parametrize("mutation", ["none", "total"])
def test_n1_rnc_confere_corpos_capturados(mutation: str, tmp_path: Path):
    capture = tmp_path / "captura"
    capture.mkdir()
    receipts = []
    for item in MANIFEST["files"]:
        original = item["original"]
        body = gzip.decompress((GOLDEN / item["golden_file"]).read_bytes())
        if mutation == "total" and item["golden_file"] == "rnc_protegidas_08.html.gz":
            body = body.replace(b"retornou 5424 registros", b"retornou 5423 registros")
        name = Path(original["file"]).name
        (capture / name).write_bytes(body)
        receipts.append(
            {
                "file": name,
                "sha256": hashlib.sha256(body).hexdigest(),
                "family": original["family"],
                "method": original["method"],
                "requested_url": original["requested_url"],
                "fetched_at": original["fetched_at"],
            }
        )
    (capture / "receipts.json").write_text(json.dumps(receipts), encoding="utf-8")
    output = tmp_path / "relatorio.json"
    assert reconciliation.run(capture, output) == int(mutation == "total")
    report = json.loads(output.read_text(encoding="utf-8"))
    status = {check["file"]: check["status"] for check in report["checks"]}
    assert len(status) == len(MANIFEST["files"])
    assert {name for name, value in status.items() if value != "ok"} == (
        {"rnc_protegidas_08.html"} if mutation == "total" else set()
    )
    rows = {check["file"]: check.get("rows") for check in report["checks"]}
    assert (rows["rnc_registradas_06.csv"], rows["rnc_protegidas_10.csv"]) == (38325, 5424)
