from __future__ import annotations

import copy
import csv
import gzip
import hashlib
import io
import json
import sys
from functools import cache
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import datasets, lista_suja
from agrobr.exceptions import ParseError
from scripts import reconciliar_lista_suja as reconciliation
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/mte"
)
MANIFEST = json.loads((GOLDEN / "manifest.json").read_bytes())
EDICAO = "Atualização periódica de 6 de abril de 2026. Cadastro atualizado em 04/09/2026."


def resource_for(name: str) -> dict[str, Any]:
    return next(item for item in MANIFEST["resources"] if item["id"] == name)


@cache
def oracle(name: str) -> list[dict[str, Any]]:
    return json.loads(gzip.decompress((GOLDEN / resource_for(name)["oracle"]["file"]).read_bytes()))


def assert_publication(frame: pd.DataFrame, case: dict[str, Any]) -> None:
    columns = resource_for(case["resource"])["oracle"]["columns"]
    expected = [oracle(case["resource"])[index]["values"] for index in case["expected_positions"]]
    assert list(frame.columns) == columns
    actual = [
        [
            None
            if pd.isna(value)
            else value.isoformat()
            if isinstance(value, pd.Timestamp)
            else value
            for value in row
        ]
        for row in frame[columns].itertuples(index=False, name=None)
    ]
    assert actual == [[row[column] for column in columns] for row in expected]
    for column in ("data_inclusao", "data_decisao", "data_atualizacao"):
        assert str(frame[column].dtype) == "datetime64[ns]"
    for column in ("trabalhadores_resgatados", "ano_acao_fiscal"):
        assert str(frame[column].dtype) == "Int64"


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize(
    "case",
    [
        pytest.param(case, marks=pytest.mark.slow)
        if case["id"] in {"pdf_all", "pdf_wrapped"}
        else case
        for case in MANIFEST["cases"]
    ],
    ids=lambda case: case["id"],
)
async def test_lista_suja_publica_todas_as_celulas(
    monkeypatch: pytest.MonkeyPatch, case: dict[str, Any], layer: str
):
    if case["resource"] == "pdf":
        pytest.importorskip("pdfplumber")
    seen = helpers.install_replay_http(monkeypatch, case, GOLDEN)
    fetch = lista_suja.empregadores if layer == "source" else datasets.empregadores_lista_suja
    frame, meta = await fetch(**case["query"], return_meta=True)
    assert_publication(frame, case)
    resource = resource_for(case["resource"])
    body = gzip.decompress((GOLDEN / resource["body_file"]).read_bytes())
    assert meta.records_count == len(frame) == len(case["expected_positions"])
    assert meta.raw_content_hash == hashlib.sha256(body).hexdigest()
    assert meta.raw_content_size == len(body)
    assert meta.parser_version == 4
    assert meta.schema_version == meta.contract_version == "2.0"
    assert meta.attempted_sources == [f"lista_suja_{case['resource']}"]
    assert meta.selected_source == f"lista_suja_{case['resource']}"
    assert not meta.from_cache
    assert meta.fetched_at.utcoffset() is not None
    details = meta.source_details
    assert details["source_rows"] == MANIFEST["statistics"]["rows"]
    assert details["output_rows"] == len(frame)
    assert details["null_counts"] == MANIFEST["statistics"]["nulls"]
    assert details["compound_inclusion_ids"] == MANIFEST["statistics"]["compound_ids"]
    assert details["companion_validated"] == (case["resource"] == "csv")
    for field in ("title", "registry_updated_at", "periodic_update", "notes"):
        assert details["publication"][field] == MANIFEST["publication"][field]
    assert details["pages"] == (45 if case["resource"] == "pdf" else None)
    assert details["format"] == details["formato"] == case["resource"]
    assert details["revision_semantics"] == "current_publication_identified_by_content_hash"
    assert details["resource"]["sha256"] == meta.raw_content_hash
    assert details["resource"]["size_bytes"] == meta.raw_content_size
    served = next(item for item in case["requests"] if item["file"] == resource["body_file"])
    assert details["resource"]["content_type"] == served["content_type"]
    assert meta.fetched_at == pd.Timestamp(details["resource"]["fetched_at"])
    if layer == "source":
        assert meta.fetch_timestamp == meta.fetched_at
    assert details["publication"]["edition_text"] == EDICAO
    compostas = len(MANIFEST["statistics"]["compound_ids"])
    assert (
        details["warnings"]
        == meta.validation_warnings
        == [f"{compostas} registros com inclusão composta: escalar nulo e texto preservado."]
    )
    assert details["discovered_publication"]["resources"] == {
        link["url"].rsplit(".", 1)[-1]: link["url"]
        for link in MANIFEST["html_links"]
        if link["decision"] == "mapped_publication_resource"
    }
    assert frame.index.equals(pd.RangeIndex(len(frame)))
    assert len(seen["served"]) == len(case["requests"])
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize("item", MANIFEST["files"], ids=lambda item: item["file"])
def test_lista_suja_corpos_integrais_e_recibos(item: dict[str, Any]):
    packed = (GOLDEN / item["file"]).read_bytes()
    raw = gzip.decompress(packed)
    assert len(packed) == item["bytes"]
    assert hashlib.sha256(packed).hexdigest() == item["sha256"]
    assert len(raw) == item["decoded_bytes"]
    assert hashlib.sha256(raw).hexdigest() == item["decoded_sha256"]
    if item.get("masked"):
        assert item["decoded_sha256"] != item["original"]["sha256"]
    else:
        assert item["decoded_bytes"] == item["original"]["bytes"]
        assert item["decoded_sha256"] == item["original"]["sha256"]
    assert pd.Timestamp(item["original"]["fetched_at"]).tzinfo is not None


@pytest.mark.parametrize("resource", MANIFEST["resources"], ids=lambda resource: resource["id"])
def test_lista_suja_oraculos_integros_com_localizadores(resource: dict[str, Any]):
    expected = resource["oracle"]
    packed = (GOLDEN / expected["file"]).read_bytes()
    assert len(packed) == expected["bytes"]
    assert hashlib.sha256(packed).hexdigest() == expected["sha256"]
    assert hashlib.sha256(gzip.decompress(packed)).hexdigest() == expected["decoded_sha256"]
    rows = oracle(resource["id"])
    assert len(rows) == expected["rows"]
    assert len({row["values"]["id_registro"] for row in rows}) == expected["rows"]
    if resource["id"] == "csv":
        body = gzip.decompress((GOLDEN / resource["body_file"]).read_bytes())
        raw_rows = reconciliation.read_csv(body)
        for row in rows:
            assert raw_rows[row["csv_record"]][0] == row["values"]["id_registro"]
    else:
        assert {row["pdf_page"] for row in rows} == set(range(1, 46))
        assert all(row["pdf_row_bbox"][1] < row["pdf_row_bbox"][3] for row in rows)


def serialized(rows: list[list[str]], delimiter: str) -> bytes:
    stream = io.StringIO(newline="")
    csv.writer(stream, delimiter=delimiter).writerows(rows)
    return stream.getvalue().encode("cp1252")


def changed_csv(mutation: str, tmp_path: Path) -> dict[str, Any]:
    case = copy.deepcopy(MANIFEST["cases"][0])
    primary = gzip.decompress((GOLDEN / "mte_csv_013.csv.gz").read_bytes())
    companion = gzip.decompress((GOLDEN / "mte_csv_014.txt.gz").read_bytes())
    rows = reconciliation.read_csv(primary)
    companion_rows = list(csv.reader(io.StringIO(companion.decode("cp1252")), delimiter="\t"))
    if mutation == "workers":
        rows[-1][6] = str(int(rows[-1][6]) + 1)
    elif mutation == "decision":
        rows[-1][8] = "01/01/2027"
    elif mutation == "inclusion":
        rows[41][9] = rows[41][9].replace("2023", "2027")
        if rows[41][9] == reconciliation.read_csv(primary)[41][9]:
            rows[41][9] = rows[41][9].replace("2024", "2027")
        assert rows[41][9] != reconciliation.read_csv(primary)[41][9]
    elif mutation == "duplicate":
        rows[-1][0] = rows[1][0]
    elif mutation == "missing":
        rows.pop()
    elif mutation != "noop":
        raise ValueError(mutation)
    if mutation in ("workers", "decision", "inclusion"):
        by_id = {row[0]: row for row in rows[1:]}
        companion_rows = [
            by_id[row[0].strip()] if len(row) == 10 and row[0].strip().isdigit() else row
            for row in companion_rows
        ]
    for suffix, payload in (
        ("csv", serialized(rows, ";")),
        ("txt", serialized(companion_rows, "\t")),
    ):
        target = tmp_path / f"{mutation}.{suffix}.gz"
        target.write_bytes(gzip.compress(payload, mtime=0))
        request = next(item for item in case["requests"] if item["file"].endswith(f".{suffix}.gz"))
        request["file"] = str(target)
    return case


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize(
    "mutation", ["noop", "workers", "decision", "inclusion", "duplicate", "missing"]
)
async def test_lista_suja_mutacao_do_corpo_completo(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mutation: str, layer: str
):
    case = changed_csv(mutation, tmp_path)
    seen = helpers.install_replay_http(monkeypatch, case, GOLDEN)
    fetch = lista_suja.empregadores if layer == "source" else datasets.empregadores_lista_suja
    if mutation in ("duplicate", "missing"):
        with pytest.raises(ParseError, match="duplicado|distintos") as caught:
            await fetch(**case["query"])
        if layer == "dataset":
            assert caught.value.errors and all(
                kind == "parse" for _, kind, _ in caught.value.errors
            )
            assert isinstance(caught.value.__cause__, ParseError)
    else:
        frame = await fetch(**case["query"])
        if mutation == "noop":
            assert_publication(frame, case)
        else:
            with pytest.raises(AssertionError):
                assert_publication(frame, case)
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize(
    "mutation", ["noop", "header", "width", "integer", "empty_date", "duplicate_id"]
)
def test_lista_suja_n1_csv_recusa_deriva(mutation: str):
    resource = resource_for("csv")
    rows = reconciliation.read_csv(gzip.decompress((GOLDEN / resource["body_file"]).read_bytes()))
    if mutation == "header":
        rows[0][-1] = "Campo novo"
    elif mutation == "width":
        rows[-1].append("Campo extra")
    elif mutation == "integer":
        rows[-1][6] = "1.5"
    elif mutation == "empty_date":
        rows[-1][9] = ""
    elif mutation == "duplicate_id":
        rows[-1][0] = rows[1][0]
    result = reconciliation.compare_csv(serialized(rows, ";"), resource)
    assert result["status"] == ("ok" if mutation == "noop" else "mismatch"), result["problems"]


def test_lista_suja_n1_txt_confere_todas_as_celulas():
    primary = gzip.decompress((GOLDEN / "mte_csv_013.csv.gz").read_bytes())
    companion = gzip.decompress((GOLDEN / "mte_csv_014.txt.gz").read_bytes())
    assert reconciliation.compare_companion(companion, primary, MANIFEST)["status"] == "ok"
    mutated = companion.replace(b"04/09/2026", b"05/09/2026")
    assert reconciliation.compare_companion(mutated, primary, MANIFEST)["status"] == "mismatch"


def test_lista_suja_n1_portal_recusa_link_nao_mapeado():
    body = gzip.decompress((GOLDEN / "mte_csv_012.html.gz").read_bytes())
    assert reconciliation.compare_portal(body, MANIFEST)["status"] == "ok"
    changed = body.replace(b"cadastro_de_empregadores.csv", b"cadastro_de_empregadores_v2.csv")
    assert reconciliation.compare_portal(changed, MANIFEST)["status"] == "mismatch"


@pytest.mark.parametrize("gravado", ["https://www.gov.br/mte#", "https://www.gov.br/mte"])
def test_lista_suja_portal_fragmento_vazio_independe_da_versao(gravado):
    corpo = b'<a href="#">topo</a><a href="#rodape">rodape</a>'
    manifesto = {
        "portal_url": "https://www.gov.br/mte",
        "html_links": [{"url": gravado}, {"url": "https://www.gov.br/mte#rodape"}],
    }
    assert reconciliation.compare_portal(corpo, manifesto) == {"status": "ok", "links": 2}
    manifesto["html_links"][1]["url"] = "https://www.gov.br/mte"
    assert reconciliation.compare_portal(corpo, manifesto)["status"] == "mismatch"


@pytest.mark.slow
def test_lista_suja_n1_cli_confere_os_cinco_corpos_locais(tmp_path, monkeypatch, capsys):
    pytest.importorskip("pdfplumber")
    saida = tmp_path / "resultado.json"
    monkeypatch.setattr(sys, "argv", ["reconciliar_lista_suja", "--output", str(saida)])
    assert reconciliation.main() == 0
    checks = json.loads(saida.read_text(encoding="utf-8"))["checks"]
    assert [(check["file"], check["status"]) for check in checks] == [
        (item["file"], "ok") for item in MANIFEST["files"]
    ]
    pdf = next(check for check in checks if check["file"].endswith(".pdf.gz"))
    assert (pdf["pages"], pdf["rows"]) == (45, MANIFEST["statistics"]["rows"])
    assert capsys.readouterr().out.strip() == f"{len(checks)} ok / 0 mismatch"
