from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import constants
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.incra.vinculos import api, budget, metadata, models, relation
from agrobr.models import MetaInfo
from tests.test_incra import replay

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/incra"
REFERENCE = re.compile(r"(?<![0-9])[0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2}(?![0-9])")
NUP = "54330.000697/2006-18"
ALIASES = {
    "cd_quilomb": "codigo",
    "no_comunidade": "nome",
    "no_municipio": "municipio",
    "sg_uf": "uf",
    "nu_area_ha": "area_ha",
    "nu_familia": "familias",
    "ds_fase": "fase",
    "st_titulad": "titulado",
    "dt_publica": "data_publicacao",
    "dt_titulo": "data_titulo",
    "co_sr": "regional",
    "nu_processo": "processo",
    "dt_public1": "data_publicacao_2",
    "no_responsavel": "responsavel",
    "no_esfera": "esfera",
    "dt_cadastro": "data_cadastro",
    "cd_sipra": "codigo_sipra",
    "ds_descricao": "descricao",
    "dt_decreto": "data_decreto",
    "tp_levanta": "tipo_levantamento",
    "nr_escalao": "escala",
}


def _frame(rows: list[dict[str, Any]], *, administrative: bool = False) -> pd.DataFrame:
    names = constants.INCRA_ANDAMENTO_COLUMNS if administrative else constants.INCRA_COLUMNS
    frame = pd.DataFrame(rows, columns=names)
    for name in names:
        dtype = (
            "Int64"
            if name in {"codigo", "familias", "numero_publicado"}
            else "float64"
            if name == "area_ha"
            else pd.Series([""]).dtype
            if administrative
            else constants.INCRA_DTYPES_TEMPORAIS.get(name, pd.Series([""]).dtype)
        )
        frame[name] = pd.Series([row[name] for row in rows], dtype=dtype)
    return frame


def _geographical(processes: list[str | None]) -> pd.DataFrame:
    rows = []
    for index, process in enumerate(processes, 1):
        row = dict.fromkeys(constants.INCRA_COLUMNS)
        row.update(
            processo=process,
            codigo=0,
            area_ha=-0.0,
            data_cadastro="2026-09-07T16:40:09Z",
            feature_id=f"published.{index}",
        )
        rows.append(row)
    return _frame(rows)


def _administrative(processes: list[str]) -> pd.DataFrame:
    rows = []
    for index, process in enumerate(processes, 1):
        row = dict.fromkeys(constants.INCRA_ANDAMENTO_COLUMNS, "")
        row.update(processo=process, numero_publicado=index, regional="SR(22)AL")
        rows.append(row)
    return _frame(rows, administrative=True)


def _national() -> tuple[pd.DataFrame, pd.DataFrame]:
    first = json.loads((GOLDEN / "nacional_20260908/page_001.json").read_bytes())["features"]
    second = json.loads((GOLDEN / "nacional_20260908/page_002.json").read_bytes())["features"]
    assert first[-1] == second[0]
    geographic = [
        {
            **{
                ALIASES[key]: replay.publicado(ALIASES[key], value)
                for key, value in item["properties"].items()
            },
            "feature_id": item["id"],
        }
        for item in first + second[1:]
    ]
    raw = json.loads((GOLDEN / "andamento_20260608/oracle.json").read_bytes())
    administrative = []
    for row in raw:
        cells = row["cells"]
        administrative.append(
            {
                name: row["numero_publicado"]
                if name == "numero_publicado"
                else cells["regional_layout"]
                if name == "regional"
                else cells[name if name in {"area_ha_texto", "familias_texto"} else name + "_texto"]
                for name in constants.INCRA_ANDAMENTO_COLUMNS
            }
        )
    return _frame(geographic), _frame(administrative, administrative=True)


def _parent(frame: pd.DataFrame, *, administrative: bool = False) -> MetaInfo:
    size = len(frame)
    if administrative:
        query = {"requested_edition": None, "resolved_edition": "2026-06-08"}
        details = {
            "query": query,
            "publication": {"internal_edition": "2026-06-08"},
            "resources": [],
            "layout": {},
        }
        coverage = {
            "status": "reconciled_publication",
            "declared_rows": size,
            "validated_rows": size,
            "returned_rows": size,
            "all_logical_requests_succeeded": True,
            "local_filters": [],
        }
    else:
        query = dict.fromkeys(("uf", "fase", "bbox", "max_records"))
        query.update(fetch_geometry=False, include_geometry=False)
        details = {"query": query, "resources": [], "pages": [], "count_checks": []}
        coverage = {
            "remote": {
                "expected_before": size,
                "expected_after": size,
                "received_rows_with_overlap": size,
                "validated_rows_with_overlap": size,
                "overlap_rows": 0,
                "accepted_rows": size,
                "max_records": None,
                "truncated": False,
                "counts_consistent": True,
                "status": "reconciled",
            },
            "local": {
                "evaluated_rows": size,
                "matched_rows": size,
                "rejected_rows": 0,
                "returned_rows": size,
                "status": "reconciled",
                "total_basis": "reconciled_remote_scan",
            },
        }
    fields = list(details)
    manifest = metadata.encode(details)
    details.update(manifest_fields=fields, coverage=coverage, resource_bytes=0)
    now = datetime.now(UTC)
    return MetaInfo(
        source="incra",
        source_url="https://example.invalid/synthetic-parent",
        source_method="synthetic_parent_fixture_no_HTTP",
        fetched_at=now,
        timestamp=now,
        fetch_timestamp=now,
        records_count=size,
        columns=frame.columns.tolist(),
        schema_version="1.0" if administrative else "2.0",
        contract_version="1.0" if administrative else "2.0",
        selected_source="incra_andamento_pdf" if administrative else "incra_geoserver",
        raw_content_hash=hashlib.sha256(manifest).hexdigest(),
        raw_content_size=len(manifest),
        source_details=details,
    )


def _install(monkeypatch: pytest.MonkeyPatch, left: pd.DataFrame, right: pd.DataFrame):
    first, second = _parent(left), _parent(right, administrative=True)
    geographical = AsyncMock(return_value=(left, first))
    administrative = AsyncMock(return_value=(right, second))
    monkeypatch.setattr(api.administrative_parser, "check_pdf", lambda: None)
    monkeypatch.setattr(api.geographical_api, "quilombolas", geographical)
    monkeypatch.setattr(api.administrative_api, "andamento_quilombola", administrative)
    return first, second, geographical, administrative


def test_relation_national_all_origin_cells_and_occurrences():
    left, right = _national()
    parsed = relation.build_relation(left, right, max_rows=50_000)
    assert parsed.frame.shape == (781, 46)
    assert parsed.counts["states"] == {
        "vinculo_exato": 285,
        "sem_referencia_administrativa": 30,
        "sem_referencia_geografica": 333,
        "referencia_nao_reconhecida": 120,
        "referencia_ausente": 13,
    }
    assert parsed.counts["geographical_rows_represented"] == 444
    assert parsed.counts["administrative_rows_represented"] == 613
    oracle = json.loads((GOLDEN / "vinculos_20260908/expected.json").read_bytes())
    for values, expected in zip(
        parsed.frame.itertuples(index=False, name=None), oracle, strict=True
    ):
        for name, value in zip(parsed.frame.columns, values, strict=True):
            esperado = (
                replay.publicado(name.removeprefix("perimetro_"), expected[name])
                if name.startswith("perimetro_")
                else expected[name]
            )
            if esperado is None:
                assert pd.isna(value)
            elif name == "perimetro_area_ha":
                assert float(value).hex() == float(esperado).hex()
            else:
                assert value == esperado
    for _, actual in parsed.frame.iterrows():
        for prefix, source, position in (
            ("perimetro_", left, actual["perimetro_posicao"]),
            ("administrativo_", right, actual["administrativo_numero_publicado"]),
        ):
            for name in source:
                expected = None if pd.isna(position) else source.iloc[int(position) - 1][name]
                value = actual[prefix + name]
                if pd.isna(expected):
                    assert pd.isna(value)
                elif name == "area_ha":
                    assert float(value).hex() == float(expected).hex()
                else:
                    assert value == expected
        if actual["estado_vinculo"] == "vinculo_exato":
            first = REFERENCE.findall(actual["perimetro_processo"])
            second = REFERENCE.findall(actual["administrativo_processo"])
            assert (
                first[int(actual["perimetro_referencia_posicao"]) - 1]
                == actual["referencia_literal"]
            )
            assert (
                second[int(actual["administrativo_referencia_posicao"]) - 1]
                == actual["referencia_literal"]
            )


async def test_vinculos_public_chain_national_replay_matches_oracle(monkeypatch, june_publication):
    replay.install_national_wfs(monkeypatch)
    replay.install_publication(monkeypatch)
    pdf = (replay.JUNE / "publication.pdf").read_bytes()

    def parse(content):
        assert content == pdf
        return june_publication

    monkeypatch.setattr(api.administrative_parser, "parse_publication", parse)
    frame, meta = await api.vinculos_quilombolas(return_meta=True)
    oracle = json.loads((GOLDEN / "vinculos_20260908/expected.json").read_bytes())
    assert frame.shape == (len(oracle), 46) == (781, 46)
    for values, literal in zip(frame.itertuples(index=False, name=None), oracle, strict=True):
        expected = {
            name: replay.publicado(name.removeprefix("perimetro_"), value)
            if name.startswith("perimetro_")
            else value
            for name, value in literal.items()
        }
        observed = {
            name: None if pd.isna(value) else value
            for name, value in zip(frame.columns, values, strict=True)
        }
        assert observed == expected
    administrative = meta.source_details["sources"]["administrative"]["source_details"]
    assert administrative["query"]["resolved_edition"] == "2026-06-08"
    herdados = [
        f"{parent['selected_source']}: {aviso}"
        for parent in meta.source_details["sources"].values()
        for aviso in parent["validation_warnings"]
    ]
    assert any(aviso.startswith("incra_geoserver: ") for aviso in herdados)
    assert meta.validation_warnings[1:] == herdados


def test_relation_expansion_limit_prevents_partial_result():
    with pytest.raises(ResourceLimitError, match="Expansão"):
        relation.build_relation(_geographical([NUP, NUP]), _administrative([NUP, NUP]), max_rows=3)


def test_relation_none_limit_does_not_remove_memory_budget(monkeypatch):
    monkeypatch.setattr(constants, "INCRA_VINCULOS_MAX_RETAINED_BYTES", 1)
    with pytest.raises(ResourceLimitError, match="retenção"):
        relation.build_relation(_geographical([NUP]), _administrative([NUP]), max_rows=None)


def test_evidence_rejects_unrepresented_residue():
    with pytest.raises(ValueError):
        models.CellEvidence(
            position=1, original="missing", references=[], residues=[], state="unrecognized"
        )


@pytest.mark.asyncio
async def test_api_complete_parents_forwarding_manifest_copies(monkeypatch):
    left, right = _geographical([NUP]), _administrative([NUP])
    first, second, source, publication = _install(monkeypatch, left, right)
    original = copy.deepcopy([first.to_dict(), second.to_dict()])
    frame, meta = await api.vinculos_quilombolas(
        edicao="2026-06-08", tamanho_pagina=250, return_meta=True
    )
    assert frame.shape == (1, 46)
    source.assert_awaited_once_with(
        max_registros=None, tamanho_pagina=250, as_polars=False, return_meta=True
    )
    publication.assert_awaited_once_with(edicao="2026-06-08", as_polars=False, return_meta=True)
    assert [first.to_dict(), second.to_dict()] == original
    assert meta.source_details["sources"] == {
        "geographical": original[0],
        "administrative": original[1],
    }
    encoded = metadata.encode(
        {name: meta.source_details[name] for name in meta.source_details["manifest_fields"]}
    )
    assert hashlib.sha256(encoded).hexdigest() == meta.raw_content_hash
    assert len(encoded) == meta.raw_content_size
    for timestamp in (meta.fetched_at, meta.timestamp, meta.fetch_timestamp):
        assert timestamp.utcoffset().total_seconds() == 0
    assert meta.fetch_timestamp == meta.fetched_at == max(first.fetched_at, second.fetched_at)
    assert meta.timestamp >= meta.fetched_at
    first.source_details["query"]["uf"] = "changed"
    assert meta.source_details["sources"]["geographical"]["source_details"]["query"]["uf"] is None


@pytest.mark.asyncio
async def test_api_second_failure_retains_first_provenance(monkeypatch):
    left, right = _geographical([NUP]), _administrative([NUP])
    first, _, _, publication = _install(monkeypatch, left, right)
    error = SourceUnavailableError("incra", last_error="second")
    publication.side_effect = error
    with pytest.raises(SourceUnavailableError) as caught:
        await api.vinculos_quilombolas()
    assert caught.value is error
    assert error.vinculos_completed_sources == {"geographical": first.to_dict()}


@pytest.mark.parametrize(
    "change", ["manifest", "missing_details", "version", "missing_fetch_timestamp"]
)
def test_parent_metadata_invalid_rejected(change):
    frame = _geographical([NUP])
    meta = _parent(frame)
    if change == "manifest":
        meta.source_details["query"]["uf"] = "BA"
    elif change == "missing_details":
        meta.source_details = {}
    elif change == "version":
        meta.contract_version = "9.0"
    else:
        meta.fetch_timestamp = None
    with pytest.raises(ParseError):
        metadata.parent_payload(meta, frame, geographical=True)


def test_budget_shared_references_are_counted_once():
    item = ["literal"]
    assert budget.retained_size([item, item]) < budget.retained_size([item, ["literal"]])


def test_parent_partial_geographical_coverage_rejected():
    frame = _geographical([NUP])
    meta = _parent(frame)
    remote = meta.source_details["coverage"]["remote"]
    remote.update(expected_before=2, expected_after=2, truncated=True, status="partial")
    meta.source_details["coverage"]["local"].update(
        status="unknown", total_basis="observed_remote_prefix"
    )
    with pytest.raises(ParseError, match="integral"):
        metadata.parent_payload(meta, frame, geographical=True)


def test_parent_unreconciled_administrative_coverage_rejected():
    frame = _administrative([NUP])
    meta = _parent(frame, administrative=True)
    meta.source_details["coverage"]["declared_rows"] = 2
    with pytest.raises(ParseError, match="incompleta"):
        metadata.parent_payload(meta, frame, geographical=False)


def test_evidence_preserves_nonwhitespace_spans():
    result = relation.build_relation(
        _geographical(["nota:" + NUP + "\nfim"]), _administrative([]), max_rows=None
    )
    cell = result.evidence["geographical"][0]
    assert [(item.start, item.end, item.text) for item in cell.residues] == [
        (0, 5, "nota:"),
        (25, 29, "\nfim"),
    ]
    assert cell.references[0].text == NUP


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"uf": "BA"}, TypeError),
        ({"max_vinculos": True}, InvalidParameterError),
        ({"max_vinculos": 0}, InvalidParameterError),
        ({"max_vinculos": 1.5}, InvalidParameterError),
        ({"as_polars": 1}, InvalidParameterError),
        ({"return_meta": None}, InvalidParameterError),
        ({"edicao": "2026-02-30"}, InvalidParameterError),
        ({"tamanho_pagina": 1001}, InvalidParameterError),
    ],
)
async def test_api_guards_before_optional_and_source(monkeypatch, kwargs, error):
    def forbidden():
        pytest.fail("Optional check reached")

    monkeypatch.setattr(api.administrative_parser, "check_pdf", forbidden)
    with pytest.raises(error):
        await api.vinculos_quilombolas(**kwargs)


@pytest.mark.asyncio
async def test_api_deterministic_before_optional(monkeypatch):
    monkeypatch.setattr(api, "get_snapshot", lambda: object())
    monkeypatch.setattr(
        api.administrative_parser, "check_pdf", lambda: pytest.fail("Optional reached")
    )
    with pytest.raises(InvalidParameterError, match="deterministic"):
        await api.vinculos_quilombolas()


@pytest.mark.asyncio
async def test_api_invalid_parent_contract_without_metadata(monkeypatch):
    left, right = _geographical([NUP]), _administrative([NUP])
    left["codigo"] = left["codigo"].astype("float64")
    _, _, _, publication = _install(monkeypatch, left, right)
    with pytest.raises(ContractViolationError):
        await api.vinculos_quilombolas(return_meta=False)
    publication.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_polars_missing_precedes_first_request(monkeypatch):
    left, right = _geographical([NUP]), _administrative([NUP])
    _, _, source, _ = _install(monkeypatch, left, right)

    def missing(name):
        raise ImportError(name)

    monkeypatch.setattr(api.importlib, "import_module", missing)
    with pytest.raises(ImportError, match="polars"):
        await api.vinculos_quilombolas(as_polars=True)
    source.assert_not_awaited()
