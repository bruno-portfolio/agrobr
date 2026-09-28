from __future__ import annotations

import json
from datetime import datetime

import pytest
from structlog import testing

from agrobr.constants import Fonte
from agrobr.models import Fingerprint
from agrobr.validators.structural import (
    compare_fingerprints,
    load_baseline,
    save_baseline,
    validate_against_baseline,
    validate_structure,
)


def _make_fingerprint(
    source=Fonte.CEPEA,
    structure_hash="abc123",
    table_classes=None,
    key_ids=None,
    table_headers=None,
    element_counts=None,
):
    return Fingerprint(
        source=source,
        url="https://example.com",
        collected_at=datetime(2024, 1, 1),
        table_classes=table_classes or [["class-a", "class-b"]],
        key_ids=key_ids or ["id-1", "id-2"],
        structure_hash=structure_hash,
        table_headers=table_headers or [["col1", "col2", "col3"]],
        element_counts=element_counts or {"table": 2, "form": 1},
    )


class TestCompareFingerprints:
    def test_identical_fingerprints(self):
        fp = _make_fingerprint()
        similarity, diffs = compare_fingerprints(fp, fp)
        assert similarity == 1.0
        assert diffs == {}

    def test_different_structure_hash(self):
        current = _make_fingerprint(structure_hash="new_hash")
        reference = _make_fingerprint(structure_hash="old_hash")
        similarity, diffs = compare_fingerprints(current, reference)
        assert similarity < 1.0
        assert "structure_changed" in diffs

    def test_partial_table_classes(self):
        current = _make_fingerprint(table_classes=[["class-a"]])
        reference = _make_fingerprint(table_classes=[["class-a", "class-b"]])
        similarity, diffs = compare_fingerprints(current, reference)
        assert similarity < 1.0

    def test_element_counts_major_diff(self):
        current = _make_fingerprint(element_counts={"table": 10, "form": 1})
        reference = _make_fingerprint(element_counts={"table": 2, "form": 1})
        similarity, diffs = compare_fingerprints(current, reference)
        assert "element_counts_diff" in diffs

    def test_table_headers_jaccard(self):
        current = _make_fingerprint(table_headers=[["a", "b", "c"]])
        reference = _make_fingerprint(table_headers=[["a", "b", "d"]])
        similarity, diffs = compare_fingerprints(current, reference)
        assert similarity < 1.0


class TestValidateStructure:
    def test_high_similarity_passes(self):
        fp = _make_fingerprint()
        result = validate_structure(fp, fp)
        assert result.passed is True
        assert result.level == "high"

    def test_low_similarity_fails(self):
        current = _make_fingerprint(
            structure_hash="x",
            table_classes=[["z"]],
            key_ids=["z"],
            table_headers=[["z"]],
            element_counts={"div": 100},
        )
        baseline = _make_fingerprint(
            structure_hash="y",
            table_classes=[["a"]],
            key_ids=["a"],
            table_headers=[["a"]],
            element_counts={"table": 1},
        )
        result = validate_structure(current, baseline)
        assert result.passed is False


class TestLoadBaseline:
    def test_source_specific_file(self, tmp_path):
        fp = _make_fingerprint()
        data = fp.model_dump(mode="json")
        path = tmp_path / "cepea_baseline.json"
        path.write_text(json.dumps(data, default=str))

        result = load_baseline(Fonte.CEPEA, tmp_path)
        assert result is not None
        assert result.source == Fonte.CEPEA

    def test_sources_key_in_baseline(self, tmp_path):
        fp = _make_fingerprint()
        data = {"sources": {"cepea": fp.model_dump(mode="json")}}
        path = tmp_path / "baseline.json"
        path.write_text(json.dumps(data, default=str))

        result = load_baseline(Fonte.CEPEA, tmp_path)
        assert result is not None

    def test_corrupt_file(self, tmp_path):
        path = tmp_path / "cepea_baseline.json"
        path.write_text("not valid json {{{")
        result = load_baseline(Fonte.CEPEA, tmp_path)
        assert result is None


class TestSaveBaseline:
    def test_creates_directory(self, tmp_path):
        nested = tmp_path / "sub" / "dir"
        fp = _make_fingerprint()
        save_baseline(fp, nested)
        assert (nested / "cepea_baseline.json").exists()


class TestValidateAgainstBaseline:
    def test_no_baseline_passes(self, tmp_path):
        fp = _make_fingerprint()
        result = validate_against_baseline(fp, tmp_path)
        assert result.passed is True
        assert result.level == "unknown"
        assert result.baseline_fingerprint is None


_DIVERGENTE_ATUAL = {
    "structure_hash": "x",
    "table_classes": [["z"]],
    "key_ids": ["z"],
    "table_headers": [["z"]],
    "element_counts": {"div": 100},
}
_DIVERGENTE_BASE = {
    "structure_hash": "y",
    "table_classes": [["a"]],
    "key_ids": ["a"],
    "table_headers": [["a"]],
    "element_counts": {"table": 1},
}


@pytest.mark.parametrize(
    ("atual", "base", "esperado"),
    [
        ({}, {}, ("high", True, "Structure matches baseline")),
        (
            {"structure_hash": "new"},
            {"structure_hash": "old"},
            ("medium", True, "Minor structural differences detected (75.0% similarity)"),
        ),
        (
            {"structure_hash": "new", "key_ids": ["id-1"]},
            {"structure_hash": "old"},
            ("low", False, "Significant structural changes (67.5% similarity)"),
        ),
        (
            _DIVERGENTE_ATUAL,
            _DIVERGENTE_BASE,
            ("critical", False, "Major layout change detected (8.0% similarity)"),
        ),
    ],
)
def test_validate_structure_faixas_de_similaridade(atual, base, esperado):
    resultado = validate_structure(_make_fingerprint(**atual), _make_fingerprint(**base))
    assert (resultado.level, resultado.passed, resultado.message) == esperado


def test_compare_fingerprints_detalha_cada_diferenca():
    atual = _make_fingerprint(
        table_classes=[["class-a", "class-b"], ["class-c"]],
        key_ids=["id-1", "id-9"],
        table_headers=[["col1", "x"]],
        element_counts={"table": 5, "form": 1},
    )
    base = _make_fingerprint(table_classes=[["class-a", "class-b"], ["class-d"]])
    similaridade, detalhes = compare_fingerprints(atual, base)
    assert similaridade == pytest.approx(0.58)
    assert detalhes == {
        "table_classes_diff": {"missing": [["class-d"]], "new": [["class-c"]]},
        "key_ids_diff": {"missing": ["id-2"], "new": ["id-9"]},
        "table_headers_diff": {"reference": [["col1", "col2", "col3"]], "current": [["col1", "x"]]},
        "element_counts_diff": {"table": {"reference": 2, "current": 5}},
    }


def test_load_baseline_sem_arquivo_nao_avisa(tmp_path):
    with testing.capture_logs() as registros:
        assert load_baseline(Fonte.CEPEA, tmp_path) is None
    assert [r["event"] for r in registros if r["log_level"] == "warning"] == []


def test_validate_against_baseline_compara_com_o_baseline_salvo(tmp_path):
    base = _make_fingerprint(**_DIVERGENTE_BASE)
    save_baseline(base, tmp_path)
    resultado = validate_against_baseline(_make_fingerprint(**_DIVERGENTE_ATUAL), tmp_path)
    assert (resultado.level, resultado.passed) == ("critical", False)
    assert resultado.baseline_fingerprint == base
