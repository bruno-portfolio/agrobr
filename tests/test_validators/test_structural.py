from __future__ import annotations

import json
from datetime import datetime

from agrobr.constants import Fonte
from agrobr.models import Fingerprint
from agrobr.validators.structural import (
    StructuralValidationResult,
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

    def test_missing_key_ids(self):
        current = _make_fingerprint(key_ids=["id-1"])
        reference = _make_fingerprint(key_ids=["id-1", "id-2", "id-3"])
        similarity, diffs = compare_fingerprints(current, reference)
        assert similarity < 1.0

    def test_empty_reference_table_classes(self):
        current = _make_fingerprint(table_classes=[["x"]])
        reference = _make_fingerprint(table_classes=[])
        similarity, diffs = compare_fingerprints(current, reference)
        assert similarity <= 1.0

    def test_empty_reference_key_ids(self):
        current = _make_fingerprint(key_ids=["x"])
        reference = _make_fingerprint(key_ids=[])
        similarity, _ = compare_fingerprints(current, reference)
        assert similarity <= 1.0

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

    def test_empty_reference_headers(self):
        current = _make_fingerprint(table_headers=[["a"]])
        reference = _make_fingerprint(table_headers=[])
        similarity, _ = compare_fingerprints(current, reference)
        assert similarity <= 1.0

    def test_completely_different(self):
        current = _make_fingerprint(
            structure_hash="xxx",
            table_classes=[["z"]],
            key_ids=["z"],
            table_headers=[["z"]],
            element_counts={"div": 100},
        )
        reference = _make_fingerprint(
            structure_hash="yyy",
            table_classes=[["a"]],
            key_ids=["a"],
            table_headers=[["a"]],
            element_counts={"table": 1},
        )
        similarity, _ = compare_fingerprints(current, reference)
        assert similarity < 0.5


class TestValidateStructure:
    def test_high_similarity_passes(self):
        fp = _make_fingerprint()
        result = validate_structure(fp, fp)
        assert result.passed is True
        assert result.level == "high"

    def test_medium_similarity(self):
        current = _make_fingerprint(structure_hash="new")
        baseline = _make_fingerprint(structure_hash="old")
        result = validate_structure(current, baseline)
        assert result.level in ("high", "medium", "low", "critical")

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

    def test_result_contains_fingerprints(self):
        fp1 = _make_fingerprint()
        fp2 = _make_fingerprint()
        result = validate_structure(fp1, fp2)
        assert result.current_fingerprint is fp1
        assert result.baseline_fingerprint is fp2
        assert result.source == Fonte.CEPEA


class TestLoadBaseline:
    def test_source_specific_file(self, tmp_path):
        fp = _make_fingerprint()
        data = fp.model_dump(mode="json")
        path = tmp_path / "cepea_baseline.json"
        path.write_text(json.dumps(data, default=str))

        result = load_baseline(Fonte.CEPEA, tmp_path)
        assert result is not None
        assert result.source == Fonte.CEPEA

    def test_generic_baseline_fallback(self, tmp_path):
        fp = _make_fingerprint()
        data = fp.model_dump(mode="json")
        path = tmp_path / "baseline.json"
        path.write_text(json.dumps(data, default=str))

        result = load_baseline(Fonte.CEPEA, tmp_path)
        assert result is not None

    def test_file_not_found(self, tmp_path):
        result = load_baseline(Fonte.CEPEA, tmp_path)
        assert result is None

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
    def test_saves_json(self, tmp_path):
        fp = _make_fingerprint()
        save_baseline(fp, tmp_path)

        saved_path = tmp_path / "cepea_baseline.json"
        assert saved_path.exists()

        data = json.loads(saved_path.read_text())
        assert data["source"] == "cepea"

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

    def test_with_matching_baseline(self, tmp_path):
        fp = _make_fingerprint()
        save_baseline(fp, tmp_path)
        result = validate_against_baseline(fp, tmp_path)
        assert result.passed is True

    def test_with_divergent_baseline(self, tmp_path):
        baseline = _make_fingerprint(structure_hash="old")
        save_baseline(baseline, tmp_path)

        current = _make_fingerprint(
            structure_hash="new",
            table_classes=[["z"]],
            key_ids=["z"],
            table_headers=[["z"]],
            element_counts={"div": 100},
        )
        result = validate_against_baseline(current, tmp_path)
        assert isinstance(result, StructuralValidationResult)
