from __future__ import annotations

from typing import Any

import pytest

from agrobr.cepea.parsers.fingerprint import extract_fingerprint
from agrobr.constants import Fonte
from agrobr.validators.structural import compare_fingerprints
from tests import helpers


class TestFingerprint:
    """Tests for fingerprint extraction and comparison."""

    @pytest.mark.parametrize(
        "scenario,parameters",
        [
            ("test_fingerprint_has_table_info", {}),
            ("test_fingerprint_structure_hash_changes", {}),
            ("test_fingerprint_captures_ids", {}),
        ],
        ids=[
            "fingerprint_has_table_info-0",
            "fingerprint_structure_hash_changes-0",
            "fingerprint_captures_ids-0",
        ],
    )
    def test_estrutura_identificadores_e_tabelas(
        self, scenario: str, parameters: dict[str, Any], sample_html_cepea: Any
    ):
        with (
            helpers.collect_failures() as check,
            check((scenario, parameters)),
            helpers.isolated_dataset_case((scenario, parameters)),
        ):
            if scenario == "test_fingerprint_has_table_info":
                fp = extract_fingerprint(sample_html_cepea, Fonte.CEPEA, "test_url")
                assert "tables" in fp.element_counts
                assert fp.element_counts["tables"] >= 1
            elif scenario == "test_fingerprint_structure_hash_changes":
                html1 = "<html><body><table><tr><td>A</td></tr></table></body></html>"
                html2 = "<html><body><div><table><tr><td>A</td></tr></table></div></body></html>"
                fp1 = extract_fingerprint(html1, Fonte.CEPEA, "test")
                fp2 = extract_fingerprint(html2, Fonte.CEPEA, "test")
                assert fp1.structure_hash != fp2.structure_hash
            elif scenario == "test_fingerprint_captures_ids":
                fp = extract_fingerprint(sample_html_cepea, Fonte.CEPEA, "test_url")
                assert "imagenet-indicador1" in fp.key_ids

    def test_compare_different_fingerprints(self, sample_html_cepea, sample_html_empty):
        fp1 = extract_fingerprint(sample_html_cepea, Fonte.CEPEA, "test_url")
        fp2 = extract_fingerprint(sample_html_empty, Fonte.CEPEA, "test_url")

        similarity, diff = compare_fingerprints(fp1, fp2)

        assert similarity < 1.0
        assert compare_fingerprints(fp2, fp2)[0] == 1.0
