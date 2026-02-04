"""Tests for CEPEA fingerprinting."""

from __future__ import annotations

from agrobr.cepea.parsers.fingerprint import (
    compare_fingerprints,
    extract_fingerprint,
)
from agrobr.constants import Fonte


class TestFingerprint:
    """Tests for fingerprint extraction and comparison."""

    def test_extract_fingerprint_from_html(self, sample_html_cepea):
        """Test fingerprint extraction from valid HTML."""
        fp = extract_fingerprint(sample_html_cepea, Fonte.CEPEA, "test_url")

        assert fp.source == Fonte.CEPEA
        assert fp.url == "test_url"
        assert fp.structure_hash is not None
        assert len(fp.structure_hash) > 0

    def test_fingerprint_has_table_info(self, sample_html_cepea):
        """Test fingerprint captures table information."""
        fp = extract_fingerprint(sample_html_cepea, Fonte.CEPEA, "test_url")

        assert "tables" in fp.element_counts
        assert fp.element_counts["tables"] >= 1

    def test_compare_identical_fingerprints(self, sample_html_cepea):
        """Test comparing identical fingerprints returns 1.0."""
        fp1 = extract_fingerprint(sample_html_cepea, Fonte.CEPEA, "test_url")
        fp2 = extract_fingerprint(sample_html_cepea, Fonte.CEPEA, "test_url")

        similarity, diff = compare_fingerprints(fp1, fp2)

        assert similarity == 1.0
        assert len(diff) == 0

    def test_compare_different_fingerprints(self, sample_html_cepea, sample_html_empty):
        """Test comparing different fingerprints returns low similarity."""
        fp1 = extract_fingerprint(sample_html_cepea, Fonte.CEPEA, "test_url")
        fp2 = extract_fingerprint(sample_html_empty, Fonte.CEPEA, "test_url")

        similarity, diff = compare_fingerprints(fp1, fp2)

        assert similarity < 1.0

    def test_fingerprint_structure_hash_changes(self):
        """Test that different HTML structures produce different hashes."""
        html1 = "<html><body><table><tr><td>A</td></tr></table></body></html>"
        html2 = "<html><body><div><table><tr><td>A</td></tr></table></div></body></html>"

        fp1 = extract_fingerprint(html1, Fonte.CEPEA, "test")
        fp2 = extract_fingerprint(html2, Fonte.CEPEA, "test")

        assert fp1.structure_hash != fp2.structure_hash

    def test_fingerprint_captures_ids(self, sample_html_cepea):
        """Test fingerprint captures relevant IDs."""
        fp = extract_fingerprint(sample_html_cepea, Fonte.CEPEA, "test_url")

        assert "tblIndicador" in fp.key_ids
