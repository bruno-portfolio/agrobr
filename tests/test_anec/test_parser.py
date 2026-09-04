from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr.anec import parser
from agrobr.exceptions import ParseError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "anec"


def _load_pdf(case_name: str) -> bytes:
    return (GOLDEN_DIR / case_name / "response.pdf").read_bytes()


def _load_expected(case_name: str) -> dict:
    return json.loads((GOLDEN_DIR / case_name / "expected.json").read_text(encoding="utf-8"))


@pytest.fixture
def w04_pdf() -> bytes:
    return _load_pdf("weekly_w04_2026")


class TestCheckPdfplumber:
    def test_returns_module_when_available(self):
        mod = parser._check_pdfplumber()
        assert mod is not None
        assert hasattr(mod, "open")


class TestExtractPagesWords:
    def test_invalid_bytes_raises_parse_error(self):
        with pytest.raises(ParseError, match="Erro abrindo"):
            parser._extract_pages_words(b"not a pdf")

    def test_extracts_w04(self, w04_pdf):
        pages = parser._extract_pages_words(w04_pdf)
        assert len(pages) == 14
        assert all(isinstance(p, list) for p in pages)


class TestGroupByRow:
    def test_empty_returns_empty(self):
        assert parser._group_by_row([]) == []

    def test_groups_close_y(self):
        words = [
            {"text": "A", "x0": 10, "x1": 15, "top": 100, "bottom": 110},
            {"text": "B", "x0": 20, "x1": 25, "top": 101, "bottom": 111},
            {"text": "C", "x0": 10, "x1": 15, "top": 200, "bottom": 210},
        ]
        rows = parser._group_by_row(words, y_tol=3.0)
        assert len(rows) == 2
        assert [w["text"] for w in rows[0][1]] == ["A", "B"]
        assert [w["text"] for w in rows[1][1]] == ["C"]

    def test_sorts_by_x_within_row(self):
        words = [
            {"text": "B", "x0": 50, "x1": 55, "top": 100, "bottom": 110},
            {"text": "A", "x0": 10, "x1": 15, "top": 100, "bottom": 110},
        ]
        rows = parser._group_by_row(words)
        assert [w["text"] for w in rows[0][1]] == ["A", "B"]


class TestConcatFragmentedNumbers:
    def test_merges_fragmented_numbers(self):
        row = [
            {"text": "3", "x0": 100, "x1": 105, "top": 0, "bottom": 10},
            {"text": ".250.783", "x0": 105.5, "x1": 130, "top": 0, "bottom": 10},
        ]
        out = parser._concat_fragmented_numbers(row)
        assert len(out) == 1
        assert out[0]["text"] == "3.250.783"

    def test_does_not_merge_distant_words(self):
        row = [
            {"text": "3", "x0": 100, "x1": 105, "top": 0, "bottom": 10},
            {"text": "999", "x0": 200, "x1": 220, "top": 0, "bottom": 10},
        ]
        out = parser._concat_fragmented_numbers(row)
        assert len(out) == 2

    def test_does_not_merge_text(self):
        row = [
            {"text": "January", "x0": 10, "x1": 50, "top": 0, "bottom": 10},
            {"text": "2025", "x0": 52, "x1": 70, "top": 0, "bottom": 10},
        ]
        out = parser._concat_fragmented_numbers(row)
        assert len(out) == 2

    def test_empty_row(self):
        assert parser._concat_fragmented_numbers([]) == []

    def test_does_not_merge_when_second_lacks_dot_prefix(self):
        row = [
            {"text": "1", "x0": 100, "x1": 105, "top": 0, "bottom": 10},
            {"text": "1.420.655", "x0": 105.5, "x1": 130, "top": 0, "bottom": 10},
        ]
        out = parser._concat_fragmented_numbers(row)
        assert len(out) == 2
        assert out[0]["text"] == "1"
        assert out[1]["text"] == "1.420.655"


class TestParsedReportSchema:
    def test_dataframe_dtypes(self, w04_pdf):
        report = parser.parse_anec_pdf(w04_pdf)

        assert report.weekly_shipments["valor_ton"].dtype == "Float64"
        assert report.monthly_shipments["valor_ton"].dtype == "Float64"
        assert report.monthly_shipments["ano"].dtype == "Int64"
        assert report.monthly_shipments["mes"].dtype == "Int64"
        assert report.yoy_comparison["valor_2025"].dtype == "Float64"
        assert report.yoy_comparison["valor_2026"].dtype == "Float64"
        assert report.yoy_comparison["mes"].dtype == "Int64"
        assert report.destinations["share_pct"].dtype == "Float64"


class TestParseValue:
    def test_dash_returns_none(self):
        assert parser._parse_value("-") is None

    def test_empty_returns_none(self):
        assert parser._parse_value("") is None
        assert parser._parse_value("   ") is None

    def test_br_thousands(self):
        assert parser._parse_value("2.444.711") == 2444711.0

    def test_br_thousands_4_segments(self):
        assert parser._parse_value("11.420.655") == 11420655.0

    def test_simple_decimal(self):
        assert parser._parse_value("123,45") == 123.45

    def test_percent_strips_suffix(self):
        assert parser._parse_value("66%") == 66.0

    def test_percent_with_decimal(self):
        assert parser._parse_value("3,5%") == 3.5

    def test_garbage_returns_none(self):
        assert parser._parse_value("abc") is None
        assert parser._parse_value("***") is None

    def test_negative_value(self):
        assert parser._parse_value("-1.500") == -1500.0


class TestPortsCollapsedLookup:
    def test_no_collisions_in_collapsed_keys(self):
        canon_keys = list(parser.PORTS_CANON.values())
        collapsed_seen: dict[str, str] = {}
        for canon in canon_keys:
            collapsed = parser.re.sub(r"[\s/]+", "", canon).lower()
            if collapsed in collapsed_seen:
                pytest.fail(
                    f"Colisão em PORTS collapsed: {canon!r} colidiu com "
                    f"{collapsed_seen[collapsed]!r} (chave {collapsed!r})"
                )
            collapsed_seen[collapsed] = canon


class TestFingerprintStability:
    def test_same_pdf_same_fingerprint_across_calls(self, w04_pdf):
        fp1 = parser._compute_fingerprint(parser._extract_pages_rows(w04_pdf))
        fp2 = parser._compute_fingerprint(parser._extract_pages_rows(w04_pdf))
        fp3 = parser._compute_fingerprint(parser._extract_pages_rows(w04_pdf))
        assert fp1 == fp2 == fp3

    def test_full_parse_fingerprint_stable(self, w04_pdf):
        r1 = parser.parse_anec_pdf(w04_pdf)
        r2 = parser.parse_anec_pdf(w04_pdf)
        assert r1.fingerprint == r2.fingerprint


class TestPdfErrorPaths:
    def test_truncated_pdf_raises(self):
        with pytest.raises(ParseError, match="Erro abrindo"):
            parser.parse_anec_pdf(b"%PDF-1.7\n0 0 0 0")

    def test_empty_bytes_raises(self):
        with pytest.raises(ParseError, match="Erro abrindo"):
            parser.parse_anec_pdf(b"")

    def test_html_disguised_raises(self):
        with pytest.raises(ParseError, match="Erro abrindo"):
            parser.parse_anec_pdf(b"<html><body>not a pdf</body></html>")


class TestComputeFingerprint:
    def test_deterministic(self, w04_pdf):
        rows = parser._extract_pages_rows(w04_pdf)
        assert parser._compute_fingerprint(rows) == parser._compute_fingerprint(rows)

    def test_different_pdfs_different_fingerprints(self):
        r1 = parser._extract_pages_rows(_load_pdf("weekly_w04_2026"))
        r2 = parser._extract_pages_rows(_load_pdf("weekly_w12_2026"))
        assert parser._compute_fingerprint(r1) != parser._compute_fingerprint(r2)

    def test_length_16(self, w04_pdf):
        rows = parser._extract_pages_rows(w04_pdf)
        assert len(parser._compute_fingerprint(rows)) == 16


class TestResolvePort:
    def test_canonical_match(self):
        assert parser.resolve_port("SANTOS") == "SANTOS"

    def test_accent_match(self):
        assert parser.resolve_port("PARANAGUÁ") == "PARANAGUÁ"

    def test_collapsed_match_for_letter_spaced(self):
        assert parser.resolve_port("SÃO FRANCISCO DO S U L") == "SÃO FRANCISCO DO SUL"

    def test_unknown_returns_none(self):
        assert parser.resolve_port("PORTO INEXISTENTE") is None

    def test_salvador_with_paren(self):
        assert parser.resolve_port("SALVADOR(ENSEADA)") == "SALVADOR (ENSEADA)"


class TestParseWeeklyShipments:
    def test_w04_shape(self, w04_pdf):
        pages_words = parser._extract_pages_words(w04_pdf)
        rows = parser._extract_pages_rows(w04_pdf)
        idx = parser._find_page_with_header(rows, parser.HEADER_WEEKLY)
        df = parser._parse_weekly_shipments(pages_words[idx])
        assert len(df) == 228
        assert df["porto"].nunique() == 19
        assert df["produto"].nunique() == 6
        assert set(df["periodo"].unique()) == {"last_week", "current_week"}

    def test_no_header_raises(self):
        empty_words: list[dict] = []
        with pytest.raises(ParseError, match="Header row"):
            parser._parse_weekly_shipments(empty_words)


class TestParseMonthlyShipments:
    def test_w04_has_2026(self, w04_pdf):
        pages_words = parser._extract_pages_words(w04_pdf)
        rows = parser._extract_pages_rows(w04_pdf)
        idx = parser._find_page_with_header(rows, parser.HEADER_MONTHLY)
        df = parser._parse_monthly_shipments(pages_words[idx])
        assert 2026 in df["ano"].dropna().unique().tolist()
        assert df["produto"].nunique() == 6
        assert df["mes"].nunique() == 12

    def test_january_values(self, w04_pdf):
        pages_words = parser._extract_pages_words(w04_pdf)
        rows = parser._extract_pages_rows(w04_pdf)
        idx = parser._find_page_with_header(rows, parser.HEADER_MONTHLY)
        df = parser._parse_monthly_shipments(pages_words[idx])
        jan_2026 = df[(df["ano"] == 2026) & (df["mes"] == 1)]
        soybean = jan_2026[jan_2026["produto"] == "soybean"]["valor_ton"].iloc[0]
        assert soybean == pytest.approx(2444711.0)


class TestParseYoyComparison:
    def test_w04_4_products(self, w04_pdf):
        pages_words = parser._extract_pages_words(w04_pdf)
        rows = parser._extract_pages_rows(w04_pdf)
        indexes = parser._find_pages_with_header(rows, parser.HEADER_YOY)
        df = parser._parse_yoy_comparison(pages_words, indexes)
        produtos = sorted(df["produto"].unique().tolist())
        assert produtos == ["maize", "soybean", "soybean_meal", "wheat"]


class TestParseAnecPdfOrchestrator:
    def test_returns_parsed_report(self, w04_pdf):
        report = parser.parse_anec_pdf(w04_pdf)
        assert isinstance(report, parser.ParsedReport)
        assert isinstance(report.weekly_shipments, pd.DataFrame)
        assert isinstance(report.monthly_shipments, pd.DataFrame)
        assert isinstance(report.yoy_comparison, pd.DataFrame)
        assert isinstance(report.destinations, pd.DataFrame)
        assert isinstance(report.fingerprint, str)
        assert len(report.fingerprint) == 16

    def test_invalid_pdf_raises(self):
        with pytest.raises(ParseError):
            parser.parse_anec_pdf(b"not a pdf at all")


GOLDEN_CASES = [
    "weekly_w04_2026",
    "weekly_w08_2026",
    "weekly_w12_2026",
    "weekly_w13_2026",
]


@pytest.mark.parametrize("case_name", GOLDEN_CASES)
class TestGoldenRegression:
    def test_weekly_shape(self, case_name):
        pdf = _load_pdf(case_name)
        expected = _load_expected(case_name)
        report = parser.parse_anec_pdf(pdf)
        wk = report.weekly_shipments
        assert len(wk) == expected["weekly_shipments"]["count"]
        assert wk["porto"].nunique() == expected["weekly_shipments"]["portos_count"]
        assert wk["produto"].nunique() == expected["weekly_shipments"]["produtos_count"]
        assert (
            sorted(wk["periodo"].unique().tolist()) == expected["weekly_shipments"]["periodos_set"]
        )
        assert wk["valor_ton"].notna().sum() >= expected["weekly_shipments"]["non_null_min"]

    def test_weekly_primary_key_is_unique(self, case_name):
        report = parser.parse_anec_pdf(_load_pdf(case_name))

        assert not report.weekly_shipments.duplicated(["porto", "produto", "periodo"]).any()

    def test_monthly_shape(self, case_name):
        pdf = _load_pdf(case_name)
        expected = _load_expected(case_name)
        report = parser.parse_anec_pdf(pdf)
        mt = report.monthly_shipments
        assert len(mt) == expected["monthly_shipments"]["count"]
        assert (
            sorted([int(a) for a in mt["ano"].dropna().unique()])
            == expected["monthly_shipments"]["anos_set"]
        )
        assert mt["produto"].nunique() == expected["monthly_shipments"]["produtos_count"]

    def test_yoy_shape(self, case_name):
        pdf = _load_pdf(case_name)
        expected = _load_expected(case_name)
        report = parser.parse_anec_pdf(pdf)
        yo = report.yoy_comparison
        assert len(yo) == expected["yoy_comparison"]["count"]
        produtos = set(yo["produto"].unique().tolist())
        expected_produtos = set(expected["yoy_comparison"]["produtos_min_set"])
        assert expected_produtos.issubset(produtos)

    def test_destinations_shape(self, case_name):
        pdf = _load_pdf(case_name)
        expected = _load_expected(case_name)
        report = parser.parse_anec_pdf(pdf)
        dt = report.destinations
        assert len(dt) >= expected["destinations"]["count_min"]
        assert list(dt.columns) == expected["destinations"]["columns"]

    def test_fingerprint_matches_metadata(self, case_name):
        pdf = _load_pdf(case_name)
        metadata = json.loads(
            (GOLDEN_DIR / case_name / "metadata.json").read_text(encoding="utf-8")
        )
        report = parser.parse_anec_pdf(pdf)
        assert report.fingerprint == metadata["fingerprint"]
