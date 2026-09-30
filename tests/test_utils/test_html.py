from __future__ import annotations

from agrobr.utils.html import parse_links_from_html


class TestParseLinksFromHtml:
    def test_xlsx_filter(self):
        html = '<a href="/data.xlsx">Data</a><a href="/data.csv">CSV</a>'
        links = parse_links_from_html(html, pattern=r"\.xlsx")
        assert len(links) == 1
        assert links[0]["text"] == "Data"

    def test_dedup_on(self):
        html = '<a href="/f.pdf">A</a><a href="/f.pdf">B</a>'
        links = parse_links_from_html(html, pattern=r"\.pdf", dedup=True)
        assert len(links) == 1

    def test_dedup_off(self):
        html = '<a href="/f.pdf">A</a><a href="/f.pdf">B</a>'
        links = parse_links_from_html(html, pattern=r"\.pdf", dedup=False)
        assert len(links) == 2

    def test_base_url_resolve(self):
        html = '<a href="/downloads/file.xls">XLS</a>'
        links = parse_links_from_html(html, base_url="https://example.com", pattern=r"\.xls")
        assert links[0]["url"] == "https://example.com/downloads/file.xls"

    def test_base_url_resolve_href_relativo(self):
        html = '<a href="a.pdf">A</a><a href="../b.pdf">B</a><a href="mailto:c.pdf">C</a>'
        links = parse_links_from_html(html, base_url="https://www.gov.br/conab", pattern=r"\.pdf")
        assert [link["url"] for link in links] == [
            "https://www.gov.br/conab/a.pdf",
            "https://www.gov.br/b.pdf",
            "mailto:c.pdf",
        ]

    def test_empty_text_fallback(self):
        html = '<a href="/downloads/my-report.pdf"></a>'
        links = parse_links_from_html(html, pattern=r"\.pdf")
        assert links[0]["text"] == "my report"

    def test_absolute_url_not_prefixed(self):
        html = '<a href="https://other.com/file.pdf">PDF</a>'
        links = parse_links_from_html(html, base_url="https://example.com", pattern=r"\.pdf")
        assert links[0]["url"] == "https://other.com/file.pdf"
