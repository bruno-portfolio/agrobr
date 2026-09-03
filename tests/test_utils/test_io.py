from __future__ import annotations

import pytest

from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.utils.io import read_csv_safe, validate_download


class TestValidateDownload:
    @pytest.mark.parametrize(
        ("kind", "content"),
        [
            ("zip", b"PK\x03\x04" + b"x" * 100),
            ("xlsx", b"PK\x03\x04" + b"x" * 100),
            ("xls", b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"x" * 100),
            ("pdf", b"%PDF-1.7" + b"x" * 100),
            ("csv", b"coluna,valor\nsoja,1" + b"x" * 100),
        ],
    )
    def test_assinatura_valida(self, kind, content):
        validate_download(
            content,
            kinds=(kind,),
            source="test",
            url="https://example.test/file",
            min_size=10,
        )

    @pytest.mark.parametrize("kind", ["zip", "xlsx", "xls", "pdf", "csv"])
    def test_html_disfarcado(self, kind):
        content = b"  <!DOCTYPE html><html>manutencao</html>" + b"x" * 100

        with pytest.raises(SourceUnavailableError, match="Assinatura inválida"):
            validate_download(
                content,
                kinds=(kind,),
                source="test",
                url="https://example.test/file",
                min_size=10,
            )

    def test_arquivo_pequeno(self):
        with pytest.raises(SourceUnavailableError, match="Download muito pequeno"):
            validate_download(
                b"PK\x03\x04",
                kinds=("zip",),
                source="test",
                url="https://example.test/file",
                min_size=10,
            )


class TestReadCsvSafe:
    def test_utf8_basic(self):
        data = b"col1,col2\n1,2\n3,4"
        df = read_csv_safe(data, source="test")
        assert len(df) == 2
        assert list(df.columns) == ["col1", "col2"]

    def test_latin1_fallback(self):
        data = "col1,col2\nSão Paulo,café\n".encode("latin-1")
        df = read_csv_safe(data, source="test")
        assert len(df) == 1
        assert "São Paulo" in df["col1"].iloc[0]

    def test_kwargs_forwarded_sep(self):
        data = b"col1;col2\n1;2\n3;4"
        df = read_csv_safe(data, source="test", sep=";")
        assert list(df.columns) == ["col1", "col2"]
        assert len(df) == 2

    def test_kwargs_forwarded_dtype(self):
        data = b"id,value\n001,10\n002,20"
        df = read_csv_safe(data, source="test", dtype={"id": str})
        assert df["id"].iloc[0] == "001"

    def test_invalid_data_raises_parse_error(self):
        with pytest.raises(ParseError):
            read_csv_safe(b"", source="test", label="CSV bad")

    def test_custom_label_in_error(self):
        with pytest.raises(ParseError, match="CSV PRODES"):
            read_csv_safe(b"", source="test", label="CSV PRODES")

    def test_parser_version_forwarded(self):
        with pytest.raises(ParseError) as exc_info:
            read_csv_safe(b"", source="test", parser_version=3)
        assert exc_info.value.parser_version == 3


class TestConcatCsvPages:
    def test_empty_pages(self):
        from agrobr.utils.io import concat_csv_pages

        df = concat_csv_pages([], source="test", parser_version=1, empty_columns=["a", "b"])
        assert len(df) == 0
        assert list(df.columns) == ["a", "b"]

    def test_single_page(self):
        from agrobr.utils.io import concat_csv_pages

        data = b"col1,col2\n1,2\n3,4"
        df = concat_csv_pages(
            [data], source="test", parser_version=1, empty_columns=["col1", "col2"]
        )
        assert len(df) == 2

    def test_multi_page(self):
        from agrobr.utils.io import concat_csv_pages

        page1 = b"col1,col2\n1,2"
        page2 = b"col1,col2\n3,4"
        df = concat_csv_pages(
            [page1, page2], source="test", parser_version=1, empty_columns=["col1", "col2"]
        )
        assert len(df) == 2

    def test_page_with_empty_skipped(self):
        from agrobr.utils.io import concat_csv_pages

        page1 = b"col1,col2\n1,2"
        empty_page = b"col1,col2\n"
        df = concat_csv_pages(
            [page1, empty_page], source="test", parser_version=1, empty_columns=["col1", "col2"]
        )
        assert len(df) == 1
