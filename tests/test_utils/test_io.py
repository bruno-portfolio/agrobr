from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.utils.io import open_excel_safe, read_csv_safe, read_excel_safe, validate_download
from tests.helpers import levanta_exatamente


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
    def test_invalid_data_raises_parse_error(self):
        with pytest.raises(ParseError):
            read_csv_safe(b"", source="test", label="CSV bad")

    @pytest.mark.parametrize("kwargs", [{"chunksize": 1}, {"iterator": True}])
    def test_leitura_preguicosa_recusada(self, kwargs):
        with levanta_exatamente(InvalidParameterError, match="chunksize e iterator"):
            read_csv_safe(b"a,b\n1,2\n", source="test", **kwargs)


class TestExcelSafeFallback:
    def test_read_excel_calamine_falls_back_to_openpyxl(self):
        expected = pd.DataFrame({"valor": [1]})
        with patch(
            "agrobr.utils.io.pd.read_excel",
            side_effect=[RuntimeError("calamine"), expected],
        ) as mocked:
            result = read_excel_safe(b"xlsx", source="test", engine="calamine")

        assert result is expected
        assert mocked.call_args_list[0].kwargs["engine"] == "calamine"
        assert mocked.call_args_list[1].kwargs["engine"] == "openpyxl"

    @pytest.mark.parametrize("sheet_name", [None, ["a", "b"], ("a",)])
    def test_varias_abas_recusadas_antes_de_ler(self, sheet_name):
        with (
            patch("agrobr.utils.io.pd.read_excel") as mocked,
            levanta_exatamente(InvalidParameterError, match="uma aba por vez"),
        ):
            read_excel_safe(b"xlsx", source="test", sheet_name=sheet_name)
        mocked.assert_not_called()

    def test_open_excel_calamine_falls_back_to_openpyxl(self):
        expected = object()
        with patch(
            "agrobr.utils.io.pd.ExcelFile",
            side_effect=[RuntimeError("calamine"), expected],
        ) as mocked:
            result = open_excel_safe(b"xlsx", source="test", engine="calamine")

        assert result is expected
        assert mocked.call_args_list[0].kwargs["engine"] == "calamine"
        assert mocked.call_args_list[1].kwargs["engine"] == "openpyxl"


@pytest.mark.parametrize("kind", ["zip", "xlsx", "xls", "pdf"])
def test_binario_sem_assinatura_do_tipo_e_recusado(kind):
    with levanta_exatamente(SourceUnavailableError, match="Assinatura inválida"):
        validate_download(
            b"conteudo binario sem assinatura " * 4,
            kinds=(kind,),
            source="teste",
            url="https://example.test",
            min_size=10,
        )
