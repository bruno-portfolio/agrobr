from __future__ import annotations

import io
import struct
import zipfile
import zlib
from unittest.mock import patch

import pandas as pd
import pytest

from agrobr import constants
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.utils.io import open_excel_safe, read_csv_safe, read_excel_safe, validate_download
from tests.helpers import capturar_logs, levanta_exatamente


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


class TestArgumentoDoPandasInvalido:
    @staticmethod
    def _xlsx() -> bytes:
        buffer = io.BytesIO()
        pd.DataFrame({"a": [1, 2]}).to_excel(buffer, index=False)
        return buffer.getvalue()

    @pytest.mark.parametrize(
        ("kwargs", "fragmento"),
        [
            ({"coluna_que_nao_existe": 1}, "coluna_que_nao_existe"),
            ({"encoding": "latin-1"}, "encoding"),
        ],
    )
    def test_csv_levanta_type_error_e_nao_erro_de_layout(self, kwargs, fragmento):
        with levanta_exatamente(TypeError, fragmento):
            read_csv_safe(b"a,b\n1,2\n", source="test", **kwargs)

    def test_excel_levanta_type_error_sem_abrir_o_outro_motor(self):
        with capturar_logs() as logs, levanta_exatamente(TypeError, "aba_que_nao_existe"):
            read_excel_safe(self._xlsx(), source="test", aba_que_nao_existe=0)
        assert [log for log in logs if log["event"] == "excel_engine_fallback"] == []

    def test_leitura_valida_e_falha_real_seguem_iguais(self):
        assert read_excel_safe(self._xlsx(), source="test")["a"].tolist() == [1, 2]
        assert read_csv_safe(b"a,b\n1,2\n", source="test", sep=",")["b"].tolist() == [2]
        with levanta_exatamente(ParseError, "Erro ao ler"):
            read_csv_safe(b"a,b\n1,2\n", source="test", usecols=["z"])


def _declarar_tamanho(bruto: bytearray, membro: bytes, tamanho: int) -> None:
    for assinatura, campo_nome, campo_tamanho, cabecalho in (
        (b"PK\x03\x04", 26, 22, 30),
        (b"PK\x01\x02", 28, 24, 46),
    ):
        posicao = bruto.find(assinatura)
        while posicao != -1:
            tamanho_nome = struct.unpack_from("<H", bruto, posicao + campo_nome)[0]
            if bruto[posicao + cabecalho : posicao + cabecalho + tamanho_nome] == membro:
                struct.pack_into("<I", bruto, posicao + campo_tamanho, tamanho)
            posicao = bruto.find(assinatura, posicao + 4)


def _com_crc_do_inicio(xml: bytes, tamanho: int) -> bytes:
    """Acrescenta ao XML um comentário que dá ao XML inteiro o CRC dos primeiros ``tamanho`` bytes."""
    sufixo = bytearray(b"<!--" + b"0" * 64 + b"-->")
    base = zlib.crc32(sufixo, zlib.crc32(xml))
    pivos: dict[int, tuple[int, int]] = {}
    for bit in range(64):
        variante = bytearray(sufixo)
        variante[4 + bit] ^= 1
        valor, mascara = zlib.crc32(variante, zlib.crc32(xml)) ^ base, 1 << bit
        while valor and valor.bit_length() - 1 in pivos:
            valor_pivo, mascara_pivo = pivos[valor.bit_length() - 1]
            valor, mascara = valor ^ valor_pivo, mascara ^ mascara_pivo
        if valor:
            pivos[valor.bit_length() - 1] = (valor, mascara)
    resto, solucao = zlib.crc32(xml[:tamanho]) ^ base, 0
    while resto:
        valor, mascara = pivos[resto.bit_length() - 1]
        resto, solucao = resto ^ valor, solucao ^ mascara
    for bit in range(64):
        sufixo[4 + bit] ^= solucao >> bit & 1
    return xml + bytes(sufixo)


def _xlsx_com_planilha_forjada(
    *, membro_ilegivel_antes: bool = False, crc_do_inicio: bool = False
) -> bytes:
    planilha = io.BytesIO()
    pd.DataFrame({"a": range(5000)}).to_excel(planilha, index=False)
    saida = io.BytesIO()
    with (
        zipfile.ZipFile(planilha) as origem,
        zipfile.ZipFile(saida, "w", zipfile.ZIP_DEFLATED) as destino,
    ):
        if membro_ilegivel_antes:
            destino.writestr("docProps/lixo.bin", b"A" * 4096)
        for info in origem.infolist():
            dados = origem.read(info)
            if crc_do_inicio and info.filename == "xl/worksheets/sheet1.xml":
                dados = _com_crc_do_inicio(dados, 1024)
            destino.writestr(info.filename, dados)
    bruto = bytearray(saida.getvalue())
    _declarar_tamanho(bruto, b"xl/worksheets/sheet1.xml", 1024)
    if membro_ilegivel_antes:
        tamanho_nome, tamanho_extra = struct.unpack_from("<HH", bruto, 26)
        bruto[30 + tamanho_nome + tamanho_extra] = 0xFF
    return bytes(bruto)


@pytest.mark.parametrize("leitor", [read_excel_safe, open_excel_safe])
@pytest.mark.parametrize(
    ("montar", "teto", "mensagem"),
    [
        (
            lambda: _xlsx_com_planilha_forjada(membro_ilegivel_antes=True),
            None,
            r"o ZIP do XLSX está ilegível \(error: Error -3",
        ),
        (lambda: b"\x00" + _xlsx_com_planilha_forjada(), 65536, "acima do teto de 65536"),
        (lambda: b"\x00" + _xlsx_com_planilha_forjada(), 1024, "acima do teto de 1024"),
    ],
    ids=["membro_ilegivel_antes_da_planilha", "byte_antes_do_pk", "byte_antes_do_pk_e_teto"],
)
def test_xlsx_forjado_recusado_antes_do_calamine(monkeypatch, leitor, montar, teto, mensagem):
    if teto is not None:
        monkeypatch.setitem(constants.MAX_EXPANDED_BYTES, "teste", teto)
    with capturar_logs() as logs, levanta_exatamente(ResourceLimitError, mensagem):
        leitor(montar(), source="teste")
    assert [log for log in logs if log["event"] == "excel_engine_fallback"] == []


@pytest.mark.parametrize("leitor", [read_excel_safe, open_excel_safe])
@pytest.mark.parametrize("prefixo", [b"", b"\x00"], ids=["sem_prefixo", "byte_antes_do_pk"])
def test_xlsx_com_crc_do_inicio_recusado_pela_expansao_real(monkeypatch, leitor, prefixo):
    bruto = prefixo + _xlsx_com_planilha_forjada(crc_do_inicio=True)
    with zipfile.ZipFile(io.BytesIO(bruto)) as arquivo:
        assert arquivo.testzip() is None
        teto = sum(info.file_size for info in arquivo.infolist()) + 1024
    monkeypatch.setitem(constants.MAX_EXPANDED_BYTES, "teste", teto)

    with (
        capturar_logs() as logs,
        levanta_exatamente(ResourceLimitError, f"expande de fato para mais de {teto} bytes"),
    ):
        leitor(bruto, source="teste")
    assert [log for log in logs if log["event"] == "excel_engine_fallback"] == []
