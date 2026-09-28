from __future__ import annotations

from agrobr.normalize.encoding import (
    ENCODING_CHAIN,
    decode_content,
    detect_encoding,
    detect_encoding_chain,
)
from tests.helpers import collect_failures


class TestDecodeContentDeclaredEncoding:
    def test_declared_encoding_unknown_lookup_error(self):
        with collect_failures() as check:
            with check("test_declared_encoding_used"):
                content = "São Paulo".encode("iso-8859-1")

                text, enc = decode_content(content, declared_encoding="iso-8859-1")

                assert text == "São Paulo"
                assert enc == "iso-8859-1"
            with check("test_declared_encoding_wrong_falls_through"):
                content = "café".encode()

                text, enc = decode_content(content, declared_encoding="ascii")

                assert "caf" in text
                assert enc in ENCODING_CHAIN
            with check("test_declared_encoding_unknown_lookup_error"):
                content = b"test"

                text, enc = decode_content(content, declared_encoding="nonexistent-encoding")

                assert text == "test"
                assert enc == "utf-8"


class TestDecodeContentISO88591:
    def test_iso_declared(self):
        with collect_failures() as check:
            with check("test_iso_with_accents"):
                original = "café São Paulo açúcar"
                content = original.encode("iso-8859-1")

                text, enc = decode_content(content)

                assert "caf" in text
                assert isinstance(text, str)
            with check("test_iso_declared"):
                original = "Produção de Álcool"
                content = original.encode("iso-8859-1")

                text, enc = decode_content(content, declared_encoding="iso-8859-1")

                assert text == original
                assert enc == "iso-8859-1"


class TestDetectEncoding:
    def test_detect_empty_bytes(self):
        with collect_failures() as check:
            with check("test_detect_utf8"):
                enc, conf = detect_encoding(b"hello")

                assert isinstance(enc, str)
                assert isinstance(conf, float)
                assert 0.0 <= conf <= 1.0
            with check("test_detect_latin1"):
                content = "café açúcar".encode("iso-8859-1")
                enc, conf = detect_encoding(content)

                assert isinstance(enc, str)
                assert conf > 0
            with check("test_detect_empty_bytes"):
                enc, conf = detect_encoding(b"")

                assert isinstance(enc, str)
                assert isinstance(conf, float)


class TestDetectEncodingChainStress:
    def test_all_256_bytes(self):
        with collect_failures() as check:
            with check("test_multibyte_utf8"):
                content = "价格数据".encode()
                assert detect_encoding_chain(content) == "utf-8"
            with check("test_emoji_utf8"):
                content = "teste 🌾".encode()
                assert detect_encoding_chain(content) == "utf-8"
            with check("test_cedilla_tilde_iso"):
                content = "Conceição São".encode("iso-8859-1")
                enc = detect_encoding_chain(content)
                assert enc in ("windows-1252", "iso-8859-1")
            with check("test_euro_sign_w1252"):
                content = b"pre\x80o"
                assert detect_encoding_chain(content) == "windows-1252"
            with check("test_smart_quotes_office"):
                content = b"\x93quoted\x94"
                assert detect_encoding_chain(content) == "windows-1252"
            with check("test_en_em_dash"):
                content = b"\x96\x97"
                assert detect_encoding_chain(content) == "windows-1252"
            with check("test_all_256_bytes"):
                content = bytes(range(256))
                enc = detect_encoding_chain(content)
                assert enc in ("windows-1252", "iso-8859-1")
            with check("test_round_trip_cross_encoding"):
                text = "São Paulo café"
                for encoding in ("utf-8", "iso-8859-1", "windows-1252"):
                    content = text.encode(encoding)
                    enc = detect_encoding_chain(content)
                    assert isinstance(enc, str)


def test_detect_encoding_chain_reconhece_bom_utf8():
    assert detect_encoding_chain(b"\xef\xbb\xbfsafra") == "utf-8-sig"


def test_decode_content_iso_8859_1_fecha_a_cadeia():
    text, enc = decode_content(b"Pre\xe7o\x81 \x80")

    assert text == "Preço\x81 \x80"
    assert enc == "iso-8859-1"


def test_deteccao_por_blocos_respeita_a_fronteira_e_o_fim():
    bloco = 1 << 20
    fronteira = b"a" * (bloco - 1) + "é".encode()
    fim_incompleto = b"a" * bloco + b"\xc3"
    invalido_depois_do_bloco = b"a" * (bloco + 10) + b"\x80"

    assert detect_encoding_chain(fronteira) == "utf-8"
    assert detect_encoding_chain(fim_incompleto) == "windows-1252"
    assert detect_encoding_chain(invalido_depois_do_bloco) == "windows-1252"
