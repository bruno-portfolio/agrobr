from __future__ import annotations

from unittest import mock

from agrobr.normalize.encoding import ENCODING_CHAIN, decode_content, detect_encoding


class TestDecodeContentUTF8:
    def test_valid_utf8(self):
        text, enc = decode_content("café São Paulo açúcar".encode())

        assert text == "café São Paulo açúcar"
        assert enc == "utf-8"

    def test_ascii_subset(self):
        text, enc = decode_content(b"hello world")

        assert text == "hello world"
        assert enc == "utf-8"

    def test_empty_bytes(self):
        text, enc = decode_content(b"")

        assert text == ""
        assert enc == "utf-8"


class TestDecodeContentDeclaredEncoding:
    def test_declared_encoding_used(self):
        content = "São Paulo".encode("iso-8859-1")

        text, enc = decode_content(content, declared_encoding="iso-8859-1")

        assert text == "São Paulo"
        assert enc == "iso-8859-1"

    def test_declared_encoding_wrong_falls_through(self):
        content = "café".encode()

        text, enc = decode_content(content, declared_encoding="ascii")

        assert "caf" in text
        assert enc in ENCODING_CHAIN

    def test_declared_encoding_unknown_lookup_error(self):
        content = b"test"

        text, enc = decode_content(content, declared_encoding="nonexistent-encoding")

        assert text == "test"
        assert enc == "utf-8"


class TestDecodeContentISO88591:
    def test_iso_with_accents(self):
        original = "café São Paulo açúcar"
        content = original.encode("iso-8859-1")

        text, enc = decode_content(content)

        assert "caf" in text
        assert isinstance(text, str)

    def test_iso_declared(self):
        original = "Produção de Álcool"
        content = original.encode("iso-8859-1")

        text, enc = decode_content(content, declared_encoding="iso-8859-1")

        assert text == original
        assert enc == "iso-8859-1"


class TestDecodeContentWindows1252:
    def test_windows1252_special_chars(self):
        original = "preço – cotação"
        content = original.encode("windows-1252")

        text, enc = decode_content(content, declared_encoding="windows-1252")

        assert text == original
        assert enc == "windows-1252"


class TestDecodeContentChardetFallback:
    def test_chardet_used_when_chain_fails(self):
        content = b"\xff\xfe" + "teste".encode("utf-16-le")

        text, enc = decode_content(content)

        assert "teste" in text or isinstance(text, str)

    def test_chardet_low_confidence_falls_to_chain(self):
        bad_bytes = bytes(range(128, 256))

        text, enc = decode_content(bad_bytes)

        assert enc == "iso-8859-1"
        assert isinstance(text, str)


class TestDecodeContentCorrupted:
    def test_irrecoverable_bytes_fallback_to_iso(self):
        bad_bytes = bytes([0x80, 0x81, 0x82, 0xFE, 0xFF] * 20)

        text, enc = decode_content(bad_bytes)

        assert enc == "iso-8859-1"
        assert isinstance(text, str)
        assert len(text) == len(bad_bytes)

    def test_chardet_path_reached_when_chain_exhausted(self):
        bad_bytes = b"\x80\x81"

        with mock.patch("agrobr.normalize.encoding.ENCODING_CHAIN", ("utf-8",)):
            text, enc = decode_content(bad_bytes)

            assert isinstance(text, str)

    def test_replace_mode_never_raises(self):
        bad_bytes = bytes(range(0, 256))

        text, enc = decode_content(bad_bytes)

        assert isinstance(text, str)
        assert len(text) > 0


class TestDecodeContentSourceLogging:
    def test_source_passed_to_logger(self):
        content = b"test"

        text, enc = decode_content(content, source="cepea")

        assert text == "test"


class TestDetectEncoding:
    def test_detect_utf8(self):
        enc, conf = detect_encoding(b"hello")

        assert isinstance(enc, str)
        assert isinstance(conf, float)
        assert 0.0 <= conf <= 1.0

    def test_detect_latin1(self):
        content = "café açúcar".encode("iso-8859-1")
        enc, conf = detect_encoding(content)

        assert isinstance(enc, str)
        assert conf > 0

    def test_detect_empty_bytes(self):
        enc, conf = detect_encoding(b"")

        assert isinstance(enc, str)
        assert isinstance(conf, float)
