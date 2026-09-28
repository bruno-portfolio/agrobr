from __future__ import annotations

import ssl

import certifi
import pytest

from agrobr import constants
from agrobr.comexstat import _tls


@pytest.fixture(autouse=True)
def clean_ca_environment(monkeypatch):
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)


def test_real_default_context_preserves_full_chain_and_hostname():
    context = _tls.build_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname
    assert not context.verify_flags & getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    assert context.cert_store_stats()["x509_ca"] > 1
    assert _tls.ca_source() == "certifi"


@pytest.mark.parametrize(
    "file,directory,expected",
    [
        ("custom.pem", "custom-dir", {"cafile": "custom.pem"}),
        ("", "custom-dir", {"capath": "custom-dir"}),
        (None, None, {"cafile": certifi.where()}),
    ],
)
def test_environment_ca_precedence_and_partial_chain_removal(
    monkeypatch, file, directory, expected
):
    if file is not None:
        monkeypatch.setenv("SSL_CERT_FILE", file)
    if directory is not None:
        monkeypatch.setenv("SSL_CERT_DIR", directory)
    calls = []

    def factory(**kwargs):
        calls.append(kwargs)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.verify_flags |= getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
        return context

    monkeypatch.setattr(_tls.ssl, "create_default_context", factory)
    context = _tls.build_context()
    assert calls == [expected]
    assert not context.verify_flags & getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED


def test_modified_certificate_rejected_before_context(monkeypatch):
    monkeypatch.setattr(constants, "COMEXSTAT_INTERMEDIATE_SHA256", "0" * 64)
    monkeypatch.setattr(
        _tls.ssl, "create_default_context", lambda **_: pytest.fail("created unsafe context")
    )
    with pytest.raises(ValueError, match="SHA"):
        _tls.build_context()
