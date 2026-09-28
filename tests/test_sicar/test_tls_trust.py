from __future__ import annotations

import os
import runpy
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import certifi
import pytest

import agrobr
from agrobr.alt.sicar import client
from tests.helpers import levanta_exatamente


@pytest.mark.parametrize(
    "environment,expected",
    [
        ({}, {"cafile": certifi.where()}),
        ({"SSL_CERT_FILE": "custom.pem", "SSL_CERT_DIR": "certs"}, {"cafile": "custom.pem"}),
        ({"SSL_CERT_DIR": "certs"}, {"capath": "certs"}),
    ],
)
def test_tls_usa_certifi_ou_configuracao_explicita(monkeypatch, environment, expected):
    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    factory = Mock()
    monkeypatch.setattr(client.ssl, "create_default_context", factory)
    assert client._create_ssl_context() is factory.return_value
    factory.assert_called_once_with(**expected)


def test_executar_o_modulo_nao_cria_o_contexto_ssl(monkeypatch):
    criar = Mock(return_value=None)
    monkeypatch.setattr(client.ssl, "create_default_context", criar)

    modulo = runpy.run_module("agrobr.alt.sicar.client")

    assert modulo["_ssl_ctx"] is None
    assert criar.call_count == 0


def test_import_com_certificado_invalido_nao_falha(tmp_path):
    raiz = Path(agrobr.__file__).parents[1]
    importar = subprocess.run(
        [sys.executable, "-c", "import agrobr, agrobr.sync, agrobr.alt.sicar.client"],
        cwd=raiz,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        env={
            **os.environ,
            "PYTHONPATH": str(raiz),
            "PYTHONIOENCODING": "utf-8",
            "SSL_CERT_FILE": str(tmp_path / "nao_existe.pem"),
        },
        check=False,
    )
    assert importar.returncode == 0, importar.stderr[-2000:]


def test_consulta_com_certificado_invalido_cita_a_variavel(monkeypatch, tmp_path):
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)
    monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "nao_existe.pem"))
    monkeypatch.setattr(client, "_ssl_ctx", None)

    with levanta_exatamente(OSError, "SSL_CERT_FILE"):
        client.make_session()

    assert client._ssl_ctx is None


def test_contexto_ssl_criado_uma_vez_na_primeira_consulta(monkeypatch):
    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(client, "_ssl_ctx", None)
    criar = Mock(wraps=client._create_ssl_context)
    monkeypatch.setattr(client, "_create_ssl_context", criar)

    primeira, segunda = client.make_session(), client.make_session()

    criar.assert_called_once_with()
    assert primeira._transport._pool._ssl_context is client._ssl_ctx  # type: ignore[attr-defined]
    assert segunda._transport._pool._ssl_context is client._ssl_ctx  # type: ignore[attr-defined]
