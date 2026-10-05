from __future__ import annotations

import hashlib
import os
import ssl
from pathlib import Path

import certifi

from agrobr import constants

CERTIFICADO = Path(__file__).parent / "certs" / "sectigo_ov_r36.pem"


def build_context() -> ssl.SSLContext:
    """Contexto TLS do GeoServer da FUNAI, que envia só o certificado folha, sem o intermediário Sectigo OV R36.

    O intermediário local é conferido pela impressão digital SHA-256 do DER antes de entrar no contexto (independe do EOL do
    arquivo); a raiz vem de ``SSL_CERT_FILE``, ``SSL_CERT_DIR`` ou do certifi, com cadeia integral e hostname.
    """
    pem = CERTIFICADO.read_text("ascii")
    impressao = hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest()
    if impressao != constants.FUNAI_INTERMEDIATE_SHA256:
        raise ValueError("FUNAI: certificado intermediário local diverge do SHA-256 esperado")
    if os.environ.get("SSL_CERT_FILE"):
        context = ssl.create_default_context(cafile=os.environ["SSL_CERT_FILE"])
    elif os.environ.get("SSL_CERT_DIR"):
        context = ssl.create_default_context(capath=os.environ["SSL_CERT_DIR"])
    else:
        context = ssl.create_default_context(cafile=certifi.where())
    context.verify_flags &= ~getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    context.load_verify_locations(cadata=pem)
    if context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
        raise ValueError("FUNAI: validação TLS integral e hostname são obrigatórios")
    return context
