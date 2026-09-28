from __future__ import annotations

import hashlib
import os
import ssl
from pathlib import Path

import certifi

from agrobr import constants


def ca_source() -> str:
    if os.environ.get("SSL_CERT_FILE"):
        return "SSL_CERT_FILE"
    if os.environ.get("SSL_CERT_DIR"):
        return "SSL_CERT_DIR"
    return "certifi"


def build_context() -> ssl.SSLContext:
    certificate = Path(__file__).parent / "certs" / "serpro_ar46_2025.pem"
    pem = certificate.read_bytes()
    if hashlib.sha256(pem).hexdigest() != constants.COMEXSTAT_INTERMEDIATE_SHA256:
        raise ValueError("Comex Stat: certificado intermediário local diverge do SHA esperado")
    if os.environ.get("SSL_CERT_FILE"):
        context = ssl.create_default_context(cafile=os.environ["SSL_CERT_FILE"])
    elif os.environ.get("SSL_CERT_DIR"):
        context = ssl.create_default_context(capath=os.environ["SSL_CERT_DIR"])
    else:
        context = ssl.create_default_context(cafile=certifi.where())
    context.verify_flags &= ~getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    context.load_verify_locations(cadata=pem.decode("ascii"))
    if context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
        raise ValueError("Comex Stat: validação TLS integral e hostname são obrigatórios")
    return context
