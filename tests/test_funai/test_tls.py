from __future__ import annotations

import ssl
import warnings
from datetime import UTC, datetime
from pathlib import Path

import certifi
import httpx
import pytest

from agrobr.funai import _tls
from agrobr.funai import _transport as funai_transport
from agrobr.http import wfs_transport
from agrobr.incra import _transport as incra_transport

FOLHA_GEOSERVER_20261004 = """-----BEGIN CERTIFICATE-----
MIIHPzCCBaegAwIBAgIRAMN/syuaECUIl1KkqVkPrcAwDQYJKoZIhvcNAQELBQAw
YDELMAkGA1UEBhMCR0IxGDAWBgNVBAoTD1NlY3RpZ28gTGltaXRlZDE3MDUGA1UE
AxMuU2VjdGlnbyBQdWJsaWMgU2VydmVyIEF1dGhlbnRpY2F0aW9uIENBIE9WIFIz
NjAeFw0yNjA5MTQwMDAwMDBaFw0yNzAzMzEyMzU5NTlaMHkxCzAJBgNVBAYTAkJS
MRkwFwYDVQQIExBEaXN0cml0byBGZWRlcmFsMTYwNAYDVQQKEy1GVU5EQUNBTyBO
QUNJT05BTCBET1MgUE9WT1MgSU5ESUdFTkFTIC0gRlVOQUkxFzAVBgNVBAMMDiou
ZnVuYWkuZ292LmJyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmZY5
CZO8R0Ckov9KiSxM9iE86D4gzCSQ19Gf8VwpgbEwnIsVwboLIRPyZcKIem46k0MU
dV0zyVHCjIFFV6Ovut8VJ6dsO+jCthyYeV4+g1+lk5DNLOPA+YKnGdUTTSOiKtxa
Gn7tG7BSkkt/QcAoOScf/7mqiA5W5zvYGoY20u5F7qQBmlheNsKR/SlxWknqwT2w
hpr5uLXbWlC28KTEluPZM3+zBzdDKVMSs2t5m4YC5PQ8Rnu1SFLMcBVmeH7GIqdm
bjiX4Djk7tgetEmTCJ+PRQa2RxaYmlkKtWLA8bWAcsJ6mMutlqxzt+mQu7oHAgol
qHnPGiu6+Q1cUySyRQIDAQABo4IDWTCCA1UwHwYDVR0jBBgwFoAU42Z0u3BojSxd
Tg6mSo+bNyKcgpIwHQYDVR0OBBYEFNYmz6RzRJIdCu8Rp7GDyuNp0V8pMA4GA1Ud
DwEB/wQEAwIFoDAMBgNVHRMBAf8EAjAAMBMGA1UdJQQMMAoGCCsGAQUFBwMBMEoG
A1UdIARDMEEwNQYMKwYBBAGyMQECAQMEMCUwIwYIKwYBBQUHAgEWF2h0dHBzOi8v
c2VjdGlnby5jb20vQ1BTMAgGBmeBDAECAjBUBgNVHR8ETTBLMEmgR6BFhkNodHRw
Oi8vY3JsLnNlY3RpZ28uY29tL1NlY3RpZ29QdWJsaWNTZXJ2ZXJBdXRoZW50aWNh
dGlvbkNBT1ZSMzYuY3JsMIGEBggrBgEFBQcBAQR4MHYwTwYIKwYBBQUHMAKGQ2h0
dHA6Ly9jcnQuc2VjdGlnby5jb20vU2VjdGlnb1B1YmxpY1NlcnZlckF1dGhlbnRp
Y2F0aW9uQ0FPVlIzNi5jcnQwIwYIKwYBBQUHMAGGF2h0dHA6Ly9vY3NwLnNlY3Rp
Z28uY29tMCcGA1UdEQQgMB6CDiouZnVuYWkuZ292LmJyggxmdW5haS5nb3YuYnIw
ggGMBgorBgEEAdZ5AgQCBIIBfASCAXgBdgB9AFlubDOGlLJZcqJWyKDo3ZBKdugI
PdqHOwEIOCgUPO5ZAAABoKEU+5wACAAABQAEaqhYBAMARjBEAiAQhPcp7uqe3/HP
Lyimte9lyWsK1N2ob84EaPJ/WbgaOgIgQajW2jqv1xsxXg1Pb/k3Oy5IYPRY1tGn
mSuVnQAX4IEAdQAcn2gs6frwRWlQ+BuWiofd2zIQ2EzmyLLjglJKxM9ZnwAAAaCh
FPttAAAEAwBGMEQCIHNxSQ+MzQaQDhPjLjyoXhstqzzS063APHu3GGeDksl1AiBr
hQgF2iIFQXGn8wRYcnOox2oG8FtCNX+BPB2zchKxKAB+AKKBABhzThduHUfglUDz
gbpUZpfNY6hDUHFuuAlO2vENAAABoKEU/f4ACAAABQANXzj2BAMARzBFAiAWRCpe
cz2FUEzb8u+DI5IVz5jBedoBK0cCFmYxkJy2qwIhAMZaQv0pSah6c1DoMjJf3al5
GYLIP4rDN28TgnPFMWBFMA0GCSqGSIb3DQEBCwUAA4IBgQBQ6qcBHdoLi2uKXdpi
ZhO4ZQOM1gLgC2g2OXLdpeHTtsvoJc6Y2tD0Xijm4TZVWgQ0ISlJ/FuH7lbpyHTe
vpRlGf2IXsrChjY/6Hj0/Mz6WJURhDK1cMhUeAD2AptfdhUp1T32B0JUs6qwXEpU
MSH554x744Ru4oEpo7fxyXOEN3fdJeyJJxfBfIvpbYDT0DYf6PmjQsPDw7tx8Lsi
Ua/lCHr5dDyG2aFPQ3kLWJjnSsJaJo+YueNROMf3ZC5eimtzZo6Unwad/xJ96ig7
pUV2V8Hcg0PK4z1cbQM405oaCofhb2iDHzTvdd0+X/04Qiyk3I+jJn/HAzN4DSFW
e0oXi0f6azaFLxUgITOUWxw/unhpDEUtW4Bl98hcy3zO5wEpHGoHFadDp57xUCV6
JG6t1NoX6TeAyt73azpwPY7B4agAXAm14cjVi/YWpaTds663EMMkgDdnDffJNWZM
oTfK2OrOxdUBSdV79JZyfFQBgRef7jn4DsBTLi2EAUIYJ5U=
-----END CERTIFICATE-----
"""
SUJEITO_INTERMEDIARIO = "Sectigo Public Server Authentication CA OV R36"


@pytest.fixture(autouse=True)
def sem_ca_do_ambiente(monkeypatch):
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)


def _intermediarios(contexto: ssl.SSLContext) -> list[str]:
    return [
        dict(campo[0] for campo in certificado["subject"])["commonName"]
        for certificado in contexto.get_ca_certs()
        if dict(campo[0] for campo in certificado["subject"]).get("commonName")
        == SUJEITO_INTERMEDIARIO
    ]


def test_contexto_carrega_o_intermediario_com_cadeia_integral_e_hostname():
    contexto = _tls.build_context()

    assert _intermediarios(contexto) == [SUJEITO_INTERMEDIARIO]
    assert contexto.verify_mode == ssl.CERT_REQUIRED and contexto.check_hostname
    assert not contexto.verify_flags & getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    assert contexto.cert_store_stats()["x509_ca"] > 1


def test_folha_gravada_da_funai_so_fecha_a_cadeia_com_o_intermediario_fixado():
    verificacao = pytest.importorskip("cryptography.x509.verification")
    from cryptography import x509

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        raizes = x509.load_pem_x509_certificates(Path(certifi.where()).read_bytes())
    folha = x509.load_pem_x509_certificate(FOLHA_GEOSERVER_20261004.encode())
    intermediario = x509.load_pem_x509_certificate(_tls.CERTIFICADO.read_bytes())
    verificador = (
        verificacao.PolicyBuilder()
        .store(verificacao.Store(raizes))
        .time(datetime(2026, 10, 4, tzinfo=UTC))
        .build_server_verifier(x509.DNSName("geoserver.funai.gov.br"))
    )

    with pytest.raises(verificacao.VerificationError):
        verificador.verify(folha, [])
    cadeia = verificador.verify(folha, [intermediario])
    assert [c.subject.rfc4514_string().split(",")[0] for c in cadeia] == [
        "CN=*.funai.gov.br",
        f"CN={SUJEITO_INTERMEDIARIO}",
        "CN=Sectigo Public Server Authentication Root R46",
    ]


@pytest.mark.parametrize("fim_de_linha", ["\n", "\r\n"], ids=["lf", "crlf"])
def test_impressao_digital_do_der_independe_do_fim_de_linha(monkeypatch, tmp_path, fim_de_linha):
    copia = tmp_path / "intermediario.pem"
    texto = _tls.CERTIFICADO.read_text("ascii").replace("\r\n", "\n")
    copia.write_bytes(texto.replace("\n", fim_de_linha).encode("ascii"))
    monkeypatch.setattr(_tls, "CERTIFICADO", copia)

    assert _intermediarios(_tls.build_context()) == [SUJEITO_INTERMEDIARIO]


def test_der_com_um_byte_trocado_e_recusado_antes_do_contexto(monkeypatch, tmp_path):
    der = bytearray(ssl.PEM_cert_to_DER_cert(_tls.CERTIFICADO.read_text("ascii")))
    der[100] ^= 0x01
    copia = tmp_path / "intermediario.pem"
    copia.write_text(ssl.DER_cert_to_PEM_cert(bytes(der)), "ascii")
    monkeypatch.setattr(_tls, "CERTIFICADO", copia)
    monkeypatch.setattr(
        _tls.ssl, "create_default_context", lambda **_: pytest.fail("contexto criado")
    )

    with pytest.raises(ValueError, match="SHA-256"):
        _tls.build_context()


@pytest.mark.parametrize(
    "arquivo,pasta,esperado",
    [
        ("custom.pem", "custom-dir", {"cafile": "custom.pem"}),
        ("", "custom-dir", {"capath": "custom-dir"}),
        (None, None, {"cafile": certifi.where()}),
    ],
)
def test_raizes_do_ambiente_tem_precedencia_sobre_o_certifi(monkeypatch, arquivo, pasta, esperado):
    if arquivo is not None:
        monkeypatch.setenv("SSL_CERT_FILE", arquivo)
    if pasta is not None:
        monkeypatch.setenv("SSL_CERT_DIR", pasta)
    chamadas = []

    def fabrica(**kwargs):
        chamadas.append(kwargs)
        contexto = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        contexto.verify_flags |= getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
        return contexto

    monkeypatch.setattr(_tls.ssl, "create_default_context", fabrica)
    contexto = _tls.build_context()

    assert chamadas == [esperado]
    assert not contexto.verify_flags & getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    assert _intermediarios(contexto) == [SUJEITO_INTERMEDIARIO]


async def test_sessao_wfs_da_funai_usa_o_contexto_e_as_outras_fontes_o_padrao(monkeypatch):
    recebidos = []
    original = httpx.AsyncClient

    def fabrica(**kwargs):
        recebidos.append(kwargs["verify"])
        return original(**kwargs)

    monkeypatch.setattr(wfs_transport.httpx, "AsyncClient", fabrica)
    for transporte in (funai_transport.Transport(), incra_transport.Transport()):
        async with transporte.session():
            pass

    funai, incra = recebidos
    assert isinstance(funai, ssl.SSLContext) and _intermediarios(funai) == [SUJEITO_INTERMEDIARIO]
    assert incra is True
