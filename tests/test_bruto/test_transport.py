from __future__ import annotations

import gzip
import hashlib
import time
import zlib

import httpx
import pytest

from agrobr.bruto import models, transport
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError
from tests.test_bruto.conftest import resposta

URL = "https://fonte.test/dado"


def _pedido(papel="pagina"):
    return models.PedidoHTTP(url=URL, parametros={"a": "1"}, papel=papel, numero=1, formato="json")


def _orcamento(**limites):
    return transport.Orcamento(
        "teste", models.LimitesBrutos(**limites), prazo=time.monotonic() + 60
    )


async def _baixar(respostas, *, orcamento=None, teto=10_000, papel="pagina", temporario=None):
    fila = list(respostas)
    pedidos = []

    def responder(request):
        pedidos.append(request)
        item = fila.pop(0)
        if isinstance(item, Exception):
            raise item
        return item(request) if callable(item) else item

    async with httpx.AsyncClient(transport=httpx.MockTransport(responder)) as http:
        capturada = await transport.baixar(
            http,
            _pedido(papel),
            fonte="teste",
            orcamento=orcamento or _orcamento(),
            relogio=transport.Relogio(),
            teto=teto,
            temporario=temporario,
        )
    return capturada, pedidos


@pytest.mark.parametrize(
    "corpo",
    [b"a;b\r\nS\xe3o Jos\xe9\r\n", b"a;b\nS\xc3\xa3o Jos\xc3\xa9\n", b"", b"{}"],
    ids=["latin1_crlf", "utf8_lf", "vazio", "controle_minimo"],
)
async def test_corpo_preservado_byte_a_byte(corpo):
    capturada, pedidos = await _baixar([resposta(200, corpo)])

    assert capturada.corpo == corpo and capturada.tamanho == len(corpo)
    assert capturada.sha256 == hashlib.sha256(corpo).hexdigest()
    assert (
        capturada.url_solicitada == f"{URL}?a=1"
        and pedidos[0].headers["accept-encoding"] == "gzip, deflate"
    )


@pytest.mark.parametrize("encoding", ["gzip", "deflate", "deflate_cru"])
async def test_content_encoding_decodificado_em_blocos(encoding):
    corpo = b"x" * 300_000 + b"fim"
    if encoding == "gzip":
        codificado = gzip.compress(corpo)
    elif encoding == "deflate":
        codificado = zlib.compress(corpo)
    else:
        compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        codificado = compressor.compress(corpo) + compressor.flush()
    cabecalho = "gzip" if encoding == "gzip" else "deflate"

    orcamento = _orcamento()
    capturada, _ = await _baixar(
        [resposta(200, codificado, {"Content-Encoding": cabecalho})],
        orcamento=orcamento,
        teto=400_000,
    )

    assert capturada.corpo == corpo and capturada.cabecalhos == {"content-encoding": cabecalho}
    assert (orcamento.codificados, orcamento.decodificados) == (len(codificado), len(corpo))


async def test_expansao_do_gzip_para_no_teto_sem_reter_o_corpo():
    bomba = gzip.compress(b"\x00" * 50_000_000)

    with pytest.raises(ResourceLimitError, match="decodificado passou do teto de 1000000"):
        await _baixar([resposta(200, bomba, {"Content-Encoding": "gzip"})], teto=1_000_000)


async def test_gzip_truncado_e_falha_transitoria_e_repete():
    corpo = b"dado" * 1000
    truncado = gzip.compress(corpo)[:-20]

    capturada, pedidos = await _baixar(
        [resposta(200, truncado, {"Content-Encoding": "gzip"}), resposta(200, corpo)]
    )

    assert capturada.corpo == corpo and len(pedidos) == 2


async def test_retry_de_429_e_503_e_bytes_descartados_contam():
    orcamento = _orcamento()

    capturada, pedidos = await _baixar(
        [
            resposta(429, b"x" * 10, {"Retry-After": "0"}),
            resposta(503, b"y" * 20),
            resposta(200, b"ok"),
        ],
        orcamento=orcamento,
    )

    assert capturada.corpo == b"ok" and len(pedidos) == 3
    assert orcamento.decodificados == 32


async def test_retry_esgotado_devolve_a_ultima_resposta(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_MAX_RETRIES", "2")

    capturada, pedidos = await _baixar([resposta(503, b"a"), resposta(503, b"b")])

    assert (capturada.http_status, capturada.corpo, capturada.sha256, len(pedidos)) == (
        503,
        b"b",
        None,
        2,
    )


async def test_max_retries_zero_vale_uma_tentativa(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_MAX_RETRIES", "0")

    with pytest.raises(SourceUnavailableError, match="1 tentativa"):
        await _baixar([httpx.ConnectError("recusada")])


async def test_timeout_depois_de_corpo_parcial_repete_e_conta_o_parcial():
    class Parcial(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"comeco"
            raise httpx.ReadTimeout("parou")

    orcamento = _orcamento()

    capturada, pedidos = await _baixar(
        [httpx.Response(200, stream=Parcial()), resposta(200, b"inteiro")], orcamento=orcamento
    )

    assert capturada.corpo == b"inteiro" and len(pedidos) == 2
    assert orcamento.decodificados == len(b"comeco") + len(b"inteiro")


@pytest.mark.parametrize("status", [403, 404])
async def test_403_e_404_nao_repetem(status):
    capturada, pedidos = await _baixar([resposta(status, b"<html/>")])

    assert (capturada.http_status, capturada.completo, len(pedidos)) == (status, False, 1)


async def test_redirecionamento_para_http_e_recusado():
    def redirecionar(request):
        return httpx.Response(
            302, headers={"Location": "http://fonte.test/inseguro"}, request=request
        )

    with pytest.raises(SourceUnavailableError, match="sem https"):
        await _baixar([redirecionar, resposta(200, b"x")])


async def test_content_encoding_desconhecido_e_recusado():
    with pytest.raises(SourceUnavailableError, match="não suportado"):
        await _baixar([resposta(200, b"x", {"Content-Encoding": "br"})])


async def test_content_length_acima_do_teto_recusa_antes_de_ler():
    with pytest.raises(ResourceLimitError, match="declara 999999"):
        await _baixar([resposta(200, b"x", {"Content-Length": "999999"})], teto=1000)


async def test_arquivo_vai_para_o_temporario_e_nao_fica_em_memoria(tmp_path):
    destino = tmp_path / "original.zip.part"

    capturada, _ = await _baixar([resposta(200, b"PK" * 100)], papel="arquivo", temporario=destino)

    assert capturada.corpo is None and capturada.temporario == destino
    assert destino.read_bytes() == b"PK" * 100


async def test_prazo_esgotado_na_espera_do_retry(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_RETRY_MAX_DELAY", "30")
    orcamento = transport.Orcamento(
        "teste", models.LimitesBrutos(max_segundos=0.5), prazo=time.monotonic() + 0.2
    )

    with pytest.raises(ResourceLimitError, match="passa do prazo"):
        await _baixar(
            [resposta(503, b"", {"Retry-After": "5"}), resposta(200, b"x")], orcamento=orcamento
        )


async def test_prazo_vencido_na_espera_do_limitador_nao_envia_o_get(monkeypatch):
    from contextlib import asynccontextmanager

    orcamento = _orcamento()

    @asynccontextmanager
    async def adquirir(_fonte):
        orcamento.prazo = time.monotonic() - 1
        yield

    monkeypatch.setattr(transport.RateLimiter, "acquire", adquirir)

    with pytest.raises(ResourceLimitError, match="prazo"):
        _, pedidos = await _baixar([resposta(200, b"{}")], orcamento=orcamento)
    assert orcamento.decodificados == 0
