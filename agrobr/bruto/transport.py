from __future__ import annotations

import asyncio
import hashlib
import os
import time
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO

import httpx

from agrobr import _log, constants
from agrobr.bruto import models
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError
from agrobr.http.rate_limiter import RateLimiter
from agrobr.utils import tasks

logger = _log.get_logger(__name__)

_BLOCO = 64 * 1024
_TRANSITORIAS = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)


class CorpoIncompleto(Exception):
    """Corpo comprimido terminou antes do fim do stream; conta como falha transitória da conexão."""


class Relogio:
    """Horário UTC derivado do relógio monotônico: os horários de uma coleta nunca andam para trás."""

    def __init__(self) -> None:
        self._utc = datetime.now(UTC)
        self._mono = time.monotonic()

    def agora(self) -> str:
        instante = self._utc + timedelta(seconds=time.monotonic() - self._mono)
        return instante.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class Orcamento:
    """Bytes codificados e decodificados de toda a chamada (tentativas descartadas inclusive) e prazo monotônico."""

    fonte: str
    limites: models.LimitesBrutos
    prazo: float
    codificados: int = 0
    decodificados: int = 0

    def conferir_prazo(self) -> None:
        if time.monotonic() > self.prazo:
            raise ResourceLimitError(
                self.fonte, f"prazo de {self.limites.max_segundos:g} s da coleta esgotado"
            )

    async def esperar(self, segundos: float) -> None:
        if time.monotonic() + segundos > self.prazo:
            raise ResourceLimitError(
                self.fonte,
                f"a espera de {segundos:g} s antes da próxima tentativa passa do prazo de "
                f"{self.limites.max_segundos:g} s",
            )
        await asyncio.sleep(segundos)

    def somar(self, *, codificados: int = 0, decodificados: int = 0, url: str) -> None:
        self.codificados += codificados
        self.decodificados += decodificados
        teto = self.limites.max_bytes_recurso
        if max(self.codificados, self.decodificados) > teto:
            raise ResourceLimitError(
                self.fonte, f"a coleta passou de max_bytes_recurso ({teto} bytes)", url=url
            )


class _Decodificador:
    def __init__(self, encoding: str, *, fonte: str, url: str) -> None:
        self._encoding = encoding
        self._fonte = fonte
        self._url = url
        self._zlib: zlib._Decompress | None
        if encoding in ("", "identity"):
            self._zlib = None
        elif encoding in ("gzip", "x-gzip"):
            self._zlib = zlib.decompressobj(16 + zlib.MAX_WBITS)
        elif encoding == "deflate":
            self._zlib = zlib.decompressobj()
        else:
            raise SourceUnavailableError(
                source=fonte, url=url, last_error=f"Content-Encoding não suportado: {encoding!r}"
            )
        self._primeiro = True

    def alimentar(self, dados: bytes) -> Iterator[bytes]:
        if self._zlib is None:
            if dados:
                yield dados
            return
        while dados:
            try:
                saida = self._zlib.decompress(dados, _BLOCO)
            except zlib.error:
                if not (self._primeiro and self._encoding == "deflate"):
                    raise CorpoIncompleto(f"corpo {self._encoding} inválido") from None
                self._zlib = zlib.decompressobj(-zlib.MAX_WBITS)
                saida = self._zlib.decompress(dados, _BLOCO)
            self._primeiro = False
            if saida:
                yield saida
            dados = self._zlib.unconsumed_tail
            if self._zlib.eof and self._zlib.unused_data:
                dados = self._zlib.unused_data + dados
                self._zlib = zlib.decompressobj(16 + zlib.MAX_WBITS)
        while self._zlib is not None and (pendente := self._zlib.decompress(b"", _BLOCO)):
            yield pendente

    def finalizar(self) -> Iterator[bytes]:
        if self._zlib is None:
            return
        resto = self._zlib.flush()
        if resto:
            yield resto
        if self._encoding != "deflate" and not self._zlib.eof:
            raise CorpoIncompleto(f"corpo {self._encoding} terminou antes do fim")


def _cabecalhos(response: httpx.Response) -> dict[str, str]:
    return {
        chave.lower(): valor
        for chave, valor in response.headers.items()
        if chave.lower() in models.CABECALHOS_PERMITIDOS
    }


async def _ler(
    response: httpx.Response,
    *,
    destino: BinaryIO | None,
    teto: int,
    orcamento: Orcamento,
    fonte: str,
) -> tuple[bytes | None, str, int]:
    url = str(response.url)
    tamanho = response.headers.get("content-length", "")
    if tamanho.isdigit() and int(tamanho) > teto:
        raise ResourceLimitError(
            fonte, f"a resposta declara {tamanho} bytes, acima do teto de {teto}", url=url
        )
    decodificador = _Decodificador(
        response.headers.get("content-encoding", "").strip().lower(), fonte=fonte, url=url
    )
    hasher = hashlib.sha256()
    memoria = bytearray() if destino is None else None
    codificados = decodificados = 0

    def guardar(pedaco: bytes) -> None:
        nonlocal decodificados
        decodificados += len(pedaco)
        orcamento.somar(decodificados=len(pedaco), url=url)
        if decodificados > teto:
            raise ResourceLimitError(
                fonte, f"o corpo decodificado passou do teto de {teto} bytes", url=url
            )
        hasher.update(pedaco)
        if memoria is not None:
            memoria.extend(pedaco)
        elif destino is not None:
            destino.write(pedaco)

    async for bruto in response.aiter_raw():
        codificados += len(bruto)
        orcamento.somar(codificados=len(bruto), url=url)
        if codificados > teto:
            raise ResourceLimitError(
                fonte, f"a resposta passou do teto de {teto} bytes na conexão", url=url
            )
        for pedaco in decodificador.alimentar(bruto):
            guardar(pedaco)
        orcamento.conferir_prazo()
    for pedaco in decodificador.finalizar():
        guardar(pedaco)
    return (bytes(memoria) if memoria is not None else None), hasher.hexdigest(), decodificados


async def baixar(
    http: httpx.AsyncClient,
    pedido: models.PedidoHTTP,
    *,
    fonte: str,
    orcamento: Orcamento,
    relogio: Relogio,
    teto: int,
    temporario: Path | None = None,
) -> models.RespostaCapturada:
    """GET com as tentativas do ``HTTPSettings``; devolve a última resposta (200 ou não) com o corpo inteiro conferido.

    Página e controle ficam em memória (``corpo``); o arquivo 200 vai para ``temporario``. Corpo de erro fica em
    memória até ``max_bytes_pagina``. Falha de rede esgotada vira ``SourceUnavailableError``; teto e prazo,
    ``ResourceLimitError``. Tudo que chega, inclusive de tentativa descartada, conta no orçamento.
    """
    settings = constants.HTTPSettings()
    tentativas = max(settings.max_retries, 1)
    for tentativa in range(tentativas):
        ultima = tentativa == tentativas - 1
        orcamento.conferir_prazo()
        try:
            resposta = await _tentativa(
                http,
                pedido,
                fonte=fonte,
                orcamento=orcamento,
                relogio=relogio,
                teto=teto,
                temporario=temporario,
            )
        except (*_TRANSITORIAS, CorpoIncompleto) as exc:
            if ultima:
                raise SourceUnavailableError(
                    source=fonte,
                    url=pedido.url,
                    last_error=f"{type(exc).__name__}: {exc} depois de {tentativas} tentativa(s)",
                ) from exc
            await orcamento.esperar(_espera(settings, tentativa, None))
            continue
        if resposta.http_status in constants.RETRIABLE_STATUS_CODES and not ultima:
            logger.warning(
                "bruto_retry", fonte=fonte, status=resposta.http_status, tentativa=tentativa + 1
            )
            await orcamento.esperar(
                _espera(settings, tentativa, resposta.cabecalhos.get("retry-after"))
            )
            continue
        return resposta
    raise AssertionError("laço de tentativas sem retorno")


def _espera(settings: constants.HTTPSettings, tentativa: int, retry_after: str | None) -> float:
    espera = float(
        min(
            settings.retry_base_delay * settings.retry_exponential_base**tentativa,
            settings.retry_max_delay,
        )
    )
    try:
        return min(float(retry_after), float(settings.retry_max_delay)) if retry_after else espera
    except ValueError:
        return espera


async def _tentativa(
    http: httpx.AsyncClient,
    pedido: models.PedidoHTTP,
    *,
    fonte: str,
    orcamento: Orcamento,
    relogio: Relogio,
    teto: int,
    temporario: Path | None,
) -> models.RespostaCapturada:
    request = http.build_request(
        "GET",
        pedido.url,
        params=pedido.parametros or None,
        headers={"Accept-Encoding": "gzip, deflate"},
    )
    url_solicitada = str(request.url)
    inicio = relogio.agora()
    async with RateLimiter.acquire(fonte):
        orcamento.conferir_prazo()
        response = await http.send(request, stream=True, follow_redirects=True)
        try:
            url = str(response.url)
            if response.url.scheme != "https":
                raise SourceUnavailableError(
                    source=fonte, url=url, last_error="a resposta veio de URL sem https"
                )
            ok = response.status_code == 200
            if ok and temporario is not None:
                with open(temporario, "wb") as destino:
                    corpo, sha256, tamanho = await _ler(
                        response, destino=destino, teto=teto, orcamento=orcamento, fonte=fonte
                    )
                    destino.flush()
                    await tasks.to_thread_ate_o_fim(os.fsync, destino.fileno())
            else:
                corpo, sha256, tamanho = await _ler(
                    response,
                    destino=None,
                    teto=teto if ok else min(teto, orcamento.limites.max_bytes_pagina),
                    orcamento=orcamento,
                    fonte=fonte,
                )
        finally:
            await response.aclose()
    return models.RespostaCapturada(
        pedido=pedido,
        url_solicitada=url_solicitada,
        url=url,
        http_status=response.status_code,
        cabecalhos=_cabecalhos(response),
        inicio=inicio,
        fim=relogio.agora(),
        sha256=sha256 if ok else None,
        tamanho=tamanho,
        corpo=corpo,
        temporario=temporario if ok else None,
        completo=ok,
    )
