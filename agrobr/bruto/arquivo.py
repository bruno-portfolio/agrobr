from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx

from agrobr.bruto import models, protocols
from agrobr.exceptions import ParseError
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.warnings import warn_once

ASSINATURAS_ZIP = (b"PK\x03\x04", b"PK\x05\x06")
AMOSTRA = 64 * 1024
TIMEOUT = get_timeout(read=300.0)


def sessao() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=UserAgentRotator.get_bot_headers(), follow_redirects=True, timeout=TIMEOUT
    )


def conferir_zip(inicio: bytes) -> str | None:
    if inicio[:4] in ASSINATURAS_ZIP:
        return None
    return f"não começa com a assinatura de ZIP ({inicio[:4]!r})"


def cabecalho_csv(inicio: bytes, separador: str) -> list[str] | None:
    """Colunas da 1ª linha lida em UTF-8 (com ou sem BOM) ou Windows-1252; ``None`` se a amostra não tem fim de linha."""
    if b"\n" not in inicio:
        return None
    linha = inicio.split(b"\n", 1)[0].rstrip(b"\r")
    try:
        texto = linha.decode("utf-8-sig")
    except UnicodeDecodeError:
        texto = linha.decode("cp1252", errors="replace")
    return [coluna.strip().strip('"') for coluna in texto.split(separador)]


def conferir_csv(*colunas: str, separador: str = ";") -> Callable[[bytes], str | None]:
    def conferir(inicio: bytes) -> str | None:
        cabecalho = cabecalho_csv(inicio, separador)
        if cabecalho is not None and set(colunas) <= set(cabecalho):
            return None
        return f"não é o CSV esperado, sem {list(colunas)} no cabeçalho ({inicio[:80]!r})"

    return conferir


@dataclass(frozen=True)
class AdaptadorArquivo:
    """Arquivo nacional num GET só, sem recorte: vai como a fonte publica, com só o início conferido.

    ``conferir`` recebe os primeiros bytes do corpo 200 e devolve o motivo da recusa (``ParseError``) ou
    ``None``. ``antes`` roda antes do GET, com o contexto e a mesma sessão, como a conferência da edição no
    catálogo (``contexto.consultar``, que conta no orçamento da coleta). ``aviso`` sai uma vez por processo como
    ``UserWarning`` e vai em ``avisos`` de cada entrada ``ok``, como o dado pessoal do arquivo.
    """

    url: str
    edicao: int | None
    conferir: Callable[[bytes], str | None]
    antes: Callable[[protocols.ContextoBruto, httpx.AsyncClient], Awaitable[None]] | None = None
    aviso: str | None = None

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=self.url,
            parametros={},
            selecao=models.Selecao(
                uf=pedido.uf,
                bbox=pedido.bbox,
                bbox_crs=pedido.bbox_crs,
                camada=None,
                edicao=self.edicao,
                natureza=None,
            ),
            modo="arquivo",
            formato=pedido.formato,
            opcoes=models.Opcoes(
                tamanho_pagina=pedido.tamanho_pagina,
                compactar=pedido.compactar,
                limites=pedido.limites,
            ),
            campo_id=None,
            crs_esperado=None,
        )

    async def adquirir(
        self, plano: models.PlanoBruto, *, contexto: protocols.ContextoBruto
    ) -> models.ConclusaoBruta:
        pedido = models.PedidoHTTP(
            url=plano.url_solicitada,
            parametros={},
            papel="arquivo",
            numero=1,
            formato=plano.formato,
        )
        async with sessao() as http:
            if self.antes is not None:
                await self.antes(contexto, http)
            resposta = await contexto.obter(http, pedido)
        if resposta.http_status == 404:
            return models.ConclusaoBruta(status="ausente_na_fonte")
        if resposta.temporario is not None:
            with resposta.temporario.open("rb") as arquivo:
                motivo = self.conferir(arquivo.read(AMOSTRA))
            if motivo is not None:
                raise ParseError(plano.fonte, 1, f"{resposta.url}: resposta 200 {motivo}")
        contexto.registrar_arquivo(resposta)
        if self.aviso is None:
            return models.ConclusaoBruta(status="ok")
        warn_once(f"bruto_{plano.fonte}_{plano.recurso}", self.aviso)
        return models.ConclusaoBruta(status="ok", avisos=(self.aviso,))
