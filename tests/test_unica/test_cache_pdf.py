from __future__ import annotations

import warnings
from datetime import UTC, datetime
from unittest import mock

import httpx
import pandas as pd

from agrobr import unica
from agrobr.unica import client, parser

PDF = b"%PDF-1.7\n" + b" " * 20000
PAGINA = b"arquivos/pdfs/2026/07/0123456789abcdef0123456789abcdef.pdf"
AQUISICAO = datetime(2026, 10, 4, 12, tzinfo=UTC)


async def test_pdf_reaproveitado_sai_do_cache_com_a_hora_da_aquisicao_original():
    pedidos: list[str] = []
    original = httpx.AsyncClient

    def responder(request: httpx.Request) -> httpx.Response:
        pdf = request.url.path.endswith(".pdf")
        pedidos.append("pdf" if pdf else "pagina")
        return httpx.Response(200, content=PDF if pdf else PAGINA, request=request)

    def fabrica(**kwargs: object) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(responder), **kwargs)

    resumo = pd.DataFrame({"periodo": ["acumulado"], "valor": [1.0]})
    edicao = parser.ParsedQuinzenal(
        resumo=resumo, series=pd.DataFrame(), safra="2026/2027", posicao=pd.Timestamp("2026-07-01")
    )
    with (
        mock.patch.object(client.httpx, "AsyncClient", fabrica),
        mock.patch.object(client, "utcnow", return_value=AQUISICAO),
        mock.patch.object(parser, "parse_quinzenal_pdf", return_value=edicao),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("ignore", UserWarning)
        _, frio = await unica.safra_resumo(return_meta=True)
        _, quente = await unica.safra_resumo(return_meta=True)

    assert pedidos == ["pagina", "pdf", "pagina"]
    assert (frio.from_cache, quente.from_cache) == (False, True)
    assert frio.fetch_timestamp == quente.fetch_timestamp == AQUISICAO
    assert quente.fetched_at == AQUISICAO
    assert frio.raw_content_hash == quente.raw_content_hash
