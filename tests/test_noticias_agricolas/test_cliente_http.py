from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError
from agrobr.noticias_agricolas import client
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data" / "noticias_agricolas" / "paginas_20260925"
PAGINA = (GOLDEN / "na_arroz.html").read_bytes()
COTACOES = constants.URLS[constants.Fonte.NOTICIAS_AGRICOLAS]["cotacoes"]
URL = f"{COTACOES}/{constants.NOTICIAS_AGRICOLAS_PRODUTOS['arroz']}"
NOVA = f"{URL}-nova"
AVISO = (
    "Notícias Agrícolas: classificação zona_cinza; licença própria de reutilização das "
    "cotações não localizada. Dados de origem CEPEA continuam sujeitos a CC BY-NC 4.0, "
    "inclusive no fallback automático. Veja https://www.agrobr.dev/docs/licenses/."
)
_CLIENTE_REAL = httpx.AsyncClient


def resposta(status: int = 200, conteudo: bytes = PAGINA, **cabecalhos: str) -> httpx.Response:
    return httpx.Response(
        status, content=conteudo, headers={"content-type": "text/html; charset=utf-8", **cabecalhos}
    )


def servir(monkeypatch: pytest.MonkeyPatch, respostas: list[httpx.Response]) -> list[str]:
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        return respostas[min(len(pedidos), len(respostas)) - 1]

    class Cliente(_CLIENTE_REAL):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(responder)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Cliente)
    return pedidos


async def test_cliente_repete_5xx_segue_redirecionamento_e_recusa_4xx(
    monkeypatch: pytest.MonkeyPatch,
):
    with collect_failures() as check:
        for caso, respostas, esperados in [
            ("5xx e depois 200", [resposta(503, b"indisponivel"), resposta()], [URL, URL]),
            ("redirecionamento", [resposta(301, b"", location=NOVA), resposta()], [URL, NOVA]),
        ]:
            with check(caso):
                pedidos = servir(monkeypatch, respostas)
                with sem_excecao():
                    html = await client.fetch_indicador_page("arroz")
                assert (html, pedidos) == (PAGINA.decode("utf-8"), esperados)
        for caso, recusada, motivo in [
            ("403 com página grande", resposta(403), "HTTP 403: a fonte recusou o pedido"),
            (
                "200 com página de bloqueio",
                resposta(200, b"<html><p>verifique</p></html>"),
                "Soft block",
            ),
        ]:
            with check(caso):
                pedidos = servir(monkeypatch, [recusada])
                with levanta_exatamente(SourceUnavailableError, match=motivo):
                    await client.fetch_indicador_page("arroz")
                assert pedidos == [URL]


async def test_cliente_avisa_a_licenca_uma_vez(monkeypatch: pytest.MonkeyPatch):
    servir(monkeypatch, [resposta()])
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        with sem_excecao():
            await client.fetch_indicador_page("arroz")
            await client.fetch_indicador_page("arroz")
    assert [
        str(aviso.message) for aviso in avisos if "Notícias Agrícolas" in str(aviso.message)
    ] == [AVISO]


def test_url_de_cada_produto_e_recusa_do_desconhecido():
    with sem_excecao():
        urls = {
            produto: client._get_produto_url(produto.upper())
            for produto in constants.NOTICIAS_AGRICOLAS_PRODUTOS
        }
    assert urls == {
        produto: f"{COTACOES}/{caminho}"
        for produto, caminho in constants.NOTICIAS_AGRICOLAS_PRODUTOS.items()
    }
    with levanta_exatamente(ValueError, match="Produto 'feijao' não disponível"):
        client._get_produto_url("feijao")


def test_validacao_de_conteudo_recusa_so_pagina_pequena_sem_tabela():
    with collect_failures() as check:
        for caso, html in [
            ("pequena com tabela", "<html><table><tr><td>82,48</td></tr></table></html>"),
            ("pequena com TABLE", "<html><TABLE><TR><TD>82,48</TD></TR></TABLE></html>"),
            ("grande sem tabela", "<html>" + "x" * 25_000 + "</html>"),
        ]:
            with check(caso), sem_excecao():
                client._validate_html_has_data(html, URL)
        with (
            check("pequena sem tabela"),
            levanta_exatamente(SourceUnavailableError, match="Soft block detected"),
        ):
            client._validate_html_has_data("<html><p>verifique</p></html>", URL)
