from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from agrobr.anda import client as anda_client
from agrobr.anec import client as anec_client
from agrobr.anec.models import ANECArticle
from agrobr.exceptions import SourceUnavailableError
from agrobr.zarc import client as zarc_client
from tests.helpers import levanta_exatamente, sem_excecao

PDF = b"%PDF-1.7\n" + b"x" * 20_000
CSV = b"cultura;uf;geocodigo\n" + b"soja;MT;5100000\n" * 50


def _artigo(url: str) -> ANECArticle:
    return ANECArticle(
        id=999,
        cuid="cuid-999",
        title_en="ANEC - 05.2026 Accumulated Exports",
        slug_en="anec-052026-accumulated-exports",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        pdf_url=url,
        media_updated_at=datetime(2026, 2, 5, 10, tzinfo=UTC),
    )


DOWNLOADS: dict[str, tuple[str, bytes, Callable[[str], Awaitable[Any]]]] = {
    "anda": ("https://anda.org.br/wp-content/uploads/entregas.pdf", PDF, anda_client.download_file),
    "anec": (
        "https://www.anec.com.br/uploads/test.pdf",
        PDF,
        lambda url: anec_client.fetch_pdf_bytes(_artigo(url), use_cache=False),
    ),
    "zarc": (
        "https://dados.agricultura.gov.br/dataset/zarc/resource/1/download/tabua.csv",
        CSV,
        zarc_client.download_acquisition,
    ),
}


def _fonte_que_redireciona(
    monkeypatch: pytest.MonkeyPatch, corpo: bytes, location: str
) -> list[str]:
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        if len(pedidos) == 1:
            return httpx.Response(302, headers={"Location": location})
        return httpx.Response(200, content=corpo)

    cliente = functools.partial(httpx.AsyncClient, transport=httpx.MockTransport(responder))
    monkeypatch.setattr(httpx, "AsyncClient", cliente)
    return pedidos


@pytest.mark.parametrize("fonte", list(DOWNLOADS))
@pytest.mark.parametrize(
    "location",
    [
        "http://127.0.0.1:8080/latest/meta-data/",
        "https://outro.exemplo/arquivo",
        "http://{host}/arquivo",
        "https://{host}:8443/arquivo",
    ],
)
async def test_redirecionamento_para_fora_da_origem_recusado_antes_do_envio(
    monkeypatch, tmp_path, fonte, location
):
    monkeypatch.setenv("AGROBR_CACHE_DIR", str(tmp_path))
    url, corpo, baixar = DOWNLOADS[fonte]
    destino = location.format(host=httpx.URL(url).host)
    pedidos = _fonte_que_redireciona(monkeypatch, corpo, destino)

    with levanta_exatamente(SourceUnavailableError, "fora da origem HTTPS oficial da fonte"):
        await baixar(url)

    assert pedidos == [url]


@pytest.mark.parametrize("fonte", list(DOWNLOADS))
async def test_redirecionamento_dentro_da_origem_segue(monkeypatch, tmp_path, fonte):
    monkeypatch.setenv("AGROBR_CACHE_DIR", str(tmp_path))
    url, corpo, baixar = DOWNLOADS[fonte]
    destino = f"https://{httpx.URL(url).host}/outro/caminho"
    pedidos = _fonte_que_redireciona(monkeypatch, corpo, destino)

    with sem_excecao():
        await baixar(url)

    assert pedidos == [url, destino]
