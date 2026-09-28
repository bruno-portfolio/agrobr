from __future__ import annotations

import warnings
from pathlib import Path

import httpx
import pandas as pd
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.zarc import api, cache, client
from tests.helpers import levanta_exatamente, sem_excecao

CSV = (Path(__file__).parents[1] / "golden_data/zarc/selecao_20260907/2026_2027.csv").read_bytes()
AVISO = "zarc: tamanho do arquivo não conferido (o servidor não informou o total)"


@pytest.fixture
def servidor(monkeypatch):
    """Portal do MAPA simulado: o CSV sai por partes, sem Content-Length, com os cabeçalhos pedidos."""
    estado = {"corpo": CSV, "cabecalhos": {}, "downloads": 0}
    catalogo = {
        "success": True,
        "result": {
            "resources": [
                {
                    "id": "2026_2027",
                    "name": "Safra 2026/2027",
                    "url": "https://example.org/2026_2027.csv",
                    "format": "CSV",
                    "last_modified": "2026-09-07T00:00:00",
                }
            ]
        },
    }

    async def responder(request: httpx.Request) -> httpx.Response:
        if "package_show" in request.url.path:
            return httpx.Response(200, json=catalogo)
        estado["downloads"] += 1

        async def partes():
            yield estado["corpo"]

        return httpx.Response(
            200, content=partes(), headers={"content-type": "text/csv", **estado["cabecalhos"]}
        )

    original = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(responder), **kwargs),
    )
    cache.clear()
    yield estado
    cache.clear()


def _total(tamanho: int) -> dict[str, str]:
    return {"content-range": f"bytes 0-{tamanho - 1}/{tamanho}"}


@pytest.mark.parametrize(
    ("cabecalhos", "tamanho"),
    [
        ({"content-range": "bytes 0-9/10"}, 10),
        ({"content-range": "bytes 0-9/10", "content-encoding": "gzip"}, 10),
        ({"content-range": "bytes 5-9/10"}, None),
        ({"content-length": "7"}, 7),
        ({"content-length": "7", "content-encoding": "gzip"}, None),
        ({}, None),
    ],
)
def test_tamanho_publicado_pelo_servidor(cabecalhos, tamanho):
    assert client._tamanho_publicado(httpx.Headers(cabecalhos)) == tamanho


async def test_corpo_menor_que_o_total_publicado_nao_vai_ao_store(servidor):
    corte = CSV.rindex(b"\n", 0, len(CSV) // 2) + 1
    servidor.update(corpo=CSV[:corte], cabecalhos=_total(len(CSV)))

    with levanta_exatamente(SourceUnavailableError, f"download incompleto: {corte} de {len(CSV)}"):
        await api.zoneamento(return_meta=True)

    servidor.update(corpo=CSV)
    with sem_excecao():
        completo, meta = await api.zoneamento(return_meta=True)
        repetido, de_novo = await api.zoneamento(return_meta=True)
        direto = await api.zoneamento(use_cache=False)

    estados = [item.source_details["cache"]["status"] for item in (meta, de_novo)]
    assert estados == ["store_miss", "store_hit"]
    assert meta.source_details["coverage"].get("published_size_bytes") == len(CSV)
    pd.testing.assert_frame_equal(completo, direto)
    pd.testing.assert_frame_equal(repetido, direto)


async def test_sem_tamanho_publicado_avisa_e_nao_grava(servidor):
    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        _, primeira = await api.zoneamento(return_meta=True)
        _, segunda = await api.zoneamento(return_meta=True)

    assert [meta.source_details["cache"]["status"] for meta in (primeira, segunda)] == [
        "store_skipped",
        "store_skipped",
    ]
    assert servidor["downloads"] == 2
    assert AVISO in primeira.validation_warnings
    assert [str(aviso.message) for aviso in emitidos if "não conferido" in str(aviso.message)] == [
        AVISO,
        AVISO,
    ]


async def test_pacote_do_store_sem_tamanho_conferido_e_baixado_de_novo(servidor, monkeypatch):
    with monkeypatch.context() as gravar_sem_conferir, sem_excecao():
        gravar_sem_conferir.setattr(api, "_conferido", lambda _resource, _details: True)
        _, antigo = await api.zoneamento(return_meta=True)

    with sem_excecao():
        _, novo = await api.zoneamento(return_meta=True)

    estados = [meta.source_details["cache"]["status"] for meta in (antigo, novo)]
    assert estados == ["store_miss", "store_skipped"]
    assert servidor["downloads"] == 2
